use chrono::{DateTime, Datelike, FixedOffset, Local, TimeDelta, TimeZone, Timelike, Utc};
use log::{debug, error, info, warn};
use serde::{Deserialize, de::DeserializeOwned};
use std::env;
use std::thread::sleep;
use std::time::{Duration, Instant};
use ureq::{Agent, RequestBuilder, typestate::WithoutBody};

#[derive(Debug, Deserialize)]
struct Response<T> {
    ok: bool,
    result: Option<T>,
}

#[derive(Debug, Deserialize)]
struct Chat {
    pinned_message: Option<Message>,
}

#[derive(Debug, Deserialize)]
struct Message {
    message_id: u64,
}

impl Chat {
    fn unwrap(self) -> u64 {
        self.pinned_message.unwrap().message_id
    }
}

#[derive(Debug, Deserialize)]
struct EditedMessage {
    edit_date: u64,
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ServerTime {
    Early,
    Late,
    Other,
}

impl ServerTime {
    fn from_header(s: &str, s0: u32, s1: u32) -> Self {
        if s.contains(&format!(":{s0:02} ")) {
            ServerTime::Early
        } else if s.contains(&format!(":{s1:02} ")) {
            ServerTime::Late
        } else {
            ServerTime::Other
        }
    }

    fn from_timestamp(timestamp: u64, s0: u32, s1: u32) -> Self {
        let sec = (timestamp % 60) as u32;
        if sec == s0 {
            ServerTime::Early
        } else if sec == s1 {
            ServerTime::Late
        } else {
            ServerTime::Other
        }
    }
}

impl std::fmt::Display for ServerTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerTime::Other => write!(f, "0"),
            ServerTime::Early => write!(f, "-"),
            ServerTime::Late => write!(f, "+"),
        }
    }
}

#[derive(Debug)]
struct Client {
    http: Agent,
    urls: Vec<String>,
    url_idx: u32,
    time0: Instant,
    time1: Instant,
    date: DateTime<Utc>,
    sec0: u32,
    sec1: u32,
    server_time_field: ServerTime,
    server_time_header: ServerTime,
}

fn api_url(token: &str, method: &str) -> String {
    format!("https://api.telegram.org/bot{token}/{method}")
}

impl Client {
    fn new<'a>(tokens: impl Iterator<Item = &'a str>) -> Self {
        let config = Agent::config_builder().http_status_as_error(false).build();
        let mut urls = tokens
            .map(|token| api_url(token, "editMessageText"))
            .collect::<Vec<_>>();
        urls.shrink_to_fit();
        let time = Instant::now();
        Self {
            http: Agent::new_with_config(config),
            urls,
            url_idx: 0,
            time0: time,
            time1: time,
            date: Utc::now(),
            sec0: 59,
            sec1: 0,
            server_time_field: ServerTime::Other,
            server_time_header: ServerTime::Other,
        }
    }

    fn set_second(&mut self, sec: u32) {
        self.sec0 = if sec == 0 { 59 } else { sec - 1 };
        self.sec1 = sec;
    }

    fn finalize<T: DeserializeOwned + std::fmt::Debug>(
        &mut self,
        req: RequestBuilder<WithoutBody>,
    ) -> Result<T> {
        let mut r = req.call()?;
        let code = r.status();
        if code.is_success() {
            self.time0 = Instant::now();
            self.date = Utc::now();
        } else {
            error!("response {code}: {r:#?}");
            error!("body: {}", r.body_mut().read_to_string()?);
            return Err("request failed".into());
        }
        debug!("response: {code} {r:#?}");
        if let Some(date) = r.headers().get("Date")
            && let Ok(date) = date.to_str()
        {
            self.server_time_header = ServerTime::from_header(date, self.sec0, self.sec1);
            debug!("Date: {date} {:?}", self.server_time_header);
        }
        let r: Response<T> = r.body_mut().read_json()?;
        self.time1 = Instant::now();
        debug!("body: {r:#?}");
        if !r.ok {
            return Err("not ok".into());
        }
        if let Some(result) = r.result {
            Ok(result)
        } else {
            Err("no result".into())
        }
    }

    fn get_chat(&mut self, token: &str, chat_id: &str) -> Result<Chat> {
        let req = self
            .http
            .get(api_url(token, "getChat"))
            .query("chat_id", chat_id);
        let chat = self.finalize(req)?;
        debug!("getChat: {chat:#?}");
        Ok(chat)
    }

    fn edit_message_builder(
        &mut self,
        chat_id: &str,
        message_id: &str,
        text: &str,
    ) -> RequestBuilder<WithoutBody> {
        let url = &self.urls[self.url_idx as usize];
        self.url_idx += 1;
        if self.url_idx >= self.urls.len() as u32 {
            self.url_idx = 0;
        }
        self.http
            .get(url)
            .query("chat_id", chat_id)
            .query("message_id", message_id)
            .query("text", text)
            .query("parse_mode", "MarkdownV2")
    }

    fn edit_message(&mut self, req: RequestBuilder<WithoutBody>) -> Result<()> {
        let r: EditedMessage = self.finalize(req)?;
        debug!("editMessageText: {r:#?}");
        self.server_time_field = ServerTime::from_timestamp(r.edit_date, self.sec0, self.sec1);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct Window {
    data: f64,
}

impl Window {
    const ALPHA: f64 = 0.3;

    fn new() -> Self {
        Self { data: 0.0 }
    }

    fn push(&mut self, value: Duration) {
        let value = value.as_secs_f64();
        if self.data == 0.0 {
            self.data = value;
        } else {
            self.data = (1.0 - Self::ALPHA) * self.data + Self::ALPHA * value;
        }
    }

    fn avg(self) -> Duration {
        Duration::from_secs_f64(self.data)
    }

    fn is_empty(self) -> bool {
        self.data == 0.0
    }
}

fn format_msg(dt: &DateTime<FixedOffset>) -> String {
    format!(
        "怎么都 {}/{}/{} {}:{:02}:{:02} 了",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
    )
}

fn align_date(date: DateTime<Utc>) -> DateTime<Utc> {
    date - TimeDelta::nanoseconds(date.timestamp_subsec_nanos().into())
}

#[derive(Debug, Clone, Copy)]
struct Ratio {
    v: f64,
    i: f64,
}

impl Ratio {
    const INITIAL: f64 = 0.5;
    const K_P: f64 = 0.01;
    const K_I: f64 = 0.01;

    fn new() -> Self {
        Self {
            v: Self::INITIAL,
            i: 0.0,
        }
    }

    fn update(&mut self, error: f64) {
        self.i += error;
        self.v = (self.v + Self::K_P * error + Self::K_I * self.i).clamp(0.0, 1.0);
    }

    fn apply(&mut self, dur: Duration) -> Duration {
        dur.mul_f64(self.v)
    }
}

fn main() -> Result<()> {
    pretty_env_logger::init_timed();

    let file = env::args().nth(1).unwrap();
    let config = std::fs::read_to_string(file).unwrap();

    let mut chat_id = "";
    let mut tz = i32::MAX;
    let mut tokens = config
        .split_whitespace()
        .filter(|s| match s.chars().next() {
            Some('#') => {
                chat_id = &s[1..];
                false
            }
            Some('T') => {
                tz = s[1..].parse().unwrap();
                false
            }
            _ => s.contains(':'),
        })
        .peekable();

    let token = *tokens.peek().unwrap();
    let mut cli = Client::new(tokens);

    assert!(!chat_id.is_empty(), "No chat ID (#...)");
    let tz = if tz == i32::MAX {
        Local.offset_from_utc_datetime(&DateTime::UNIX_EPOCH.naive_utc())
    } else if tz >= 0 {
        FixedOffset::east_opt(tz * 3600).unwrap()
    } else {
        FixedOffset::west_opt((-tz) * 3600).unwrap()
    };
    info!("Timezone: {tz}");

    let chat_id = chat_id.to_owned();
    let chat = cli.get_chat(token, &chat_id)?;
    drop(config);
    info!("Chat: {chat:#?}");
    let message_id = chat.unwrap().to_string();

    let mut win = Window::new();
    let mut avg = Duration::default();
    let mut ratio = Ratio::new();

    let mut date = align_date(Utc::now());
    cli.set_second(date.second());

    let mut msg = format_msg(&date.with_timezone(&tz));
    let mut req = cli.edit_message_builder(&chat_id, &message_id, &msg);

    loop {
        const DELAY: TimeDelta = TimeDelta::seconds(1);

        let t0 = Instant::now();
        let resp = cli.edit_message(req);
        let now = Utc::now();
        let off = if let Err(e) = resp {
            error!("edit failed: {e:#?}");
            ratio.apply(avg)
        } else {
            debug!("msg: {msg}");

            if !win.is_empty() {
                use ServerTime::{Early, Late};
                match (cli.server_time_field, cli.server_time_header) {
                    (Early, Early) => ratio.update(-1.0),
                    (Early, Late) => {}
                    (Late, Late) => ratio.update(1.0),
                    (Late, Early) => error!("Server Date is earlier!"),
                    _ => warn!("Unexpected server time (too slow?)"),
                }
            }

            let rtt = cli.time0 - t0;
            let (sign, diff) = if avg >= rtt {
                ('+', avg - rtt)
            } else {
                ('-', rtt - avg)
            };
            win.push(rtt);
            avg = win.avg();

            let t1 = cli.time1 - t0;
            let t0 = cli.date - date;
            let (sign0, t0) = match t0.to_std() {
                Ok(dur) => ('+', dur),
                Err(_) => ('-', (date - cli.date).to_std().unwrap()),
            };
            let off = ratio.apply(avg);
            info!(
                "rtt={rtt:.3?} avg={avg:.3?} err={sign}{diff:.3?} t0={sign0}{t0:.3?} t1={t1:.3?} off={off:.3?} rr={} r={:.3} i={} S={}{}",
                cli.url_idx, ratio.v, ratio.i, cli.server_time_field, cli.server_time_header,
            );
            off
        };

        date += DELAY;
        if date < now {
            warn!("Too slow: {date} < {now}");
            date = align_date(now) + DELAY;
        }
        cli.set_second(date.second());

        let date_tz = date.with_timezone(&tz);
        msg = format_msg(&date_tz);
        req = cli.edit_message_builder(&chat_id, &message_id, &msg);
        debug!("{msg:?} at {date_tz} - {off:?}");

        let until = date - off;
        let td = until - Utc::now();
        if let Ok(dur) = td.to_std() {
            debug!("Sleeping for {dur:?}");
            sleep(dur);
        } else {
            warn!(
                "Can't keep up! Is the server overloaded? Running {:?} behind",
                (-td).to_std().unwrap()
            );
        }
    }
}
