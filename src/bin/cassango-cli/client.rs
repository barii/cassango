//! HTTP client for a cassango cluster, with failover across the given hosts.

use reqwest::blocking::Client as Http;
use reqwest::{Method, StatusCode};
use serde_json::Value;
use std::time::Duration;

pub struct Client {
    hosts: Vec<String>,
    current: usize,
    http: Http,
    pub consistency: String,
}

pub struct Response {
    pub status: StatusCode,
    pub body: Value,
}

impl Client {
    pub fn new(hosts: Vec<String>, consistency: String) -> Self {
        let http = Http::builder()
            .timeout(Duration::from_secs(600)) // admin calls (repair, cleanup) can be slow
            .connect_timeout(Duration::from_secs(2))
            .build()
            .expect("http client");
        Client { hosts, current: 0, http, consistency }
    }

    pub fn host(&self) -> &str {
        &self.hosts[self.current]
    }

    pub fn set_hosts(&mut self, hosts: Vec<String>) {
        self.hosts = hosts;
        self.current = 0;
    }

    /// Send a request, failing over to the next host if one is unreachable.
    /// `with_cl` appends `?consistency=` for document operations.
    pub fn call(&mut self, method: Method, path: &str, body: Option<&Value>, with_cl: bool) -> Result<Response, String> {
        let mut last_err = String::new();
        for _ in 0..self.hosts.len() {
            let mut url = format!("http://{}{}", self.host(), path);
            if with_cl {
                url.push(if url.contains('?') { '&' } else { '?' });
                url.push_str("consistency=");
                url.push_str(&self.consistency);
            }
            let mut req = self.http.request(method.clone(), &url);
            if let Some(b) = body {
                req = req.json(b);
            }
            match req.send() {
                Ok(resp) => {
                    let status = resp.status();
                    let text = resp.text().unwrap_or_default();
                    let body = if text.is_empty() { Value::Null } else { serde_json::from_str(&text).unwrap_or(Value::String(text)) };
                    return Ok(Response { status, body });
                }
                // Only connection failures fail over: a slow request may still be running.
                Err(e) if e.is_connect() => {
                    last_err = format!("{}: {e}", self.host());
                    self.current = (self.current + 1) % self.hosts.len();
                    if self.hosts.len() > 1 {
                        eprintln!("warning: {last_err}; failing over to {}", self.host());
                    }
                }
                Err(e) => return Err(format!("{}: {e}", self.host())),
            }
        }
        Err(format!("no reachable host ({last_err})"))
    }

    /// Like `call`, but turns non-2xx responses into errors using the server's message.
    pub fn ok(&mut self, method: Method, path: &str, body: Option<&Value>, with_cl: bool) -> Result<Value, String> {
        let r = self.call(method, path, body, with_cl)?;
        if r.status.is_success() {
            Ok(r.body)
        } else {
            let msg = r.body.get("error").and_then(Value::as_str).map(str::to_string).unwrap_or_else(|| r.body.to_string());
            Err(format!("{} {}", r.status.as_u16(), msg))
        }
    }
}

/// Percent-encode a path segment (document ids and index fields can be arbitrary strings).
pub fn enc(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
