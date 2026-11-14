use std::fmt;

/// Errors returned by the driver.
#[derive(Debug)]
pub enum Error {
    /// The server rejected the request: invalid filter, update or document (HTTP 400).
    BadRequest(String),
    /// The document, index or node doesn't exist (HTTP 404).
    NotFound(String),
    /// The operation conflicts with one in progress, e.g. a repair or range movement (HTTP 409).
    Conflict(String),
    /// Not enough replicas are alive for the requested consistency level (HTTP 503).
    /// Nothing was attempted; retrying with a lower consistency level may succeed.
    Unavailable(String),
    /// Replicas were asked but too few acknowledged in time (HTTP 504). The write may
    /// still have been applied on some replicas.
    Timeout(String),
    /// Any other non-success response.
    Server { status: u16, message: String },
    /// No host could be reached, or the connection failed mid-request.
    Transport(String),
    /// A response or document couldn't be (de)serialized.
    Serde(serde_json::Error),
    /// `update_many`/`delete_many` refused to run because some nodes were unreachable,
    /// so the set of matching documents may be incomplete.
    Partial,
    /// Invalid client configuration (e.g. no hosts).
    Config(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::BadRequest(m) => write!(f, "bad request: {m}"),
            Error::NotFound(m) => write!(f, "not found: {m}"),
            Error::Conflict(m) => write!(f, "conflict: {m}"),
            Error::Unavailable(m) => write!(f, "unavailable: {m}"),
            Error::Timeout(m) => write!(f, "timeout: {m}"),
            Error::Server { status, message } => write!(f, "server error {status}: {message}"),
            Error::Transport(m) => write!(f, "transport error: {m}"),
            Error::Serde(e) => write!(f, "serialization error: {e}"),
            Error::Partial => write!(f, "some nodes are unreachable; the matching set may be incomplete"),
            Error::Config(m) => write!(f, "invalid configuration: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Serde(e) => Some(e),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Serde(e)
    }
}

impl Error {
    /// True for errors where retrying later (or against other replicas) might succeed.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::Unavailable(_) | Error::Timeout(_) | Error::Transport(_) | Error::Partial)
    }

    pub(crate) fn from_status(status: u16, message: String) -> Self {
        match status {
            400 => Error::BadRequest(message),
            404 => Error::NotFound(message),
            409 => Error::Conflict(message),
            503 => Error::Unavailable(message),
            504 => Error::Timeout(message),
            _ => Error::Server { status, message },
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
