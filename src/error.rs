use std::fmt::{self, Display};

use serde::de;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    // There must be at least one row which acts as the heading row when the
    // deserializer needs to match field names to columns.
    ZeroRows,

    HeaderMustBeString,

    MissingValue,

    NotNumber,

    NotBool,

    // One or more variants that can be created by data structures through the
    // `ser::Error` and `de::Error` traits. For example the Serialize impl for
    // Mutex<T> might return an error because the mutex is poisoned, or the
    // Deserialize impl for a struct may return an error because a required
    // field is missing.
    Message(String),

    // Zero or more variants that can be created directly by the Serializer and
    // Deserializer without going through `ser::Error` and `de::Error`. These
    // are specific to the format, in this case JSON.
    Eof,
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::ZeroRows => formatter.write_str("zero rows in sheet"),
            Error::HeaderMustBeString => formatter.write_str("header cell must be of string type"),
            Error::Message(msg) => formatter.write_str(msg),
            Error::Eof => formatter.write_str("unexpected end of input"),
            Error::MissingValue => formatter.write_str("expected value but it wasn't present"),
            Error::NotNumber => formatter.write_str("expected number value"),
            Error::NotBool => formatter.write_str("expected bool value"),
            /* and so forth */
        }
    }
}

impl std::error::Error for Error {}
