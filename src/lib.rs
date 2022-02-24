use chrono::DateTime;
use std::str::FromStr;

/// struct that represents a single log entry in CRI log format.
///  CRI Log format example:
///    2016-10-06T00:17:09.669794202Z stdout P log content 1
//     2016-10-06T00:17:09.669794203Z stderr F log content 2
//  See: https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/kuberuntime/logs/logs.go#L128
pub struct CriLog {
    timestamp: DateTime<chrono::offset::FixedOffset>,
    stream_type: StreamType,
    tag: String,
    log: String,
}

impl CriLog {
    /// Get timestamp associated to log entry
    pub fn timestamp(&self) -> &DateTime<chrono::offset::FixedOffset> {
        &self.timestamp
    }

    /// Returns true if log entry is of type stderr
    pub fn is_stderr(&self) -> bool {
        self.stream_type == StreamType::StdErr
    }

    /// Returns true if log entry is of type stdout
    pub fn is_stdout(&self) -> bool {
        self.stream_type == StreamType::StdOut
    }

    /// Get tag attribute from log entry
    pub fn tag(&self) -> &str {
        &self.tag
    }

    /// Get message from log entry
    pub fn log(&self) -> &str {
        &self.log
    }
}

impl FromStr for CriLog {
    type Err = ParsingError;

    fn from_str(input: &str) -> Result<Self, <Self as FromStr>::Err> {
        let mut iter = input.split_whitespace();

        let timestamp_str = iter.next().ok_or(ParsingError::MissingTimestamp)?;
        let timestamp = DateTime::parse_from_rfc3339(timestamp_str)
            .map_err(|_| ParsingError::TimestampFormat(timestamp_str.into()))?;

        let stream_type_str = iter.next().ok_or(ParsingError::MissingStreamType)?;
        let stream_type = StreamType::from_str(stream_type_str)
            .map_err(|_| ParsingError::InvalidStreamType(stream_type_str.into()))?;

        let tag = iter.next().ok_or(ParsingError::MissingLogTag)?.to_owned();

        let log = iter.collect::<Vec<&str>>().join(" ");

        Ok(CriLog {
            timestamp,
            stream_type,
            tag,
            log,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParsingError {
    #[error("Missing timestamp in log entry")]
    MissingTimestamp,
    #[error("Timestamp format error: {0}")]
    TimestampFormat(String),
    #[error("Missing stream type")]
    MissingStreamType,
    #[error("Invalid stream type: {0}")]
    InvalidStreamType(String),
    #[error("Missing log tag")]
    MissingLogTag,
}

#[derive(Debug, PartialEq)]
pub enum StreamType {
    StdOut,
    StdErr,
}

impl FromStr for StreamType {
    type Err = InvalidStreamType;
    fn from_str(input: &str) -> Result<Self, <Self as FromStr>::Err> {
        match input {
            "stderr" => Ok(StreamType::StdErr),
            "stdout" => Ok(StreamType::StdOut),
            input => Err(InvalidStreamType(input.into())),
        }
    }
}

pub struct InvalidStreamType(String);

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stdout() {
        //   2016-10-06T00:17:09.669794202Z stdout P log content 1
        //   2016-10-06T00:17:09.669794203Z stderr F log content 2
        let log_str = "2016-10-06T00:17:09.669794202Z stdout P log content 1";
        let crilog = CriLog::from_str(log_str).expect("failed to parse");
        assert!(crilog.is_stdout());
        assert_eq!(crilog.tag(), "P");
        assert_eq!(crilog.log(), "log content 1");
    }

    #[test]
    fn stderr() {
        let log_str = "2016-10-06T00:17:09.669794203Z stderr F log content 2";
        let crilog = CriLog::from_str(log_str).expect("failed to parse");
        assert!(crilog.is_stderr());
        assert_eq!(crilog.tag(), "F");
        assert_eq!(crilog.log(), "log content 2");
    }
}
