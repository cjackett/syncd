use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    Hard,
    Transient,
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorClass::Hard => write!(f, "hard"),
            ErrorClass::Transient => write!(f, "transient"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncError {
    pub class: ErrorClass,
    pub message: String,
}

impl SyncError {
    pub fn new(class: ErrorClass, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
        }
    }
}

impl fmt::Display for SyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.class, self.message)
    }
}
