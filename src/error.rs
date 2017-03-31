pub enum MonetError {
    DatabaseError,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
}

impl MonetError {
    pub fn from_mapi_code(code: &str) -> Option<MonetError> {
        use self::MonetError::*;

        match code {
            "42S02!" => Some(OperationalError),  // no such table
            "M0M29!" => Some(IntegrityError),    // INSERT INTO: UNIQUE constraint violated
            "2D000!" => Some(IntegrityError),    // COMMIT: failed
            "40000!" => Some(IntegrityError),    // DROP TABLE: FOREIGN KEY constraint violated
            _        => None,

        }
    }
}
