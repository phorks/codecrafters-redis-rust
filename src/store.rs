use std::time::SystemTime;

pub struct StoreValue {
    pub value: String,
    pub expires_on: Option<SystemTime>,
}

impl StoreValue {
    pub fn new(value: String, expires_on: Option<SystemTime>) -> Self {
        StoreValue { value, expires_on }
    }
}
