use serde_json::{Map, Value};
use std::fmt::Debug;

use crate::assert_within;

pub struct JsonAssert<'a> {
    inner: &'a Map<String, Value>,
}

impl<'a> JsonAssert<'a> {
    pub fn new(json_map: &'a Map<String, Value>) -> Self {
        Self { inner: json_map }
    }

    pub fn has<T>(&self, key: &str, value: T)
    where
        T: PartialEq + Debug,
        Value: PartialEq<T>,
    {
        let is_value = self.inner.get(key).unwrap();
        assert!(
            is_value.eq(&value),
            "key '{key}' is: {is_value:?}, want: {value:?}"
        );
    }

    pub fn has_array_value<T>(&self, key: &str, value: Vec<T>)
    where
        T: PartialEq + Debug,
        Value: PartialEq<T>,
    {
        let is_value = self.inner.get(key).unwrap().as_array().unwrap();
        assert!(
            is_value.eq(&value),
            "key '{key}' is: {is_value:?}, want: {value:?}"
        );
    }

    pub fn has_null(&self, key: &str) {
        let value = self.inner.get(key).unwrap();
        assert!(value.is_null(), "{} is not null", key);
    }

    pub fn has_string(&self, key: &str) {
        let value = self.inner.get(key).unwrap();
        assert!(value.is_string(), "{} is not a string", key);
    }

    pub fn has_number(&self, key: &str) {
        let value = self.inner.get(key).unwrap();
        assert!(value.is_number(), "{} is not a numver", key);
    }

    pub fn has_number_within<T, U>(&self, key: &str, value: T, tolerance: U)
    where
        T: Into<f64>,
        U: Into<f64>,
    {
        let value: f64 = value.into();
        let tolerance: f64 = tolerance.into();
        let is_value = self.inner.get(key).unwrap().as_f64().unwrap();

        assert_within!(value, is_value, tolerance);
    }
}
