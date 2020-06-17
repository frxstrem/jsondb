use serde::{
    de::{self, Deserializer},
    ser::Serializer,
    Deserialize, Serialize,
};

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct True;

impl<'de> Deserialize<'de> for True {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = bool::deserialize(deserializer)?;
        if value == true {
            Ok(True)
        } else {
            Err(de::Error::invalid_value(
                de::Unexpected::Bool(value),
                &"true",
            ))
        }
    }
}

impl Serialize for True {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        true.serialize(serializer)
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct False;

impl<'de> Deserialize<'de> for False {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = bool::deserialize(deserializer)?;
        if value == false {
            Ok(False)
        } else {
            Err(de::Error::invalid_value(
                de::Unexpected::Bool(value),
                &"false",
            ))
        }
    }
}

impl Serialize for False {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        false.serialize(serializer)
    }
}
