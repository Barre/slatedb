use bytes::Bytes;
use object_store::{Attribute, Attributes, ObjectMeta};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalCacheHead {
    pub location: String,
    pub last_modified: String,
    pub size: u64,
    pub e_tag: Option<String>,
    pub version: Option<String>,
    pub attributes: HashMap<String, String>,
}

impl LocalCacheHead {
    pub fn meta(&self) -> ObjectMeta {
        ObjectMeta {
            location: self.location.clone().into(),
            last_modified: self.last_modified.parse().unwrap_or_default(),
            size: self.size,
            e_tag: self.e_tag.clone(),
            version: self.version.clone(),
        }
    }

    pub fn attributes(&self) -> Attributes {
        let mut attrs = Attributes::new();
        for (key, value) in self.attributes.iter() {
            let key = match key.as_str() {
                "Cache-Control" => Attribute::CacheControl,
                "Content-Disposition" => Attribute::ContentDisposition,
                "Content-Encoding" => Attribute::ContentEncoding,
                "Content-Language" => Attribute::ContentLanguage,
                "Content-Type" => Attribute::ContentType,
                _ => Attribute::Metadata(key.to_string().into()),
            };
            let value = value.to_string().into();
            attrs.insert(key, value);
        }
        attrs
    }
}

impl From<(&ObjectMeta, &Attributes)> for LocalCacheHead {
    fn from((meta, attrs): (&ObjectMeta, &Attributes)) -> Self {
        let mut attrs_map = HashMap::new();
        for (key, value) in attrs.iter() {
            let key = match key {
                Attribute::CacheControl => "Cache-Control",
                Attribute::ContentDisposition => "Content-Disposition",
                Attribute::ContentEncoding => "Content-Encoding",
                Attribute::ContentLanguage => "Content-Language",
                Attribute::ContentType => "Content-Type",
                Attribute::Metadata(key) => key,
                _ => continue,
            };
            attrs_map.insert(key.to_string(), value.to_string());
        }
        LocalCacheHead {
            location: meta.location.to_string(),
            last_modified: meta.last_modified.to_rfc3339(),
            size: meta.size,
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
            attributes: attrs_map,
        }
    }
}

/// Unified cache key for both parts and heads
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum ObjectStoreCacheKey {
    Part { location: String, part_id: usize },
    Head { location: String },
}

/// Unified cache value for both parts and heads
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjectStoreCacheValue {
    Part(Bytes),
    Head(LocalCacheHead),
}

pub type PartID = usize;
