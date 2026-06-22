pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub group: String,
}

pub struct AeronConfig {
    pub channel: String,
    pub stream_id: i32,
}

