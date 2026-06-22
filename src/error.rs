use thiserror::Error;

#[derive(Debug, Error)]
pub enum SwitchBoardError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("aeron error: {0}")]
    Aeron(String),

    #[error("config missing: {0}")]
    Config(&'static str),
}

