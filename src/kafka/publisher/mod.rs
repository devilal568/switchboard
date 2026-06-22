use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

use crate::config::KafkaConfig;
use crate::error::SwitchBoardError;
use crate::publication::Publication;
use crate::publisher::Publisher;

pub struct KafkaPublisher {
    producer: BaseProducer,
    topic: String,
}

impl Publication for KafkaPublisher {
    type Config = KafkaConfig;

    fn new(config: &KafkaConfig) -> Result<Self, SwitchBoardError> {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .create()?;
        Ok(Self { producer, topic: config.topic.clone() })
    }

    fn cleanup(&mut self) {
        self.producer.flush(Duration::from_secs(5)).ok();
    }
}

impl Publisher for KafkaPublisher {
    fn publish(&mut self, payload: &[u8]) -> Result<(), SwitchBoardError> {
        self.producer
            .send(BaseRecord::<(), [u8]>::to(&self.topic).payload(payload))
            .map_err(|(e, _)| SwitchBoardError::Kafka(e))?;
        Ok(())
    }
}

impl KafkaPublisher {
    /// Number of messages pending delivery (queued + in-flight).
    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }

    /// Block until all pending messages are delivered or timeout elapses.
    pub fn flush(&self, timeout: Duration) -> Result<(), SwitchBoardError> {
        self.producer.flush(timeout).map_err(SwitchBoardError::Kafka)
    }

    /// Drive delivery callbacks manually; returns number of events processed.
    pub fn poll(&self, timeout: Duration) -> i32 {
        self.producer.poll(timeout)
    }
}

impl Drop for KafkaPublisher {
    fn drop(&mut self) {
        self.cleanup();
    }
}
