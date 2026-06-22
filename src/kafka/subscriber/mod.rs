use std::time::Duration;

use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};

use crate::config::KafkaConfig;
use crate::error::SwitchBoardError;
use crate::subscriber::Subscriber;
use crate::subscription::Subscription;

pub struct KafkaSubscriber {
    consumer: BaseConsumer,
}

impl Subscription for KafkaSubscriber {
    type Config = KafkaConfig;

    fn new(config: KafkaConfig) -> Result<Self, SwitchBoardError> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group)
            .set("auto.offset.reset", "earliest")
            .create()?;
        consumer.subscribe(&[config.topic.as_str()])?;
        Ok(Self { consumer })
    }

    fn cleanup(&mut self) {
        self.consumer.unsubscribe();
    }
}

impl Subscriber for KafkaSubscriber {
    fn listen(&mut self) -> Result<Option<Vec<u8>>, SwitchBoardError> {
        match self.consumer.poll(Duration::from_millis(100)) {
            Some(Ok(msg)) => Ok(msg.payload().map(|v| v.to_vec())),
            Some(Err(e)) => return Err(SwitchBoardError::Kafka(e)),
            None => Ok(None),
        }
    }
}

impl Drop for KafkaSubscriber {
    fn drop(&mut self) {
        self.cleanup();
    }
}
