//! Transport library
//!
//! Supported transports:
//! - Kafka
//! - Aeron IPC
//! - Aeron UDP

pub mod config;
pub mod error;
pub mod publisher;
pub mod subscriber;
pub mod publication;
pub mod subscription;

mod kafka;
mod aeron;
pub use config::{AeronConfig, KafkaConfig};
pub use error::SwitchBoardError;
pub use publication::Publication;
pub use subscription::Subscription;
pub use publisher::Publisher;
pub use subscriber::Subscriber;

pub use kafka::{KafkaPublisher, KafkaSubscriber};
pub use aeron::{AeronIpcPublisher, AeronIpcExclusivePublisher, AeronIpcSubscriber};
pub use aeron::{AeronUdpPublisher, AeronUdpExclusivePublisher, AeronUdpSubscriber};
