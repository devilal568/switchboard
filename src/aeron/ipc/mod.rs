use std::ffi::CString;
use std::time::Duration;

use rusteron_client::*;

use crate::config::AeronConfig;
use crate::error::SwitchBoardError;
use crate::publication::Publication;
use crate::publisher::Publisher;
use crate::subscriber::Subscriber;
use crate::subscription::Subscription;

fn connect(config: &AeronConfig) -> Result<(Aeron, CString), SwitchBoardError> {
    let ctx = AeronContext::new().map_err(|e| SwitchBoardError::Aeron(e.to_string()))?;
    let aeron = Aeron::new(&ctx).map_err(|e| SwitchBoardError::Aeron(e.to_string()))?;
    aeron.start().map_err(|e| SwitchBoardError::Aeron(e.to_string()))?;
    let channel = CString::new(config.channel.as_str())
        .map_err(|e| SwitchBoardError::Aeron(e.to_string()))?;
    Ok((aeron, channel))
}

// Concurrent publisher

pub struct AeronIpcPublisher {
    publication: AeronPublication,
    _aeron: Aeron,
}

impl Publication for AeronIpcPublisher {
    type Config = AeronConfig;

    fn new(config: &AeronConfig) -> Result<Self, SwitchBoardError> {
        let (aeron, channel) = connect(config)?;
        let publication = aeron
            .async_add_publication(channel.as_c_str(), config.stream_id)
            .map_err(|e| SwitchBoardError::Aeron(e.to_string()))?
            .poll_blocking(Duration::from_secs(5))
            .map_err(|e| SwitchBoardError::Aeron(e.to_string()))?;
        Ok(Self { publication, _aeron: aeron })
    }

    fn cleanup(&mut self) {}
}

impl Publisher for AeronIpcPublisher {
    fn publish(&mut self, payload: &[u8]) -> Result<(), SwitchBoardError> {
        let result = self
            .publication
            .offer(payload, Handlers::no_reserved_value_supplier_handler());
        if result < 0 {
            Err(SwitchBoardError::Aeron(format!("offer failed: {result}")))
        } else {
            Ok(())
        }
    }
}

impl Drop for AeronIpcPublisher {
    fn drop(&mut self) {
        self.cleanup();
    }
}

// Exclusive publisher 

pub struct AeronIpcExclusivePublisher {
    publication: AeronExclusivePublication,
    _aeron: Aeron,
}

impl Publication for AeronIpcExclusivePublisher {
    type Config = AeronConfig;

    fn new(config: &AeronConfig) -> Result<Self, SwitchBoardError> {
        let (aeron, channel) = connect(config)?;
        let publication = aeron
            .async_add_exclusive_publication(channel.as_c_str(), config.stream_id)
            .map_err(|e| SwitchBoardError::Aeron(e.to_string()))?
            .poll_blocking(Duration::from_secs(5))
            .map_err(|e| SwitchBoardError::Aeron(e.to_string()))?;
        Ok(Self { publication, _aeron: aeron })
    }

    fn cleanup(&mut self) {}
}

impl Publisher for AeronIpcExclusivePublisher {
    fn publish(&mut self, payload: &[u8]) -> Result<(), SwitchBoardError> {
        let result = self.publication.offer_block(payload);
        if result < 0 {
            Err(SwitchBoardError::Aeron(format!("offer_block failed: {result}")))
        } else {
            Ok(())
        }
    }
}

impl Drop for AeronIpcExclusivePublisher {
    fn drop(&mut self) {
        self.cleanup();
    }
}

// Subscriber

pub struct AeronIpcSubscriber {
    subscription: AeronSubscription,
    _aeron: Aeron,
}

impl Subscription for AeronIpcSubscriber {
    type Config = AeronConfig;

    fn new(config: AeronConfig) -> Result<Self, SwitchBoardError> {
        let (aeron, channel) = connect(&config)?;
        let subscription = aeron
            .async_add_subscription(
                channel.as_c_str(),
                config.stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )
            .map_err(|e| SwitchBoardError::Aeron(e.to_string()))?
            .poll_blocking(Duration::from_secs(5))
            .map_err(|e| SwitchBoardError::Aeron(e.to_string()))?;
        Ok(Self { subscription, _aeron: aeron })
    }

    fn cleanup(&mut self) {}
}

impl Subscriber for AeronIpcSubscriber {
    fn listen(&mut self) -> Result<Option<Vec<u8>>, SwitchBoardError> {
        let mut received: Option<Vec<u8>> = None;
        self.subscription
            .poll_once(|buffer, _header| { received = Some(buffer.to_vec()); }, 1)
            .map_err(|e| SwitchBoardError::Aeron(e.to_string()))?;
        Ok(received)
    }
}

impl Drop for AeronIpcSubscriber {
    fn drop(&mut self) {
        self.cleanup();
    }
}
