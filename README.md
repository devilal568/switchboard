# switchboard

A Rust transport library that provides a unified interface over Kafka and Aeron (IPC and UDP).
The library is byte-level — it moves `&[u8]` payloads. Serialization is the caller's responsibility.

## Transports

| Type | Struct | Use case |
|---|---|---|
| Kafka | `KafkaPublisher` / `KafkaSubscriber` | Persistent, broker-based messaging |
| Aeron IPC | `AeronIpcPublisher` / `AeronIpcSubscriber` | Same-machine, shared-memory, lowest latency |
| Aeron IPC (exclusive) | `AeronIpcExclusivePublisher` | Single writer, no lock overhead |
| Aeron UDP | `AeronUdpPublisher` / `AeronUdpSubscriber` | Cross-machine, low-latency UDP |
| Aeron UDP (exclusive) | `AeronUdpExclusivePublisher` | Single writer over UDP |

## Usage

Add to `Cargo.toml`:

```toml
[dependencies]
switchboard = { path = "../switchboard" }
```

Load config in the calling application (library never reads `.env`):

```rust
dotenvy::dotenv().ok();
```

---

### Kafka

```rust
use switchboard::{KafkaConfig, KafkaPublisher, KafkaSubscriber, Publication, Subscription, Publisher, Subscriber};
use std::time::Duration;

// Publish
let config = KafkaConfig {
    brokers: std::env::var("KAFKA_BROKERS").unwrap(),
    topic:   std::env::var("KAFKA_TOPIC").unwrap(),
    group:   std::env::var("KAFKA_GROUP").unwrap(),
};
let mut pub = KafkaPublisher::new(&config)?;
pub.publish(b"hello")?;

// Kafka-specific
println!("inflight: {}", pub.in_flight_count());
pub.flush(Duration::from_secs(5))?;

// Subscribe — call listen() in your own loop; returns None if no message arrived
let mut sub = KafkaSubscriber::new(config)?;
loop {
    if let Some(msg) = sub.listen()? {
        println!("received: {:?}", msg);
    }
}
```

---

### Aeron IPC

Used when publisher and subscriber are on the **same machine**. Communicates via shared memory — no network involved.

```rust
use switchboard::{AeronConfig, AeronIpcPublisher, AeronIpcSubscriber, Publication, Subscription, Publisher, Subscriber};

let config = AeronConfig {
    channel:   "aeron:ipc".into(),
    stream_id: 1001,
};

// Concurrent publisher (multiple writers allowed)
let mut pub = AeronIpcPublisher::new(&config)?;
pub.publish(b"order data")?;

// Exclusive publisher (single writer, no lock — prefer this when only one publisher exists)
use switchboard::AeronIpcExclusivePublisher;
let mut pub = AeronIpcExclusivePublisher::new(&config)?;
pub.publish(b"order data")?;

// Subscribe — call listen() in your own loop; returns None if no message arrived
let mut sub = AeronIpcSubscriber::new(config)?;
loop {
    if let Some(msg) = sub.listen()? {
        println!("received: {:?}", msg);
    }
}
```

---

### Aeron UDP

Used when publisher and subscriber are on **different machines**. Same API as IPC, different channel URI.

```rust
use switchboard::{AeronConfig, AeronUdpPublisher, AeronUdpSubscriber, Publication, Subscription, Publisher, Subscriber};

let config = AeronConfig {
    channel:   "aeron:udp?endpoint=192.168.1.10:20121".into(),
    stream_id: 2001,
};

// Concurrent publisher
let mut pub = AeronUdpPublisher::new(&config)?;
pub.publish(b"market data")?;

// Exclusive publisher
use switchboard::AeronUdpExclusivePublisher;
let mut pub = AeronUdpExclusivePublisher::new(&config)?;
pub.publish(b"market data")?;

// Subscribe — call listen() in your own loop; returns None if no message arrived
let mut sub = AeronUdpSubscriber::new(config)?;
loop {
    if let Some(msg) = sub.listen()? {
        println!("received: {:?}", msg);
    }
}
```

---

## Error handling

All constructors and trait methods return `Result<_, SwitchBoardError>`:

```rust
use switchboard::SwitchBoardError;

// Publishers return Result<(), SwitchBoardError>
match publisher.publish(b"data") {
    Err(SwitchBoardError::Kafka(e))      => { /* rdkafka error */ }
    Err(SwitchBoardError::Aeron(msg))    => { /* aeron driver error */ }
    Err(SwitchBoardError::Config(field)) => { /* missing config field */ }
    Ok(()) => {}
}

// Subscribers return Result<Option<Vec<u8>>, SwitchBoardError>
match subscriber.listen() {
    Ok(Some(bytes)) => { /* message received */ }
    Ok(None)        => { /* no message this poll */ }
    Err(e)          => { /* transport error */ }
}
```

## Cleanup

All types implement `Drop` — resources are released automatically when the publisher or subscriber goes out of scope. No explicit cleanup call needed.
