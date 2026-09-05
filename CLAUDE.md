# datalake-mq-intake

MQ to HDFS landing service — transactional, at-least-once delivery.

## Build and Test

```bash
# Build all modules
mvn clean install

# Build without tests
mvn clean install -DskipTests

# Run tests only
mvn test

# Build a specific module
mvn clean install -pl core
mvn clean install -pl rms -am   # -am includes dependencies (core)
mvn clean install -pl claims -am
```

## Java Version

Java 11

## Module Dependencies

```
core/        (depends on neither module)
rms/         (depends on core)
claims/      (depends on core)
```

- `core` contains the shared machinery: transactional receive loop, batching, sequence file writer, path stamping, audit, Kerberos, RecordSerializer interface
- The receive loop is decomposed: `TransactedReceiveLoop` receives and decides when a batch is full; `BatchAccumulator` holds it; `BatchTransactionProcessor` owns the unit of work (screen → write → balance → tracker → audit → commit, in that order); `SessionRecoveryCoordinator` rebuilds a faulted session; `LoopStateReporter` records health and metrics. The ordering inside the processor IS the delivery guarantee — see `LoopInvariantCharacterisationTest`, and re-run its mutations rather than trusting a green suite when changing any of them
- `rms` and `claims` contain binding-specific implementations (RecordSerializer, TrackerMessageBuilder)
- If a module needs shared functionality, it goes in `core` — never duplicate logic across `rms` and `claims`

## Standing Constraints

These three rules are non-negotiable. Violating any of them breaks the core delivery guarantee.

### 1. Hand-Rolled Message Loop

The message loop is hand-rolled. Do NOT use `@JmsListener`, `DefaultMessageListenerContainer`, or `JmsTemplate` anywhere. They impose a per-message transaction boundary, which is the exact defect this project exists to remove. Spring Boot is for configuration, lifecycle, health, and wiring only, and stays out of the message loop.

### 2. One Session Per Thread

One transacted JMS `Session` per listener thread. `Session`, `MessageConsumer`, and `MessageProducer` are never shared across threads. A thread's consumer and producer are created from that same `Session`.

### 3. Acknowledge Only After HDFS Visibility

Nothing is acknowledged to MQ before the HDFS file is closed AND renamed into its partition. The order is always: close (durability) → rename (visibility) → commit (delivery).
