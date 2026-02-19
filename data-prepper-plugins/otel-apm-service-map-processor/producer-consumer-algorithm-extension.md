# PRODUCER→CONSUMER Algorithm Extension

## Overview

This document describes the extension of the CLIENT-primary algorithm to support asynchronous PRODUCER→CONSUMER relationships and INTERNAL span processing for complete service map coverage.

## Current Gap Analysis

### Missing Span Kinds
- **SPAN_KIND_PRODUCER (4)**: Message publishing to queues/topics
- **SPAN_KIND_CONSUMER (5)**: Message consumption from queues/topics
- **SPAN_KIND_INTERNAL (1)**: Internal operations within services

### Missing Relationship Types
```yaml
# Async Message Flows (Currently Invisible):
Order Service (PRODUCER) → Kafka:order-events → Inventory Service (CONSUMER)
Payment API (PRODUCER) → SQS:notifications → Email Service (CONSUMER)
User Registration (PRODUCER) → EventBridge → Analytics Service (CONSUMER)

# Internal Service Breakdown (Currently Invisible):
API Gateway (SERVER)
  └── Auth Logic (INTERNAL)
  └── Business Rules (INTERNAL)
  └── Data Access (INTERNAL)
```

## Extended Algorithm Design

### 1. Three-Phase Processing Model

**Current Two-Phase:**
```
Phase 1: CLIENT Decoration  → Annotate CLIENT spans with remote service info
Phase 2: Event Generation   → CLIENT-primary emission + leaf SERVER emission
```

**Extended Three-Phase:**
```
Phase 1: CLIENT Decoration     → Annotate CLIENT spans with remote service info
Phase 2: PRODUCER Decoration   → Annotate PRODUCER spans with destination info
Phase 3: Event Generation      → CLIENT/PRODUCER-primary + CONSUMER/SERVER/INTERNAL emission
```

### 2. PRODUCER Span Decoration (New Phase 2)

**PRODUCER Decoration Logic:**
```java
// New decoration method in OTelApmServiceMapProcessor
private void decorateProducerSpans(Collection<SpanStateData> spans) {
    Map<String, List<SpanStateData>> consumersByDestination = new HashMap<>();

    // Index CONSUMER spans by destination (queue/topic)
    spans.stream()
        .filter(span -> "SPAN_KIND_CONSUMER".equals(span.getKind()))
        .forEach(consumer -> {
            String destination = extractDestination(consumer);
            consumersByDestination.computeIfAbsent(destination, k -> new ArrayList<>())
                .add(consumer);
        });

    // Decorate PRODUCER spans with consumer information
    spans.stream()
        .filter(span -> "SPAN_KIND_PRODUCER".equals(span.getKind()))
        .forEach(producer -> {
            String destination = extractDestination(producer);
            List<SpanStateData> consumers = consumersByDestination.get(destination);

            if (consumers != null && !consumers.isEmpty()) {
                // For messaging systems, one producer can have multiple consumers
                decorateProducerWithConsumers(producer, consumers, destination);
            }
        });
}
```

### 3. Destination Resolution Strategy

**Messaging System Detection:**
```java
private String extractDestination(SpanStateData span) {
    // Priority order for destination extraction:

    // 1. OpenTelemetry Semantic Conventions
    String messagingDestination = span.getAttribute("messaging.destination.name");
    if (messagingDestination != null) {
        return messagingDestination;
    }

    // 2. AWS SQS/SNS
    String awsQueue = span.getAttribute("aws.sqs.queue_name");
    String awsTopic = span.getAttribute("aws.sns.topic_name");
    if (awsQueue != null) return "sqs:" + awsQueue;
    if (awsTopic != null) return "sns:" + awsTopic;

    // 3. Kafka
    String kafkaTopic = span.getAttribute("messaging.kafka.destination.name");
    if (kafkaTopic != null) return "kafka:" + kafkaTopic;

    // 4. RabbitMQ
    String rabbitQueue = span.getAttribute("messaging.rabbitmq.destination.routing_key");
    if (rabbitQueue != null) return "rabbitmq:" + rabbitQueue;

    // 5. Fallback to span name
    return "unknown:" + span.getSpanName();
}
```

### 4. Extended NodeOperationDetail Generation

**New PRODUCER-Primary Emission:**
```java
private List<NodeOperationDetail> generateNodeOperationDetailEvents(Collection<SpanStateData> spans) {
    List<NodeOperationDetail> events = new ArrayList<>();

    // 1. CLIENT-primary emission (existing)
    events.addAll(generateClientPrimaryEvents(spans));

    // 2. PRODUCER-primary emission (new)
    events.addAll(generateProducerPrimaryEvents(spans));

    // 3. Leaf SERVER emission (existing)
    events.addAll(generateLeafServerEvents(spans));

    // 4. CONSUMER emission (new)
    events.addAll(generateConsumerEvents(spans));

    // 5. INTERNAL aggregation (new)
    events.addAll(generateInternalEvents(spans));

    return events;
}
```

**PRODUCER-Primary Event Generation:**
```java
private List<NodeOperationDetail> generateProducerPrimaryEvents(Collection<SpanStateData> spans) {
    return spans.stream()
        .filter(span -> "SPAN_KIND_PRODUCER".equals(span.getKind()))
        .filter(span -> hasValidDestinationDecoration(span))
        .map(this::createProducerNodeOperationDetail)
        .collect(Collectors.toList());
}

private NodeOperationDetail createProducerNodeOperationDetail(SpanStateData producerSpan) {
    // Source: Producer service
    Node sourceNode = Node.builder()
        .keyAttributes(Map.of(
            "environment", producerSpan.getEnvironment(),
            "serviceName", producerSpan.getServiceName()
        ))
        .groupByAttributes(extractGroupByAttributes(producerSpan))
        .type("service")  // Source is always a service
        .build();

    // Target: Message destination (queue/topic) or consumer service
    Node targetNode = createTargetNodeFromDestination(producerSpan);

    // Operation: Publishing operation
    Operation sourceOperation = Operation.builder()
        .name(producerSpan.getOperationName())
        .attributes(Map.of(
            "messaging.operation", "publish",
            "messaging.destination", producerSpan.getDestination(),
            "messaging.system", extractMessagingSystem(producerSpan)
        ))
        .build();

    return NodeOperationDetail.builder()
        .sourceNode(sourceNode)
        .targetNode(targetNode)
        .sourceOperation(sourceOperation)
        .targetOperation(getTargetOperation(producerSpan))
        .timestamp(producerSpan.getStartTime())
        .build();
}
```

### 5. Messaging System Patterns

**Pattern A: Queue-Based (Point-to-Point)**
```yaml
# SQS, RabbitMQ Work Queues
Producer Service → Queue → Single Consumer Service

NodeOperationDetail:
  sourceNode: {serviceName: "order-service", environment: "prod"}
  targetNode: {serviceName: "inventory-service", environment: "prod"}
  sourceOperation: {name: "publish_order", messaging.destination: "order-queue"}
  targetOperation: {name: "process_order"}
```

**Pattern B: Topic-Based (Pub/Sub)**
```yaml
# Kafka, SNS, RabbitMQ Exchanges
Producer Service → Topic → Multiple Consumer Services

# Generate separate NodeOperationDetail for each consumer
NodeOperationDetail 1:
  sourceNode: {serviceName: "user-service"}
  targetNode: {serviceName: "email-service"}
  sourceOperation: {name: "user_registered", messaging.topic: "user-events"}
  targetOperation: {name: "send_welcome_email"}

NodeOperationDetail 2:
  sourceNode: {serviceName: "user-service"}
  targetNode: {serviceName: "analytics-service"}
  sourceOperation: {name: "user_registered", messaging.topic: "user-events"}
  targetOperation: {name: "track_user_signup"}
```

**Pattern C: Intermediate Destination Node**
```yaml
# When consumer service cannot be determined
Producer Service → Message Destination (Virtual Node) → Unknown Consumers

NodeOperationDetail:
  sourceNode: {serviceName: "order-service", type: "service"}
  targetNode: {serviceName: "order-events", type: "messaging", environment: "kafka"}
  sourceOperation: {name: "publish_order"}
  # targetOperation: null (destination node has no operations)
```

### 6. CONSUMER Span Processing

**CONSUMER Event Generation:**
```java
private List<NodeOperationDetail> generateConsumerEvents(Collection<SpanStateData> spans) {
    return spans.stream()
        .filter(span -> "SPAN_KIND_CONSUMER".equals(span.getKind()))
        .filter(span -> !hasMatchingProducer(span))  // Only if no PRODUCER found
        .map(this::createConsumerNodeOperationDetail)
        .collect(Collectors.toList());
}

private NodeOperationDetail createConsumerNodeOperationDetail(SpanStateData consumerSpan) {
    // Source: Message destination (when producer unknown)
    Node sourceNode = Node.builder()
        .keyAttributes(Map.of(
            "environment", extractMessagingEnvironment(consumerSpan),
            "serviceName", consumerSpan.getDestination()
        ))
        .type("messaging")
        .build();

    // Target: Consumer service
    Node targetNode = Node.builder()
        .keyAttributes(Map.of(
            "environment", consumerSpan.getEnvironment(),
            "serviceName", consumerSpan.getServiceName()
        ))
        .groupByAttributes(extractGroupByAttributes(consumerSpan))
        .type("service")
        .build();

    Operation targetOperation = Operation.builder()
        .name(consumerSpan.getOperationName())
        .attributes(Map.of(
            "messaging.operation", "receive",
            "messaging.destination", consumerSpan.getDestination()
        ))
        .build();

    return NodeOperationDetail.builder()
        .sourceNode(sourceNode)
        .targetNode(targetNode)
        .targetOperation(targetOperation)
        .timestamp(consumerSpan.getStartTime())
        .build();
}
```

### 7. INTERNAL Span Aggregation

**Service-Internal Operations:**
```java
private List<NodeOperationDetail> generateInternalEvents(Collection<SpanStateData> spans) {
    // Group INTERNAL spans by service and parent operation
    Map<InternalGroupKey, List<SpanStateData>> internalGroups = spans.stream()
        .filter(span -> "SPAN_KIND_INTERNAL".equals(span.getKind()))
        .collect(Collectors.groupingBy(this::createInternalGroupKey));

    return internalGroups.entrySet().stream()
        .map(entry -> createInternalNodeOperationDetail(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
}

private NodeOperationDetail createInternalNodeOperationDetail(
    InternalGroupKey key, List<SpanStateData> internalSpans) {

    // Both source and target are the same service (internal operation)
    Node serviceNode = Node.builder()
        .keyAttributes(Map.of(
            "environment", key.getEnvironment(),
            "serviceName", key.getServiceName()
        ))
        .groupByAttributes(key.getGroupByAttributes())
        .type("service")
        .build();

    // Aggregate internal operations
    Operation internalOperation = Operation.builder()
        .name(aggregateInternalOperationName(internalSpans))
        .attributes(Map.of(
            "internal.operation_count", internalSpans.size(),
            "internal.parent_operation", key.getParentOperation()
        ))
        .build();

    return NodeOperationDetail.builder()
        .sourceNode(serviceNode)
        .targetNode(serviceNode)  // Same service for internal
        .sourceOperation(internalOperation)
        // targetOperation: null for internal operations
        .timestamp(internalSpans.get(0).getStartTime())
        .build();
}
```

### 8. Extended Metrics Generation

**New Async Metrics for PRODUCER Spans:**
```java
private void generateProducerMetrics(SpanStateData span, Map<MetricKey, MetricAggregationState> metricsState) {
    MetricKey key = MetricKey.builder()
        .attributes(Map.of(
            "namespace", "span_derived",
            "environment", span.getEnvironment(),
            "service", span.getServiceName(),
            "operation", span.getOperationName(),
            "messaging.destination", span.getDestination(),
            "messaging.system", extractMessagingSystem(span),
            // Plus groupByAttributes
        ))
        .timestamp(getWindowBoundary(span.getStartTime()))
        .build();

    MetricAggregationState state = metricsState.computeIfAbsent(key, k -> new MetricAggregationState());

    // Standard metrics
    state.incrementRequestCount(1);
    if (span.getError() > 0) state.incrementErrorCount(1);
    if (span.getFault() > 0) state.incrementFaultCount(1);

    // Producer-specific latency (time to publish)
    if (span.getDurationInNanos() != null) {
        double publishLatency = span.getDurationInNanos() / 1_000_000_000.0;
        state.addLatencyDuration(publishLatency);
    }
}
```

**New End-to-End Latency for CONSUMER Spans:**
```java
private void generateConsumerMetrics(SpanStateData span, Map<MetricKey, MetricAggregationState> metricsState) {
    // Standard consumer metrics
    generateStandardMetrics(span, metricsState, "consumer");

    // End-to-end processing latency (if producer correlation available)
    SpanStateData correlatedProducer = findCorrelatedProducer(span);
    if (correlatedProducer != null) {
        double endToEndLatency = calculateEndToEndLatency(correlatedProducer, span);

        MetricKey e2eKey = createEndToEndMetricKey(correlatedProducer, span);
        MetricAggregationState e2eState = metricsState.computeIfAbsent(e2eKey, k -> new MetricAggregationState());
        e2eState.addLatencyDuration(endToEndLatency);
    }
}
```

### 9. Correlation Strategies

**Message Correlation Approaches:**

**A. Trace-Based Correlation:**
```java
private SpanStateData findCorrelatedProducer(SpanStateData consumerSpan) {
    // Same trace ID indicates direct correlation
    return spansByTraceId.get(consumerSpan.getTraceId()).stream()
        .filter(span -> "SPAN_KIND_PRODUCER".equals(span.getKind()))
        .filter(span -> sameDestination(span, consumerSpan))
        .findFirst()
        .orElse(null);
}
```

**B. Message ID Correlation:**
```java
private SpanStateData findCorrelatedProducerByMessageId(SpanStateData consumerSpan) {
    String messageId = consumerSpan.getAttribute("messaging.message_id");
    if (messageId == null) return null;

    return recentProducerSpans.stream()
        .filter(producer -> messageId.equals(producer.getAttribute("messaging.message_id")))
        .filter(producer -> sameDestination(producer, consumerSpan))
        .findFirst()
        .orElse(null);
}
```

**C. Temporal Correlation:**
```java
private List<SpanStateData> findTemporallyCorrelatedProducers(SpanStateData consumerSpan) {
    Instant consumerStart = consumerSpan.getStartTime();
    String destination = consumerSpan.getDestination();

    // Look for producers in recent time windows (e.g., last 10 minutes)
    return recentProducerSpans.stream()
        .filter(producer -> destination.equals(producer.getDestination()))
        .filter(producer -> producer.getStartTime().isBefore(consumerStart))
        .filter(producer -> Duration.between(producer.getStartTime(), consumerStart)
            .compareTo(Duration.ofMinutes(10)) <= 0)
        .collect(Collectors.toList());
}
```

### 10. Implementation Strategy

**Phase 1: Basic PRODUCER→CONSUMER Support**
1. Extend span decoration to handle PRODUCER spans
2. Add destination extraction utilities
3. Implement PRODUCER-primary event generation
4. Add basic CONSUMER event generation

**Phase 2: Advanced Correlation**
1. Implement message ID correlation
2. Add temporal correlation for approximate relationships
3. Support cross-window correlation (producers from previous windows)

**Phase 3: Internal Operations**
1. Add INTERNAL span aggregation
2. Implement service-internal metrics
3. Support complex internal operation hierarchies

**Phase 4: Performance Optimization**
1. Optimize correlation lookups with indexing
2. Implement efficient cross-window state management
3. Add configuration for correlation strategies

### 11. Configuration Extensions

**New Configuration Options:**
```yaml
processor:
  - otel_apm_service_map:
      # Existing config
      window_duration: 60s
      db_path: "data/otel-apm-service-map/"
      group_by_attributes: ["service.version"]

      # New async config
      async_correlation:
        enabled: true
        correlation_window: 600s  # Look back 10 minutes for producers
        correlation_strategies:
          - "trace_id"      # Same trace correlation
          - "message_id"    # Message ID correlation
          - "temporal"      # Time-based approximate correlation

      internal_operations:
        enabled: true
        aggregation_level: "operation"  # "operation" | "service"

      messaging_systems:
        kafka:
          destination_attribute: "messaging.kafka.destination.name"
        sqs:
          destination_attribute: "aws.sqs.queue_name"
        sns:
          destination_attribute: "aws.sns.topic_name"
```

This extension transforms the APM processor from sync-only CLIENT→SERVER relationship detection to comprehensive async PRODUCER→CONSUMER and INTERNAL operation support, providing complete visibility into modern event-driven architectures.