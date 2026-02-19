/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import com.google.common.primitives.SignedBytes;
import org.apache.commons.codec.binary.Hex;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.Node;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.NodeOperationDetail;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.Operation;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.SpanStateData;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ClientSpanDecoration;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ServerSpanDecoration;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ProducerSpanDecoration;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ConsumerSpanDecoration;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ThreeWindowTraceData;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ThreeWindowTraceDataWithDecorations;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.EphemeralSpanDecorations;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.MetricKey;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.MetricAggregationState;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.utils.ApmServiceMapMetricsUtil;
import org.opensearch.dataprepper.plugins.processor.state.MapDbProcessorState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@SingleThread
@DataPrepperPlugin(name = "otel_apm_service_map", pluginType = Processor.class,
        pluginConfigurationType = OTelApmServiceMapProcessorConfig.class)
public class OTelApmServiceMapProcessor extends AbstractProcessor<Record<Event>, Record<Event>> implements RequiresPeerForwarding {

    private static final String SPANS_DB_SIZE = "spansDbSize";
    private static final String SPANS_DB_COUNT = "spansDbCount";

    private static final Logger LOG = LoggerFactory.getLogger(OTelApmServiceMapProcessor.class);
    private static final String EVENT_TYPE_OTEL_APM_SERVICE_MAP = "SERVICE_MAP";
    private static final Collection<Record<Event>> EMPTY_COLLECTION = Collections.emptySet();
    private static final String SPAN_KIND_SERVER = "SPAN_KIND_SERVER";
    private static final String SPAN_KIND_CLIENT = "SPAN_KIND_CLIENT";
    private static final String SPAN_KIND_PRODUCER = "SPAN_KIND_PRODUCER";
    private static final String SPAN_KIND_CONSUMER = "SPAN_KIND_CONSUMER";
    private static final String NODE_TYPE_SERVICE = "service";

    // TODO: This should not be tracked in this class, move it up to the creator
    private static final AtomicInteger processorsCreated = new AtomicInteger(0);
    private static Instant previousTimestamp;
    private static Duration windowDuration;
    private static CyclicBarrier allThreadsCyclicBarrier;

    private static volatile MapDbProcessorState<Collection<SpanStateData>> previousWindow;
    private static volatile MapDbProcessorState<Collection<SpanStateData>> currentWindow;
    private static volatile MapDbProcessorState<Collection<SpanStateData>> nextWindow;
    private static File dbPath;
    private static Clock clock;

    private final int thisProcessorId;
    private final List<String> groupByAttributes;
    private final EventFactory eventFactory;

    @DataPrepperPluginConstructor
    public OTelApmServiceMapProcessor(
            final OTelApmServiceMapProcessorConfig config,
            final PluginMetrics pluginMetrics,
            final EventFactory eventFactory,
            final PipelineDescription pipelineDescription) {
        this(config.getWindowDuration(),
                new File(config.getDbPath()),
                Clock.systemUTC(),
                pipelineDescription.getNumberOfProcessWorkers(),
                eventFactory,
                pluginMetrics,
                config.getGroupByAttributes());
    }

    OTelApmServiceMapProcessor(final Duration windowDuration,
                               final File databasePath,
                               final Clock clock,
                               final int processWorkers,
                               final EventFactory eventFactory,
                               final PluginMetrics pluginMetrics) {
        this(windowDuration, databasePath, clock, processWorkers, eventFactory, pluginMetrics, Collections.emptyList());
    }

    OTelApmServiceMapProcessor(final Duration windowDuration,
                               final File databasePath,
                               final Clock clock,
                               final int processWorkers,
                               final EventFactory eventFactory,
                               final PluginMetrics pluginMetrics,
                               final List<String> groupByAttributes) {
        super(pluginMetrics);

        this.groupByAttributes = groupByAttributes != null ? Collections.unmodifiableList(groupByAttributes) : Collections.emptyList();

        this.eventFactory = eventFactory;
        OTelApmServiceMapProcessor.clock = clock;
        this.thisProcessorId = processorsCreated.getAndIncrement();

        if (isMasterInstance()) {
            previousTimestamp = OTelApmServiceMapProcessor.clock.instant();
            OTelApmServiceMapProcessor.windowDuration = windowDuration;
            OTelApmServiceMapProcessor.dbPath = createPath(databasePath);

            currentWindow = new MapDbProcessorState<>(dbPath, getNewDbName(), processWorkers);
            previousWindow = new MapDbProcessorState<>(dbPath, getNewDbName() + "-previous", processWorkers);
            nextWindow = new MapDbProcessorState<>(dbPath, getNewDbName() + "-next", processWorkers);

            allThreadsCyclicBarrier = new CyclicBarrier(processWorkers);
        }

        pluginMetrics.gauge(SPANS_DB_SIZE, this, processor -> processor.getSpansDbSize());
        pluginMetrics.gauge(SPANS_DB_COUNT, this, processor -> processor.getSpansDbCount());
    }

    /**
     * Adds the data for spans from the ResourceSpans object to the current window
     *
     * @param records Input records that will be modified/processed
     * @return If the window is reached, returns a list of ServiceDetails and ServiceRemoteDetails events.
     * Otherwise, returns an empty set.
     */
    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        final Collection<Record<Event>> apmEvents = windowDurationHasPassed() ? evaluateApmEvents() : EMPTY_COLLECTION;
        final Map<byte[], Collection<SpanStateData>> batchStateData = new TreeMap<>(SignedBytes.lexicographicalComparator());

        records.forEach(i -> processSpan((Span) i.getData(), batchStateData));

        try {
            // Update next window with batch data organized by traceId
            for (Map.Entry<byte[], Collection<SpanStateData>> entry : batchStateData.entrySet()) {
                final byte[] traceId = entry.getKey();
                final Collection<SpanStateData> spansForTrace = entry.getValue();

                Collection<SpanStateData> existingSpans = nextWindow.get(traceId);
                if (existingSpans == null) {
                    existingSpans = new HashSet<>();
                }
                existingSpans.addAll(spansForTrace);
                nextWindow.put(traceId, existingSpans);
            }
        } catch (RuntimeException e) {
            LOG.error("Caught exception trying to put batch state data", e);
        }
        return apmEvents;
    }

    public void prepareForShutdown() {
        previousTimestamp = Instant.EPOCH;
    }

    @Override
    public boolean isReadyForShutdown() {
        return currentWindow.size() == 0;
    }

    @Override
    public void shutdown() {
        previousWindow.delete();
        currentWindow.delete();
        if (nextWindow != null) {
            nextWindow.delete();
        }
        processorsCreated.set(0);
        allThreadsCyclicBarrier.reset();
    }

    /**
     * @return Spans database size in bytes
     */
    public double getSpansDbSize() {
        return currentWindow.sizeInBytes() + previousWindow.sizeInBytes() +
                (nextWindow != null ? nextWindow.sizeInBytes() : 0);
    }

    public double getSpansDbCount() {
        return currentWindow.size() + previousWindow.size() +
                (nextWindow != null ? nextWindow.size() : 0);
    }

    @Override
    public Collection<String> getIdentificationKeys() {
        return Collections.singleton("traceId");
    }

    /**
     * This function creates the directory if it doesn't exists and returns the File.
     *
     * @param path
     * @return path
     * @throws RuntimeException if the directory can not be created.
     */
    private static File createPath(File path) {
        if (!path.exists()) {
            if (!path.mkdirs()) {
                throw new RuntimeException(String.format("Unable to create the directory at the provided path: %s", path.getName()));
            }
        }
        return path;
    }

    private void processSpan(final Span span, final Map<byte[], Collection<SpanStateData>> batchStateData) {
        if (span.getServiceName() != null) {
            final String serviceName = span.getServiceName();
            final String spanId = span.getSpanId();
            final String traceId = span.getTraceId();
            final String parentSpanId = span.getParentSpanId();
            final String spanKind = span.getKind();
            final String spanName = span.getName();
            final String operation = span.getName();
            final Long durationInNanos = span.getDurationInNanos();
            final String status = extractSpanStatus(span);
            final String endTime = span.getEndTime();
            final Map<String, String> groupByAttrs = extractGroupByAttributes(span);
            final Map<String, Object> spanAttributes = extractSpanAttributes(span);

            try {
                final SpanStateData spanStateData = new SpanStateData(
                        serviceName,
                        spanId,
                        parentSpanId.isEmpty() ? null : parentSpanId,
                        traceId,
                        spanKind,
                        spanName,
                        operation,
                        durationInNanos,
                        status,
                        endTime,
                        groupByAttrs,
                        spanAttributes);

                Collection<SpanStateData> spansForTrace = batchStateData.computeIfAbsent(Hex.decodeHex(traceId),
                        k -> new HashSet<>());
                spansForTrace.add(spanStateData);
            } catch (Exception e) {
                LOG.error("Caught exception trying to put span state data into batch", e);
            }
        }
    }

    /**
     * Extract span status from the span's status field
     *
     * @param span The span to extract status from
     * @return String representation of the span status, or "OK" if not available
     */
    private String extractSpanStatus(final Span span) {
        try {
            final Map<String, Object> status = span.getStatus();
            if (status != null && status.containsKey("code")) {
                final Object code = status.get("code");
                if (code != null) {
                    return code.toString();
                }
            }
        } catch (Exception e) {
            LOG.debug("Error extracting span status: {}", e.getMessage());
        }
        return "OK"; // Default to OK if status is not available or extractable
    }

    /**
     * Extract span attributes including HTTP status codes and resource for error/fault/environment determination
     *
     * @param span The span to extract attributes from
     * @return Map of span attributes with resource information, or empty map if not available
     */
    private Map<String, Object> extractSpanAttributes(final Span span) {
        try {
            final Map<String, Object> combinedAttributes = new HashMap<>();

            final Map<String, Object> attributes = span.getAttributes();
            if (attributes != null) {
                combinedAttributes.putAll(attributes);
            }

            final Map<String, Object> resource = span.getResource();
            if (resource != null) {
                combinedAttributes.put("resource", resource);
            }
            final Map<String, Object> scope = span.getScope();
            if (scope != null ) {
                final Map<String, Object> scopeAttributes = (Map<String, Object>)scope.get("attributes");
                if (attributes != null) {
                    combinedAttributes.putAll(scopeAttributes);
                }
            }

            return combinedAttributes;
        } catch (Exception e) {
            LOG.debug("Error extracting span attributes: {}", e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * This method checks for master instance and let master instance process the current window and rotate the window.
     *
     * @return Set of Record<Event> containing json representation of NodeOperationDetail found
     */
    private Collection<Record<Event>> evaluateApmEvents() {
        LOG.debug("Evaluating APM service map events with three-window semantics");
        try {
            allThreadsCyclicBarrier.await();

            Collection<Record<Event>> apmEvents = new HashSet<>();
            if (isMasterInstance()) {
                apmEvents = processCurrentWindowSpans();
                rotateWindows();
            }

            allThreadsCyclicBarrier.await();

            return apmEvents;
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Processes spans from the current window using three-window semantics (previous, current, next)
     * to generate APM service map events and metrics. The method operates in two phases:
     *
     * Phase 1: Decorates spans with ephemeral client/server relationship information using
     * two-pass decoration (CLIENT spans first, then SERVER spans with back-annotation).
     *
     * Phase 2: Generates NodeOperationDetail events using CLIENT-span-primary algorithm.
     * CLIENT spans are the primary emission source since their decoration contains all needed
     * data (sourceNode, targetNode, sourceOperation from parentServerOperationName, targetOperation
     * from remoteOperation). Leaf SERVER spans (no CLIENT descendants) emit separately.
     * Metrics (latency, throughput, error rates) are generated for all SERVER spans and for
     * CLIENT spans with full operation context.
     */
    private Collection<Record<Event>> processCurrentWindowSpans() {
        final Collection<Record<Event>> apmEvents = new HashSet<>();
        final Instant currentTime = clock.instant();

        final EphemeralSpanDecorations ephemeralDecorations = new EphemeralSpanDecorations();

        final Map<MetricKey, MetricAggregationState> metricsStateByKey = new HashMap<>();

        final Map<String, Collection<SpanStateData>> previousSpansByTraceId = buildSpansByTraceIdMap(previousWindow);
        final Map<String, Collection<SpanStateData>> currentSpansByTraceId = buildSpansByTraceIdMap(currentWindow);
        final Map<String, Collection<SpanStateData>> nextSpansByTraceId = buildSpansByTraceIdMap(nextWindow);

        for (String traceId : currentSpansByTraceId.keySet()) {
            final ThreeWindowTraceDataWithDecorations traceData = buildThreeWindowTraceDataWithDecorations(
                    traceId, previousSpansByTraceId, currentSpansByTraceId, nextSpansByTraceId, ephemeralDecorations);

            if (!traceData.getProcessingSpans().isEmpty()) {
                decorateSpansInTraceWithEphemeralStorage(traceData);

                apmEvents.addAll(generateNodeOperationDetailEvents(traceData, currentTime, metricsStateByKey));
            }
        }

        final List<JacksonMetric> metrics = ApmServiceMapMetricsUtil.createMetricsFromAggregatedState(metricsStateByKey);
        metrics.sort(Comparator.comparing(JacksonMetric::getTime));

        final List<Record<Event>> apmEventsSorted = new ArrayList<>();
        apmEventsSorted.addAll(metrics.stream().map(metric -> new Record<Event>(metric)).collect(Collectors.toList()));
        apmEventsSorted.addAll(apmEvents);

        return apmEventsSorted;
    }


    /**
     * Extract groupByAttributes from a span's resource attributes
     *
     * @param span The span to extract resource attributes from
     * @return Map of configured resource attributes or empty map if none configured/found
     */
    private Map<String, String> extractGroupByAttributes(final Span span) {
        if (groupByAttributes == null || groupByAttributes.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, String> result = new HashMap<>();

        try {
            final Map<String, Object> resource = span.getResource();
            if (resource == null) {
                return Collections.emptyMap();
            }

            final Object attributesObject = resource.get("attributes");
            if (!(attributesObject instanceof Map)) {
                return Collections.emptyMap();
            }

            @SuppressWarnings("unchecked")
            final Map<String, Object> resourceAttributes = (Map<String, Object>) attributesObject;

            for (String attrKey : groupByAttributes) {
                final Object value = resourceAttributes.get(attrKey);
                if (value != null) {
                    result.put(attrKey, value.toString());
                }
            }
        } catch (Exception e) {
            LOG.debug("Error extracting group by attributes from span resource: {}", e.getMessage());
        }

        return result.isEmpty() ? Collections.emptyMap() : result;
    }

    /**
     * Get anchor timestamp from span's endTime, truncated to minute boundary
     *
     * @param spanStateData The span to extract timestamp from
     * @param fallbackTime Current system time to use if span endTime is null
     * @return Instant truncated to the lower 1-minute boundary
     */
    private Instant getAnchorTimestampFromSpan(final SpanStateData spanStateData, final Instant fallbackTime) {
        Instant timestamp = fallbackTime; // Default to current system time

        final String endTime = spanStateData.getEndTime();
        try {
            if (endTime != null && !endTime.isEmpty()) {
                timestamp = Instant.parse(endTime);
            }
        } catch (Exception e) {
            LOG.debug("Failed to parse span endTime '{}', using fallback time: {}",
                     endTime, e.getMessage());
        }

        return timestamp.truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
    }

    /**
     * Rotate windows for processor state using three-window slot-machine semantics
     */
    private void rotateWindows() throws InterruptedException {
        LOG.debug("Rotating APM service map windows at " + clock.instant().toString());

        MapDbProcessorState<Collection<SpanStateData>> tempWindow = previousWindow;
        previousWindow = currentWindow;
        currentWindow = nextWindow;
        nextWindow = tempWindow;
        nextWindow.clear();

        previousTimestamp = clock.instant();
        LOG.debug("Done rotating APM service map windows - All metrics cleared for new window");
    }

    /**
     * @return Next database name
     */
    private String getNewDbName() {
        return "apm-db-" + clock.millis();
    }

    /**
     * @return Boolean indicating whether the window duration has lapsed
     */
    private boolean windowDurationHasPassed() {
        final Duration elapsed = Duration.between(previousTimestamp, clock.instant());
        return elapsed.compareTo(windowDuration) >= 0;
    }

    /**
     * Master instance is needed to do things like window rotation that should only be done once
     *
     * @return Boolean indicating whether this object is the master OTelApmServiceMapProcessor instance
     */
    private boolean isMasterInstance() {
        return thisProcessorId == 0;
    }

    /**
     * Build a map of traceId -> spans from a window
     *
     * @param window The window to extract spans from
     * @return Map of traceId to collection of spans
     */
    private Map<String, Collection<SpanStateData>> buildSpansByTraceIdMap(final MapDbProcessorState<Collection<SpanStateData>> window) {
        final Map<String, Collection<SpanStateData>> spansByTraceId = new HashMap<>();

        if (window != null && window.getAll() != null && window.size() > 0) {
            try {
                window.getIterator(processorsCreated.get(), thisProcessorId).forEachRemaining(entry -> {
                    final String traceId = Hex.encodeHexString(entry.getKey());
                    final Collection<SpanStateData> spans = entry.getValue();
                    if (spans != null && !spans.isEmpty()) {
                        spansByTraceId.put(traceId, spans);
                    }
                });
            } catch (NoSuchElementException e) {
                LOG.debug("Window is empty, skipping iteration: {}", e.getMessage());
            }
        }

        return spansByTraceId;
    }

    /**
     * Build three-window trace data for a specific trace
     *
     * @param traceId The trace ID
     * @param previousSpansByTraceId Previous window spans by trace ID
     * @param currentSpansByTraceId Current window spans by trace ID
     * @param nextSpansByTraceId next window spans by trace ID
     * @return ThreeWindowTraceData containing all necessary data for processing
     */
    private ThreeWindowTraceData buildThreeWindowTraceData(final String traceId,
                                                           final Map<String, Collection<SpanStateData>> previousSpansByTraceId,
                                                           final Map<String, Collection<SpanStateData>> currentSpansByTraceId,
                                                           final Map<String, Collection<SpanStateData>> nextSpansByTraceId) {
        final Collection<SpanStateData> previousSpans = previousSpansByTraceId.getOrDefault(traceId, Collections.emptyList());
        final Collection<SpanStateData> processingSpans = currentSpansByTraceId.getOrDefault(traceId, Collections.emptyList());
        final Collection<SpanStateData> nextSpans = nextSpansByTraceId.getOrDefault(traceId, Collections.emptyList());

        final Collection<SpanStateData> lookupSpans = new HashSet<>();
        lookupSpans.addAll(previousSpans);
        lookupSpans.addAll(processingSpans);
        lookupSpans.addAll(nextSpans);

        final Map<String, SpanStateData> spansBySpanId = new HashMap<>();
        final Map<String, Collection<SpanStateData>> childrenByParentId = new HashMap<>();
        final Set<String> processingSpanIds = new HashSet<>();

        for (SpanStateData spanStateData : lookupSpans) {
            final String spanId = spanStateData.getSpanId();
            spansBySpanId.put(spanId, spanStateData);

            if (spanStateData.getParentSpanId() != null) {
                final String parentSpanId = spanStateData.getParentSpanId();
                childrenByParentId.computeIfAbsent(parentSpanId, k -> new HashSet<>()).add(spanStateData);
            }
        }

        for (SpanStateData spanStateData : processingSpans) {
            processingSpanIds.add(spanStateData.getSpanId());
        }

        return new ThreeWindowTraceData(processingSpans, lookupSpans, spansBySpanId, childrenByParentId, processingSpanIds);
    }

    /**
     * Build three-window trace data with ephemeral decorations for a specific trace
     *
     * @param traceId The trace ID
     * @param previousSpansByTraceId Previous window spans by trace ID
     * @param currentSpansByTraceId Current window spans by trace ID
     * @param nextSpansByTraceId next window spans by trace ID
     * @param decorations Ephemeral decoration storage for this processing cycle
     * @return ThreeWindowTraceDataWithDecorations containing all necessary data for processing
     */
    private ThreeWindowTraceDataWithDecorations buildThreeWindowTraceDataWithDecorations(
            final String traceId,
            final Map<String, Collection<SpanStateData>> previousSpansByTraceId,
            final Map<String, Collection<SpanStateData>> currentSpansByTraceId,
            final Map<String, Collection<SpanStateData>> nextSpansByTraceId,
            final EphemeralSpanDecorations decorations) {

        final ThreeWindowTraceData baseTraceData = buildThreeWindowTraceData(
                traceId, previousSpansByTraceId, currentSpansByTraceId, nextSpansByTraceId);

        return new ThreeWindowTraceDataWithDecorations(
                baseTraceData.getProcessingSpans(),
                baseTraceData.getLookupSpans(),
                baseTraceData.getSpansBySpanId(),
                baseTraceData.getChildrenByParentId(),
                baseTraceData.getProcessingSpanIds(),
                decorations);
    }

    /**
     * PHASE 1: DECORATE SPANS with ephemeral storage - Four-pass decoration
     *
     * Pass 1: Decorate CLIENT spans with remote server information
     * Pass 2: Decorate PRODUCER spans with child CONSUMER span information
     * Pass 3: Decorate SERVER spans, find CLIENT and PRODUCER descendants, back-annotate
     * Pass 4: Decorate CONSUMER spans, find PRODUCER and CLIENT descendants, back-annotate
     *
     * @param traceData Three-window trace data with ephemeral decorations containing spans and indexes
     */
    private void decorateSpansInTraceWithEphemeralStorage(final ThreeWindowTraceDataWithDecorations traceData) {
        decorateClientSpansFirstPassWithEphemeralStorage(traceData);
        decorateProducerSpansSecondPassWithEphemeralStorage(traceData);
        decorateServerSpansThirdPassWithEphemeralStorage(traceData);
        decorateConsumerSpansFourthPassWithEphemeralStorage(traceData);
    }

    /**
     * First pass: decorate CLIENT spans with child SERVER span information using ephemeral storage
     * Traverse ALL CLIENT spans in the trace and find their child SERVER spans (remote servers)
     *
     * @param traceData Three-window trace data with ephemeral decorations containing spans and indexes
     */
    private void decorateClientSpansFirstPassWithEphemeralStorage(final ThreeWindowTraceDataWithDecorations traceData) {
        for (SpanStateData clientSpan : traceData.getLookupSpans()) {
            if (SPAN_KIND_CLIENT.equals(clientSpan.getSpanKind())) {
                final String clientSpanId = clientSpan.getSpanId();
                final Collection<SpanStateData> childServerSpans = traceData.getChildrenByParentId().getOrDefault(clientSpanId, Collections.emptyList())
                        .stream()
                        .filter(span -> SPAN_KIND_SERVER.equals(span.getSpanKind()))
                        .collect(java.util.stream.Collectors.toList());

                String remoteService = "unknown";
                String remoteOperation = "unknown";
                String remoteEnvironment = "generic:default"; // Default environment string
                Map<String, String> remoteGroupByAttributes = Collections.emptyMap();

                if (!childServerSpans.isEmpty()) {
                    final SpanStateData childServerSpan = childServerSpans.iterator().next();
                    remoteService = childServerSpan.getServiceName();
                    remoteOperation = childServerSpan.getOperationName();
                    remoteEnvironment = childServerSpan.getEnvironment();
                    remoteGroupByAttributes = childServerSpan.getGroupByAttributes();
                }

                final ClientSpanDecoration decoration = new ClientSpanDecoration(
                        null,
                        remoteEnvironment,
                        remoteService,
                        remoteOperation,
                        remoteGroupByAttributes
                );
                traceData.getDecorations().setClientDecoration(clientSpanId, decoration);
            }
        }
    }

    /**
     * Second pass: decorate PRODUCER spans with child CONSUMER span information.
     * For each PRODUCER span, find its direct child CONSUMER spans (remote consumers)
     * and extract messaging attributes.
     *
     * @param traceData Three-window trace data with ephemeral decorations containing spans and indexes
     */
    private void decorateProducerSpansSecondPassWithEphemeralStorage(final ThreeWindowTraceDataWithDecorations traceData) {
        for (SpanStateData producerSpan : traceData.getLookupSpans()) {
            if (SPAN_KIND_PRODUCER.equals(producerSpan.getSpanKind())) {
                final String producerSpanId = producerSpan.getSpanId();
                final Collection<SpanStateData> childConsumerSpans = traceData.getChildrenByParentId()
                        .getOrDefault(producerSpanId, Collections.emptyList())
                        .stream()
                        .filter(span -> SPAN_KIND_CONSUMER.equals(span.getSpanKind()))
                        .collect(java.util.stream.Collectors.toList());

                String remoteService = "unknown";
                String remoteOperation = "unknown";
                String remoteEnvironment = "generic:default";
                Map<String, String> remoteGroupByAttributes = Collections.emptyMap();

                if (!childConsumerSpans.isEmpty()) {
                    final SpanStateData childConsumerSpan = childConsumerSpans.iterator().next();
                    remoteService = childConsumerSpan.getServiceName();
                    remoteOperation = childConsumerSpan.getOperationName();
                    remoteEnvironment = childConsumerSpan.getEnvironment();
                    remoteGroupByAttributes = childConsumerSpan.getGroupByAttributes();
                }

                // Messaging attributes from the producer span itself
                final String messagingSystem = producerSpan.getMessagingSystem();
                final String messagingDestination = producerSpan.getMessagingDestination();

                final ProducerSpanDecoration decoration = new ProducerSpanDecoration(
                        null,
                        remoteEnvironment,
                        remoteService,
                        remoteOperation,
                        remoteGroupByAttributes,
                        messagingSystem,
                        messagingDestination
                );
                traceData.getDecorations().setProducerDecoration(producerSpanId, decoration);
            }
        }
    }

    /**
     * Third pass: decorate SERVER spans and back-annotate CLIENT and PRODUCER spans with parent server information
     * Traverse ALL SERVER spans in the trace and find their descendant CLIENT and PRODUCER spans from same service
     *
     * @param traceData Three-window trace data with ephemeral decorations containing spans and indexes
     */
    private void decorateServerSpansThirdPassWithEphemeralStorage(final ThreeWindowTraceDataWithDecorations traceData) {
        for (SpanStateData serverSpan : traceData.getLookupSpans()) {
            if (SPAN_KIND_SERVER.equals(serverSpan.getSpanKind())) {
                final Map<String, Collection<SpanStateData>> descendants =
                        findProducerAndClientDescendantsForEntrySpan(serverSpan, traceData);
                final Collection<SpanStateData> clientDescendants = descendants.get("client");
                final Collection<SpanStateData> producerDescendants = descendants.get("producer");

                final ServerSpanDecoration serverDecoration = new ServerSpanDecoration(clientDescendants);
                traceData.getDecorations().setServerDecoration(serverSpan.getSpanId(), serverDecoration);

                // Back-annotate CLIENT descendants with parentServerOperationName
                for (SpanStateData clientSpan : clientDescendants) {
                    final String clientSpanId = clientSpan.getSpanId();
                    final ClientSpanDecoration existingDecoration = traceData.getDecorations().getClientDecoration(clientSpanId);

                    if (existingDecoration != null) {
                        final ClientSpanDecoration updatedDecoration = new ClientSpanDecoration(
                                serverSpan.getOperationName(),
                                existingDecoration.getRemoteEnvironment(),
                                existingDecoration.getRemoteService(),
                                existingDecoration.getRemoteOperation(),
                                existingDecoration.getRemoteGroupByAttributes()
                        );
                        traceData.getDecorations().setClientDecoration(clientSpanId, updatedDecoration);
                    } else {
                        final ClientSpanDecoration newDecoration = new ClientSpanDecoration(
                                serverSpan.getOperationName(),
                                clientSpan.getEnvironment(),
                                "unknown",
                                "unknown",
                                Collections.emptyMap()
                        );
                        traceData.getDecorations().setClientDecoration(clientSpanId, newDecoration);
                    }
                }

                // Back-annotate PRODUCER descendants with parentEntryOperationName
                for (SpanStateData producerSpan : producerDescendants) {
                    final String producerSpanId = producerSpan.getSpanId();
                    final ProducerSpanDecoration existingDecoration = traceData.getDecorations().getProducerDecoration(producerSpanId);

                    if (existingDecoration != null) {
                        final ProducerSpanDecoration updatedDecoration = new ProducerSpanDecoration(
                                serverSpan.getOperationName(),
                                existingDecoration.getRemoteEnvironment(),
                                existingDecoration.getRemoteService(),
                                existingDecoration.getRemoteOperation(),
                                existingDecoration.getRemoteGroupByAttributes(),
                                existingDecoration.getMessagingSystem(),
                                existingDecoration.getMessagingDestination()
                        );
                        traceData.getDecorations().setProducerDecoration(producerSpanId, updatedDecoration);
                    } else {
                        final ProducerSpanDecoration newDecoration = new ProducerSpanDecoration(
                                serverSpan.getOperationName(),
                                producerSpan.getEnvironment(),
                                "unknown",
                                "unknown",
                                Collections.emptyMap(),
                                producerSpan.getMessagingSystem(),
                                producerSpan.getMessagingDestination()
                        );
                        traceData.getDecorations().setProducerDecoration(producerSpanId, newDecoration);
                    }
                }
            }
        }
    }

    /**
     * PHASE 2: Generate NodeOperationDetail events and metrics from ephemeral decorations.
     *
     * Step 1 (CLIENT spans): Each CLIENT span emits a full NodeOperationDetail.
     * Step 1.5 (PRODUCER spans): Each PRODUCER span emits NodeOperationDetail with messaging info.
     * Step 2 (SERVER spans): Metrics for all; leaf NodeOperationDetail for those with no outgoing descendants.
     * Step 3 (CONSUMER spans): Metrics for all; leaf NodeOperationDetail for those with no PRODUCER/CLIENT descendants.
     *
     * @param traceData Three-window trace data with ephemeral decorations (only processing spans are used)
     * @param currentTime Current timestamp
     * @param metricsStateByKey Shared map for metric aggregation across all traces
     * @return Collection of NodeOperationDetail events
     */
    private Collection<Record<Event>> generateNodeOperationDetailEvents(final ThreeWindowTraceDataWithDecorations traceData,
                                                                        final Instant currentTime,
                                                                        final Map<MetricKey, MetricAggregationState> metricsStateByKey) {
        final Collection<Record<Event>> events = new HashSet<>();

        // Step 1: CLIENT spans — primary emission path
        for (SpanStateData clientSpan : traceData.getProcessingSpans()) {
            if (SPAN_KIND_CLIENT.equals(clientSpan.getSpanKind())) {
                final ClientSpanDecoration decoration = traceData.getDecorations().getClientDecoration(clientSpan.getSpanId());

                if (decoration != null && !"unknown".equals(decoration.getRemoteService())) {
                    final Node sourceNode = new Node(
                            NODE_TYPE_SERVICE,
                            new Node.KeyAttributes(clientSpan.getEnvironment(), clientSpan.getServiceName()),
                            clientSpan.getGroupByAttributes()
                    );

                    final Node targetNode = new Node(
                            NODE_TYPE_SERVICE,
                            new Node.KeyAttributes(decoration.getRemoteEnvironment(), decoration.getRemoteService()),
                            decoration.getRemoteGroupByAttributes()
                    );

                    final Operation sourceOp = decoration.getParentServerOperationName() != null
                            ? new Operation(decoration.getParentServerOperationName())
                            : null;
                    final Operation targetOp = new Operation(decoration.getRemoteOperation());

                    final Instant anchorTimestamp = getAnchorTimestampFromSpan(clientSpan, currentTime);

                    final NodeOperationDetail nodeOperationDetail = new NodeOperationDetail(
                            sourceNode, targetNode, sourceOp, targetOp, anchorTimestamp);

                    final EventMetadata eventMetadata = new DefaultEventMetadata.Builder()
                            .withEventType(EVENT_TYPE_OTEL_APM_SERVICE_MAP).build();

                    final Event event = eventFactory.eventBuilder(EventBuilder.class)
                            .withEventMetadata(eventMetadata)
                            .withData(nodeOperationDetail)
                            .build();

                    events.add(new Record<>(event));

                    if (decoration.getParentServerOperationName() != null) {
                        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                                clientSpan, decoration, currentTime, metricsStateByKey, anchorTimestamp);
                    }
                }
            }
        }

        // Step 1.5: PRODUCER spans — emit NodeOperationDetail with messaging info and generate metrics
        for (SpanStateData producerSpan : traceData.getProcessingSpans()) {
            if (SPAN_KIND_PRODUCER.equals(producerSpan.getSpanKind())) {
                final ProducerSpanDecoration decoration = traceData.getDecorations().getProducerDecoration(producerSpan.getSpanId());

                if (decoration != null && !"unknown".equals(decoration.getRemoteService())) {
                    final Node sourceNode = new Node(
                            NODE_TYPE_SERVICE,
                            new Node.KeyAttributes(producerSpan.getEnvironment(), producerSpan.getServiceName()),
                            producerSpan.getGroupByAttributes()
                    );

                    final Node targetNode = new Node(
                            NODE_TYPE_SERVICE,
                            new Node.KeyAttributes(decoration.getRemoteEnvironment(), decoration.getRemoteService()),
                            decoration.getRemoteGroupByAttributes()
                    );

                    final Operation sourceOp = decoration.getParentEntryOperationName() != null
                            ? new Operation(decoration.getParentEntryOperationName())
                            : null;
                    final Operation targetOp = new Operation(decoration.getRemoteOperation());

                    final Instant anchorTimestamp = getAnchorTimestampFromSpan(producerSpan, currentTime);

                    final NodeOperationDetail nodeOperationDetail = new NodeOperationDetail(
                            sourceNode, targetNode, sourceOp, targetOp, anchorTimestamp,
                            decoration.getMessagingSystem(), decoration.getMessagingDestination());

                    final EventMetadata eventMetadata = new DefaultEventMetadata.Builder()
                            .withEventType(EVENT_TYPE_OTEL_APM_SERVICE_MAP).build();

                    final Event event = eventFactory.eventBuilder(EventBuilder.class)
                            .withEventMetadata(eventMetadata)
                            .withData(nodeOperationDetail)
                            .build();

                    events.add(new Record<>(event));

                    if (decoration.getParentEntryOperationName() != null) {
                        ApmServiceMapMetricsUtil.generateMetricsForProducerSpan(
                                producerSpan, decoration, currentTime, metricsStateByKey, anchorTimestamp);
                    }
                }
            }
        }

        // Step 2: SERVER spans — metrics for all, leaf NodeOperationDetail for those with no CLIENT descendants
        for (SpanStateData serverSpan : traceData.getProcessingSpans()) {
            if (SPAN_KIND_SERVER.equals(serverSpan.getSpanKind())) {
                final Instant anchorTimestamp = getAnchorTimestampFromSpan(serverSpan, currentTime);
                ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                        serverSpan, currentTime, metricsStateByKey, anchorTimestamp);

                final ServerSpanDecoration decoration = traceData.getDecorations().getServerDecoration(serverSpan.getSpanId());

                if (decoration == null || decoration.getClientDescendants().isEmpty()) {
                    final Node sourceNode = new Node(
                            NODE_TYPE_SERVICE,
                            new Node.KeyAttributes(serverSpan.getEnvironment(), serverSpan.getServiceName()),
                            serverSpan.getGroupByAttributes()
                    );

                    final Operation sourceOp = new Operation(serverSpan.getOperationName());

                    final NodeOperationDetail nodeOperationDetail = new NodeOperationDetail(
                            sourceNode, null, sourceOp, null, anchorTimestamp);

                    final EventMetadata eventMetadata = new DefaultEventMetadata.Builder()
                            .withEventType(EVENT_TYPE_OTEL_APM_SERVICE_MAP).build();

                    final Event event = eventFactory.eventBuilder(EventBuilder.class)
                            .withEventMetadata(eventMetadata)
                            .withData(nodeOperationDetail)
                            .build();

                    events.add(new Record<>(event));
                }
            }
        }

        // Step 3: CONSUMER spans — metrics for all, leaf NodeOperationDetail for those with no PRODUCER/CLIENT descendants
        for (SpanStateData consumerSpan : traceData.getProcessingSpans()) {
            if (SPAN_KIND_CONSUMER.equals(consumerSpan.getSpanKind())) {
                final Instant anchorTimestamp = getAnchorTimestampFromSpan(consumerSpan, currentTime);
                ApmServiceMapMetricsUtil.generateMetricsForConsumerSpan(
                        consumerSpan, currentTime, metricsStateByKey, anchorTimestamp);

                final ConsumerSpanDecoration decoration = traceData.getDecorations().getConsumerDecoration(consumerSpan.getSpanId());

                if (decoration == null || (decoration.getProducerDescendants().isEmpty() && decoration.getClientDescendants().isEmpty())) {
                    final Node sourceNode = new Node(
                            NODE_TYPE_SERVICE,
                            new Node.KeyAttributes(consumerSpan.getEnvironment(), consumerSpan.getServiceName()),
                            consumerSpan.getGroupByAttributes()
                    );

                    final Operation sourceOp = new Operation(consumerSpan.getOperationName());

                    final NodeOperationDetail nodeOperationDetail = new NodeOperationDetail(
                            sourceNode, null, sourceOp, null, anchorTimestamp);

                    final EventMetadata eventMetadata = new DefaultEventMetadata.Builder()
                            .withEventType(EVENT_TYPE_OTEL_APM_SERVICE_MAP).build();

                    final Event event = eventFactory.eventBuilder(EventBuilder.class)
                            .withEventMetadata(eventMetadata)
                            .withData(nodeOperationDetail)
                            .build();

                    events.add(new Record<>(event));
                }
            }
        }

        return events;
    }

    /**
     * Fourth pass: decorate CONSUMER spans with PRODUCER and CLIENT descendants.
     * CONSUMER spans are entry points (like SERVER spans) for async flows.
     * Find PRODUCER and CLIENT descendants via BFS within the same service,
     * and back-annotate them with the CONSUMER's operation as parentEntryOperationName.
     *
     * @param traceData Three-window trace data with ephemeral decorations containing spans and indexes
     */
    private void decorateConsumerSpansFourthPassWithEphemeralStorage(final ThreeWindowTraceDataWithDecorations traceData) {
        for (SpanStateData consumerSpan : traceData.getLookupSpans()) {
            if (SPAN_KIND_CONSUMER.equals(consumerSpan.getSpanKind())) {
                final Map<String, Collection<SpanStateData>> descendants =
                        findProducerAndClientDescendantsForEntrySpan(consumerSpan, traceData);
                final Collection<SpanStateData> producerDescendants = descendants.get("producer");
                final Collection<SpanStateData> clientDescendants = descendants.get("client");

                final ConsumerSpanDecoration consumerDecoration =
                        new ConsumerSpanDecoration(producerDescendants, clientDescendants);
                traceData.getDecorations().setConsumerDecoration(consumerSpan.getSpanId(), consumerDecoration);

                // Back-annotate PRODUCER descendants with parentEntryOperationName from CONSUMER
                for (SpanStateData producerSpan : producerDescendants) {
                    final String producerSpanId = producerSpan.getSpanId();
                    final ProducerSpanDecoration existingDecoration =
                            traceData.getDecorations().getProducerDecoration(producerSpanId);

                    if (existingDecoration != null && existingDecoration.getParentEntryOperationName() == null) {
                        final ProducerSpanDecoration updatedDecoration = new ProducerSpanDecoration(
                                consumerSpan.getOperationName(),
                                existingDecoration.getRemoteEnvironment(),
                                existingDecoration.getRemoteService(),
                                existingDecoration.getRemoteOperation(),
                                existingDecoration.getRemoteGroupByAttributes(),
                                existingDecoration.getMessagingSystem(),
                                existingDecoration.getMessagingDestination()
                        );
                        traceData.getDecorations().setProducerDecoration(producerSpanId, updatedDecoration);
                    }
                }

                // Back-annotate CLIENT descendants with parentEntryOperationName from CONSUMER
                for (SpanStateData clientSpan : clientDescendants) {
                    final String clientSpanId = clientSpan.getSpanId();
                    final ClientSpanDecoration existingDecoration =
                            traceData.getDecorations().getClientDecoration(clientSpanId);

                    if (existingDecoration != null && existingDecoration.getParentServerOperationName() == null) {
                        final ClientSpanDecoration updatedDecoration = new ClientSpanDecoration(
                                consumerSpan.getOperationName(),
                                existingDecoration.getRemoteEnvironment(),
                                existingDecoration.getRemoteService(),
                                existingDecoration.getRemoteOperation(),
                                existingDecoration.getRemoteGroupByAttributes()
                        );
                        traceData.getDecorations().setClientDecoration(clientSpanId, updatedDecoration);
                    }
                }
            }
        }
    }

    /**
     * Find PRODUCER and CLIENT descendant spans from the same service as the entry span using BFS.
     * Generalizes the previous findClientDescendantsForServerThreeWindow to collect both types.
     * Stops traversing when service name changes.
     *
     * @param entrySpan The entry span (SERVER or CONSUMER)
     * @param traceData Three-window trace data
     * @return Map with "producer" and "client" keys mapping to their respective descendant collections
     */
    private Map<String, Collection<SpanStateData>> findProducerAndClientDescendantsForEntrySpan(
            final SpanStateData entrySpan, final ThreeWindowTraceData traceData) {
        final Collection<SpanStateData> clientDescendants = new HashSet<>();
        final Collection<SpanStateData> producerDescendants = new HashSet<>();
        final String entrySpanId = entrySpan.getSpanId();

        final Set<String> visited = new HashSet<>();
        final java.util.Queue<String> queue = new java.util.LinkedList<>();
        queue.offer(entrySpanId);
        visited.add(entrySpanId);

        while (!queue.isEmpty()) {
            final String currentSpanId = queue.poll();
            final Collection<SpanStateData> children = traceData.getChildrenByParentId()
                    .getOrDefault(currentSpanId, Collections.emptyList());

            for (SpanStateData child : children) {
                final String childSpanId = child.getSpanId();

                if (!visited.contains(childSpanId)) {
                    visited.add(childSpanId);

                    if (entrySpan.getServiceName().equals(child.getServiceName())) {
                        if (SPAN_KIND_CLIENT.equals(child.getSpanKind())) {
                            clientDescendants.add(child);
                        } else if (SPAN_KIND_PRODUCER.equals(child.getSpanKind())) {
                            producerDescendants.add(child);
                        }

                        queue.offer(childSpanId);
                    }
                }
            }
        }

        final Map<String, Collection<SpanStateData>> result = new HashMap<>();
        result.put("client", clientDescendants);
        result.put("producer", producerDescendants);
        return result;
    }
}
