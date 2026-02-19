/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal;

import java.util.HashMap;
import java.util.Map;

/**
 * Ephemeral decoration storage that exists only during processing cycles.
 * Never persisted - created fresh for each processCurrentWindowSpans() call.
 * Decorations are stored in memory-only data structures and automatically
 * garbage collected when processing completes.
 */
public class EphemeralSpanDecorations {
    private final Map<String, ClientSpanDecoration> clientDecorations = new HashMap<>();
    private final Map<String, ServerSpanDecoration> serverDecorations = new HashMap<>();
    private final Map<String, ProducerSpanDecoration> producerDecorations = new HashMap<>();
    private final Map<String, ConsumerSpanDecoration> consumerDecorations = new HashMap<>();

    /**
     * Set CLIENT span decoration
     *
     * @param spanIdHex The span ID in hex format
     * @param decoration The client decoration to store
     */
    public void setClientDecoration(final String spanIdHex, final ClientSpanDecoration decoration) {
        clientDecorations.put(spanIdHex, decoration);
    }

    /**
     * Get CLIENT span decoration
     *
     * @param spanIdHex The span ID in hex format
     * @return Client decoration or null if not found
     */
    public ClientSpanDecoration getClientDecoration(final String spanIdHex) {
        return clientDecorations.get(spanIdHex);
    }

    /**
     * Set SERVER span decoration
     *
     * @param spanIdHex The span ID in hex format
     * @param decoration The server decoration to store
     */
    public void setServerDecoration(final String spanIdHex, final ServerSpanDecoration decoration) {
        serverDecorations.put(spanIdHex, decoration);
    }

    /**
     * Get SERVER span decoration
     *
     * @param spanIdHex The span ID in hex format
     * @return Server decoration or null if not found
     */
    public ServerSpanDecoration getServerDecoration(final String spanIdHex) {
        return serverDecorations.get(spanIdHex);
    }

    /**
     * Check if CLIENT decoration exists for span
     *
     * @param spanIdHex The span ID in hex format
     * @return true if CLIENT decoration exists
     */
    public boolean hasClientDecoration(final String spanIdHex) {
        return clientDecorations.containsKey(spanIdHex);
    }

    /**
     * Check if SERVER decoration exists for span
     *
     * @param spanIdHex The span ID in hex format
     * @return true if SERVER decoration exists
     */
    public boolean hasServerDecoration(final String spanIdHex) {
        return serverDecorations.containsKey(spanIdHex);
    }

    /**
     * Set PRODUCER span decoration
     *
     * @param spanIdHex The span ID in hex format
     * @param decoration The producer decoration to store
     */
    public void setProducerDecoration(final String spanIdHex, final ProducerSpanDecoration decoration) {
        producerDecorations.put(spanIdHex, decoration);
    }

    /**
     * Get PRODUCER span decoration
     *
     * @param spanIdHex The span ID in hex format
     * @return Producer decoration or null if not found
     */
    public ProducerSpanDecoration getProducerDecoration(final String spanIdHex) {
        return producerDecorations.get(spanIdHex);
    }

    /**
     * Check if PRODUCER decoration exists for span
     *
     * @param spanIdHex The span ID in hex format
     * @return true if PRODUCER decoration exists
     */
    public boolean hasProducerDecoration(final String spanIdHex) {
        return producerDecorations.containsKey(spanIdHex);
    }

    /**
     * Set CONSUMER span decoration
     *
     * @param spanIdHex The span ID in hex format
     * @param decoration The consumer decoration to store
     */
    public void setConsumerDecoration(final String spanIdHex, final ConsumerSpanDecoration decoration) {
        consumerDecorations.put(spanIdHex, decoration);
    }

    /**
     * Get CONSUMER span decoration
     *
     * @param spanIdHex The span ID in hex format
     * @return Consumer decoration or null if not found
     */
    public ConsumerSpanDecoration getConsumerDecoration(final String spanIdHex) {
        return consumerDecorations.get(spanIdHex);
    }

    /**
     * Check if CONSUMER decoration exists for span
     *
     * @param spanIdHex The span ID in hex format
     * @return true if CONSUMER decoration exists
     */
    public boolean hasConsumerDecoration(final String spanIdHex) {
        return consumerDecorations.containsKey(spanIdHex);
    }

    /**
     * Clear all decorations from memory
     */
    public void clear() {
        clientDecorations.clear();
        serverDecorations.clear();
        producerDecorations.clear();
        consumerDecorations.clear();
    }

    /**
     * Get total number of decorations stored
     *
     * @return Total count of all decorations
     */
    public int size() {
        return clientDecorations.size() + serverDecorations.size()
                + producerDecorations.size() + consumerDecorations.size();
    }
}
