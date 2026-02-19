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

import lombok.Getter;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

/**
 * Decoration for CONSUMER spans containing pre-computed relationship data.
 * CONSUMER spans are entry points (like SERVER spans) and track their
 * PRODUCER and CLIENT descendants within the same service.
 */
@Getter
public class ConsumerSpanDecoration implements Serializable {
    private final Collection<SpanStateData> producerDescendants;
    private final Collection<SpanStateData> clientDescendants;

    public ConsumerSpanDecoration(final Collection<SpanStateData> producerDescendants,
                                  final Collection<SpanStateData> clientDescendants) {
        this.producerDescendants = producerDescendants != null
                ? Collections.unmodifiableCollection(producerDescendants) : Collections.emptyList();
        this.clientDescendants = clientDescendants != null
                ? Collections.unmodifiableCollection(clientDescendants) : Collections.emptyList();
    }
}
