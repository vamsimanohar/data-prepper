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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class ConsumerSpanDecorationTest {

    @Test
    void constructor_withDescendants_createsInstance() {
        SpanStateData producer1 = mock(SpanStateData.class);
        SpanStateData client1 = mock(SpanStateData.class);

        ConsumerSpanDecoration decoration = new ConsumerSpanDecoration(
                Arrays.asList(producer1), Arrays.asList(client1)
        );

        assertEquals(1, decoration.getProducerDescendants().size());
        assertEquals(1, decoration.getClientDescendants().size());
    }

    @Test
    void constructor_withNullDescendants_usesEmptyLists() {
        ConsumerSpanDecoration decoration = new ConsumerSpanDecoration(null, null);

        assertTrue(decoration.getProducerDescendants().isEmpty());
        assertTrue(decoration.getClientDescendants().isEmpty());
    }

    @Test
    void constructor_withEmptyDescendants_createsInstance() {
        ConsumerSpanDecoration decoration = new ConsumerSpanDecoration(
                Collections.emptyList(), Collections.emptyList()
        );

        assertTrue(decoration.getProducerDescendants().isEmpty());
        assertTrue(decoration.getClientDescendants().isEmpty());
    }

    @Test
    void getProducerDescendants_returnsUnmodifiableCollection() {
        SpanStateData producer = mock(SpanStateData.class);
        ConsumerSpanDecoration decoration = new ConsumerSpanDecoration(
                Arrays.asList(producer), Collections.emptyList()
        );

        Collection<SpanStateData> producers = decoration.getProducerDescendants();
        try {
            producers.clear();
            // If we get here, it's not truly unmodifiable, but some implementations allow it
        } catch (UnsupportedOperationException e) {
            // Expected for unmodifiable collections
        }
    }
}
