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

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerSpanDecorationTest {

    @Test
    void constructor_withAllFields_createsInstance() {
        Map<String, String> groupByAttrs = Map.of("key", "value");
        ProducerSpanDecoration decoration = new ProducerSpanDecoration(
                "parentOp", "remote-env", "remote-service", "remote-op",
                groupByAttrs, "kafka", "orders-topic"
        );

        assertEquals("parentOp", decoration.getParentEntryOperationName());
        assertEquals("remote-env", decoration.getRemoteEnvironment());
        assertEquals("remote-service", decoration.getRemoteService());
        assertEquals("remote-op", decoration.getRemoteOperation());
        assertEquals(groupByAttrs, decoration.getRemoteGroupByAttributes());
        assertEquals("kafka", decoration.getMessagingSystem());
        assertEquals("orders-topic", decoration.getMessagingDestination());
    }

    @Test
    void constructor_withNullParentOp_createsInstance() {
        ProducerSpanDecoration decoration = new ProducerSpanDecoration(
                null, "env", "service", "op",
                Collections.emptyMap(), "rabbitmq", "events-queue"
        );

        assertNull(decoration.getParentEntryOperationName());
        assertEquals("rabbitmq", decoration.getMessagingSystem());
    }

    @Test
    void constructor_withNullGroupByAttributes_usesEmptyMap() {
        ProducerSpanDecoration decoration = new ProducerSpanDecoration(
                "parentOp", "env", "service", "op",
                null, "kafka", "topic"
        );

        assertTrue(decoration.getRemoteGroupByAttributes().isEmpty());
    }

    @Test
    void constructor_withNullMessagingFields_createsInstance() {
        ProducerSpanDecoration decoration = new ProducerSpanDecoration(
                "parentOp", "env", "service", "op",
                Collections.emptyMap(), null, null
        );

        assertNull(decoration.getMessagingSystem());
        assertNull(decoration.getMessagingDestination());
    }
}
