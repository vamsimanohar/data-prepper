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
import java.util.Collections;
import java.util.Map;

/**
 * Decoration for PRODUCER spans containing pre-computed relationship data.
 * Mirrors ClientSpanDecoration with additional messaging-specific fields.
 */
@Getter
public class ProducerSpanDecoration implements Serializable {
    private final String parentEntryOperationName;
    private final String remoteEnvironment;
    private final String remoteService;
    private final String remoteOperation;
    private final Map<String, String> remoteGroupByAttributes;
    private final String messagingSystem;
    private final String messagingDestination;

    public ProducerSpanDecoration(final String parentEntryOperationName,
                                  final String remoteEnvironment,
                                  final String remoteService,
                                  final String remoteOperation,
                                  final Map<String, String> remoteGroupByAttributes,
                                  final String messagingSystem,
                                  final String messagingDestination) {
        this.parentEntryOperationName = parentEntryOperationName;
        this.remoteEnvironment = remoteEnvironment;
        this.remoteService = remoteService;
        this.remoteOperation = remoteOperation;
        this.remoteGroupByAttributes = remoteGroupByAttributes != null ? remoteGroupByAttributes : Collections.emptyMap();
        this.messagingSystem = messagingSystem;
        this.messagingDestination = messagingDestination;
    }
}
