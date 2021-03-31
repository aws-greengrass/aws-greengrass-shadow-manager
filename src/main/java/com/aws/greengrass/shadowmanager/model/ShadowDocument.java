/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.shadowmanager.JsonUtil;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;

import java.io.IOException;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_METADATA;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;

@Getter
public class ShadowDocument {
    @JsonProperty(value = SHADOW_DOCUMENT_STATE, required = true)
    private final ShadowState state;

    @JsonProperty(SHADOW_DOCUMENT_METADATA)
    private final ShadowStateMetadata metadata;

    @JsonProperty(SHADOW_DOCUMENT_VERSION)
    private final long version;

    /**
     * Constructor needed to
     */
    public ShadowDocument() {
        this(null, null, null);
    }

    public ShadowDocument(byte[] documentBytes) throws IOException {
        this(JsonUtil.OBJECT_MAPPER.convertValue(JsonUtil.getPayloadJson(documentBytes).get(), ShadowDocument.class));
    }

    private ShadowDocument(ShadowDocument document) {
        this(document.getState(), document.getMetadata(), document.getVersion());
    }

    public ShadowDocument(final ShadowState state, final ShadowStateMetadata metadata, Long version) {
        this.state = state;
        this.metadata = metadata;
        this.version = version == null ? -1 : version;
    }

    public boolean isNewDocument() {
        return this.version == -1;
    }
}
