/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_DESIRED;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_REPORTED;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.isNullOrMissing;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.nullIfEmpty;

@Getter
public class ShadowStateMetadata {
    @JsonProperty(SHADOW_DOCUMENT_STATE_DESIRED)
    private final JsonNode desired;

    @JsonProperty(SHADOW_DOCUMENT_STATE_REPORTED)
    private final JsonNode reported;

    public ShadowStateMetadata() {
        this(null, null);
    }

    public ShadowStateMetadata(final JsonNode desired, final JsonNode reported) {
        this.desired = nullIfEmpty(desired);
        this.reported = nullIfEmpty(reported);
    }

    /**
     * Creates a new instance of the shadow state by deep copying the desired and reported nodes.
     *
     * @return the new instance of the shadow state.
     */
    public ShadowStateMetadata deepCopy() {
        return new ShadowStateMetadata(
                isNullOrMissing(this.desired) ? this.desired : this.desired.deepCopy(),
                isNullOrMissing(this.reported) ? this.reported : this.reported.deepCopy());
    }

    public void update(JsonNode updateDocumentRequest) {

    }


    public JsonNode toJson() {
        return null;
    }
}
