/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.shadowmanager.JsonUtil;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;

import static com.aws.greengrass.shadowmanager.JsonUtil.nullIfEmpty;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_DELTA;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_DESIRED;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_REPORTED;

@Getter
public class ShadowState {
    @JsonProperty(SHADOW_DOCUMENT_STATE_DESIRED)
    private final JsonNode desired;

    @JsonProperty(SHADOW_DOCUMENT_STATE_REPORTED)
    private final JsonNode reported;

    public ShadowState(@JsonProperty(SHADOW_DOCUMENT_STATE_DESIRED) final JsonNode desired,
                       @JsonProperty(SHADOW_DOCUMENT_STATE_REPORTED) final JsonNode reported) {
        this.desired = nullIfEmpty(desired);
        this.reported = nullIfEmpty(reported);
    }

    public JsonNode getDelta() {
        if (desired == null) {
            return null;
        }

        if (reported == null) {
            return desired;
        }
        return JsonUtil.calculateDelta(reported, desired);
    }

    /**
     * Converts this state object to json adding metadata if there is any
     */
    public JsonNode toJsonWithDelta() {
        final JsonNode state = JsonUtil.OBJECT_MAPPER.valueToTree(this);
        final JsonNode delta = getDelta();
        if (!JsonUtil.isNullOrMissing(delta)) {
            ((ObjectNode) state).set(SHADOW_DOCUMENT_STATE_DELTA, delta);
        }
        return state;
    }
}
