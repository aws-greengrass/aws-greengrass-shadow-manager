/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.shadowmanager.util.JsonMerger;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;

import java.util.Iterator;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_DELTA;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_DESIRED;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_REPORTED;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.isNullOrMissing;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.nullIfEmpty;

/**
 * Class for managing operations on the Shadow Document State.
 */
@Getter
public class ShadowState {
    @JsonProperty(SHADOW_DOCUMENT_STATE_DESIRED)
    private JsonNode desired;

    @JsonProperty(SHADOW_DOCUMENT_STATE_REPORTED)
    private JsonNode reported;

    public ShadowState() {
        this(null, null);
    }

    public ShadowState(final JsonNode desired, final JsonNode reported) {
        this.desired = nullIfEmpty(desired);
        this.reported = nullIfEmpty(reported);
    }

    /**
     * Creates a new instance of the shadow state by deep copying the desired and reported nodes.
     *
     * @return the new instance of the shadow state.
     */
    public ShadowState deepCopy() {
        return new ShadowState(
                isNullOrMissing(this.desired) ? this.desired : this.desired.deepCopy(),
                isNullOrMissing(this.reported) ? this.reported : this.reported.deepCopy());
    }

    /**
     * Updates the shadow state's reported and desired JSON nodes from the update request's state node.
     *
     * @param updatedStateNode The state node in the update shadow request.
     */
    @SuppressWarnings({"PMD.ForLoopCanBeForeach", "PMD.NullAssignment"})
    public void update(JsonNode updatedStateNode) {
        for (final Iterator<String> i = updatedStateNode.fieldNames(); i.hasNext(); ) {
            final String field = i.next();
            final JsonNode value = updatedStateNode.get(field);

            if (SHADOW_DOCUMENT_STATE_DESIRED.equals(field)) {
                if (isNullOrMissing(value)) {
                    this.desired = null;
                } else if (this.desired == null) {
                    this.desired = nullIfEmpty(value);
                } else {
                    JsonMerger.merge(this.desired, value);
                    this.desired = nullIfEmpty(this.desired);
                }
                continue;
            }

            if (SHADOW_DOCUMENT_STATE_REPORTED.equals(field)) {
                if (isNullOrMissing(value)) {
                    this.reported = null;
                } else if (this.reported == null) {
                    this.reported = nullIfEmpty(value);
                } else {
                    JsonMerger.merge(this.reported, value);
                    this.reported = nullIfEmpty(reported);
                }
            }
        }
    }

    /**
     * Converts the class to its JSON representation.
     *
     * @return a JSON node containing the shadow state.
     */
    public JsonNode toJson() {
        final ObjectNode result = JsonUtil.createObjectNode();
        if (this.desired != null) {
            result.set(SHADOW_DOCUMENT_STATE_DESIRED, this.desired);
        }
        if (this.reported != null) {
            result.set(SHADOW_DOCUMENT_STATE_REPORTED, this.reported);
        }
        return result;
    }

    /**
     * Converts the class to its JSON representation along with the delta node if applicable.
     *
     * @return a JSON node containing the shadow state.
     */
    public JsonNode toJsonWithDelta() {
        final JsonNode state = toJson();
        final JsonNode delta = getDelta();
        if (!isNullOrMissing(delta)) {
            ((ObjectNode) state).set(SHADOW_DOCUMENT_STATE_DELTA, delta);
        }
        return state;
    }

    /**
     * Calculates the delta node based on the current version of the shadow document's desired and reported state.
     *
     * @return an optional value of the delta node.
     */
    public JsonNode getDelta() {
        if (desired == null) {
            return null;
        }

        if (reported == null) {
            return desired;
        }
        return JsonUtil.calculateDelta(reported, desired);
    }
}
