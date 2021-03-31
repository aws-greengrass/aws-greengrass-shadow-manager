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

@Getter
public class ShadowStateMetadata {
    @JsonProperty(SHADOW_DOCUMENT_STATE_DESIRED)
    private JsonNode desired;

    @JsonProperty(SHADOW_DOCUMENT_STATE_REPORTED)
    private JsonNode reported;
}
