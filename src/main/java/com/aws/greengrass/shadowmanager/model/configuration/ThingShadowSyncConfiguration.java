/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_CLASSIC_SHADOW_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_NAMED_SHADOWS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_THING_NAME_TOPIC;

@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class ThingShadowSyncConfiguration {
    @Setter
    @JsonProperty(value = CONFIGURATION_THING_NAME_TOPIC, required = true)
    private String thingName;
    @JsonProperty(value = CONFIGURATION_CLASSIC_SHADOW_TOPIC, defaultValue = "true")
    @Builder.Default
    private boolean syncClassicShadow = true;
    @JsonProperty(CONFIGURATION_NAMED_SHADOWS_TOPIC)
    private List<String> syncNamedShadows;
    @Builder.Default
    private boolean isNucleusThing = false;
}
