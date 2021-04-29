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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
    @Builder.Default
    private List<String> syncNamedShadows = Collections.emptyList();
    @Builder.Default
    private boolean isNucleusThing = false;

    @Override
    public boolean equals(Object o) {
        // If the object is compared with itself then return true
        if (o == this) {
            return true;
        }
        /* Check if o is an instance of LoggerConfiguration or not "null instanceof [type]" also returns false */
        if (!(o instanceof ThingShadowSyncConfiguration)) {
            return false;
        }

        // typecast o to LoggerConfiguration so that we can compare data members
        ThingShadowSyncConfiguration newConfiguration = (ThingShadowSyncConfiguration) o;

        // Compare the data members and return accordingly
        return Objects.equals(this.thingName, newConfiguration.thingName)
                && Objects.equals(this.syncClassicShadow, newConfiguration.syncClassicShadow)
                && Objects.equals(this.isNucleusThing, newConfiguration.isNucleusThing)
                && Objects.equals(this.syncNamedShadows, newConfiguration.syncNamedShadows);
    }

    @SuppressWarnings("PMD.UselessOverridingMethod")
    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
