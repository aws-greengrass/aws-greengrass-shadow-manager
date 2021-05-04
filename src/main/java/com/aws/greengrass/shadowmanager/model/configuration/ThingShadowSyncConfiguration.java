/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model.configuration;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Builder(toBuilder = true)
@Getter
public class ThingShadowSyncConfiguration {
    @Setter
    private String thingName;
    private final String shadowName;

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
                && Objects.equals(this.shadowName, newConfiguration.shadowName);
    }

    @SuppressWarnings("PMD.UselessOverridingMethod")
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s-%s", thingName, shadowName);
    }
}
