/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model.configuration;

import lombok.Builder;
import lombok.Getter;

import java.util.Objects;

@Getter
@Builder
public class ThingShadow {
    private final String thingName;
    private final String shadowName;

    @Override
    public boolean equals(Object o) {
        // If the object is compared with itself then return true
        if (o == this) {
            return true;
        }
        // Check if o is an instance of ThingShadow or not "null instanceof [type]" also returns false
        if (!(o instanceof ThingShadow)) {
            return false;
        }

        // typecast o to ThingShadow so that we can compare data members
        ThingShadow newThingShadow = (ThingShadow) o;

        // Compare the data members and return accordingly
        return Objects.equals(this.thingName, newThingShadow.thingName)
            && Objects.equals(this.shadowName, newThingShadow.shadowName);
    }

    @SuppressWarnings("PMD.UselessOverridingMethod")
    @Override
    public int hashCode() {
        return Objects.hashCode(toString());
    }

    @Override
    public String toString() {
        return String.format("%s-%s", thingName, shadowName);
    }
}
