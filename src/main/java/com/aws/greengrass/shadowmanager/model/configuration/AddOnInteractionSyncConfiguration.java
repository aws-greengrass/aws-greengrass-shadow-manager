/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model.configuration;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
public class AddOnInteractionSyncConfiguration {
    @Setter
    private Boolean enabled;

    @Override
    public String toString() {
        return String.format("enabled=%b", enabled);
    }
}
