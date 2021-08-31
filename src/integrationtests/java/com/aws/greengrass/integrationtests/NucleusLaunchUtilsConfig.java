/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.dependency.State;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

@Builder
@Data
@Getter
public class NucleusLaunchUtilsConfig {
    String configFile;
    boolean mockCloud;
    boolean mockDao;
    boolean mockDatabase;
    @Builder.Default
    State expectedState = State.RUNNING;
    @Builder.Default
    boolean mqttConnected = true;
}
