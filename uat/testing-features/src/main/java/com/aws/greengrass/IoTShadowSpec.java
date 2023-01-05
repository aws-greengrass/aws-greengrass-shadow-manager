/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Builder
@Data
@EqualsAndHashCode(callSuper = false)
public class IoTShadowSpec
 {
    String thingName;
    String shadowName;
    @Builder.Default
    byte[] initialPayload = "{\"state\":{\"reported\":\"SomeKey\":\"SomeValue\"}}}".getBytes();

    IoTShadow resultingIotShadow;
}
