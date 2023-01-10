/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.testing.resources.AWSResourceLifecycle;
import com.aws.greengrass.testing.resources.AbstractAWSResourceLifecycle;
import com.google.auto.service.AutoService;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;

import javax.inject.Inject;

@AutoService(AWSResourceLifecycle.class)
public class IoTShadowLifecycle extends AbstractAWSResourceLifecycle<IotDataPlaneClient> {
    @Inject
    public IoTShadowLifecycle(IotDataPlaneClient client) {
        super(client, IoTShadowSpec.class);
    }
}
