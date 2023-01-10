/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.testing.api.model.TestingModel;
import com.aws.greengrass.testing.resources.AWSResource;
import org.immutables.value.Value;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;
import software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;

@TestingModel
@Value.Immutable
interface IoTShadowModel extends AWSResource<IotDataPlaneClient> {
    String thingName();

    String shadowName();

    SdkBytes payload();

    @Override
    default void remove(IotDataPlaneClient client) {
        try {
            client.deleteThingShadow(DeleteThingShadowRequest.builder().thingName(thingName()).shadowName(shadowName())
                    .build());
        } catch (ResourceNotFoundException ignored) {

        }
    }
}
