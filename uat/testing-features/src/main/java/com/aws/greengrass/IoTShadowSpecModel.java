/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.testing.api.model.TestingModel;
import com.aws.greengrass.testing.resources.AWSResources;
import com.aws.greengrass.testing.resources.ResourceSpec;
import org.immutables.value.Value;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;

import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;

@TestingModel
@Value.Immutable
interface IoTShadowSpecModel extends ResourceSpec<IotDataPlaneClient, IoTShadow> {
    String initialPayload = "{\"state\":{\"reported\":{\"SomeKey\":\"SomeValue\"}}}";

    @Override
    @Nullable
    IoTShadow resource();

    String thingName();

    String shadowName();

    @Override
    default IoTShadowSpec create(IotDataPlaneClient client, AWSResources resources) {
        String thingName = thingName();
        String shadowName = shadowName();
        UpdateThingShadowResponse response = client.updateThingShadow(UpdateThingShadowRequest.builder()
                .thingName(thingName)
                .shadowName(shadowName)
                .payload(SdkBytes.fromString(initialPayload, StandardCharsets.UTF_8))
                .build());
        return IoTShadowSpec.builder()
                .from(this)
                .created(true)
                .resource(IoTShadow.builder()
                        .shadowName(shadowName)
                        .thingName(thingName)
                        .payload(response.payload())
                        .build())
                .build();
    }
}
