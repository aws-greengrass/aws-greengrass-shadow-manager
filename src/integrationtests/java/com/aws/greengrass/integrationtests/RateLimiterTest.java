/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class RateLimiterTest extends NucleusLaunchUtils {
    private static final String localShadowContentV1 = "{\"version\":1,\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
    private static final String lastSyncedDocument = "{\"state\":{\"desired\":{\"SomeKey\":\"boo\"}},\"metadata\":{}}";

    @Mock
    UpdateThingShadowResponse mockUpdateThingShadowResponse;

    @BeforeEach
    void setup() {
        kernel = new Kernel();
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    @Test
    void GIVEN_throttled_cloud_update_requests_WHEN_cloud_updates_THEN_cloud_updates_eventually(ExtensionContext context) throws IOException, InterruptedException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        // mock actual calls to the cloud
        when(mockUpdateThingShadowResponse.payload()).thenReturn(SdkBytes.fromString("{\"version\": 1}", UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class)))
                .thenReturn(mockUpdateThingShadowResponse);

        // mock dao calls in cloud update
        when(dao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(new ShadowDocument(localShadowContentV1.getBytes())));
        when(dao.getShadowSyncInformation(anyString(), anyString())).thenReturn(
                Optional.of(SyncInformation.builder()
                        .lastSyncedDocument(lastSyncedDocument.getBytes())
                        .cloudVersion(0).build()));
        lenient().when(dao.updateSyncInformation(any(SyncInformation.class))).thenReturn(true);

        startNucleusWithConfig("rateLimits.yaml", true, true);
        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        JsonNode updateDocument = JsonUtil.getPayloadJson(localShadowContentV1.getBytes()).get();

        // thingName has to be unique to prevent requests from being merged
        final int totalRequestCalls = 10;
        for (int i = 0; i < totalRequestCalls; i++) {
            syncHandler.pushCloudUpdateSyncRequest(String.valueOf(i), CLASSIC_SHADOW_IDENTIFIER, updateDocument);
        }

        // verify that some requests have been throttled
        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), after(1000).atMost(6)).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));

        // verify that the rest of the requests are eventually handled
        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), after(5000).times(totalRequestCalls)).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }
}
