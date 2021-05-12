/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.integrationtests.ipc.IPCTestUtils;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
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
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClient;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.eventstreamrpc.EventStreamRPCConnection;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class RateLimiterTest extends NucleusLaunchUtils {
    private static final int MAX_CALLS = 10;

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

    @Test
    void GIVEN_multiple_local_shadow_requests_for_thing_WHEN_requests_processed_THEN_requests_throttled(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ThrottledRequestException.class);

        when(dao.getShadowThing(anyString(), any())).thenReturn(Optional.of(new ShadowDocument(localShadowContentV1.getBytes())));

        startNucleusWithConfig("rateLimits.yaml", true, true);

        try (EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            GetThingShadowRequest getShadowRequest = new GetThingShadowRequest();
            getShadowRequest.setThingName(THING_NAME);

            // attempts to keep making requests until throttled and expected exception is thrown
            // will attempt the rest of the requests afterwards which should not be throttled after
            // waiting for one second
            int callsLeft = MAX_CALLS;
            boolean exceptionTriggered = false;
            for (int i = 1; i <= MAX_CALLS; i++) {
                try {
                    ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    assertThat(e.getCause(), instanceOf(ServiceError.class));
                    assertThat(e.getMessage(), containsString("Too Many Requests"));
                    callsLeft = MAX_CALLS - i;
                    exceptionTriggered = true;
                    break;
                }
            }

            assertThat("Expected exception was not triggered", exceptionTriggered, is(true));

            TimeUnit.SECONDS.sleep(1);
            for (int i = 0; i < callsLeft; i++) {
                assertDoesNotThrow(() -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void GIVEN_multiple_classic_and_named_shadow_request_for_thing_WHEN_requests_processed_THEN_requests_throttled_classic_and_named_shadows_for_thing(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ThrottledRequestException.class);

        when(dao.getShadowThing(anyString(), any())).thenReturn(Optional.of(new ShadowDocument(localShadowContentV1.getBytes())));

        startNucleusWithConfig("rateLimits.yaml", true, true);

        try (EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            GetThingShadowRequest getShadowRequest = new GetThingShadowRequest();
            getShadowRequest.setThingName(THING_NAME);
            getShadowRequest.setShadowName("shadowName");

            // once requests are throttled check that classic and named shadows are also throttled
            boolean exceptionTriggered = false;
            for (int i = 1; i <= MAX_CALLS; i++) {
                try {
                    ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    assertThat(e.getCause(), instanceOf(ServiceError.class));
                    assertThat(e.getMessage(), containsString("Too Many Requests"));

                    getShadowRequest.setShadowName(CLASSIC_SHADOW_IDENTIFIER);
                    ExecutionException err = assertThrows(ExecutionException.class, () -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
                    assertThat(err.getCause(), instanceOf(ServiceError.class));

                    getShadowRequest.setShadowName("differentShadowName");
                    err = assertThrows(ExecutionException.class, () -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
                    assertThat(err.getCause(), instanceOf(ServiceError.class));

                    exceptionTriggered = true;
                    break;
                }
            }

            assertThat("Expected exception was not triggered", exceptionTriggered, is(true));
        }
    }

    @Test
    void GIVEN_requests_throttled_for_thing_WHEN_request_sent_for_different_thing_THEN_request_for_different_thing_not_throttled(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ThrottledRequestException.class);

        when(dao.getShadowThing(anyString(), any())).thenReturn(Optional.of(new ShadowDocument(localShadowContentV1.getBytes())));

        startNucleusWithConfig("rateLimits.yaml", true, true);

        try (EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            GetThingShadowRequest originalThingShadowRequest = new GetThingShadowRequest();
            originalThingShadowRequest.setThingName(THING_NAME);

            GetThingShadowRequest newThingShadowRequest = new GetThingShadowRequest();
            newThingShadowRequest.setThingName("separateThing");

            // attempts to keep making requests until throttled and expected exception is thrown
            // will immediately make request for separate thing which should not be throttled
            boolean exceptionTriggered = false;
            for (int i = 1; i <= MAX_CALLS; i++) {
                try {
                    ipcClient.getThingShadow(originalThingShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    assertThat(e.getCause(), instanceOf(ServiceError.class));
                    assertThat(e.getMessage(), containsString("Too Many Requests"));
                    exceptionTriggered = true;

                    assertDoesNotThrow(() -> ipcClient.getThingShadow(newThingShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
                    break;
                }
            }

            assertThat("Expected exception was not triggered", exceptionTriggered, is(true));
        }
    }
}
