/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.integrationtests.ipc.IPCTestUtils;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logging.impl.config.LogConfig;
import com.aws.greengrass.shadowmanager.exception.IoTDataPlaneClientCreationException;
import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.RequestQueue;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.sync.strategy.RealTimeSyncStrategy;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClient;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.eventstreamrpc.EventStreamRPCConnection;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class RateLimiterTest extends NucleusLaunchUtils {
    private static final int MAX_CALLS = 10;

    private static final String localShadowContentV1 = "{\"version\":1,\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
    private static final String lastSyncedDocument = "{\"state\":{\"desired\":{\"SomeKey\":\"boo\"}},\"metadata\":{}}";

    @Mock
    UpdateThingShadowResponse mockUpdateThingShadowResponse;

    @BeforeAll
    static void setupValidator() throws IOException {
        LogConfig.getRootLogConfig().setLevel(Level.TRACE);
        JsonUtil.loadSchema();
    }

    @BeforeEach
    void setup() {
        // Set this property for kernel to scan its own classpath to find plugins
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");
        kernel = new Kernel();
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    @Test
    void GIVEN_throttled_cloud_update_requests_WHEN_cloud_updates_THEN_cloud_updates_eventually(ExtensionContext context) throws IOException, InterruptedException, IoTDataPlaneClientCreationException {
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
        RequestQueue queue = spy(kernel.getContext().get(RequestQueue.class));
        kernel.getContext().put(RequestQueue.class, queue);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder().configFile("rateLimits.yaml").mockCloud(true).mockDao(true).build());
        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        JsonNode updateDocument = JsonUtil.getPayloadJson(localShadowContentV1.getBytes()).get();

        assertThat("syncing has started", () -> kernel.getContext().get(RealTimeSyncStrategy.class).isSyncing(), eventuallyEval(is(true)));
        verify(queue, timeout(2000).times(1)).clear();
        // thingName has to be unique to prevent requests from being merged
        final int totalRequestCalls = 10;
        for (int i = 0; i < totalRequestCalls; i++) {
            syncHandler.pushCloudUpdateSyncRequest(String.valueOf(i), CLASSIC_SHADOW_IDENTIFIER, updateDocument);
        }

        // verify that some requests have been throttled
        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), after(500).atMost(4)).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));

        // verify that the rest of the requests are eventually handled
        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), after(10000).times(totalRequestCalls)).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @Test
    void GIVEN_multiple_local_shadow_requests_for_thing_WHEN_requests_processed_THEN_requests_throttled(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ThrottledRequestException.class);

        when(dao.getShadowThing(anyString(), any())).thenReturn(Optional.of(new ShadowDocument(localShadowContentV1.getBytes())));

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder().configFile("rateLimits.yaml").mockCloud(true)
                .mockDao(true).build());
        assertThat("syncing has started", () -> kernel.getContext().get(RealTimeSyncStrategy.class).isSyncing(), eventuallyEval(is(true)));

        try (EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            GetThingShadowRequest getShadowRequest = new GetThingShadowRequest();
            getShadowRequest.setThingName(THING_NAME);

            for (int i = 1; i <= MAX_CALLS; i++) {
                assertDoesNotThrow(() -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            }
            ExecutionException thrown = assertThrows(ExecutionException.class, () -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            assertThat(thrown.getCause(), instanceOf(ServiceError.class));
            assertThat(thrown.getMessage(), containsString("Too Many Requests"));
        }
    }

    @Test
    void GIVEN_local_request_throttled_for_thing_WHEN_requests_processed_after_one_second_THEN_requests_not_throttled(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ThrottledRequestException.class);

        when(dao.getShadowThing(anyString(), any())).thenReturn(Optional.of(new ShadowDocument(localShadowContentV1.getBytes())));

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder().configFile("rateLimits.yaml").mockCloud(true)
                .mockDao(true).build());
        assertThat("syncing has started", () -> kernel.getContext().get(RealTimeSyncStrategy.class).isSyncing(), eventuallyEval(is(true)));

        try (EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            GetThingShadowRequest getShadowRequest = new GetThingShadowRequest();
            getShadowRequest.setThingName(THING_NAME);

            for (int i = 1; i <= MAX_CALLS; i++) {
                assertDoesNotThrow(() -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            }
            ExecutionException thrown = assertThrows(ExecutionException.class, () -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            assertThat(thrown.getCause(), instanceOf(ServiceError.class));
            assertThat(thrown.getMessage(), containsString("Too Many Requests"));

            // call after one second should not be throttled
            TimeUnit.SECONDS.sleep(1);
            assertDoesNotThrow(() -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
        }
    }

    @Test
    void GIVEN_multiple_classic_and_named_shadow_request_for_thing_WHEN_requests_processed_THEN_requests_throttled_classic_and_named_shadows_for_thing(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ThrottledRequestException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        when(dao.getShadowThing(anyString(), any())).thenReturn(Optional.of(new ShadowDocument(localShadowContentV1.getBytes())));

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder().configFile("rateLimits.yaml").mockCloud(true)
                .mockDao(true).build());

        assertThat("syncing has started", () -> kernel.getContext().get(RealTimeSyncStrategy.class).isSyncing(), eventuallyEval(is(true), Duration.ofSeconds(30L)));

        try (EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            GetThingShadowRequest getShadowRequest = new GetThingShadowRequest();
            getShadowRequest.setThingName(THING_NAME);
            getShadowRequest.setShadowName("shadowName");

            // classic and named shadows should count towards rate limiter for thing
            for (int i = 1; i <= MAX_CALLS; i++) {
                if (i % 2 == 0) {
                    getShadowRequest.setShadowName(CLASSIC_SHADOW_IDENTIFIER);
                } else {
                    getShadowRequest.setShadowName("shadowName");
                }
                assertDoesNotThrow(() -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            }

            getShadowRequest.setShadowName(CLASSIC_SHADOW_IDENTIFIER);
            ExecutionException thrown = assertThrows(ExecutionException.class, () -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            assertThat(thrown.getCause(), instanceOf(ServiceError.class));
            assertThat(thrown.getMessage(), containsString("Too Many Requests"));

            getShadowRequest.setShadowName("shadowName");
            thrown = assertThrows(ExecutionException.class, () -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            assertThat(thrown.getCause(), instanceOf(ServiceError.class));
            assertThat(thrown.getMessage(), containsString("Too Many Requests"));

            getShadowRequest.setShadowName("differentShadowName");
            thrown = assertThrows(ExecutionException.class, () -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            assertThat(thrown.getCause(), instanceOf(ServiceError.class));
            assertThat(thrown.getMessage(), containsString("Too Many Requests"));
        }
    }

    @Test
    void GIVEN_max_inbound_shadow_request_rate_exceeded_WHEN_requests_processed_THEN_any_request_throttled(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ThrottledRequestException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        when(dao.getShadowThing(anyString(), any())).thenReturn(Optional.of(new ShadowDocument(localShadowContentV1.getBytes())));

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder().configFile("rateLimitsWithTotalLocalRate.yaml").mockCloud(true)
                .mockDao(true).build());

        assertThat("syncing has started", () -> kernel.getContext().get(RealTimeSyncStrategy.class).isSyncing(), eventuallyEval(is(true)));

        try (EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            GetThingShadowRequest getShadowRequest = new GetThingShadowRequest();
            getShadowRequest.setThingName(THING_NAME);
            getShadowRequest.setShadowName("shadowName");

            // calls with different thing names should trigger total rate
            for (int i = 1; i <= MAX_CALLS; i++) {
                getShadowRequest.setThingName(String.valueOf(i));
                assertDoesNotThrow(() -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            }

            getShadowRequest.setThingName("NewClassicThing");
            getShadowRequest.setShadowName(CLASSIC_SHADOW_IDENTIFIER);
            ExecutionException thrown = assertThrows(ExecutionException.class, () -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            assertThat(thrown.getCause(), instanceOf(ServiceError.class));
            assertThat(thrown.getMessage(), containsString("Too Many Requests"));

            getShadowRequest.setThingName("NewNamedShadowThing");
            getShadowRequest.setShadowName("NewShadowName");
            thrown = assertThrows(ExecutionException.class, () -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            assertThat(thrown.getCause(), instanceOf(ServiceError.class));
            assertThat(thrown.getMessage(), containsString("Too Many Requests"));
        }
    }

    @Test
    void GIVEN_requests_throttled_for_thing_WHEN_request_sent_for_different_thing_THEN_request_for_different_thing_not_throttled(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ThrottledRequestException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        when(dao.getShadowThing(anyString(), any())).thenReturn(Optional.of(new ShadowDocument(localShadowContentV1.getBytes())));

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder().configFile("rateLimits.yaml").mockCloud(true)
                .mockDao(true).build());

        assertThat("syncing has started", () -> kernel.getContext().get(RealTimeSyncStrategy.class).isSyncing(), eventuallyEval(is(true)));

        try (EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            GetThingShadowRequest originalThingShadowRequest = new GetThingShadowRequest();
            originalThingShadowRequest.setThingName(THING_NAME);

            GetThingShadowRequest newThingShadowRequest = new GetThingShadowRequest();
            newThingShadowRequest.setThingName("separateThing");

            for (int i = 1; i <= MAX_CALLS; i++) {
                assertDoesNotThrow(() -> ipcClient.getThingShadow(originalThingShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            }

            ExecutionException thrown = assertThrows(ExecutionException.class, () -> ipcClient.getThingShadow(originalThingShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            assertThat(thrown.getCause(), instanceOf(ServiceError.class));
            assertThat(thrown.getMessage(), containsString("Too Many Requests"));

            assertDoesNotThrow(() -> ipcClient.getThingShadow(newThingShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
        }
    }
}
