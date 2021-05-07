/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.integrationtests.ipc.IPCTestUtils;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.shadowmanager.ShadowManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClient;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.eventstreamrpc.EventStreamRPCConnection;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class RateLimiterTest {
    private static final long TEST_TIME_OUT_SEC = 30L;
    private static final int MAX_CALLS = 10;

    private static final String localShadowContentV1 = "{\"version\":1,\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
    private static final String lastSyncedDocument = "{\"state\":{\"desired\":{\"SomeKey\":\"boo\"}},\"metadata\":{}}";

    Kernel kernel;
    ShadowManager shadowManager;
    GlobalStateChangeListener listener;

    @TempDir
    Path rootDir;

    @Mock
    MqttClient mqttClient;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    IotDataPlaneClientFactory iotDataPlaneClientFactory;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    ShadowManagerDAOImpl dao;

    @BeforeEach
    void setup() {
        kernel = new Kernel();
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    private void startNucleusWithConfig(String configFile) throws InterruptedException {
        CountDownLatch shadowManagerRunning = new CountDownLatch(1);
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString(), "-i",
                getClass().getResource(configFile).toString());
        listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(ShadowManager.SERVICE_NAME) && service.getState().equals(State.RUNNING)) {
                shadowManagerRunning.countDown();
                shadowManager = (ShadowManager) service;
            }
        };
        kernel.getContext().addGlobalStateChangeListener(listener);

        kernel.getContext().put(MqttClient.class, mqttClient);
        // assume we are always connected
        lenient().when(mqttClient.connected()).thenReturn(true);
        kernel.getContext().put(IotDataPlaneClientFactory.class, iotDataPlaneClientFactory);
        kernel.getContext().put(ShadowManagerDAOImpl.class, dao);

        kernel.launch();

        assertTrue(shadowManagerRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_throttled_cloud_update_requests_WHEN_cloud_updates_THEN_cloud_updates_eventually(ExtensionContext context) throws IOException, InterruptedException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        // mock actual calls to the cloud
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class)))
                .thenReturn(mock(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse.class));

        // mock dao calls in cloud update
        when(dao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(new ShadowDocument(localShadowContentV1.getBytes())));
        when(dao.getShadowSyncInformation(anyString(), anyString())).thenReturn(
                Optional.of(SyncInformation.builder()
                        .lastSyncedDocument(lastSyncedDocument.getBytes())
                        .cloudVersion(0).build()));
        lenient().when(dao.updateSyncInformation(any(SyncInformation.class))).thenReturn(true);

        startNucleusWithConfig("rateLimits.yaml");
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

        startNucleusWithConfig("rateLimits.yaml");

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
                    assertThat(e.getMessage(), containsString("request throttled"));
                    callsLeft = MAX_CALLS - i;
                    exceptionTriggered = true;
                    break;
                }
            }

            assertThat("Expected exception was not triggered", exceptionTriggered, is(true));

            Thread.sleep(1000);
            for (int i = 0; i < callsLeft; i++) {
                assertDoesNotThrow(() -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void GIVEN_multiple_classic_and_named_shadow_request_for_thing_WHEN_requests_processed_THEN_requests_throttled_classic_and_named_shadows_for_thing(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ThrottledRequestException.class);

        when(dao.getShadowThing(anyString(), any())).thenReturn(Optional.of(new ShadowDocument(localShadowContentV1.getBytes())));

        startNucleusWithConfig("rateLimits.yaml");

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
                    assertThat(e.getMessage(), containsString("request throttled"));

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

        startNucleusWithConfig("rateLimits.yaml");

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
                    assertThat(e.getMessage(), containsString("request throttled"));
                    exceptionTriggered = true;

                    assertDoesNotThrow(() -> ipcClient.getThingShadow(newThingShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
                    break;
                }
            }

            assertThat("Expected exception was not triggered", exceptionTriggered, is(true));
        }
    }
}