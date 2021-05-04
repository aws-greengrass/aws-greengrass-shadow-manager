/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.shadowmanager.ShadowManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.aws.greengrass.util.Pair;
import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.exceptions.base.MockitoException;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class SyncTest extends GGServiceTestUtil  {
    private static final long TEST_TIME_OUT_SEC = 30L;
    public static final String MOCK_THING_NAME = "Thing1";
    public static final String CLASSIC_SHADOW = "";
    private static final String cloudShadowContentV10 = "{\"version\":10,\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
    private static final String cloudShadowContentV1 = "{\"version\":1,\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
    private static final String localShadowContentV1 = "{\"state\":{\"desired\":{\"SomeKey\":\"foo\"}},\"metadata\":{}}";

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

    @Captor
    private ArgumentCaptor<SyncInformation> syncInformationCaptor;
    @Captor
    private ArgumentCaptor<software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest> cloudUpdateThingShadowRequestCaptor;
    @Captor
    private ArgumentCaptor<software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest> cloudDeleteThingShadowRequestCaptor;


    @BeforeEach
    void setup() {
        kernel = new Kernel();
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    private void startNucleusWithConfig(String configFile, boolean mockCloud, boolean mockDao) throws InterruptedException {
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
        if (mockCloud) {
            kernel.getContext().put(IotDataPlaneClientFactory.class, iotDataPlaneClientFactory);
        }
        if (mockDao) {
            kernel.getContext().put(ShadowManagerDAOImpl.class, dao);
        }

        kernel.launch();

        assertTrue(shadowManagerRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    void eventually(Supplier<Void> supplier, long timeout, ChronoUnit unit) throws InterruptedException {
        Instant expire = Instant.now().plus(Duration.of(timeout, unit));
        while (expire.isAfter(Instant.now())) {
            try {
                supplier.get();
                return;
            } catch (MockitoException | AssertionError e) {
                // ignore
            }
            Thread.sleep(500);
        }
        supplier.get();
    }

    @Test
    void GIVEN_sync_config_and_no_local_WHEN_startup_THEN_local_version_updated_via_full_sync(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, InterruptedException.class);

        GetThingShadowResponse shadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(shadowResponse.payload().asByteArray()).thenReturn(cloudShadowContentV10.getBytes(UTF_8));

        // existing document
        when(iotDataPlaneClientFactory.getIotDataPlaneClient()
                .getThingShadow(any(GetThingShadowRequest.class))).thenReturn(shadowResponse);
        startNucleusWithConfig("sync.yaml", true, false);

        ShadowManagerDAO dao = kernel.getContext().get(ShadowManagerDAOImpl.class);

        JsonNode v1 = JsonUtil.getPayloadJson(localShadowContentV1.getBytes(UTF_8)).get();
        eventually(() -> {
            Optional<SyncInformation> syncInformation =
                    dao.getShadowSyncInformation(MOCK_THING_NAME, CLASSIC_SHADOW);
            assertThat("sync info exists", syncInformation.isPresent(), is(true));
            assertThat(syncInformation.get().getCloudVersion(), is(10L));
            assertThat(syncInformation.get().getLocalVersion(), is(1L));

            Optional<ShadowDocument> shadow = dao.getShadowThing(MOCK_THING_NAME, CLASSIC_SHADOW);
            assertThat("local shadow exists", shadow.isPresent(), is(true));
            ShadowDocument shadowDocument = shadow.get();
            // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
            shadowDocument = new ShadowDocument(shadowDocument.getState(), null, null);
            assertThat(shadowDocument.toJson(false), is(v1));
            return null;
        }, 10, ChronoUnit.SECONDS);

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), never()).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @Test
    void GIVEN_sync_config_and_no_cloud_WHEN_startup_THEN_cloud_version_updated_via_full_sync(ExtensionContext context) throws InterruptedException, IOException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ResourceNotFoundException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mock(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse.class));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);
        when(dao.updateSyncInformation(syncInformationCaptor.capture())).thenReturn(true);
        when(dao.listSyncedShadows()).thenReturn(Collections.singletonList(new Pair<>(MOCK_THING_NAME, CLASSIC_SHADOW)));

        ShadowDocument localDocument = new ShadowDocument(localShadowContentV1.getBytes(UTF_8), 1);
        when(dao.getShadowThing(eq(MOCK_THING_NAME), eq(CLASSIC_SHADOW))).thenReturn(Optional.of(localDocument));
        when(dao.getShadowSyncInformation(eq(MOCK_THING_NAME), eq(CLASSIC_SHADOW)))
                .thenReturn(Optional.of(SyncInformation.builder()
                        .thingName(MOCK_THING_NAME)
                        .shadowName(CLASSIC_SHADOW)
                        .lastSyncTime(Instant.EPOCH.getEpochSecond())
                        .cloudUpdateTime(Instant.EPOCH.getEpochSecond())
                        .localVersion(0)
                        .cloudVersion(0)
                        .lastSyncedDocument(null)
                        .build()));

        startNucleusWithConfig("sync.yaml", true, true);

        eventually(() -> {
            assertThat(cloudUpdateThingShadowRequestCaptor.getValue(), is(notNullValue()));
            assertThat(syncInformationCaptor.getValue(), is(notNullValue()));

            assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(1L));
            assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(1L));
            assertThat(syncInformationCaptor.getValue().getThingName(), is(MOCK_THING_NAME));
            assertThat(syncInformationCaptor.getValue().getShadowName(), is(CLASSIC_SHADOW));

            assertThat(cloudUpdateThingShadowRequestCaptor.getValue().thingName(), is(MOCK_THING_NAME));
            assertThat(cloudUpdateThingShadowRequestCaptor.getValue().shadowName(), is(CLASSIC_SHADOW));
            return null;
        }, 10, ChronoUnit.SECONDS);

        verify(dao, never()).updateShadowThing(anyString(), anyString(), any(byte[].class), anyLong());
        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), times(1)).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @Test
    void GIVEN_synced_shadow_WHEN_local_update_THEN_cloud_updates(ExtensionContext context) throws InterruptedException, IOException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mock(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse.class));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        startNucleusWithConfig("sync.yaml", true, false);

        ShadowManagerDAO dao = kernel.getContext().get(ShadowManagerDAOImpl.class);

        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        JsonNode v1 = JsonUtil.getPayloadJson(localShadowContentV1.getBytes(UTF_8)).get();
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(MOCK_THING_NAME);
        request.setShadowName(CLASSIC_SHADOW);
        request.setPayload(localShadowContentV1.getBytes(UTF_8));
        updateHandler.handleRequest(request, "DoAll");
        eventually(() -> {
            Optional<SyncInformation> syncInformation =
                    dao.getShadowSyncInformation(MOCK_THING_NAME, CLASSIC_SHADOW);
            assertThat("sync info exists", syncInformation.isPresent(), is(true));
            assertThat(syncInformation.get().getCloudVersion(), is(1L));
            assertThat(syncInformation.get().getLocalVersion(), is(1L));

            Optional<ShadowDocument> shadow = dao.getShadowThing(MOCK_THING_NAME, CLASSIC_SHADOW);
            assertThat("local shadow exists", shadow.isPresent(), is(true));
            ShadowDocument shadowDocument = shadow.get();
            // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
            shadowDocument = new ShadowDocument(shadowDocument.getState(), null, null);
            assertThat(shadowDocument.toJson(false), is(v1));
            return null;
        }, 10, ChronoUnit.SECONDS);
        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), times(1)).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @Test
    void GIVEN_synced_shadow_WHEN_cloud_update_THEN_local_updates(ExtensionContext context) throws IOException, InterruptedException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ResourceNotFoundException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mock(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse.class));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        JsonNode cloudDocument = JsonUtil.getPayloadJson(cloudShadowContentV1.getBytes(UTF_8)).get();

        startNucleusWithConfig("sync.yaml", true, false);
        ShadowManagerDAO dao = kernel.getContext().get(ShadowManagerDAOImpl.class);
        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudDocument));

        eventually(() -> {
            Optional<SyncInformation> syncInformation =
                    dao.getShadowSyncInformation(MOCK_THING_NAME, CLASSIC_SHADOW);
            assertThat("sync info exists", syncInformation.isPresent(), is(true));
            assertThat(syncInformation.get().getCloudVersion(), is(1L));
            assertThat(syncInformation.get().getLocalVersion(), is(1L));

            Optional<ShadowDocument> shadow = dao.getShadowThing(MOCK_THING_NAME, CLASSIC_SHADOW);
            assertThat("local shadow exists", shadow.isPresent(), is(true));
            ShadowDocument shadowDocument = shadow.get();
            // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
            shadowDocument = new ShadowDocument(shadowDocument.getState(), null, null);
            assertThat(shadowDocument.toJson(false).get(SHADOW_DOCUMENT_STATE), is(cloudDocument.get(SHADOW_DOCUMENT_STATE)));
            return null;
        }, 10, ChronoUnit.SECONDS);

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), never()).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));

    }

    @Test
    void GIVEN_synced_shadow_WHEN_local_delete_THEN_cloud_deletes(ExtensionContext context) throws IOException, InterruptedException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().deleteThingShadow(cloudDeleteThingShadowRequestCaptor.capture()))
                .thenReturn(mock(software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowResponse.class));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        startNucleusWithConfig("sync.yaml", true, false);

        // Sleep here so that there is no race condition between executing the initial full sync
        TimeUnit.SECONDS.sleep(2L);

        ShadowManagerDAO dao = kernel.getContext().get(ShadowManagerDAOImpl.class);
        dao.updateSyncInformation(SyncInformation.builder()
                .localVersion(1L)
                .cloudVersion(1L)
                .lastSyncedDocument(localShadowContentV1.getBytes(UTF_8))
                .cloudUpdateTime(Instant.now().getEpochSecond())
                .cloudDeleted(false)
                .lastSyncTime(Instant.now().getEpochSecond())
                .shadowName(CLASSIC_SHADOW)
                .thingName(MOCK_THING_NAME)
                .build());
        dao.updateShadowThing(MOCK_THING_NAME, CLASSIC_SHADOW, localShadowContentV1.getBytes(UTF_8), 1L);

        DeleteThingShadowRequestHandler deleteHandler = shadowManager.getDeleteThingShadowRequestHandler();

        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(MOCK_THING_NAME);
        request.setShadowName(CLASSIC_SHADOW);
        deleteHandler.handleRequest(request, "DoAll");
        eventually(() -> {
            Optional<SyncInformation> syncInformation =
                    dao.getShadowSyncInformation(MOCK_THING_NAME, CLASSIC_SHADOW);
            assertThat("sync info exists", syncInformation.isPresent(), is(true));
            assertThat(syncInformation.get().getCloudVersion(), is(1L));
            assertThat(syncInformation.get().getLocalVersion(), is(1L));
            assertThat(syncInformation.get().getLastSyncedDocument(), is(nullValue()));
            assertThat(syncInformation.get().isCloudDeleted(), is(true));

            Optional<ShadowDocument> shadow = dao.getShadowThing(MOCK_THING_NAME, CLASSIC_SHADOW);
            assertThat("local shadow should not exist", shadow.isPresent(), is(false));
            return null;
        }, 10, ChronoUnit.SECONDS);
        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), times(1)).deleteThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest.class));
    }

    @Disabled
    @Test
    void GIVEN_synced_shadow_WHEN_cloud_delete_THEN_local_deletes(ExtensionContext context) throws IOException, InterruptedException {

    }

    @Disabled
    @Test
    void GIVEN_unsynced_shadow_WHEN_cloud_updates_THEN_no_local_update(ExtensionContext context) throws IOException, InterruptedException {

    }
    @Disabled
    @Test
    void GIVEN_unsynced_shadow_WHEN_local_updates_THEN_no_cloud_update(ExtensionContext context) throws IOException, InterruptedException {

    }
}
