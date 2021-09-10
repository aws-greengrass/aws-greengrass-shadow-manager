/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logging.impl.config.LogConfig;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.configuration.ThingShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.sync.strategy.PeriodicSyncStrategy;
import com.aws.greengrass.shadowmanager.sync.strategy.RealTimeSyncStrategy;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Pair;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class SyncTest extends NucleusLaunchUtils {
    public static final String MOCK_THING_NAME_1 = "Thing1";
    public static final String MOCK_THING_NAME_2 = "Thing2";
    public static final String CLASSIC_SHADOW = "";
    public static final String RANDOM_SHADOW = "badShadowName";

    private static final String cloudShadowContentV10 = "{ \"state\": { \"desired\": { \"SomeKey\": \"foo\" }, "
            + "\"reported\": { \"SomeKey\": \"bar\", \"OtherKey\": 1}, \"delta\": { \"SomeKey\": \"foo\" } }, "
            + "\"metadata\": { \"desired\": { \"SomeKey\": { \"timestamp\": 1624980501 } }, "
            + "\"reported\": { \"SomeKey\": { \"timestamp\": 1624980501 } } },"
            + " \"version\": 10, \"timestamp\": 1624986665 }";

    private static final String cloudShadowContentV1 = "{\"version\":1,\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
    private static final String localShadowContentV1 = "{\"state\":{ \"desired\": { \"SomeKey\": \"foo\"}, "
            + "\"reported\":{\"SomeKey\":\"bar\",\"OtherKey\": 1}}}";

    private static final String localUpdate1 = "{\"state\":{\"reported\":{\"SomeKey\":\"foo\", \"OtherKey\": 1}}}";
    private static final String localUpdate2 = "{\"state\":{\"reported\":{\"OtherKey\":2, \"AnotherKey\":\"foobar\"}}}";
    private static final String mergedLocalShadowContentV2 =
            "{\"state\":{\"reported\":{\"SomeKey\":\"foo\",\"OtherKey\":2,\"AnotherKey\":\"foobar\"}}}";

    @Mock
    UpdateThingShadowResponse mockUpdateThingShadowResponse;

    @Captor
    private ArgumentCaptor<SyncInformation> syncInformationCaptor;
    @Captor
    private ArgumentCaptor<software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest> cloudUpdateThingShadowRequestCaptor;
    @Captor
    private ArgumentCaptor<software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest> cloudDeleteThingShadowRequestCaptor;

    private Supplier<Optional<SyncInformation>> syncInfo;
    private Supplier<Optional<ShadowDocument>> localShadow;

    @BeforeEach
    void setup() {
        kernel = new Kernel();
        syncInfo = () -> kernel.getContext().get(ShadowManagerDAOImpl.class).getShadowSyncInformation(MOCK_THING_NAME_1,
                CLASSIC_SHADOW);
        localShadow = () -> kernel.getContext().get(ShadowManagerDAOImpl.class).getShadowThing(MOCK_THING_NAME_1,
                CLASSIC_SHADOW);
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    private String getSyncConfigFile(Class<?> clazz) {
        if (RealTimeSyncStrategy.class.equals(clazz)) {
            return "sync.yaml";
        } else {
            return "periodic_sync.yaml";
        }
    }

    private void assertEmptySyncQueue(Class<?> clazz) {
        if (RealTimeSyncStrategy.class.equals(clazz)) {
            assertThat(() -> kernel.getContext().get(RealTimeSyncStrategy.class).getSyncQueue().size(), eventuallyEval(is(0)));
        } else {
            assertThat(() -> kernel.getContext().get(PeriodicSyncStrategy.class).getSyncQueue().size(), eventuallyEval(is(0)));
        }
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_sync_config_and_no_local_WHEN_startup_THEN_local_version_updated_via_full_sync(Class<?> clazz, ExtensionContext context)
            throws IOException, InterruptedException {
        ignoreExceptionOfType(context, InterruptedException.class);

        GetThingShadowResponse shadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(shadowResponse.payload().asByteArray()).thenReturn(cloudShadowContentV10.getBytes(UTF_8));

        // existing document
        when(iotDataPlaneClientFactory.getIotDataPlaneClient()
                .getThingShadow(any(GetThingShadowRequest.class))).thenReturn(shadowResponse);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .mockCloud(true)
                .syncClazz(clazz)
                .build());
    
        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(10L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));

        assertThat("local shadow exists", localShadow.get().isPresent(), is(true));
        ShadowDocument shadowDocument = localShadow.get().get();

        JsonNode v1 = new ShadowDocument(localShadowContentV1.getBytes(UTF_8)).toJson(false);
        // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
        shadowDocument = new ShadowDocument(shadowDocument.getState(), null, null);
        assertThat(shadowDocument.toJson(false), is(equalTo(v1)));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), never()).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @Test
    void GIVEN_sync_config_map_and_no_local_WHEN_startup_THEN_local_version_updated_via_full_sync(ExtensionContext context)
            throws IOException, InterruptedException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ResourceNotFoundException.class);

        GetThingShadowResponse shadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(shadowResponse.payload().asByteArray()).thenReturn(cloudShadowContentV10.getBytes(UTF_8));

        // existing document
        when(iotDataPlaneClientFactory.getIotDataPlaneClient()
                .getThingShadow(any(GetThingShadowRequest.class))).thenAnswer(invocation -> {
            GetThingShadowRequest request = invocation.getArgument(0);
            if (request.thingName().equals(MOCK_THING_NAME_1) && request.shadowName().equals(CLASSIC_SHADOW)) {
                return shadowResponse;
            }
            throw ResourceNotFoundException.builder().build();
        });

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile("sync_map.yaml")
                .mockCloud(true)
                .build());

        assertThat(shadowManager.getSyncConfiguration().getSyncConfigurations(),
                containsInAnyOrder(
                        ThingShadowSyncConfiguration.builder().thingName(MOCK_THING_NAME_1).shadowName("").build(),
                        ThingShadowSyncConfiguration.builder().thingName(MOCK_THING_NAME_2).shadowName("bar").build(),
                        ThingShadowSyncConfiguration.builder().thingName(MOCK_THING_NAME_2).shadowName("").build(),
                        ThingShadowSyncConfiguration.builder().thingName(MOCK_THING_NAME_2).shadowName("foo").build()));

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(10L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));

        assertThat("local shadow exists", localShadow.get().isPresent(), is(true));
        ShadowDocument shadowDocument = localShadow.get().get();

        JsonNode v1 = new ShadowDocument(localShadowContentV1.getBytes(UTF_8)).toJson(false);
        // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
        shadowDocument = new ShadowDocument(shadowDocument.getState(), null, null);
        assertThat(shadowDocument.toJson(false), is(equalTo(v1)));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), never()).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_sync_config_and_no_cloud_WHEN_startup_THEN_cloud_version_updated_via_full_sync(Class<?> clazz, ExtensionContext context)
            throws IOException, InterruptedException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ResourceNotFoundException.class);

        when(mockUpdateThingShadowResponse.payload()).thenReturn(SdkBytes.fromString("{\"version\": 1}", UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);
        when(dao.updateSyncInformation(syncInformationCaptor.capture())).thenReturn(true);
        when(dao.listSyncedShadows()).thenReturn(Collections.singletonList(new Pair<>(MOCK_THING_NAME_1, CLASSIC_SHADOW)));

        ShadowDocument localDocument = new ShadowDocument(localShadowContentV1.getBytes(UTF_8), 1);
        when(dao.getShadowThing(eq(MOCK_THING_NAME_1), eq(CLASSIC_SHADOW))).thenReturn(Optional.of(localDocument));
        when(dao.getShadowSyncInformation(eq(MOCK_THING_NAME_1), eq(CLASSIC_SHADOW)))
                .thenReturn(Optional.of(SyncInformation.builder()
                        .thingName(MOCK_THING_NAME_1)
                        .shadowName(CLASSIC_SHADOW)
                        .lastSyncTime(Instant.EPOCH.getEpochSecond())
                        .cloudUpdateTime(Instant.EPOCH.getEpochSecond())
                        .localVersion(0)
                        .cloudVersion(0)
                        .lastSyncedDocument(null)
                        .build()));

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .mockDao(true)
                .build());

        assertThat(() -> cloudUpdateThingShadowRequestCaptor.getValue(), eventuallyEval(is(notNullValue())));
        assertThat(() -> syncInformationCaptor.getValue(), eventuallyEval(is(notNullValue())));

        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(1L));
        assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(1L));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(MOCK_THING_NAME_1));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(CLASSIC_SHADOW));

        assertThat(cloudUpdateThingShadowRequestCaptor.getValue().thingName(), is(MOCK_THING_NAME_1));
        assertThat(cloudUpdateThingShadowRequestCaptor.getValue().shadowName(), is(CLASSIC_SHADOW));

        verify(dao, never()).updateShadowThing(anyString(), anyString(), any(byte[].class), anyLong());
        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), times(1)).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_synced_shadow_WHEN_local_update_THEN_cloud_updates(Class<?> clazz, ExtensionContext context) throws IOException,
            InterruptedException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        // no shadow exists in cloud
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        // mock response to update cloud
        when(mockUpdateThingShadowResponse.payload()).thenReturn(SdkBytes.fromString("{\"version\": 1}", UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        // update local shadow
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(MOCK_THING_NAME_1);
        request.setShadowName(CLASSIC_SHADOW);
        request.setPayload(localShadowContentV1.getBytes(UTF_8));

        updateHandler.handleRequest(request, "DoAll");
        assertEmptySyncQueue(clazz);

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));

        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(1L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));

        assertThat("local shadow exists", localShadow.get().isPresent(), is(true));
        ShadowDocument shadowDocument = localShadow.get().get();
        // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
        shadowDocument = new ShadowDocument(shadowDocument.getState(), null, null);
        JsonNode v1 = new ShadowDocument(localShadowContentV1.getBytes(UTF_8)).toJson(false);
        assertThat(shadowDocument.toJson(false), is(v1));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), times(1)).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_synced_shadow_WHEN_cloud_update_THEN_local_updates(Class<?> clazz, ExtensionContext context) throws IOException, InterruptedException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ResourceNotFoundException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        JsonNode cloudDocument = JsonUtil.getPayloadJson(cloudShadowContentV1.getBytes(UTF_8)).get();

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        assertEmptySyncQueue(clazz);
        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudDocument));
        assertEmptySyncQueue(clazz);

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));

        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(1L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));

        assertThat("local shadow exists", localShadow.get().isPresent(), is(true));
        ShadowDocument shadowDocument = localShadow.get().get();
        // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
        shadowDocument = new ShadowDocument(shadowDocument.getState(), null, null);
        assertThat(shadowDocument.toJson(false).get(SHADOW_DOCUMENT_STATE), is(cloudDocument.get(SHADOW_DOCUMENT_STATE)));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), never()).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_synced_shadow_WHEN_local_delete_THEN_cloud_deletes(Class<?> clazz, ExtensionContext context) throws IOException, InterruptedException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().deleteThingShadow(cloudDeleteThingShadowRequestCaptor.capture()))
                .thenReturn(mock(software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowResponse.class));

        GetThingShadowResponse shadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(shadowResponse.payload().asByteArray()).thenReturn(cloudShadowContentV10.getBytes(UTF_8));

        // return existing doc for full sync
        when(iotDataPlaneClientFactory.getIotDataPlaneClient()
                .getThingShadow(any(GetThingShadowRequest.class))).thenReturn(shadowResponse);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        // we have a local shadow from the initial full sync
        assertThat("local shadow exists", () -> localShadow.get().isPresent(), eventuallyEval(is(true)));

        DeleteThingShadowRequestHandler deleteHandler = shadowManager.getDeleteThingShadowRequestHandler();
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(MOCK_THING_NAME_1);
        request.setShadowName(CLASSIC_SHADOW);
        deleteHandler.handleRequest(request, "DoAll");

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud deleted", () -> syncInfo.get().get().isCloudDeleted(), eventuallyEval(is(true)));
        assertThat("cloud version", syncInfo.get().get().getCloudVersion(), is(10L));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));
        assertThat("sync doc", syncInfo.get().get().getLastSyncedDocument(), is(nullValue()));

        assertThat("local shadow should not exist", localShadow.get().isPresent(), is(false));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), times(1)).deleteThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_synced_shadow_WHEN_cloud_delete_THEN_local_deletes(Class<?> clazz, ExtensionContext context) throws InterruptedException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ResourceNotFoundException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().deleteThingShadow(cloudDeleteThingShadowRequestCaptor.capture()))
                .thenReturn(mock(software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowResponse.class));

        GetThingShadowResponse shadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(shadowResponse.payload().asByteArray()).thenReturn(cloudShadowContentV10.getBytes(UTF_8));

        // return existing doc for full sync
        when(iotDataPlaneClientFactory.getIotDataPlaneClient()
                .getThingShadow(any(GetThingShadowRequest.class))).thenReturn(shadowResponse);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);

        // we have a local shadow from the initial full sync
        assertThat("local shadow exists", () -> localShadow.get().isPresent(), eventuallyEval(is(true)));
        assertThat("local shadow version", localShadow.get().get().getVersion(), is(1L));

        syncHandler.pushLocalDeleteSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, "{\"version\": 10}".getBytes(UTF_8));

        // wait for it to process
        assertEmptySyncQueue(clazz);

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud deleted", () -> syncInfo.get().get().isCloudDeleted(), eventuallyEval(is(true)));

        assertThat("cloud version", syncInfo.get().get().getCloudVersion(), is(10L));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));
        assertThat("sync doc", syncInfo.get().get().getLastSyncedDocument(), is(nullValue()));

        assertThat("local shadow should not exist", localShadow.get().isPresent(), is(false));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), never()).deleteThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_unsynced_shadow_WHEN_local_deletes_THEN_no_cloud_delete(Class<?> clazz, ExtensionContext context) throws InterruptedException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().deleteThingShadow(cloudDeleteThingShadowRequestCaptor.capture()))
                .thenReturn(mock(software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowResponse.class));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        assertEmptySyncQueue(clazz);

        ShadowManagerDAO dao = kernel.getContext().get(ShadowManagerDAOImpl.class);
        dao.updateSyncInformation(SyncInformation.builder()
                .localVersion(1L)
                .cloudVersion(1L)
                .lastSyncedDocument(localShadowContentV1.getBytes(UTF_8))
                .cloudUpdateTime(Instant.now().getEpochSecond())
                .cloudDeleted(false)
                .lastSyncTime(Instant.now().getEpochSecond())
                .shadowName(RANDOM_SHADOW)
                .thingName(MOCK_THING_NAME_1)
                .build());
        dao.updateShadowThing(MOCK_THING_NAME_1, RANDOM_SHADOW, localShadowContentV1.getBytes(UTF_8), 1L);

        DeleteThingShadowRequestHandler deleteHandler = shadowManager.getDeleteThingShadowRequestHandler();

        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(MOCK_THING_NAME_1);
        request.setShadowName(RANDOM_SHADOW);

        deleteHandler.handleRequest(request, "DoAll");

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), after(Duration.ofSeconds(10).toMillis()).never()).deleteThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_unsynced_shadow_WHEN_local_updates_THEN_no_cloud_update(Class<?> clazz, ExtensionContext context) throws InterruptedException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(MOCK_THING_NAME_1);
        request.setShadowName(RANDOM_SHADOW);
        request.setPayload(localShadowContentV1.getBytes(UTF_8));
        updateHandler.handleRequest(request, "DoAll");

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), after(Duration.ofSeconds(10).toMillis()).never()).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }


    static class TestException extends RuntimeException { // NOPMD

    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_cloud_update_request_WHEN_retryable_thrown_AND_new_cloud_update_request_THEN_retries_with_merged_request(Class<?> clazz, ExtensionContext context)
            throws InterruptedException, IOException {
        LogConfig.getRootLogConfig().setLevel(Level.DEBUG);
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, RetryableException.class);
        ignoreExceptionOfType(context, TestException.class);

        UpdateThingShadowRequest request1 = new UpdateThingShadowRequest();
        request1.setThingName(MOCK_THING_NAME_1);
        request1.setShadowName(CLASSIC_SHADOW);
        request1.setPayload(localUpdate1.getBytes(UTF_8));

        UpdateThingShadowRequest request2 = new UpdateThingShadowRequest();
        request2.setThingName(MOCK_THING_NAME_1);
        request2.setShadowName(CLASSIC_SHADOW);
        request2.setPayload(localUpdate2.getBytes(UTF_8));

        // on startup a full sync is executed. mock a cloud response
        GetThingShadowResponse shadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(shadowResponse.payload().asByteArray()).thenThrow(ResourceNotFoundException.class);

        // return the "V10" existing document for full sync
        when(iotDataPlaneClientFactory.getIotDataPlaneClient()
                .getThingShadow(any(GetThingShadowRequest.class))).thenReturn(shadowResponse);
        AtomicInteger updateThingShadowCalled = new AtomicInteger(0);
        AtomicReference<UpdateThingShadowRequestHandler> handler = new AtomicReference<>();

        // throw an exception first - before throwing we make another request so that there is another request for
        // to update this shadow in the queue
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenAnswer(invocation -> {
                    software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest r = invocation.getArgument(0);
                    ShadowDocument s = new ShadowDocument(r.payload().asByteArray());
                    if (s.getState().getReported().has("AnotherKey")) {
                        updateThingShadowCalled.incrementAndGet();
                        return UpdateThingShadowResponse.builder().payload(SdkBytes.fromString("{\"version\": 11}", UTF_8)).build();
                    }
                    if (handler.get() != null) {
                        // request #2 comes in as we are processing request #1
                        handler.get().handleRequest(request2, "DoAll");
                    }

                    throw new RetryableException(new TestException());
                });

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        handler.set(shadowManager.getUpdateThingShadowRequestHandler());
        assertEmptySyncQueue(clazz);


        // Fire the initial request
        handler.get().handleRequest(request1, "DoAll");

        assertThat("update thing shadow called", updateThingShadowCalled::get, eventuallyEval(is(1)));
        assertThat("dao cloud version updated", () -> syncInfo.get()
                .map(SyncInformation::getCloudVersion).orElse(0L), eventuallyEval(is(11L)));
        assertThat("dao local version updated", () -> syncInfo.get()
                .map(SyncInformation::getLocalVersion).orElse(0L), eventuallyEval(is(2L)));

        assertThat("number of cloud update attempts", cloudUpdateThingShadowRequestCaptor.getAllValues(), hasSize(2));

        // get last cloud update request
        software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest actualRequest =
                cloudUpdateThingShadowRequestCaptor.getAllValues().get(1);

        JsonNode actualNode = JsonUtil.getPayloadJson(actualRequest.payload().asByteArray()).get();
        JsonNode expectedNode = JsonUtil.getPayloadJson(mergedLocalShadowContentV2.getBytes(UTF_8)).get();

        // check that it is request 2 merged on top of request 1 and *not* request 1 merged on top of request 2
        assertThat(actualNode, is(equalTo(expectedNode)));
    }
}


