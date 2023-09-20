/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logging.impl.config.LogConfig;
import com.aws.greengrass.shadowmanager.ShadowManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.IoTDataPlaneClientCreationException;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.configuration.ThingShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.RequestBlockingQueue;
import com.aws.greengrass.shadowmanager.sync.RequestMerger;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.DirectionWrapper;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.strategy.BaseSyncStrategy;
import com.aws.greengrass.shadowmanager.sync.strategy.PeriodicSyncStrategy;
import com.aws.greengrass.shadowmanager.sync.strategy.RealTimeSyncStrategy;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Pair;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.mockito.stubbing.Answer;
import org.slf4j.event.Level;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
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
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_METADATA;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("PMD.ExcessiveClassLength")
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
    private static final String cloudShadowContentV2 = "{\"version\":2,\"state\":{\"desired\":{\"SomeKey\":\"foo2\"}}}";
    private static final String cloudShadowContentV3 = "{\"version\":3,\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
    private static final String cloudShadowContentV4 = "{\"version\":4,\"state\":{\"desired\":{\"SomeKey\":\"foo2\"}}}";
    private static final String localShadowContentV1 = "{\"state\":{ \"desired\": { \"SomeKey\": \"foo\"}, "
            + "\"reported\":{\"SomeKey\":\"bar\",\"OtherKey\": 1}}}";
    private static final String localShadowContentV2 = "{\"state\":{ \"desired\": { \"SomeKey\": \"foo\"}, "
            + "\"reported\":{\"SomeKey\":\"bar\",\"OtherKey\": 2}}}";

    private static final String localUpdate1 = "{\"state\":{\"reported\":{\"SomeKey\":\"foo\", \"OtherKey\": 1}}}";
    private static final String localUpdate2 = "{\"state\":{\"reported\":{\"OtherKey\":2, \"AnotherKey\":\"foobar\"}}}";
    private static final String mergedLocalShadowContentV2 =
            "{\"state\":{\"reported\":{\"SomeKey\":\"foo\",\"OtherKey\":2,\"AnotherKey\":\"foobar\"}}}";

    @Mock
    UpdateThingShadowResponse mockUpdateThingShadowResponse;

    RequestBlockingQueue syncQueue;

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
        // Set this property for kernel to scan its own classpath to find plugins
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");
        kernel = new Kernel();
        syncQueue = spy(new RequestBlockingQueue(new RequestMerger(new DirectionWrapper())));
        kernel.getContext().put(RequestBlockingQueue.class, syncQueue);
        syncInfo = () -> kernel.getContext().get(ShadowManagerDAOImpl.class).getShadowSyncInformation(MOCK_THING_NAME_1,
                CLASSIC_SHADOW);
        localShadow = () -> kernel.getContext().get(ShadowManagerDAOImpl.class).getShadowThing(MOCK_THING_NAME_1,
                CLASSIC_SHADOW);
    }

    @AfterEach
    void cleanup() throws InterruptedException {
        TimeUnit.SECONDS.sleep(1);
        kernel.shutdown();
    }

    private String getSyncConfigFile(Class<?extends BaseSyncStrategy> clazz) {
        if (RealTimeSyncStrategy.class.equals(clazz)) {
            return "sync.yaml";
        } else {
            return "periodic_sync.yaml";
        }
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_sync_config_and_no_local_WHEN_startup_THEN_local_version_updated_via_full_sync(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context)
            throws IOException, InterruptedException, IoTDataPlaneClientCreationException {
        LogConfig.getRootLogConfig().setLevel(Level.DEBUG);
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
        assertThat("local version", ()->syncInfo.get().get().getLocalVersion(), eventuallyEval(is(1L)));

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
    @ValueSource(classes = {RealTimeSyncStrategy.class})
    void GIVEN_sync_config_WHEN_repeat_turn_sync_on_and_off_THEN_no_exceptions_thrown(Class<?
            extends BaseSyncStrategy> clazz, ExtensionContext context) throws InterruptedException, IoTDataPlaneClientCreationException {
        Level l = LogConfig.getRootLogConfig().getLevel();
        LogConfig.getRootLogConfig().setLevel(Level.ERROR); // set to ERROR level to avoid spamming logs
        // this test is more of a sanity check to ensure that cancelling threads doesn't cause any errors

        try {
            GetThingShadowResponse shadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
            byte[] bytes = cloudShadowContentV10.getBytes(UTF_8);
            lenient().when(shadowResponse.payload().asByteArray()).thenReturn(bytes);

            // existing document
            when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class))).thenReturn(shadowResponse);

            startNucleusWithConfig(
                    NucleusLaunchUtilsConfig.builder().configFile(getSyncConfigFile(clazz)).mockCloud(true).syncClazz(clazz).build());

            ShadowManager.StartSyncInfo syncInfo =
                    ShadowManager.StartSyncInfo.builder().reInitializeSyncInfo(true).overrideRunningCheck(true).updateCloudSubscriptions(true).build();

            SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);


            // set up more shadows to sync
            Set<ThingShadowSyncConfiguration> syncConfigs = new HashSet<>();
            ThingShadowSyncConfiguration.ThingShadowSyncConfigurationBuilder b =
                    ThingShadowSyncConfiguration.builder()
                            .thingName(MOCK_THING_NAME_1)
                            .shadowName(CLASSIC_SHADOW)
                            .shadowName("foo");
            for (int i = 0; i < 100; i++) {
                b = b.shadowName("bar" + i);
            }
            syncConfigs.add(b.build());

            syncHandler.setSyncConfigurations(syncConfigs);
            AtomicBoolean done = new AtomicBoolean();
            final Random r = new Random();
            CountDownLatch finished = new CountDownLatch(1);

            Supplier<byte[]> updateBytes = () -> {
                String s = "{\"state\":{ \"desired\": { \"SomeKey\": \"foo\"}, "
                        + "\"reported\":{\"SomeKey\":\"bar\",\"OtherKey\":" + r.nextLong() + "}}}";
                return s.getBytes(UTF_8);
            };

            // avoid race condition where we send an update request
            // during startup. e.g. while sync info is being recreated
            ReentrantLock restartingSync = new ReentrantLock();

            // create thread to make local updated that will sync to the cloud
            Executors.newSingleThreadExecutor().submit(() -> {
                try {
                    while (!done.get()) {
                        try {
                            if (!restartingSync.tryLock(1, TimeUnit.SECONDS)) {
                                continue;
                            }
                        } catch (InterruptedException e) {
                            return;
                        }
                        try {
                            UpdateThingShadowRequest request = new UpdateThingShadowRequest();
                            request.setThingName(MOCK_THING_NAME_1);
                            request.setShadowName(r.nextBoolean() ? "foo" : "bar" + r.nextInt(100));
                            request.setPayload(updateBytes.get());
                            shadowManager.getUpdateThingShadowRequestHandler().handleRequest(request, "DoAll");
                            try {
                                Thread.sleep(r.nextInt(10));
                            } catch (InterruptedException e) {
                                return;
                            }
                        } finally {
                            restartingSync.unlock();
                        }
                    }
                } finally {
                    finished.countDown();
                }
            });

            Instant end = Instant.now().plusSeconds(30);
            // stop and start syncing for 30 seconds.
            while (end.isAfter(Instant.now())) {
                restartingSync.lockInterruptibly();
                try {
                    syncHandler.stop();
                    Thread.sleep(r.nextInt(2000)); // sleep for up to 2 seconds to let sync happen at various speeds
                    shadowManager.startSyncingShadows(syncInfo);
                } finally {
                    restartingSync.unlock();
                }
            }
            // no exceptions should be thrown so this test should pass
            done.set(true);
            if (!finished.await(5, TimeUnit.SECONDS)) {
                fail("did not finish sync request generation thread");
            }
        } finally {
            LogConfig.getRootLogConfig().setLevel(l);
        }
    }

    @Test
    void GIVEN_sync_config_map_and_no_local_WHEN_startup_THEN_local_version_updated_via_full_sync(ExtensionContext context)
            throws IOException, InterruptedException, IoTDataPlaneClientCreationException {
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
    void GIVEN_sync_config_and_no_cloud_WHEN_startup_THEN_cloud_version_updated_via_full_sync(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context)
            throws IOException, InterruptedException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        CountDownLatch cdl = new CountDownLatch(2);
        when(mockUpdateThingShadowResponse.payload()).thenReturn(SdkBytes.fromString("{\"version\": 1}", UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenAnswer(invocation->{
                    cdl.countDown();
                    return mockUpdateThingShadowResponse;
                });
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        when(dao.updateSyncInformation(syncInformationCaptor.capture())).thenAnswer(invocation->{
            cdl.countDown();
            return true;
        });
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
        assertThat("cloud shadow updated", cdl.await(30, TimeUnit.SECONDS), is(true));
        assertThat(() -> cloudUpdateThingShadowRequestCaptor.getValue(), eventuallyEval(is(notNullValue())));
        assertThat(() -> syncInformationCaptor.getValue(), eventuallyEval(is(notNullValue())));

        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(1L));
        assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(1L));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(MOCK_THING_NAME_1));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(CLASSIC_SHADOW));

        assertThat(cloudUpdateThingShadowRequestCaptor.getValue().thingName(), is(MOCK_THING_NAME_1));
        assertThat(cloudUpdateThingShadowRequestCaptor.getValue().shadowName(), is(CLASSIC_SHADOW));

        verify(dao, never()).updateShadowThing(anyString(), anyString(), any(byte[].class), anyLong());
        // Checking that the cloud shadow is updated at least once since there is a possibility that the older
        // sync strategy (specifically real time syncing) can start executing a request before we have had a chance to
        // replace it.
        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), atLeastOnce()).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_synced_shadow_WHEN_local_update_THEN_cloud_updates(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context) throws IOException,
            InterruptedException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);
        CountDownLatch cdl = new CountDownLatch(1);
        // no shadow exists in cloud
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        // mock response to update cloud
        when(mockUpdateThingShadowResponse.payload()).thenReturn(SdkBytes.fromString("{\"version\": 1}", UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenAnswer((ignored)->{
                    cdl.countDown();
                    return mockUpdateThingShadowResponse;
                });

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());
        // Let the initial FullShadowSyncRequests finish before the local update request. Otherwise, the local shadow
        // will be delete during the FullShadowSyncRequest.
        verify(syncQueue, after(10000).atMost(4)).put(any(FullShadowSyncRequest.class));
        assertEmptySyncQueue(clazz);
        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        // update local shadow
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(MOCK_THING_NAME_1);
        request.setShadowName(CLASSIC_SHADOW);
        request.setPayload(localShadowContentV1.getBytes(UTF_8));

        updateHandler.handleRequest(request, "DoAll");
        assertEmptySyncQueue(clazz);
        assertThat("requests processed", cdl.await(10, TimeUnit.SECONDS));
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
    void GIVEN_synced_shadow_WHEN_multiple_local_updates_THEN_cloud_updates(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context) throws IOException,
            InterruptedException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        // no shadow exists in cloud
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        // mock response to update cloud
        when(mockUpdateThingShadowResponse.payload())
                .thenReturn(SdkBytes.fromString("{\"version\": 1}", UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient()
                .updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse)
                .thenReturn(mockUpdateThingShadowResponse)
                .thenAnswer((Answer<UpdateThingShadowResponse>) invocationOnMock -> {
                    Thread.sleep(1000L);
                    return mockUpdateThingShadowResponse;
                })
                .thenReturn(mockUpdateThingShadowResponse);

        SyncHandler syncHandler = spy(kernel.getContext().get(SyncHandler.class));
        kernel.getContext().put(SyncHandler.class, syncHandler);
        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());
        assertEmptySyncQueue(clazz);
        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        UpdateThingShadowRequest requestA = new UpdateThingShadowRequest();
        requestA.setThingName(MOCK_THING_NAME_1);
        requestA.setShadowName(CLASSIC_SHADOW);
        requestA.setPayload(localShadowContentV1.getBytes(UTF_8));

        UpdateThingShadowRequest requestB = new UpdateThingShadowRequest();
        requestB.setThingName(MOCK_THING_NAME_1);
        requestB.setShadowName(CLASSIC_SHADOW);
        requestB.setPayload(localShadowContentV2.getBytes(UTF_8));

        updateHandler.handleRequest(requestA, "DoAll");
        assertEmptySyncQueue(clazz);
        updateHandler.handleRequest(requestB, "DoAll");
        assertEmptySyncQueue(clazz);
        updateHandler.handleRequest(requestA, "DoAll");
        updateHandler.handleRequest(requestB, "DoAll");

        assertEmptySyncQueue(clazz);
        verify(syncHandler, after(10000).atLeast(4))
                .pushCloudUpdateSyncRequest(anyString(), anyString(), any(JsonNode.class));
        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("local shadow exists", localShadow.get().isPresent(), is(true));
        ShadowDocument shadowDocument = localShadow.get().get();

        // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
        shadowDocument = new ShadowDocument(shadowDocument.getState(), null, null);
        JsonNode v1 = new ShadowDocument(localShadowContentV2.getBytes(UTF_8)).toJson(false);
        assertThat(shadowDocument.toJson(false), is(v1));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), atLeast(1)).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_synced_shadow_WHEN_multiple_cloud_and_local_received_THEN_cloud_updated(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context) throws IOException, InterruptedException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        // no shadow exists in cloud
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        // mock response to update cloud
        when(mockUpdateThingShadowResponse.payload())
                .thenReturn(SdkBytes.fromString("{\"version\": 1}", UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse)
                .thenReturn(mockUpdateThingShadowResponse)
                .thenAnswer((Answer<UpdateThingShadowResponse>) invocationOnMock -> {
                    Thread.sleep(1000L);
                    return mockUpdateThingShadowResponse;
                })
                .thenReturn(mockUpdateThingShadowResponse);
        SyncHandler syncHandler = spy(kernel.getContext().get(SyncHandler.class));
        kernel.getContext().put(SyncHandler.class, syncHandler);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());
        // wait for initial full sync to complete
        verify(syncQueue, after(7000).atMost(4)).put(any(FullShadowSyncRequest.class));
        assertEmptySyncQueue(clazz);

        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        UpdateThingShadowRequest requestA = new UpdateThingShadowRequest();
        requestA.setThingName(MOCK_THING_NAME_1);
        requestA.setShadowName(CLASSIC_SHADOW);
        requestA.setPayload(localShadowContentV1.getBytes(UTF_8));

        UpdateThingShadowRequest requestB = new UpdateThingShadowRequest();
        requestB.setThingName(MOCK_THING_NAME_1);
        requestB.setShadowName(CLASSIC_SHADOW);
        requestB.setPayload(localShadowContentV2.getBytes(UTF_8));

        String cloudShadowContent = "{\"version\": 1,\"state\":{ \"desired\": { \"SomeKey\": \"foo\"}, "
                + "\"reported\":{\"SomeKey\":\"bar\",\"OtherKey\": 2}}}";
        JsonNode cloudDocument = JsonUtil.getPayloadJson(cloudShadowContent.getBytes(UTF_8)).get();

        updateHandler.handleRequest(requestA, "DoAll");
        assertEmptySyncQueue(clazz);
        updateHandler.handleRequest(requestB, "DoAll");
        assertEmptySyncQueue(clazz);

        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudDocument));

        updateHandler.handleRequest(requestA, "DoAll");
        updateHandler.handleRequest(requestB, "DoAll");

        assertEmptySyncQueue(clazz);
        verify(syncHandler, after(10000).times(4))
                .pushCloudUpdateSyncRequest(anyString(), anyString(), any(JsonNode.class));
        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("local shadow exists", localShadow.get().isPresent(), is(true));
        ShadowDocument shadowDocument = localShadow.get().get();

        // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
        shadowDocument = new ShadowDocument(shadowDocument.getState(), null, null);
        JsonNode v1 = new ShadowDocument(localShadowContentV2.getBytes(UTF_8)).toJson(false);
        assertThat(shadowDocument.toJson(false), is(v1));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), atLeast(1)).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_synced_shadow_WHEN_cloud_update_THEN_local_updates(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context) throws IOException, InterruptedException, IoTDataPlaneClientCreationException {
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

        // wait for initial full sync to complete
        verify(syncQueue, after(7000).atMost(4)).put(any(FullShadowSyncRequest.class));
        assertEmptySyncQueue(clazz);

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(0L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(0L));

        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudDocument));

        assertLocalShadowEquals(cloudShadowContentV1);

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(1L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), never()).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_synced_shadow_WHEN_multiple_cloud_updates_THEN_local_updates(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context) throws IOException, InterruptedException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, ConflictError.class);
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        GetThingShadowResponse shadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(shadowResponse.payload().asByteArray()).thenReturn(cloudShadowContentV1.getBytes(UTF_8));
        GetThingShadowResponse shadowResponseV2 = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(shadowResponseV2.payload().asByteArray()).thenReturn(cloudShadowContentV2.getBytes(UTF_8));
        GetThingShadowResponse shadowResponseV3 = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(shadowResponseV3.payload().asByteArray()).thenReturn(cloudShadowContentV3.getBytes(UTF_8));
        GetThingShadowResponse shadowResponseV4 = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(shadowResponseV4.payload().asByteArray()).thenReturn(cloudShadowContentV4.getBytes(UTF_8));

        JsonNode cloudDocument = JsonUtil.getPayloadJson(cloudShadowContentV1.getBytes(UTF_8)).get();
        JsonNode cloudDocumentV2 = JsonUtil.getPayloadJson(cloudShadowContentV2.getBytes(UTF_8)).get();
        JsonNode cloudDocumentV3 = JsonUtil.getPayloadJson(cloudShadowContentV3.getBytes(UTF_8)).get();
        JsonNode cloudDocumentV4 = JsonUtil.getPayloadJson(cloudShadowContentV4.getBytes(UTF_8)).get();

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        // verify initial full sync
        verify(syncQueue, after(7000).atMost(4)).put(any(FullShadowSyncRequest.class));
        assertEmptySyncQueue(clazz);

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));

        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(0L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(0L));

        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        lenient().when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenReturn(shadowResponse);
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudDocument));
        lenient().when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenReturn(shadowResponseV2);
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudDocumentV2));
        lenient().when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenReturn(shadowResponseV3);
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudDocumentV3));
        lenient().when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenReturn(shadowResponseV4);
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudDocumentV4));

        assertEmptySyncQueue(clazz);

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(4L)));
        assertThat("local version", () -> syncInfo.get().get().getLocalVersion(), eventuallyEval(greaterThanOrEqualTo(1L)));

        assertThat("local shadow exists", localShadow.get().isPresent(), is(true));
        ShadowDocument shadowDocument = localShadow.get().get();
        // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
        shadowDocument = new ShadowDocument(shadowDocument.getState(), null, null);
        assertThat(shadowDocument.toJson(false).get(SHADOW_DOCUMENT_STATE), is(cloudDocumentV4.get(SHADOW_DOCUMENT_STATE)));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), never()).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_synced_shadow_WHEN_local_delete_THEN_cloud_deletes(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context) throws InterruptedException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().deleteThingShadow(cloudDeleteThingShadowRequestCaptor.capture()))
                .thenReturn(mock(software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowResponse.class));

        GetThingShadowResponse shadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(shadowResponse.payload().asByteArray()).thenReturn(cloudShadowContentV10.getBytes(UTF_8));

        // return existing doc for full sync
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenReturn(shadowResponse);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        // we have a local shadow from the initial full sync
        assertThat("local shadow exists", () -> localShadow.get().isPresent(), eventuallyEval(is(true), Duration.ofSeconds(15)));
        assertEmptySyncQueue(clazz);

        DeleteThingShadowRequestHandler deleteHandler = shadowManager.getDeleteThingShadowRequestHandler();
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(MOCK_THING_NAME_1);
        request.setShadowName(CLASSIC_SHADOW);
        deleteHandler.handleRequest(request, "DoAll");

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud deleted", () -> syncInfo.get().get().isCloudDeleted(), eventuallyEval(is(true)));
        assertThat("cloud version", syncInfo.get().get().getCloudVersion(), is(11L));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(2L));
        assertThat("sync doc", syncInfo.get().get().getLastSyncedDocument(), is(nullValue()));

        assertThat("local shadow should not exist", localShadow.get().isPresent(), is(false));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), times(1)).deleteThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_synced_shadow_WHEN_cloud_delete_THEN_local_deletes(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context) throws InterruptedException, IoTDataPlaneClientCreationException {
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

        // wait for it to process all full sync requests to finish.
        assertEmptySyncQueue(clazz);

        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);

        // we have a local shadow from the initial full sync
        assertThat("local shadow exists", () -> localShadow.get().isPresent(), eventuallyEval(is(true)));
        assertThat("local shadow version", localShadow.get().get().getVersion(), is(1L));

        syncHandler.pushLocalDeleteSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, "{\"version\": 10}".getBytes(UTF_8));

        // Wait for Local Delete to finish.
        assertEmptySyncQueue(clazz);

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud deleted", () -> syncInfo.get().get().isCloudDeleted(), eventuallyEval(is(true)));

        assertThat("cloud version", syncInfo.get().get().getCloudVersion(), is(11L));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(2L));
        assertThat("sync doc", syncInfo.get().get().getLastSyncedDocument(), is(nullValue()));

        assertThat("local shadow should not exist", localShadow.get().isPresent(), is(false));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), never()).deleteThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_unsynced_shadow_WHEN_local_deletes_THEN_no_cloud_delete(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context) throws InterruptedException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().deleteThingShadow(cloudDeleteThingShadowRequestCaptor.capture()))
                .thenReturn(mock(software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowResponse.class));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);
        SyncHandler syncHandler = spy(kernel.getContext().get(SyncHandler.class));
        kernel.getContext().put(SyncHandler.class, syncHandler);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        assertEmptySyncQueue(clazz);

        ShadowManagerDAO dao = kernel.getContext().get(ShadowManagerDAOImpl.class);

        dao.updateShadowThing(MOCK_THING_NAME_1, RANDOM_SHADOW, localShadowContentV1.getBytes(UTF_8), 1L);

        DeleteThingShadowRequestHandler deleteHandler = shadowManager.getDeleteThingShadowRequestHandler();

        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(MOCK_THING_NAME_1);
        request.setShadowName(RANDOM_SHADOW);

        deleteHandler.handleRequest(request, "DoAll");
        verify(syncHandler, timeout(Duration.ofSeconds(10).toMillis()).times(1))
                .pushCloudDeleteSyncRequest(MOCK_THING_NAME_1, RANDOM_SHADOW);
        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), after(Duration.ofSeconds(5).toMillis()).never())
                .deleteThingShadow(any(software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_unsynced_shadow_WHEN_local_updates_THEN_no_cloud_update(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context) throws InterruptedException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);
        SyncHandler syncHandler = spy(kernel.getContext().get(SyncHandler.class));
        kernel.getContext().put(SyncHandler.class, syncHandler);
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
        verify(syncHandler, timeout(Duration.ofSeconds(10).toMillis()).times(1))
                .pushCloudUpdateSyncRequest(any(), any(), any());
        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), after(Duration.ofSeconds(5).toMillis()).never())
                .updateThingShadow(any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }


    static class TestException extends RuntimeException { // NOPMD

    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_cloud_update_request_WHEN_retryable_thrown_AND_new_cloud_update_request_THEN_retries_with_merged_request(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context)
            throws InterruptedException, IOException, IoTDataPlaneClientCreationException {
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
        AtomicReference<UpdateThingShadowRequestHandler> handler = new AtomicReference<>();
        CountDownLatch cdl = new CountDownLatch(2);
        // throw an exception first - before throwing we make another request so that there is another request for
        // to update this shadow in the queue
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenAnswer(invocation -> {
                    software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest r = invocation.getArgument(0);
                    ShadowDocument s = new ShadowDocument(r.payload().asByteArray());
                    if (s.getState().getReported().has("AnotherKey") &&
                    "2".equals(s.getState().getReported().get("OtherKey").toString())) {
                        cdl.countDown();
                        return UpdateThingShadowResponse.builder().payload(SdkBytes.fromString("{\"version\": 11}", UTF_8)).build();
                    }
                    if (handler.get() != null) {
                        // request #2 comes in as we are processing request #1
                        handler.get().handleRequest(request2, "DoAll");
                        handler.set(null);
                        cdl.countDown();
                    }
                    throw new RetryableException(new TestException());
                });

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        handler.set(shadowManager.getUpdateThingShadowRequestHandler());
        verify(syncQueue, after(7000).atMost(4)).put(any(FullShadowSyncRequest.class));
        assertEmptySyncQueue(clazz);


        // Fire the initial request
        handler.get().handleRequest(request1, "DoAll");
        assertThat("retried with merge request", cdl.await(10, TimeUnit.SECONDS), is(true));
        assertEmptySyncQueue(clazz);
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
        ((ObjectNode) actualNode).remove(SHADOW_DOCUMENT_METADATA);
        ((ObjectNode) expectedNode).remove(SHADOW_DOCUMENT_METADATA);

        // check that it is request 2 merged on top of request 1 and *not* request 1 merged on top of request 2
        assertThat(actualNode, is(equalTo(expectedNode)));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_multiple_cloud_updates_WHEN_updates_received_out_of_order_THEN_local_shadow_is_updated(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context)
            throws InterruptedException, IOException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ConflictError.class);

        String initialCloudState = "{\"version\":1,\"state\":{\"desired\":{}}}";
        String cloudUpdate1 = "{\"version\":2,\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
        String cloudUpdate2 = "{\"version\":3,\"state\":{\"desired\":{\"OtherKey\":\"foo\"}}}";
        String finalCloudState = "{\"version\":3,\"state\":{\"desired\":{\"SomeKey\":\"foo\",\"OtherKey\":\"foo\"}}}";
        String expectedLocalShadowState = "{\"state\":{\"desired\":{\"SomeKey\":\"foo\",\"OtherKey\":\"foo\"}}}";

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);

        // setup initial cloud state
        GetThingShadowResponse initialCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(initialCloudStateShadowResponse.payload().asByteArray()).thenReturn(initialCloudState.getBytes(UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class))).thenReturn(initialCloudStateShadowResponse);

        GetThingShadowResponse finalCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(finalCloudStateShadowResponse.payload().asByteArray()).thenReturn(finalCloudState.getBytes(UTF_8));

        JsonNode cloudUpdateDocument = JsonUtil.getPayloadJson(cloudUpdate1.getBytes(UTF_8)).get();
        JsonNode cloudUpdateDocument2 = JsonUtil.getPayloadJson(cloudUpdate2.getBytes(UTF_8)).get();

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        // verify sync info is empty
        assertEmptySyncQueue(clazz);
        verify(syncQueue, after(7000).atMost(4)).put(any(FullShadowSyncRequest.class));
        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(1L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));

        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        CountDownLatch cdl = new CountDownLatch(1);
        // at this point:
        //  * two cloud updates have happened. So far the updates haven't been received, but they will be received out of order
        //  * cloud shadow reflects shadow state after two updates
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenAnswer((i)->{
                    cdl.countDown();
                    return finalCloudStateShadowResponse;
                });

        // receive cloud update 2 of 2 (out of order)
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudUpdateDocument2));
        assertThat("processed first cloud update request", cdl.await(5, TimeUnit.SECONDS));
        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(3L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(2L));
        assertLocalShadowEquals(expectedLocalShadowState);

        // receive cloud update 1 of 2 (out of order)
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudUpdateDocument));
        assertEmptySyncQueue(clazz);
        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(3L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(2L));
        assertLocalShadowEquals(expectedLocalShadowState);
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_multiple_cloud_updates_WHEN_updates_received_out_of_order_and_merged_THEN_local_shadow_is_updated(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context)
            throws InterruptedException, IOException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ConflictError.class);

        String initialCloudState = "{\"version\":1,\"state\":{\"desired\":{}}}";
        String cloudUpdate = "{\"version\":2,\"state\":{\"desired\":{\"SomeKey\":\"bar\"}}}";
        String cloudUpdate2 = "{\"version\":3,\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
        String cloudUpdate3 = "{\"version\":4,\"state\":{\"desired\":{\"OtherKey\":\"foo\"}}}";
        String finalCloudState = "{\"version\":4,\"state\":{\"desired\":{\"SomeKey\":\"foo\",\"OtherKey\":\"foo\"}}}";
        String expectedLocalShadowState = "{\"state\":{\"desired\":{\"SomeKey\":\"foo\",\"OtherKey\":\"foo\"}}}";

        CountDownLatch outOfOrderRequestsHaveBeenQueued = new CountDownLatch(1);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);

        // setup initial cloud state
        GetThingShadowResponse initialCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(initialCloudStateShadowResponse.payload().asByteArray()).thenReturn(initialCloudState.getBytes(UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class))).thenReturn(initialCloudStateShadowResponse);

        GetThingShadowResponse finalCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(finalCloudStateShadowResponse.payload().asByteArray()).thenReturn(finalCloudState.getBytes(UTF_8));

        JsonNode cloudUpdateDocument = JsonUtil.getPayloadJson(cloudUpdate.getBytes(UTF_8)).get();
        JsonNode cloudUpdateDocument2 = JsonUtil.getPayloadJson(cloudUpdate2.getBytes(UTF_8)).get();
        JsonNode cloudUpdateDocument3 = JsonUtil.getPayloadJson(cloudUpdate3.getBytes(UTF_8)).get();

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        // verify initial full sync
        verify(syncQueue, after(7000).atMost(4)).put(any(FullShadowSyncRequest.class));
        assertEmptySyncQueue(clazz);
        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(1L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));

        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);

        // block cloud update 1 of 3 to ensure the remaining two updates
        // are merged when they are received.
        lenient().when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse)
                .thenAnswer(invocationOnMock -> {
                    assertTrue(outOfOrderRequestsHaveBeenQueued.await(5000L, TimeUnit.SECONDS));
                    return mockUpdateThingShadowResponse;
                })
                .thenReturn(mockUpdateThingShadowResponse);
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudUpdateDocument));
        verify(syncQueue, timeout(5000).atLeast(1)).put(any(CloudUpdateSyncRequest.class));
        assertEmptySyncQueue(clazz);

        // cloud state to return on full sync when version conflict is detected below
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenReturn(finalCloudStateShadowResponse);

        // receive cloud update 3 of 3 (out of order)
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudUpdateDocument3));
        // receive cloud update 2 of 3 (out of order)
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudUpdateDocument2));
        outOfOrderRequestsHaveBeenQueued.countDown();

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertLocalShadowEquals(expectedLocalShadowState);
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_multiple_local_updates_WHEN_updates_received_out_of_order_THEN_local_shadow_and_sync_info_are_updated(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context)
            throws InterruptedException, IOException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ConflictError.class);

        String initialCloudState = "{\"version\":1,\"state\":{\"desired\":{}}}";
        String localUpdate1 = "{\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
        String localUpdate2 = "{\"state\":{\"desired\":{\"OtherKey\":\"foo\"}}}";
        String finalLocalState = "{\"state\":{\"desired\":{\"SomeKey\":\"foo\",\"OtherKey\":\"foo\"}}}";

        UpdateThingShadowRequest updateRequest1 = new UpdateThingShadowRequest();
        updateRequest1.setThingName(MOCK_THING_NAME_1);
        updateRequest1.setShadowName(CLASSIC_SHADOW);
        updateRequest1.setPayload(localUpdate1.getBytes(UTF_8));

        UpdateThingShadowRequest updateRequest2 = new UpdateThingShadowRequest();
        updateRequest2.setThingName(MOCK_THING_NAME_1);
        updateRequest2.setShadowName(CLASSIC_SHADOW);
        updateRequest2.setPayload(localUpdate2.getBytes(UTF_8));

        when(mockUpdateThingShadowResponse.payload())
                .thenReturn(SdkBytes.fromString("{\"version\": 2}", UTF_8))
                .thenReturn(SdkBytes.fromString("{\"version\": 3}", UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);

        // setup initial cloud state
        // which will be used as initial local state during full sync on startup
        GetThingShadowResponse initialCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(initialCloudStateShadowResponse.payload().asByteArray()).thenReturn(initialCloudState.getBytes(UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class))).thenReturn(initialCloudStateShadowResponse);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        // verify initial state
        assertEmptySyncQueue(clazz);
        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(1L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));

        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        // receive local update 2 of 2 (out of order)
        updateHandler.handleRequest(updateRequest2, "DoAll");
        assertEmptySyncQueue(clazz);
        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(2L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(2L));
        assertLocalShadowEquals(localUpdate2);

        // receive local update 1 of 2 (out of order)
        updateHandler.handleRequest(updateRequest1, "DoAll");
        assertEmptySyncQueue(clazz);
        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(3L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(3L));
        assertLocalShadowEquals(finalLocalState);
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_bidirectional_updates_WHEN_updates_merged_THEN_full_shadow_sync(Class<? extends BaseSyncStrategy> clazz, ExtensionContext context)
            throws InterruptedException, IOException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ConflictError.class);
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);

        String initialCloudState = "{\"version\":1,\"state\":{\"desired\":{}}}";
        String blockingCloudUpdate = "{\"version\":1,\"state\":{\"desired\":{\"SomeKey\":\"bar\"}}}";
        String cloudUpdate = "{\"version\":2,\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
        String localUpdate = "{\"state\":{\"desired\":{\"OtherKey\":\"foo\"}}}";
        String finalCloudState = "{\"version\":3,\"state\":{\"desired\":{\"SomeKey\":\"foo\",\"OtherKey\":\"foo\"}}}";
        String expectedLocalShadowState = "{\"state\":{\"desired\":{\"SomeKey\":\"foo\",\"OtherKey\":\"foo\"}}}";

        GetThingShadowResponse initialCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(initialCloudStateShadowResponse.payload().asByteArray()).thenReturn(initialCloudState.getBytes(UTF_8));

        GetThingShadowResponse blockingCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(blockingCloudStateShadowResponse.payload().asByteArray()).thenReturn(blockingCloudUpdate.getBytes(UTF_8));

        GetThingShadowResponse updateCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(updateCloudStateShadowResponse.payload().asByteArray()).thenReturn(cloudUpdate.getBytes(UTF_8));

        GetThingShadowResponse finalCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(finalCloudStateShadowResponse.payload().asByteArray()).thenReturn(finalCloudState.getBytes(UTF_8));

        CountDownLatch bidirectionalRequestsHaveBeenQueued = new CountDownLatch(1);
        CountDownLatch finalCloudRequestProcessed = new CountDownLatch(1);
        CountDownLatch executingBlockingCloudUpdate = new CountDownLatch(1);
        when(iotDataPlaneClientFactory.getIotDataPlaneClient()
                .getThingShadow(any(GetThingShadowRequest.class)))
                .thenReturn(initialCloudStateShadowResponse)
                .thenReturn(finalCloudStateShadowResponse);

        lenient().when(mockUpdateThingShadowResponse.payload())
                .thenReturn(SdkBytes.fromString("{}", UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenAnswer(unused -> {
                    byte[] payload = cloudUpdateThingShadowRequestCaptor.getValue().payload().asByteArray();
                    ShadowDocument doc = new ShadowDocument(payload);
                    ShadowDocument finalCloudStateDoc = new ShadowDocument(finalCloudState.getBytes(UTF_8));
                    ShadowDocument blockingCloudUpdateDoc = new ShadowDocument(blockingCloudUpdate.getBytes(UTF_8));
                    if (doc.getState().toJson().equals(finalCloudStateDoc.getState().toJson())) {
                        finalCloudRequestProcessed.countDown();
                    }
                    if (doc.getState().toJson().equals(blockingCloudUpdateDoc.getState().toJson())) {
                        executingBlockingCloudUpdate.countDown();
                        assertTrue(bidirectionalRequestsHaveBeenQueued.await(5000L, TimeUnit.SECONDS));
                    }
                    return mockUpdateThingShadowResponse;
                });

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        // wait for initial full sync to complete
        verify(syncQueue, after(7000).atMost(4)).put(any(FullShadowSyncRequest.class));
        assertEmptySyncQueue(clazz);

        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        // send a cloud update that takes a while to execute,
        // so that subsequent requests can pile up in the queue
        UpdateThingShadowRequest blockingRequest = new UpdateThingShadowRequest();
        blockingRequest.setThingName(MOCK_THING_NAME_1);
        blockingRequest.setShadowName(CLASSIC_SHADOW);
        blockingRequest.setPayload(blockingCloudUpdate.getBytes(UTF_8));
        updateHandler.handleRequest(blockingRequest, "DoAll");
        // If the next cloud request comes in before this one is executed, both of the cloud requests are merged.
        // But the test is to verify merging of cloud and local requests.
        assertThat("executing blocking cloud update request", executingBlockingCloudUpdate
                .await(5000L, TimeUnit.SECONDS));

        // receive cloud update
        UpdateThingShadowRequest updateRequest = new UpdateThingShadowRequest();
        updateRequest.setThingName(MOCK_THING_NAME_1);
        updateRequest.setShadowName(CLASSIC_SHADOW);
        updateRequest.setPayload(localUpdate.getBytes(UTF_8));
        updateHandler.handleRequest(updateRequest, "DoAll");

        // receive local update
        JsonNode cloudUpdateDocument = JsonUtil.getPayloadJson(cloudUpdate.getBytes(UTF_8)).get();
        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudUpdateDocument));

        // make sure that the requests merge as expected
        verify(syncQueue, timeout(7000).atLeast(1)).put(any(FullShadowSyncRequest.class));
        bidirectionalRequestsHaveBeenQueued.countDown();
        assertThat("all requests processed", finalCloudRequestProcessed.await(10, TimeUnit.SECONDS));
        assertLocalShadowEquals(expectedLocalShadowState);

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(4L)));
        assertThat("local version", () -> syncInfo.get().get().getLocalVersion(), eventuallyEval(is(4L)));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_bidirectional_update_not_fully_necessary_WHEN_full_sync_executed_THEN_update_only_one_direction(Class<? extends BaseSyncStrategy> clazz, ExtensionContext context)
            throws InterruptedException, IOException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ConflictError.class);
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);

        String initialCloudState = "{\"version\":1,\"state\":{\"desired\":{}}}";
        String blockingCloudUpdate = "{\"version\":1,\"state\":{\"desired\":{\"SomeKey\":\"bar\"}}}";
        String cloudUpdate = "{\"version\":2,\"state\":{\"desired\":{\"SomeKey\":\"bar\"}}}";
        String localUpdate = "{\"state\":{\"desired\":{\"OtherKey\":\"foo\"}}}";
        String finalCloudState = "{\"version\":3,\"state\":{\"desired\":{\"SomeKey\":\"bar\",\"OtherKey\":\"bar\"}}}";
        String expectedLocalShadowState = "{\"state\":{\"desired\":{\"SomeKey\":\"bar\",\"OtherKey\":\"foo\"}}}";

        GetThingShadowResponse initialCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(initialCloudStateShadowResponse.payload().asByteArray()).thenReturn(initialCloudState.getBytes(UTF_8));

        GetThingShadowResponse blockingCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(blockingCloudStateShadowResponse.payload().asByteArray()).thenReturn(blockingCloudUpdate.getBytes(UTF_8));

        GetThingShadowResponse updateCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(updateCloudStateShadowResponse.payload().asByteArray()).thenReturn(cloudUpdate.getBytes(UTF_8));

        GetThingShadowResponse finalCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(finalCloudStateShadowResponse.payload().asByteArray()).thenReturn(finalCloudState.getBytes(UTF_8));

        CountDownLatch bidirectionalRequestsHaveBeenQueued = new CountDownLatch(1);
        CountDownLatch finalCloudRequestProcessed = new CountDownLatch(1);
        CountDownLatch executingBlockingCloudUpdate = new CountDownLatch(1);
        when(iotDataPlaneClientFactory.getIotDataPlaneClient()
                .getThingShadow(any(GetThingShadowRequest.class)))
                .thenReturn(initialCloudStateShadowResponse)
                .thenReturn(finalCloudStateShadowResponse);

        lenient().when(mockUpdateThingShadowResponse.payload())
                .thenReturn(SdkBytes.fromString("{}", UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenAnswer(unused -> {
                    ShadowDocument doc = new ShadowDocument(cloudUpdateThingShadowRequestCaptor.getValue().payload().asByteArray());
                    ShadowDocument blockedShadowDocument = new ShadowDocument(blockingCloudUpdate.getBytes(UTF_8));
                    if (doc.getState().getDesired().has("OtherKey")) {
                        finalCloudRequestProcessed.countDown();
                    }
                    if (doc.toJson(true).equals(blockedShadowDocument.toJson(true))) {
                        executingBlockingCloudUpdate.countDown();
                        assertTrue(bidirectionalRequestsHaveBeenQueued.await(5000L, TimeUnit.SECONDS));
                    }
                    return mockUpdateThingShadowResponse;
                });

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        // wait for initial full sync to complete
        verify(syncQueue, after(7000).atMost(4)).put(any(FullShadowSyncRequest.class));
        assertEmptySyncQueue(clazz);

        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        // send a cloud update that takes a while to execute,
        // so that subsequent requests can pile up in the queue
        UpdateThingShadowRequest blockingRequest = new UpdateThingShadowRequest();
        blockingRequest.setThingName(MOCK_THING_NAME_1);
        blockingRequest.setShadowName(CLASSIC_SHADOW);
        blockingRequest.setPayload(blockingCloudUpdate.getBytes(UTF_8));
        updateHandler.handleRequest(blockingRequest, "DoAll");
        // If the next cloud request comes in before this one is executed, both of the cloud requests are merged.
        // But the test is to verify merging of cloud and local requests.
        assertThat("executing blocking cloud update request", executingBlockingCloudUpdate
                .await(5000L, TimeUnit.SECONDS));

        // receive cloud update
        UpdateThingShadowRequest updateRequest = new UpdateThingShadowRequest();
        updateRequest.setThingName(MOCK_THING_NAME_1);
        updateRequest.setShadowName(CLASSIC_SHADOW);
        updateRequest.setPayload(localUpdate.getBytes(UTF_8));
        updateHandler.handleRequest(updateRequest, "DoAll");

        // receive local update
        JsonNode cloudUpdateDocument = JsonUtil.getPayloadJson(cloudUpdate.getBytes(UTF_8)).get();
        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudUpdateDocument));

        // make sure that the requests merge as expected
        verify(syncQueue, timeout(7000).atLeast(1)).put(any(FullShadowSyncRequest.class));
        bidirectionalRequestsHaveBeenQueued.countDown();
        assertThat("all requests processed", finalCloudRequestProcessed.await(10, TimeUnit.SECONDS));
        assertLocalShadowEquals(expectedLocalShadowState);

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(3L)));
        assertThat("local version", () -> syncInfo.get().get().getLocalVersion(), eventuallyEval(is(3L)));
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_cloud_shadow_state_empty_WHEN_shadow_manager_syncs_THEN_local_shadow_is_cleared(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context)
            throws InterruptedException, IOException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ConflictError.class);

        String initialCloudState = "{\"version\":1,\"state\":{}}";
        String expectedLocalShadowState = "{\"state\":{}}";

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);

        // setup initial cloud state
        GetThingShadowResponse initialCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(initialCloudStateShadowResponse.payload().asByteArray()).thenReturn(initialCloudState.getBytes(UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class))).thenReturn(initialCloudStateShadowResponse);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        // verify initial full sync
        verify(syncQueue, after(7000).atMost(4)).put(any(FullShadowSyncRequest.class));
        assertEmptySyncQueue(clazz);
        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(1L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));
        assertLocalShadowEquals(expectedLocalShadowState);
    }

    @ParameterizedTest
    @ValueSource(classes = {RealTimeSyncStrategy.class, PeriodicSyncStrategy.class})
    void GIVEN_local_shadow_state_empty_WHEN_shadow_manager_syncs_THEN_cloud_shadow_is_cleared(Class<?extends BaseSyncStrategy> clazz, ExtensionContext context)
            throws InterruptedException, IOException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ConflictError.class);

        String initialCloudState = "{\"version\":1,\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
        String initialLocalState = "{\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
        String localUpdate1 = "{\"state\":{}}";
        String finalLocalState = "{\"state\":{}}";

        UpdateThingShadowRequest updateRequest1 = new UpdateThingShadowRequest();
        updateRequest1.setThingName(MOCK_THING_NAME_1);
        updateRequest1.setShadowName(CLASSIC_SHADOW);
        updateRequest1.setPayload(localUpdate1.getBytes(UTF_8));

        when(mockUpdateThingShadowResponse.payload())
                .thenReturn(SdkBytes.fromString("{\"version\": 2}", UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);

        // setup initial cloud state
        // which will be used as initial local state during full sync on startup
        GetThingShadowResponse initialCloudStateShadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(initialCloudStateShadowResponse.payload().asByteArray()).thenReturn(initialCloudState.getBytes(UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class))).thenReturn(initialCloudStateShadowResponse);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(getSyncConfigFile(clazz))
                .syncClazz(clazz)
                .mockCloud(true)
                .build());

        // verify initial state
        assertEmptySyncQueue(clazz);
        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(1L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));
        assertLocalShadowEquals(initialLocalState);

        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        updateHandler.handleRequest(updateRequest1, "DoAll");
        assertEmptySyncQueue(clazz);
        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(2L)));
        assertThat("local version", () -> syncInfo.get().get().getLocalVersion(), eventuallyEval(is(2L)));
        assertLocalShadowEquals(finalLocalState);
    }

    private void assertLocalShadowEquals(String state) throws IOException {
        System.out.println(getLocalShadowState());
        assertThat(this::getLocalShadowState, eventuallyEval(is(JsonUtil.getPayloadJson(state.getBytes(UTF_8)).get().get(SHADOW_DOCUMENT_STATE))));
    }

    private JsonNode getLocalShadowState() {
        if (!localShadow.get().isPresent()) {
            return null;
        }
        // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
        ShadowDocument shadowDocument = new ShadowDocument(localShadow.get().get().getState(), null, null);
        return shadowDocument.toJson(false).get(SHADOW_DOCUMENT_STATE);
    }
}
