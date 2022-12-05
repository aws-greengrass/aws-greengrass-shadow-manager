/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logging.impl.config.LogConfig;
import com.aws.greengrass.shadowmanager.ShadowManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.sync.strategy.RealTimeSyncStrategy;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.slf4j.event.Level;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class SyncDirectionalityTest extends NucleusLaunchUtils {
    public static final String MOCK_THING_NAME_1 = "Thing1";
    public static final String CLASSIC_SHADOW = "";

    private Supplier<Optional<SyncInformation>> syncInfo;
    private Supplier<Optional<ShadowDocument>> localShadow;
    private static final String localShadowContentV1 = "{\"state\":{ \"desired\": { \"SomeKey\": \"foo\"}, "
            + "\"reported\":{\"SomeKey\":\"bar\",\"OtherKey\": 1}}}";
    private static final String cloudShadowContentV1 = "{\"version\":1,\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";

    @Mock
    UpdateThingShadowResponse mockUpdateThingShadowResponse;

    @Captor
    private ArgumentCaptor<UpdateThingShadowRequest> cloudUpdateThingShadowRequestCaptor;

    @BeforeEach
    void setup() {
        LogConfig.getRootLogConfig().setLevel(Level.DEBUG);
        // Set this property for kernel to scan its own classpath to find plugins
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");
        kernel = new Kernel();
        syncInfo = () -> kernel.getContext().get(ShadowManagerDAOImpl.class).getShadowSyncInformation(MOCK_THING_NAME_1,
                CLASSIC_SHADOW);
        localShadow = () -> kernel.getContext().get(ShadowManagerDAOImpl.class).getShadowThing(MOCK_THING_NAME_1,
                CLASSIC_SHADOW);
        reset(iotDataPlaneClientFactory);
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"sync_directionality_bidirectional.yaml", "sync_directionality_fromdeviceonly.yaml"})
    void GIVEN_cloud_sync_enabled_WHEN_local_update_THEN_syncs_shadow_to_cloud(String configFileName, ExtensionContext context) throws InterruptedException, IOException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ResourceNotFoundException.class);

        // no shadow exists in cloud
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        // mock response to update cloud
        when(mockUpdateThingShadowResponse.payload()).thenReturn(SdkBytes.fromString("{\"version\": 1}", UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(configFileName)
                .mockCloud(true)
                .resetRetryConfig(false)
                .build());

        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        // update local shadow
        software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest request = new software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest();
        request.setThingName(MOCK_THING_NAME_1);
        request.setShadowName(CLASSIC_SHADOW);
        request.setPayload(localShadowContentV1.getBytes(UTF_8));

        updateHandler.handleRequest(request, "DoAll");
        assertEmptySyncQueue(RealTimeSyncStrategy.class);

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
                any(UpdateThingShadowRequest.class));
    }

    @Test
    void GIVEN_cloud_sync_not_enabled_WHEN_local_update_THEN_does_not_sync_shadow_to_cloud(ExtensionContext context) throws InterruptedException, IOException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, ResourceNotFoundError.class);
        CountDownLatch cdl = new CountDownLatch(4); // 4 shadows in the config file
        // no shadow exists in cloud
        when(iotDataPlaneClientFactory.getIotDataPlaneClient()
                .getThingShadow(any(GetThingShadowRequest.class))).thenAnswer(invocation -> {
            cdl.countDown();
            throw ResourceNotFoundException.builder().build();
        });
        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile("sync_directionality_fromcloudonly.yaml")
                .mockCloud(true)
                .resetRetryConfig(false)
                .build());
        shadowManager.startSyncingShadows(ShadowManager.StartSyncInfo.builder().build());
        // There is a race condition which can cause us to check queue is empty before having inserted any full sync
        // requests in it. This check avoids that.
        assertThat("all the sync requests are processed", cdl.await(10, TimeUnit.SECONDS), is(true));
        assertEmptySyncQueue(RealTimeSyncStrategy.class);
        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        // update local shadow
        software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest request = new software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest();
        request.setThingName(MOCK_THING_NAME_1);
        request.setShadowName(CLASSIC_SHADOW);
        request.setPayload(localShadowContentV1.getBytes(UTF_8));

        updateHandler.handleRequest(request, "DoAll");
        assertEmptySyncQueue(RealTimeSyncStrategy.class);

        assertThat("local shadow exists", () -> localShadow.get().isPresent(), eventuallyEval(is(true)));
        ShadowDocument shadowDocument = localShadow.get().get();
        // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
        shadowDocument = new ShadowDocument(shadowDocument.getState(), null, null);
        JsonNode v1 = new ShadowDocument(localShadowContentV1.getBytes(UTF_8)).toJson(false);
        assertThat(shadowDocument.toJson(false), is(v1));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), never()).updateThingShadow(
                any(UpdateThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"sync_directionality_bidirectional.yaml", "sync_directionality_fromcloudonly.yaml"})
    void GIVEN_device_sync_enabled_WHEN_local_update_THEN_syncs_shadow_to_cloud(String configFileName, ExtensionContext context) throws InterruptedException, IOException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, ResourceNotFoundError.class);
        ignoreExceptionOfType(context, ResourceNotFoundException.class);

        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        JsonNode cloudDocument = JsonUtil.getPayloadJson(cloudShadowContentV1.getBytes(UTF_8)).get();

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(configFileName)
                .mockCloud(true)
                .resetRetryConfig(false)
                .build());

        shadowManager.startSyncingShadows(ShadowManager.StartSyncInfo.builder().build());
        assertEmptySyncQueue(RealTimeSyncStrategy.class);

        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudDocument));
        assertEmptySyncQueue(RealTimeSyncStrategy.class);

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(1L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));

        assertThat("local shadow exists", localShadow.get().isPresent(), is(true));
        ShadowDocument shadowDocument = localShadow.get().get();
        // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
        shadowDocument = new ShadowDocument(shadowDocument.getState(), null, null);
        assertThat(shadowDocument.toJson(false).get(SHADOW_DOCUMENT_STATE), is(cloudDocument.get(SHADOW_DOCUMENT_STATE)));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), never()).updateThingShadow(
                any(UpdateThingShadowRequest.class));
    }

    @Test
    void GIVEN_device_sync_not_enabled_WHEN_local_update_THEN_does_not_sync_shadow_to_cloud() throws InterruptedException, IOException {
        JsonNode cloudDocument = JsonUtil.getPayloadJson(cloudShadowContentV1.getBytes(UTF_8)).get();

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile("sync_directionality_fromdeviceonly.yaml")
                .mockCloud(true)
                .resetRetryConfig(false)
                .build());
        shadowManager.startSyncingShadows(ShadowManager.StartSyncInfo.builder().build());
        assertEmptySyncQueue(RealTimeSyncStrategy.class);

        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        syncHandler.pushLocalUpdateSyncRequest(MOCK_THING_NAME_1, CLASSIC_SHADOW, JsonUtil.getPayloadBytes(cloudDocument));
        assertEmptySyncQueue(RealTimeSyncStrategy.class);

        TimeUnit.SECONDS.sleep(2);

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));
        assertThat("local shadow exists", () -> localShadow.get().isPresent(), eventuallyEval(is(false)));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), never()).updateThingShadow(
                any(UpdateThingShadowRequest.class));
    }
}
