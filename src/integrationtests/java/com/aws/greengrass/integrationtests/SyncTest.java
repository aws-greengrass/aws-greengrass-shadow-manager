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
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class SyncTest extends GGServiceTestUtil  {
    private static final long TEST_TIME_OUT_SEC = 30L;
    public static final String MOCK_THING_NAME = "Thing1";
    public static final String CLASSIC_SHADOW = "";
    private static final String cloudShadowContentV10 = "{\"version\":10,\"state\":{\"desired\":{\"SomeKey\":\"foo\"}}}";
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
        kernel.launch();

        assertTrue(shadowManagerRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_sync_config_and_no_local_WHEN_startup_THEN_local_version_updated_via_full_sync(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, InterruptedException.class);

        GetThingShadowResponse shadowResponse = mock(GetThingShadowResponse.class, Answers.RETURNS_DEEP_STUBS);
        lenient().when(shadowResponse.payload().asByteArray()).thenReturn(cloudShadowContentV10.getBytes(UTF_8));

        // existing document
        when(iotDataPlaneClientFactory.getIotDataPlaneClient()
                .getThingShadow(any(GetThingShadowRequest.class))).thenReturn(shadowResponse);
        startNucleusWithConfig("sync.yaml");

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

    void eventually(Supplier<Void> supplier, long timeout, ChronoUnit unit) throws InterruptedException {
        Instant expire = Instant.now().plus(Duration.of(timeout, unit));
        while (expire.isAfter(Instant.now())) {
            try {
                supplier.get();
                return;
            } catch (AssertionError e) {
                // ignore
            }
            Thread.sleep(500);
        }
        supplier.get();
    }

    @Disabled
    @Test
    void GIVEN_sync_config_and_no_cloud_WHEN_startup_THEN_cloud_version_updated_via_full_sync() {

    }

    @Disabled
    @Test
    void GIVEN_synced_shadow_WHEN_local_update_THEN_cloud_updates() {

    }

    @Disabled
    @Test
    void GIVEN_synced_shadow_WHEN_cloud_update_THEN_local_updates() {

    }

    @Disabled
    @Test
    void GIVEN_synced_shadow_WHEN_local_delete_THEN_cloud_deletes() {

    }

    @Disabled
    @Test
    void GIVEN_synced_shadow_WHEN_cloud_delete_THEN_local_deletes() {

    }

    @Disabled
    @Test
    void GIVEN_unsynced_shadow_WHEN_cloud_updates_THEN_no_local_update() {

    }
    @Disabled
    @Test
    void GIVEN_unsynced_shadow_WHEN_local_updates_THEN_no_cloud_update() {

    }
}
