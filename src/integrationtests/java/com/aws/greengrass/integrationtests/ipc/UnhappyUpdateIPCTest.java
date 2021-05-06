/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.ipc;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.shadowmanager.ShadowManager;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.aws.greengrass.util.Coerce;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_DOCUMENT_SIZE;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class UnhappyUpdateIPCTest extends GGServiceTestUtil {
    private static final long TEST_TIME_OUT_SEC = 30L;
    public static final String MOCK_THING_NAME = "Thing1";
    public static final String CLASSIC_SHADOW = "";
    private static final String shadowContentFormat = "{\"state\":{\"desired\":{\"SomeKey\":\"%s\"}},\"metadata\":{}}";

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

    private String getStringWithLengthAndFilledWithCharacter(int length, char charToFill) {
        if (length > 0) {
            char[] array = new char[length];
            Arrays.fill(array, charToFill);
            return new String(array);
        }
        return "";
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

    @Test
    void GIVEN_non_default_max_shadow_size_WHEN_update_shadow_THEN_throws_invalid_arguments_error(ExtensionContext context) throws InterruptedException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);

        startNucleusWithConfig("shadow.yaml");

        shadowManager.getConfig().lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC).withValue(20 * 1024);
        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(MOCK_THING_NAME);
        request.setShadowName(CLASSIC_SHADOW);
        request.setPayload(new byte[20 * 1024 + 1]);
        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () ->
                updateHandler.handleRequest(request, "DoAll"));
        assertThat(thrown.getMessage(), is(ErrorMessage.PAYLOAD_TOO_LARGE_MESSAGE.getMessage()));
    }

    @Test
    void GIVEN_non_default_max_shadow_size_WHEN_update_shadow_document_size_and_update_shadow_THEN_throws_invalid_arguments_error(ExtensionContext context) throws InterruptedException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);

        startNucleusWithConfig("shadow.yaml");

        shadowManager.getConfig().lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC).withValue(20 * 1024);
        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        eventually(() -> {
            assertThat(Coerce.toInt(shadowManager.getConfig().lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC)), is(20 * 1024));
            assertThat(Coerce.toInt(updateHandler.getMaxShadowSize()), is(20 * 1024));
            return null;
        }, 10, ChronoUnit.SECONDS);


        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(MOCK_THING_NAME);
        request.setShadowName(CLASSIC_SHADOW);
        request.setPayload(String.format(shadowContentFormat, getStringWithLengthAndFilledWithCharacter(DEFAULT_DOCUMENT_SIZE + 1, '*')).getBytes(UTF_8));
        assertDoesNotThrow(() -> updateHandler.handleRequest(request, "DoAll"));

        shadowManager.getConfig().remove();
        eventually(() -> {
            assertThat(Coerce.toInt(updateHandler.getMaxShadowSize()), is(DEFAULT_DOCUMENT_SIZE));
            return null;
        }, 10, ChronoUnit.SECONDS);


        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () ->
                updateHandler.handleRequest(request, "DoAll"));
        assertThat(thrown.getMessage(), is(ErrorMessage.PAYLOAD_TOO_LARGE_MESSAGE.getMessage()));
    }
}
