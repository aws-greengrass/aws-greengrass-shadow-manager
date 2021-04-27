/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.ipc;

import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.deployment.exceptions.DeviceConfigurationException;
import com.aws.greengrass.integrationtests.util.ConfigPlatformResolver;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.NoOpPathOwnershipHandler;
import com.aws.greengrass.testcommons.testutilities.UniqueRootPathExtension;
import com.aws.greengrass.util.Pair;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClient;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ListNamedShadowsForThingRequest;
import software.amazon.awssdk.aws.greengrass.model.ListNamedShadowsForThingResponse;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;
import software.amazon.awssdk.eventstreamrpc.EventStreamRPCConnection;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.aws.greengrass.integrationtests.ipc.IPCTestUtils.subscribeToTopicOveripcForBinaryMessages;
import static com.aws.greengrass.shadowmanager.model.Constants.ERROR_CODE_FIELD_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.ERROR_MESSAGE_FIELD_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_METADATA;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_CURRENT;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_PREVIOUS;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_TIMESTAMP;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionUltimateCauseOfType;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionUltimateCauseWithMessage;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionWithMessage;
import static com.aws.greengrass.testcommons.testutilities.TestUtils.asyncAssertOnConsumer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith({GGExtension.class, UniqueRootPathExtension.class})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ShadowIPCTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int TIMEOUT_FOR_PUBSUB_SECONDS = 10;
    public static final String MOCK_THING_NAME = "mockThing";
    public static final String SHADOW_NAME_1 = "testShadowName";
    private static Kernel kernel;
    private static final String nullVersionErrorMessage = "Invalid JSON\\nInvalid version. instance type (null) does not match any allowed primitive type (allowed: [\\\"integer\\\",\\\"number\\\"])";
    private static final String expectedShadowDocumentV1Str = "{\"version\":1,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}";
    private static final String expectedShadowDocumentV2Str = "{\"state\":{\"desired\":{\"color\":{\"r\":0,\"a\":255,\"g\":255},\"NewArray\":[1,2,3,5,8,13]},\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}},\"version\":2}";
    private static final String expectedShadowDocumentV3Str = "{\"version\":3,\"state\":{\"reported\":{\"color\":{\"r\":0,\"g\":255,\"b\":255,\"a\":255},\"SomeKey\":\"SomeValue\",\"NewArray\": [1,2,3,5,8,13]}}}";
    private static final String expectedAcceptedMessageV2Str = "{\"version\":2,\"state\":{\"desired\":{\"color\":{\"r\":0,\"a\":255,\"g\":255},\"NewArray\":[1,2,3,5,8,13]}}}";
    private static final String expectedAcceptedMessageV3Str = "{\"version\":3,\"state\":{\"desired\":null,\"reported\":{\"color\":{\"r\":0,\"a\":255,\"g\":255},\"NewArray\":[1,2,3,5,8,13]}}}";
    private static final String expectedDeltaMessageV2Str = "{\"version\":2,\"state\":{\"color\":{\"r\":0,\"a\":255},\"NewArray\":[1,2,3,5,8,13]}}";
    private static final String invalidVersionMessageStr = "{\"code\":409,\"message\":\"Version conflict\"}";
    private static final String invalidVersionMessageStr2 = "{\"code\":400,\"message\": \"" + nullVersionErrorMessage +"\"}";
    private static JsonNode expectedShadowDocumentV1Json;
    private static JsonNode expectedShadowDocumentV2Json;
    private static JsonNode expectedAcceptedMessageV2Json;
    private static JsonNode expectedDeltaMessageV2Json;
    private static JsonNode expectedShadowDocumentV3Json;
    private static JsonNode expectedAcceptedMessageV3Json;
    private static JsonNode invalidVersionMessageJson;
    private static JsonNode invalidVersionMessageJson2;


    static Stream<Arguments> invalidVersionUpdateTests() {
        return Stream.of(
                arguments(0, invalidVersionMessageJson),
                arguments(4, invalidVersionMessageJson),
                arguments(null, invalidVersionMessageJson2)
        );
    }

    static Stream<Arguments> invalidGetShadowTests() {
        return Stream.of(
                arguments("BadThingName", SHADOW_NAME_1),
                arguments(MOCK_THING_NAME, "BadShadowName"),
                arguments(MOCK_THING_NAME, null),
                arguments(null, SHADOW_NAME_1)
        );
    }

    static Stream<Arguments> invalidDeleteShadowTests() {
        return Stream.of(
                arguments("BadThingName", SHADOW_NAME_1),
                arguments(MOCK_THING_NAME, "BadShadowName"),
                arguments(MOCK_THING_NAME, null),
                arguments(null, SHADOW_NAME_1)
        );
    }

    @BeforeAll
    static void beforeEach(ExtensionContext context) throws InterruptedException, IOException, DeviceConfigurationException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionWithMessage(context, "Connection reset by peer");
        // Ignore if IPC can't send us more lifecycle updates because the test is already done.
        ignoreExceptionUltimateCauseWithMessage(context, "Channel not found for given connection context");

        kernel = new Kernel();
        kernel.getContext().put(DeviceConfiguration.class,
                new DeviceConfiguration(kernel, MOCK_THING_NAME, "us-east-1", "us-east-1", "mock", "mock", "mock", "us-east-1",
                        "mock"));

        NoOpPathOwnershipHandler.register(kernel);
        ConfigPlatformResolver.initKernelWithMultiPlatformConfig(kernel, ShadowIPCTest.class.getResource("shadow.yaml"));
        // ensure awaitIpcServiceLatch starts
        CountDownLatch awaitIpcServiceLatch = new CountDownLatch(2);
        GlobalStateChangeListener listener = IPCTestUtils.getListenerForServiceRunning(awaitIpcServiceLatch, "DoAll", "aws.greengrass.ShadowManager");
        kernel.getContext().addGlobalStateChangeListener(listener);

        kernel.launch();
        assertTrue(awaitIpcServiceLatch.await(10, TimeUnit.SECONDS));
        kernel.getContext().removeGlobalStateChangeListener(listener);

        expectedShadowDocumentV1Json = MAPPER.readTree(expectedShadowDocumentV1Str);
        expectedShadowDocumentV3Json = MAPPER.readTree(expectedShadowDocumentV3Str);
        expectedAcceptedMessageV3Json = MAPPER.readTree(expectedAcceptedMessageV3Str);
        expectedShadowDocumentV2Json = MAPPER.readTree(expectedShadowDocumentV2Str);
        expectedAcceptedMessageV2Json = MAPPER.readTree(expectedAcceptedMessageV2Str);
        expectedDeltaMessageV2Json = MAPPER.readTree(expectedDeltaMessageV2Str);
        invalidVersionMessageJson = MAPPER.readTree(invalidVersionMessageStr);
        invalidVersionMessageJson2 = MAPPER.readTree(invalidVersionMessageStr2);
    }

    @AfterAll
    static void stopKernel() {
        kernel.shutdown();
    }

    @Test
    @Order(1)
    void GIVEN_shadow_client_WHEN_subscribed_to_accept_and_documents_and_update_shadow_THEN_succeeds_and_receives_correct_message_over_pubsub() throws Exception {
        try(EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            String updateDocumentStr = "{\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}";
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);
            final long currentEpochSeconds = Instant.now().getEpochSecond();

            Pair<CompletableFuture<Void>, Consumer<byte[]>> acceptedConsumer = asyncAssertOnConsumer((m) -> {
                try {
                    JsonNode receivedShadowDocumentJson = MAPPER.readTree(m);
                    assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_TIMESTAMP));
                    assertThat(receivedShadowDocumentJson.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(greaterThanOrEqualTo(currentEpochSeconds)));
                    ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_TIMESTAMP);
                    assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_METADATA));
                    ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_METADATA);
                    assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_STATE));
                    assertThat(receivedShadowDocumentJson, is(expectedShadowDocumentV1Json));
                } catch (IOException e) {
                    fail("Unable to parse message on accept");
                }
            });

            Pair<CompletableFuture<Void>, Consumer<byte[]>> documentsConsumer = asyncAssertOnConsumer((m) -> {
                try {
                    JsonNode receivedDocumentsJson = MAPPER.readTree(m);
                    assertTrue(JsonUtil.isNullOrMissing(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_PREVIOUS)));
                    assertTrue(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_CURRENT).has(SHADOW_DOCUMENT_METADATA));
                    ((ObjectNode) receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_CURRENT)).remove(SHADOW_DOCUMENT_METADATA);

                    assertThat(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_CURRENT), is(expectedShadowDocumentV1Json));
                } catch (IOException e) {
                    fail("Unable to parse message on documents");
                }

            });

            String acceptedTopic = "$aws/things/" + MOCK_THING_NAME + "/shadow/name/" + SHADOW_NAME_1 + "/update/accepted";
            String documentsTopic = "$aws/things/" + MOCK_THING_NAME + "/shadow/name/" + SHADOW_NAME_1 + "/update/documents";
            subscribeToTopicOveripcForBinaryMessages(ipcClient, acceptedTopic, acceptedConsumer.getRight());
            subscribeToTopicOveripcForBinaryMessages(ipcClient, documentsTopic, documentsConsumer.getRight());

            UpdateThingShadowRequest updateThingShadowRequest = new UpdateThingShadowRequest();
            updateThingShadowRequest.setThingName(MOCK_THING_NAME);
            updateThingShadowRequest.setShadowName(SHADOW_NAME_1);
            updateThingShadowRequest.setPayload(updateDocumentStr.getBytes(UTF_8));

            CompletableFuture<UpdateThingShadowResponse> fut =
                    ipcClient.updateThingShadow(updateThingShadowRequest, Optional.empty()).getResponse();

            UpdateThingShadowResponse updateThingShadowResponse = fut.get(90, TimeUnit.SECONDS);
            JsonNode receivedShadowDocumentJson = MAPPER.readTree(updateThingShadowResponse.getPayload());
            assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_TIMESTAMP));
            assertThat(receivedShadowDocumentJson.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(greaterThanOrEqualTo(currentEpochSeconds)));
            ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_TIMESTAMP);
            assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_METADATA));
            ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_METADATA);
            assertThat(receivedShadowDocumentJson, is(expectedShadowDocumentV1Json));

            documentsConsumer.getLeft().get(TIMEOUT_FOR_PUBSUB_SECONDS, TimeUnit.SECONDS);
            acceptedConsumer.getLeft().get(TIMEOUT_FOR_PUBSUB_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    @Order(2)
    void GIVEN_shadow_client_WHEN_subscribed_to_accept_delta_and_documents_and_update_shadow_desired_THEN_succeeds_and_receives_correct_message_over_pubsub() throws Exception {
        try(EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            String updateDocumentStr = "{\"state\":{\"desired\":{\"color\":{\"r\":0,\"a\":255,\"g\":255},\"NewArray\": [1,2,3,5,8,13]}}}";
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);
            final long currentEpochSeconds = Instant.now().getEpochSecond();

            Pair<CompletableFuture<Void>, Consumer<byte[]>> acceptedConsumer = asyncAssertOnConsumer((m) -> {
                try {
                    JsonNode receivedShadowDocumentJson = MAPPER.readTree(m);
                    assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_TIMESTAMP));
                    assertThat(receivedShadowDocumentJson.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(greaterThanOrEqualTo(currentEpochSeconds)));
                    ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_TIMESTAMP);
                    assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_METADATA));
                    ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_METADATA);
                    assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_STATE));
                    assertThat(receivedShadowDocumentJson, is(expectedAcceptedMessageV2Json));
                } catch (IOException e) {
                    fail("Unable to parse message on accept");
                }
            });

            Pair<CompletableFuture<Void>, Consumer<byte[]>> documentsConsumer = asyncAssertOnConsumer((m) -> {
                try {
                    JsonNode receivedDocumentsJson = MAPPER.readTree(m);
                    assertFalse(JsonUtil.isNullOrMissing(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_PREVIOUS)));
                    assertFalse(JsonUtil.isNullOrMissing(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_CURRENT)));
                    assertTrue(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_PREVIOUS).has(SHADOW_DOCUMENT_METADATA));
                    ((ObjectNode) receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_PREVIOUS)).remove(SHADOW_DOCUMENT_METADATA);
                    assertThat(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_PREVIOUS), is(expectedShadowDocumentV1Json));
                    assertTrue(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_CURRENT).has(SHADOW_DOCUMENT_METADATA));
                    ((ObjectNode) receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_CURRENT)).remove(SHADOW_DOCUMENT_METADATA);
                    assertThat(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_CURRENT), is(expectedShadowDocumentV2Json));
                } catch (IOException e) {
                    fail("Unable to parse message on documents");
                }

            });

            Pair<CompletableFuture<Void>, Consumer<byte[]>> deltaConsumer = asyncAssertOnConsumer((m) -> {
                try {
                    JsonNode receivedDeltaJson = MAPPER.readTree(m);
                    assertTrue(receivedDeltaJson.has(SHADOW_DOCUMENT_TIMESTAMP));
                    assertThat(receivedDeltaJson.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(greaterThanOrEqualTo(currentEpochSeconds)));
                    ((ObjectNode) receivedDeltaJson).remove(SHADOW_DOCUMENT_TIMESTAMP);
                    assertTrue(receivedDeltaJson.has(SHADOW_DOCUMENT_METADATA));
                    ((ObjectNode) receivedDeltaJson).remove(SHADOW_DOCUMENT_METADATA);
                    assertThat(receivedDeltaJson, is(expectedDeltaMessageV2Json));
                } catch (IOException e) {
                    fail("Unable to parse message on delta");
                }

            });

            String acceptedTopic = "$aws/things/" + MOCK_THING_NAME + "/shadow/name/" + SHADOW_NAME_1 + "/update/accepted";
            String documentsTopic = "$aws/things/" + MOCK_THING_NAME + "/shadow/name/" + SHADOW_NAME_1 + "/update/documents";
            String deltaTopic = "$aws/things/" + MOCK_THING_NAME + "/shadow/name/" + SHADOW_NAME_1 + "/update/delta";
            subscribeToTopicOveripcForBinaryMessages(ipcClient, acceptedTopic, acceptedConsumer.getRight());
            subscribeToTopicOveripcForBinaryMessages(ipcClient, documentsTopic, documentsConsumer.getRight());
            subscribeToTopicOveripcForBinaryMessages(ipcClient, deltaTopic, deltaConsumer.getRight());

            UpdateThingShadowRequest updateThingShadowRequest = new UpdateThingShadowRequest();
            updateThingShadowRequest.setThingName(MOCK_THING_NAME);
            updateThingShadowRequest.setShadowName(SHADOW_NAME_1);
            updateThingShadowRequest.setPayload(updateDocumentStr.getBytes(UTF_8));

            CompletableFuture<UpdateThingShadowResponse> fut =
                    ipcClient.updateThingShadow(updateThingShadowRequest, Optional.empty()).getResponse();

            UpdateThingShadowResponse updateThingShadowResponse = fut.get(90, TimeUnit.SECONDS);
            JsonNode receivedShadowDocumentJson = MAPPER.readTree(updateThingShadowResponse.getPayload());
            assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_TIMESTAMP));
            assertThat(receivedShadowDocumentJson.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(greaterThanOrEqualTo(currentEpochSeconds)));
            ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_TIMESTAMP);
            assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_METADATA));
            ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_METADATA);
            assertThat(receivedShadowDocumentJson, is(expectedAcceptedMessageV2Json));

            deltaConsumer.getLeft().get(TIMEOUT_FOR_PUBSUB_SECONDS, TimeUnit.SECONDS);
            documentsConsumer.getLeft().get(TIMEOUT_FOR_PUBSUB_SECONDS, TimeUnit.SECONDS);
            acceptedConsumer.getLeft().get(TIMEOUT_FOR_PUBSUB_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    @Order(3)
    void GIVEN_shadow_client_WHEN_subscribed_to_accept_and_documents_reported_and_update_shadow_THEN_succeeds_and_receives_correct_message_over_pubsub() throws Exception {
        try(EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            String updateDocumentStr = "{\"state\":{\"desired\": null,\"reported\":{\"color\":{\"r\":0,\"a\":255,\"g\":255},\"NewArray\": [1,2,3,5,8,13]}}}";
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);
            final long currentEpochSeconds = Instant.now().getEpochSecond();

            Pair<CompletableFuture<Void>, Consumer<byte[]>> acceptedConsumer = asyncAssertOnConsumer((m) -> {
                try {
                    JsonNode receivedShadowDocumentJson = MAPPER.readTree(m);
                    assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_TIMESTAMP));
                    assertThat(receivedShadowDocumentJson.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(greaterThanOrEqualTo(currentEpochSeconds)));
                    ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_TIMESTAMP);
                    assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_METADATA));
                    ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_METADATA);
                    assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_STATE));
                    assertThat(receivedShadowDocumentJson, is(expectedAcceptedMessageV3Json));
                } catch (IOException e) {
                    fail("Unable to parse message on accept");
                }
            });

            Pair<CompletableFuture<Void>, Consumer<byte[]>> documentsConsumer = asyncAssertOnConsumer((m) -> {
                try {
                    JsonNode receivedDocumentsJson = MAPPER.readTree(m);
                    assertFalse(JsonUtil.isNullOrMissing(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_PREVIOUS)));
                    assertFalse(JsonUtil.isNullOrMissing(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_CURRENT)));
                    assertTrue(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_PREVIOUS).has(SHADOW_DOCUMENT_METADATA));
                    ((ObjectNode) receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_PREVIOUS)).remove(SHADOW_DOCUMENT_METADATA);
                    assertThat(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_PREVIOUS), is(expectedShadowDocumentV2Json));
                    assertTrue(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_CURRENT).has(SHADOW_DOCUMENT_METADATA));
                    ((ObjectNode) receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_CURRENT)).remove(SHADOW_DOCUMENT_METADATA);
                    assertThat(receivedDocumentsJson.get(SHADOW_DOCUMENT_STATE_CURRENT), is(expectedShadowDocumentV3Json));
                } catch (IOException e) {
                    fail("Unable to parse message on documents");
                }

            });

            String acceptedTopic = "$aws/things/" + MOCK_THING_NAME + "/shadow/name/" + SHADOW_NAME_1 + "/update/accepted";
            String documentsTopic = "$aws/things/" + MOCK_THING_NAME + "/shadow/name/" + SHADOW_NAME_1 + "/update/documents";
            subscribeToTopicOveripcForBinaryMessages(ipcClient, acceptedTopic, acceptedConsumer.getRight());
            subscribeToTopicOveripcForBinaryMessages(ipcClient, documentsTopic, documentsConsumer.getRight());

            UpdateThingShadowRequest updateThingShadowRequest = new UpdateThingShadowRequest();
            updateThingShadowRequest.setThingName(MOCK_THING_NAME);
            updateThingShadowRequest.setShadowName(SHADOW_NAME_1);
            updateThingShadowRequest.setPayload(updateDocumentStr.getBytes(UTF_8));

            CompletableFuture<UpdateThingShadowResponse> fut =
                    ipcClient.updateThingShadow(updateThingShadowRequest, Optional.empty()).getResponse();

            UpdateThingShadowResponse updateThingShadowResponse = fut.get(90, TimeUnit.SECONDS);
            JsonNode receivedShadowDocumentJson = MAPPER.readTree(updateThingShadowResponse.getPayload());
            assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_TIMESTAMP));
            assertThat(receivedShadowDocumentJson.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(greaterThanOrEqualTo(currentEpochSeconds)));
            ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_TIMESTAMP);
            assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_METADATA));
            ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_METADATA);
            assertThat(receivedShadowDocumentJson, is(expectedAcceptedMessageV3Json));

            documentsConsumer.getLeft().get(TIMEOUT_FOR_PUBSUB_SECONDS, TimeUnit.SECONDS);
            acceptedConsumer.getLeft().get(TIMEOUT_FOR_PUBSUB_SECONDS, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest
    @MethodSource("invalidVersionUpdateTests")
    @Order(4)
    void GIVEN_shadow_client_WHEN_update_shadow_with_bad_version_and_subscribed_to_rejected_THEN_fails_and_receives_correct_message_over_pubsub(Integer version, JsonNode expectedErrorJson, ExtensionContext context) throws Exception {
        try(EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            ignoreExceptionUltimateCauseOfType(context, InvalidRequestParametersException.class);
            ignoreExceptionUltimateCauseOfType(context, InvalidArgumentsError.class);
            ignoreExceptionUltimateCauseOfType(context, ConflictError.class);

            String updateDocumentWithLowerVersionStr = "{\"version\": " + version + ", \"state\":{\"reported\":{\"color\":{\"r\":0,\"a\":255,\"g\":255},\"NewArray\": [1,2,3,5,8,13]}}}";
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);
            final long currentEpochSeconds = Instant.now().getEpochSecond();

            Pair<CompletableFuture<Void>, Consumer<byte[]>> rejectedConsumer = asyncAssertOnConsumer((m) -> {
                try {
                    JsonNode receivedShadowDocumentJson = MAPPER.readTree(m);
                    assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_TIMESTAMP));
                    assertThat(receivedShadowDocumentJson.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(greaterThanOrEqualTo(currentEpochSeconds)));
                    ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_TIMESTAMP);
                    assertThat(receivedShadowDocumentJson, is(expectedErrorJson));

                } catch (IOException e) {
                    fail("Unable to parse message on accept");
                }
            });

            String rejectedTopic = "$aws/things/" + MOCK_THING_NAME + "/shadow/name/" + SHADOW_NAME_1 + "/update/rejected";
            subscribeToTopicOveripcForBinaryMessages(ipcClient, rejectedTopic, rejectedConsumer.getRight());

            UpdateThingShadowRequest updateThingShadowRequest = new UpdateThingShadowRequest();
            updateThingShadowRequest.setThingName(MOCK_THING_NAME);
            updateThingShadowRequest.setShadowName(SHADOW_NAME_1);
            updateThingShadowRequest.setPayload(updateDocumentWithLowerVersionStr.getBytes(UTF_8));

            ExecutionException executionException = assertThrows(ExecutionException.class, () -> ipcClient.updateThingShadow(updateThingShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            if (version == null) {
                assertTrue(executionException.getCause() instanceof InvalidArgumentsError);
                InvalidArgumentsError serviceErrorThrown = (InvalidArgumentsError) executionException.getCause();
                assertThat(serviceErrorThrown.getMessage(), is(expectedErrorJson.get(ERROR_MESSAGE_FIELD_NAME).asText()));
            } else {
                assertTrue(executionException.getCause() instanceof ConflictError);
                ConflictError thrown = (ConflictError) executionException.getCause();
                assertThat(thrown.getMessage(), is(expectedErrorJson.get(ERROR_MESSAGE_FIELD_NAME).asText()));
            }
            rejectedConsumer.getLeft().get(TIMEOUT_FOR_PUBSUB_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    @Order(5)
    void GIVEN_shadow_client_WHEN_get_shadow_THEN_gets_the_correctly_gets_the_latest_shadow_document() throws Exception {
        try(EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            final long currentEpochSeconds = Instant.now().getEpochSecond();
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            GetThingShadowRequest getShadowRequest = new GetThingShadowRequest();
            getShadowRequest.setThingName(MOCK_THING_NAME);
            getShadowRequest.setShadowName(SHADOW_NAME_1);

            GetThingShadowResponse getThingShadowResponse = ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS);
            assertThat(getThingShadowResponse.getPayload(), is(notNullValue()));
            JsonNode receivedShadowDocumentJson = MAPPER.readTree(getThingShadowResponse.getPayload());
            assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_TIMESTAMP));
            assertThat(receivedShadowDocumentJson.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(greaterThanOrEqualTo(currentEpochSeconds)));
            ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_TIMESTAMP);
            assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_METADATA));
            ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_METADATA);
            assertThat(receivedShadowDocumentJson, is(expectedShadowDocumentV3Json));
        }
    }

    @ParameterizedTest
    @MethodSource("invalidGetShadowTests")
    @Order(6)
    void GIVEN_shadow_client_WHEN_get_shadow_with_bad_request_parameters_and_subscribed_to_rejected_THEN_fails_and_receives_correct_message_over_pubsub(String thingName, String shadowName, ExtensionContext context) throws Exception {
        try(EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            ignoreExceptionUltimateCauseOfType(context, InvalidRequestParametersException.class);
            ignoreExceptionUltimateCauseOfType(context, InvalidArgumentsError.class);
            ignoreExceptionUltimateCauseOfType(context, ResourceNotFoundError.class);

            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            GetThingShadowRequest getShadowRequest = new GetThingShadowRequest();
            getShadowRequest.setThingName(thingName);
            getShadowRequest.setShadowName(shadowName);

            ExecutionException executionException = assertThrows(ExecutionException.class, () -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            if (thingName == null) {
                assertTrue(executionException.getCause() instanceof InvalidArgumentsError);
                InvalidArgumentsError thrown = (InvalidArgumentsError) executionException.getCause();
                assertThat(thrown.getMessage(), is("ThingName is missing"));
            } else {
                assertTrue(executionException.getCause() instanceof ResourceNotFoundError);
                ResourceNotFoundError thrown = (ResourceNotFoundError) executionException.getCause();
                assertThat(thrown.getMessage(), is("No shadow found"));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("invalidDeleteShadowTests")
    @Order(7)
    void GIVEN_shadow_client_WHEN_delete_shadow_with_bad_request_parameters_and_subscribed_to_rejected_THEN_fails_and_receives_correct_message_over_pubsub(String thingName, String shadowName, ExtensionContext context) throws Exception {
        try(EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            final long currentEpochSeconds = Instant.now().getEpochSecond();
            ignoreExceptionUltimateCauseOfType(context, InvalidRequestParametersException.class);
            ignoreExceptionUltimateCauseOfType(context, InvalidArgumentsError.class);
            ignoreExceptionUltimateCauseOfType(context, ResourceNotFoundError.class);

            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            Pair<CompletableFuture<Void>, Consumer<byte[]>> rejectedConsumer = asyncAssertOnConsumer((m) -> {
                try {
                    JsonNode receivedShadowDocumentJson = MAPPER.readTree(m);
                    assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_TIMESTAMP));
                    assertThat(receivedShadowDocumentJson.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(greaterThanOrEqualTo(currentEpochSeconds)));
                    ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_TIMESTAMP);
                    if (thingName == null) {
                        assertThat(receivedShadowDocumentJson.get(ERROR_CODE_FIELD_NAME).asInt(), is(400));
                        assertThat(receivedShadowDocumentJson.get(ERROR_MESSAGE_FIELD_NAME).asText(), is("ThingName is missing"));
                    } else {
                        assertThat(receivedShadowDocumentJson.get(ERROR_CODE_FIELD_NAME).asInt(), is(404));
                        assertThat(receivedShadowDocumentJson.get(ERROR_MESSAGE_FIELD_NAME).asText(), anyOf(is("No shadow exists with name: testShadowName"),
                                is("No shadow exists with name: Unnamed Shadow"),
                                is("No shadow exists with name: BadShadowName")));
                    }
                } catch (IOException e) {
                    fail("Unable to parse message on reject");
                }
            });

            StringBuilder rejectedTopic = new StringBuilder("$aws/things/" + thingName + "/shadow");
            if (shadowName != null) {
                rejectedTopic.append("/name/").append(shadowName);
            }
            rejectedTopic.append("/delete/rejected");
            subscribeToTopicOveripcForBinaryMessages(ipcClient, rejectedTopic.toString(), rejectedConsumer.getRight());

            DeleteThingShadowRequest deleteThingShadowRequest = new DeleteThingShadowRequest();
            deleteThingShadowRequest.setThingName(thingName);
            deleteThingShadowRequest.setShadowName(shadowName);

            ExecutionException executionException = assertThrows(ExecutionException.class, () -> ipcClient.deleteThingShadow(deleteThingShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
            if (thingName == null) {
                assertTrue(executionException.getCause() instanceof InvalidArgumentsError);
                InvalidArgumentsError thrown = (InvalidArgumentsError) executionException.getCause();
                assertThat(thrown.getMessage(), is("ThingName is missing"));
            } else {
                assertTrue(executionException.getCause() instanceof ResourceNotFoundError);
                ResourceNotFoundError thrown = (ResourceNotFoundError) executionException.getCause();
                assertThat(thrown.getMessage(), is("No shadow found"));
            }
            rejectedConsumer.getLeft().get(TIMEOUT_FOR_PUBSUB_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    @Order(8)
    void GIVEN_shadow_client_WHEN_list_thing_shadow_THEN_correctly_gets_shadow() throws Exception {
        try(EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
            request.setThingName(MOCK_THING_NAME);

            ListNamedShadowsForThingResponse response = ipcClient.listNamedShadowsForThing(request, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS);
            assertThat(response.getResults().toArray(), arrayContaining(SHADOW_NAME_1));
            assertThat(response.getNextToken(), is(nullValue()));
        }
    }

    @Test
    @Order(9)
    void GIVEN_shadow_client_list_request_with_page_size_WHEN_list_thing_shadow_THEN_correctly_gets_shadow_with_token() throws Exception {
        try(EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);
            int pageSize = 1;

            ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
            request.setThingName(MOCK_THING_NAME);
            request.setPageSize(pageSize);

            ListNamedShadowsForThingResponse response = ipcClient.listNamedShadowsForThing(request, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS);
            List<String> shadowNameResults = response.getResults();
            assertThat(shadowNameResults.toArray(), arrayContaining(SHADOW_NAME_1));
            assertThat(shadowNameResults.size(), is(equalTo(1)));
            assertThat(response.getNextToken(), is(notNullValue()));
            assertThat(response.getNextToken(), is("hh5oROXTiAG1Zw1yPAV+gg=="));
        }
    }

    @Test
    @Order(10)
    void GIVEN_shadow_client_list_request_with_token_and_only_one_shadow_name_WHEN_list_thing_shadow_THEN_return_empty_list() throws Exception {
        try(EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
            request.setThingName(MOCK_THING_NAME);
            request.setNextToken("hh5oROXTiAG1Zw1yPAV+gg==");

            ListNamedShadowsForThingResponse response = ipcClient.listNamedShadowsForThing(request, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS);
            List<String> shadowNameResults = response.getResults();
            assertThat(shadowNameResults.toArray(), is(notNullValue()));
            assertThat(shadowNameResults.size(), is(equalTo(0)));
        }
    }

    @Test
    @Order(11)
    void GIVEN_shadow_client_WHEN_subscribed_to_accept_and_delete_shadow_THEN_correctly_deletes_shadow() throws Exception {
        try(EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            final long currentEpochSeconds = Instant.now().getEpochSecond();
            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            Pair<CompletableFuture<Void>, Consumer<byte[]>> acceptedConsumer = asyncAssertOnConsumer((m) -> {
                try {
                    JsonNode receivedShadowDocumentJson = MAPPER.readTree(m);
                    assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_TIMESTAMP));
                    assertThat(receivedShadowDocumentJson.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(greaterThanOrEqualTo(currentEpochSeconds)));
                    ((ObjectNode) receivedShadowDocumentJson).remove(SHADOW_DOCUMENT_TIMESTAMP);
                    assertTrue(receivedShadowDocumentJson.has(SHADOW_DOCUMENT_VERSION));
                    assertThat(receivedShadowDocumentJson.get(SHADOW_DOCUMENT_VERSION).asInt(), is(3));
                } catch (IOException e) {
                    fail("Unable to parse message on accept");
                }
            });

            String acceptedTopic = "$aws/things/" + MOCK_THING_NAME + "/shadow/name/" + SHADOW_NAME_1 + "/delete/accepted";
            subscribeToTopicOveripcForBinaryMessages(ipcClient, acceptedTopic, acceptedConsumer.getRight());

            DeleteThingShadowRequest deleteThingShadowRequest = new DeleteThingShadowRequest();
            deleteThingShadowRequest.setThingName(MOCK_THING_NAME);
            deleteThingShadowRequest.setShadowName(SHADOW_NAME_1);

            DeleteThingShadowResponse response = ipcClient.deleteThingShadow(deleteThingShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS);
            assertEquals(0, response.getPayload().length);
            acceptedConsumer.getLeft().get(TIMEOUT_FOR_PUBSUB_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    @Order(12)
    void GIVEN_shadow_client_WHEN_get_shadow_with_deleted_shadow_and_subscribed_to_rejected_THEN_fails_and_receives_correct_message_over_pubsub(ExtensionContext context) throws Exception {
        try(EventStreamRPCConnection connection = IPCTestUtils.getEventStreamRpcConnection(kernel, "DoAll")) {
            ignoreExceptionUltimateCauseOfType(context, InvalidRequestParametersException.class);
            ignoreExceptionUltimateCauseOfType(context, InvalidArgumentsError.class);
            ignoreExceptionUltimateCauseOfType(context, ResourceNotFoundError.class);

            GreengrassCoreIPCClient ipcClient = new GreengrassCoreIPCClient(connection);

            GetThingShadowRequest getShadowRequest = new GetThingShadowRequest();
            getShadowRequest.setThingName(MOCK_THING_NAME);
            getShadowRequest.setShadowName(SHADOW_NAME_1);

            ExecutionException executionException = assertThrows(ExecutionException.class, () -> ipcClient.getThingShadow(getShadowRequest, Optional.empty()).getResponse().get(90, TimeUnit.SECONDS));
                assertTrue(executionException.getCause() instanceof ResourceNotFoundError);
                ResourceNotFoundError thrown = (ResourceNotFoundError) executionException.getCause();
                assertThat(thrown.getMessage(), is("No shadow found"));
            }
        }
}
