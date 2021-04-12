/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.aws.greengrass.shadowmanager.ipc.model.PubSubRequest;
import com.aws.greengrass.shadowmanager.model.Constants;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.model.ResponseMessageBuilder;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@ExtendWith({MockitoExtension.class, GGExtension.class})
class PubSubClientWrapperTest {
    private static final String THING_NAME = "testThingName";
    private static final String SHADOW_NAME = "testShadowName";
    private static final ObjectMapper STRICT_MAPPER_JSON = new ObjectMapper(new JsonFactory());
    private static final byte[] UPDATE_DOCUMENT = "{\"id\": 1, \"name\": \"The Beatles\"}".getBytes();

    @Mock
    PubSubIPCEventStreamAgent mockPubSubIPCEventStreamAgent;

    @Captor
    ArgumentCaptor<String> serviceNameCaptor;
    @Captor
    ArgumentCaptor<String> topicCaptor;
    @Captor
    ArgumentCaptor<byte[]> payloadCaptor;

    @BeforeAll
    static void beforeAll() {
        STRICT_MAPPER_JSON.findAndRegisterModules();
        STRICT_MAPPER_JSON.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
    }

    @ParameterizedTest
    @EnumSource(Operation.class)
    void GIVEN_good_shadow_accept_request_WHEN_accept_THEN_publishes_message(Operation operation) {
        PubSubClientWrapper pubSubClientWrapper = new PubSubClientWrapper(mockPubSubIPCEventStreamAgent);
        pubSubClientWrapper.accept(PubSubRequest.builder().shadowName(SHADOW_NAME).thingName(THING_NAME)
                .payload(UPDATE_DOCUMENT)
                .publishOperation(operation)
                .build());
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertThat(topicCaptor.getValue(), Matchers.is("$aws/things/testThingName/shadow/name/testShadowName" + operation.getOp() + "/accepted"));
        assertThat(serviceNameCaptor.getValue(), Matchers.is(SERVICE_NAME));
        assertThat(payloadCaptor.getValue(), Matchers.is(UPDATE_DOCUMENT));
    }

    @ParameterizedTest
    @EnumSource(Operation.class)
    void GIVEN_good_shadow_accept_request_WHEN_delta_THEN_publishes_message(Operation operation) {
        PubSubClientWrapper pubSubClientWrapper = new PubSubClientWrapper(mockPubSubIPCEventStreamAgent);
        pubSubClientWrapper.delta(PubSubRequest.builder().shadowName(SHADOW_NAME).thingName(THING_NAME)
                .payload(UPDATE_DOCUMENT)
                .publishOperation(operation)
                .build());
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertThat(topicCaptor.getValue(), Matchers.is("$aws/things/testThingName/shadow/name/testShadowName" + operation.getOp() + "/delta"));
        assertThat(serviceNameCaptor.getValue(), Matchers.is(SERVICE_NAME));
        assertThat(payloadCaptor.getValue(), Matchers.is(UPDATE_DOCUMENT));
    }

    @ParameterizedTest
    @EnumSource(Operation.class)
    void GIVEN_good_shadow_accept_request_WHEN_documents_THEN_publishes_message(Operation operation) {
        PubSubClientWrapper pubSubClientWrapper = new PubSubClientWrapper(mockPubSubIPCEventStreamAgent);
        pubSubClientWrapper.documents(PubSubRequest.builder().shadowName(SHADOW_NAME).thingName(THING_NAME)
                .payload(UPDATE_DOCUMENT)
                .publishOperation(operation)
                .build());
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertThat(topicCaptor.getValue(), Matchers.is("$aws/things/testThingName/shadow/name/testShadowName" + operation.getOp() + "/documents"));
        assertThat(serviceNameCaptor.getValue(), Matchers.is(SERVICE_NAME));
        assertThat(payloadCaptor.getValue(), Matchers.is(UPDATE_DOCUMENT));
    }

    @ParameterizedTest
    @EnumSource(Operation.class)
    void GIVEN_good_shadow_reject_request_WHEN_reject_THEN_publishes_message(Operation operation) throws IOException {
        PubSubClientWrapper pubSubClientWrapper = new PubSubClientWrapper(mockPubSubIPCEventStreamAgent);
        JsonNode errorResponse = ResponseMessageBuilder.builder()
                .withTimestamp(Instant.now())
                .withClientToken(Optional.empty())
                .withError(ErrorMessage.INTERNAL_SERVICE_FAILURE_MESSAGE).build();

        pubSubClientWrapper.reject(PubSubRequest.builder().shadowName(SHADOW_NAME).thingName(THING_NAME)
                .payload(JsonUtil.getPayloadBytes(errorResponse))
                .publishOperation(operation)
                .build());
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertThat(topicCaptor.getValue(), Matchers.is("$aws/things/testThingName/shadow/name/testShadowName" + operation.getOp() + "/rejected"));
        assertThat(serviceNameCaptor.getValue(), Matchers.is(SERVICE_NAME));

        assertNotNull(payloadCaptor.getValue());
        JsonNode errorMessage = STRICT_MAPPER_JSON.readValue(payloadCaptor.getValue(), JsonNode.class);
        assertThat(errorMessage.get(Constants.ERROR_CODE_FIELD_NAME).asInt(), Matchers.is(500));
        assertThat(errorMessage.get(Constants.ERROR_MESSAGE_FIELD_NAME).asText(), Matchers.is("Internal service failure"));
    }

    @Test
    void GIVEN_shadow_accept_request_WHEN_accept_and_publish_throws_error_THEN_error_is_caught(ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        InvalidArgumentsError e = new InvalidArgumentsError();
        doThrow(e).when(mockPubSubIPCEventStreamAgent).publish(any(), any(), any());
        PubSubClientWrapper pubSubClientWrapper = new PubSubClientWrapper(mockPubSubIPCEventStreamAgent);
        pubSubClientWrapper.accept(PubSubRequest.builder().shadowName(SHADOW_NAME).thingName(THING_NAME)
                .payload(UPDATE_DOCUMENT)
                .publishOperation(Operation.GET_SHADOW)
                .build());
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
    }

    @Test
    void GIVEN_shadow_accept_request_WHEN_delta_and_publish_throws_error_THEN_error_is_caught(ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        InvalidArgumentsError e = new InvalidArgumentsError();
        doThrow(e).when(mockPubSubIPCEventStreamAgent).publish(any(), any(), any());
        PubSubClientWrapper pubSubClientWrapper = new PubSubClientWrapper(mockPubSubIPCEventStreamAgent);
        pubSubClientWrapper.delta(PubSubRequest.builder().shadowName(SHADOW_NAME).thingName(THING_NAME)
                .payload(UPDATE_DOCUMENT)
                .publishOperation(Operation.GET_SHADOW)
                .build());
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
    }

    @Test
    void GIVEN_shadow_accept_request_WHEN_documents_and_publish_throws_error_THEN_error_is_caught(ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        InvalidArgumentsError e = new InvalidArgumentsError();
        doThrow(e).when(mockPubSubIPCEventStreamAgent).publish(any(), any(), any());
        PubSubClientWrapper pubSubClientWrapper = new PubSubClientWrapper(mockPubSubIPCEventStreamAgent);
        pubSubClientWrapper.documents(PubSubRequest.builder().shadowName(SHADOW_NAME).thingName(THING_NAME)
                .payload(UPDATE_DOCUMENT)
                .publishOperation(Operation.GET_SHADOW)
                .build());
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
    }

    @Test
    void GIVEN_shadow_reject_request_WHEN_reject_and_publish_throws_error_THEN_error_is_caught(ExtensionContext context) throws JsonProcessingException {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        InvalidArgumentsError e = new InvalidArgumentsError();
        doThrow(e).when(mockPubSubIPCEventStreamAgent).publish(any(), any(), any());
        PubSubClientWrapper pubSubClientWrapper = new PubSubClientWrapper(mockPubSubIPCEventStreamAgent);
        JsonNode errorResponse = ResponseMessageBuilder.builder()
                .withTimestamp(Instant.now())
                .withClientToken(Optional.empty())
                .withError(ErrorMessage.INTERNAL_SERVICE_FAILURE_MESSAGE).build();

        pubSubClientWrapper.reject(PubSubRequest.builder().shadowName(SHADOW_NAME).thingName(THING_NAME)
                .payload(JsonUtil.getPayloadBytes(errorResponse))
                .publishOperation(Operation.GET_SHADOW)
                .build());
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
    }
}
