/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.AuthorizationHandlerWrapper;
import com.aws.greengrass.shadowmanager.ipc.model.PubSubRequest;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.*;
import software.amazon.awssdk.crt.eventstream.ServerConnectionContinuation;
import software.amazon.awssdk.eventstreamrpc.AuthenticationData;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.TestUtils.SAMPLE_EXCEPTION_MESSAGE;
import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.TEST_SERVICE;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.ERROR_CODE_FIELD_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.ERROR_MESSAGE_FIELD_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_TIMESTAMP;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//TODO: Change the names of the tests to be in the correct format.
//TODO: Use Hamcrest assertions
@ExtendWith({MockitoExtension.class, GGExtension.class})
class DeleteThingShadowIPCHandlerTest {

    @Mock
    OperationContinuationHandlerContext mockContext;

    @Mock
    AuthenticationData mockAuthenticationData;

    @Mock
    AuthorizationHandlerWrapper mockAuthorizationHandlerWrapper;

    @Mock
    ShadowManagerDAO mockDao;

    @Mock
    PubSubClientWrapper mockPubSubClientWrapper;

    @Captor
    ArgumentCaptor<PubSubRequest> pubSubRequestCaptor;

    @BeforeEach
    void setup() {
        when(mockContext.getContinuation()).thenReturn(mock(ServerConnectionContinuation.class));
        when(mockContext.getAuthenticationData()).thenReturn(mockAuthenticationData);
        when(mockAuthenticationData.getIdentityLabel()).thenReturn(TEST_SERVICE);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {SHADOW_NAME})
    void GIVEN_valid_request_WHEN_handle_request_THEN_return_valid_response(String shadowName) throws URISyntaxException, IOException {
        File f = new File(getClass().getResource("json_shadow_examples/good_new_document.json").toURI());
        byte[] allByteData = Files.readAllBytes(f.toPath());
        Optional<JsonNode> shadowDocumentJson = JsonUtil.getPayloadJson(allByteData);
        assertThat("Found shadowDocumentJson", shadowDocumentJson.isPresent(), is(true));
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(shadowName);

        DeleteThingShadowResponse expectedResponse = new DeleteThingShadowResponse();
        expectedResponse.setPayload(new byte[0]);

        try (DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper)) {
            when(mockDao.deleteShadowThing(any(), any())).thenReturn(Optional.of(new ShadowDocument(allByteData)));

            DeleteThingShadowResponse actualResponse = deleteThingShadowIPCHandler.handleRequest(request);
            assertThat(actualResponse, is(equalTo(expectedResponse)));
            verify(mockPubSubClientWrapper, times(1)).accept(pubSubRequestCaptor.capture());
            assertThat(pubSubRequestCaptor.getValue(), is(notNullValue()));

            Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(pubSubRequestCaptor.getValue().getPayload());
            assertThat("Retrieved acceptedJson", acceptedJson.isPresent(), is(true));
            assertThat("acceptedJson has timestamp", acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP), is(equalTo(true)));
            ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

            // IPCRequest does not accept null value for shadowName
            if (shadowName != null) {
                assertThat(pubSubRequestCaptor.getValue().getShadowName(), is(equalTo(shadowName)));
            }

            assertThat("Expected shadow document version", acceptedJson.get().get(SHADOW_DOCUMENT_VERSION), is(equalTo(shadowDocumentJson.get().get(SHADOW_DOCUMENT_VERSION))));
            assertThat(pubSubRequestCaptor.getValue().getThingName(), is(equalTo(THING_NAME)));
            assertThat("Expected operation", pubSubRequestCaptor.getValue().getPublishOperation(), is(Operation.DELETE_SHADOW));
            assertThat("Expected log code", pubSubRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(LogEvents.DELETE_THING_SHADOW.code()));
        }
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_document_not_found_THEN_throw_resource_not_found_exception(ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, ResourceNotFoundError.class);
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        when(mockDao.deleteShadowThing(any(), any())).thenReturn(Optional.empty());

        try (DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper)) {
            ResourceNotFoundError thrown = assertThrows(ResourceNotFoundError.class, () -> deleteThingShadowIPCHandler.handleRequest(request));
            assertThat(thrown.getMessage(), is(equalTo("No shadow found")));

            verify(mockPubSubClientWrapper, times(1))
                    .reject(pubSubRequestCaptor.capture());

            assertThat(pubSubRequestCaptor.getValue(), is(notNullValue()));
            assertThat(pubSubRequestCaptor.getValue().getShadowName(), is(equalTo(SHADOW_NAME)));
            assertThat("Expected operation", pubSubRequestCaptor.getValue().getPublishOperation(), is(Operation.DELETE_SHADOW));
            assertThat("Expected log code", pubSubRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(LogEvents.DELETE_THING_SHADOW.code()));

            JsonNode errorNode = JsonUtil.getPayloadJson(pubSubRequestCaptor.getValue().getPayload()).get();

            assertThat(errorNode.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(not(equalTo(Instant.EPOCH.toEpochMilli()))));
            assertThat(errorNode.get(ERROR_CODE_FIELD_NAME).asInt(), is(404));
            assertThat(errorNode.get(ERROR_MESSAGE_FIELD_NAME).asText(), startsWith("No shadow exists"));
        }
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_dao_sends_data_exception_THEN_throw_service_exception(ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, ShadowManagerDataException.class);
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        doThrow(new ShadowManagerDataException(new Exception(SAMPLE_EXCEPTION_MESSAGE))).when(mockDao).deleteShadowThing(any(), any());

        try (DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper)) {
            ServiceError thrown = assertThrows(ServiceError.class, () -> deleteThingShadowIPCHandler.handleRequest(request));
            assertThat(thrown.getMessage(), containsString(SAMPLE_EXCEPTION_MESSAGE));

            verify(mockPubSubClientWrapper, times(1))
                    .reject(pubSubRequestCaptor.capture());

            assertThat(pubSubRequestCaptor.getValue(), is(notNullValue()));
            assertThat(pubSubRequestCaptor.getValue().getShadowName(), is(equalTo(SHADOW_NAME)));
            assertThat("Expected operation", pubSubRequestCaptor.getValue().getPublishOperation(), is(Operation.DELETE_SHADOW));
            assertThat("Expected log code", pubSubRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(LogEvents.DELETE_THING_SHADOW.code()));

            JsonNode errorNode = JsonUtil.getPayloadJson(pubSubRequestCaptor.getValue().getPayload()).get();
            assertThat(errorNode.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(not(equalTo(Instant.EPOCH.toEpochMilli()))));
            assertThat(errorNode.get(ERROR_CODE_FIELD_NAME).asInt(), is(500));
            assertThat(errorNode.get(ERROR_MESSAGE_FIELD_NAME).asText(), startsWith("Internal service failure"));
        }
    }

    @Test
    void GIVEN_unauthorized_service_WHEN_handle_request_THEN_throw_unauthorized_error(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, AuthorizationException.class);
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        doThrow(new AuthorizationException(SAMPLE_EXCEPTION_MESSAGE)).when(mockAuthorizationHandlerWrapper).doAuthorization(any(), any(), any());

        try (DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper)) {
            UnauthorizedError thrown = assertThrows(UnauthorizedError.class, () -> deleteThingShadowIPCHandler.handleRequest(request));
            assertThat(thrown.getMessage(), is(equalTo(SAMPLE_EXCEPTION_MESSAGE)));

            verify(mockPubSubClientWrapper, times(1)).reject(pubSubRequestCaptor.capture());

            assertThat(pubSubRequestCaptor.getValue(), is(notNullValue()));
            assertThat(pubSubRequestCaptor.getValue().getShadowName(), is(equalTo(SHADOW_NAME)));
            assertThat("Expected operation", pubSubRequestCaptor.getValue().getPublishOperation(), is(Operation.DELETE_SHADOW));
            assertThat("Expected log code", pubSubRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(LogEvents.DELETE_THING_SHADOW.code()));

            JsonNode errorNode = JsonUtil.getPayloadJson(pubSubRequestCaptor.getValue().getPayload()).get();
            assertThat(errorNode.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(not(equalTo(Instant.EPOCH.toEpochMilli()))));
            assertThat(errorNode.get(ERROR_CODE_FIELD_NAME).asInt(), is(401));
            assertThat(errorNode.get(ERROR_MESSAGE_FIELD_NAME).asText(), startsWith("Unauthorized"));
        }
    }

    @ParameterizedTest
    @MethodSource("com.aws.greengrass.shadowmanager.TestUtils#invalidThingAndShadowName")
    void GIVEN_invalid_request_input_WHEN_handle_request_THEN_throw_invalid_arguments_error(String thingName, String shadowName, ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(thingName);
        request.setShadowName(shadowName);

        try (DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper)) {
            InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> deleteThingShadowIPCHandler.handleRequest(request));
            assertThat(thrown.getMessage(), either(startsWith("ShadowName")).or(startsWith("ThingName")));

            verify(mockPubSubClientWrapper, times(1))
                    .reject(pubSubRequestCaptor.capture());

            assertThat(pubSubRequestCaptor.getValue(), is(notNullValue()));
            assertThat(pubSubRequestCaptor.getValue().getShadowName(), is(equalTo(shadowName)));
            assertThat("Expected operation found", pubSubRequestCaptor.getValue().getPublishOperation(), is(Operation.DELETE_SHADOW));
            assertThat("Expected log code", pubSubRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(LogEvents.DELETE_THING_SHADOW.code()));

            JsonNode errorNode = JsonUtil.getPayloadJson(pubSubRequestCaptor.getValue().getPayload()).get();
            assertThat(errorNode.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(not(equalTo(Instant.EPOCH.toEpochMilli()))));
            assertThat(errorNode.get(ERROR_CODE_FIELD_NAME).asInt(), is(400));
            assertThat(errorNode.get(ERROR_MESSAGE_FIELD_NAME).asText(), either(startsWith("ShadowName")).or(startsWith("ThingName")));
        }
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_handle_stream_event_THEN_nothing_happens() {
        try (DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper)) {
            assertDoesNotThrow(() -> deleteThingShadowIPCHandler.handleStreamEvent(mock(EventStreamJsonMessage.class)));
        }
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_stream_closes_THEN_nothing_happens() {
        try (DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper)) {
            assertDoesNotThrow(deleteThingShadowIPCHandler::onStreamClosed);
        }
    }
}
