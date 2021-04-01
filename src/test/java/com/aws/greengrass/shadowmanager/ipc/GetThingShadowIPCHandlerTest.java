/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.JsonUtil;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.ipc.model.AcceptRequest;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.aws.greengrass.shadowmanager.ipc.model.RejectRequest;
import com.aws.greengrass.shadowmanager.model.Constants;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.hamcrest.Matchers;
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

import static com.aws.greengrass.shadowmanager.TestUtils.*;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
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
class GetThingShadowIPCHandlerTest {

    @Mock
    OperationContinuationHandlerContext mockContext;

    @Mock
    AuthenticationData mockAuthenticationData;

    @Mock
    AuthorizationHandler mockAuthorizationHandler;

    @Mock
    ShadowManagerDAO mockDao;

    @Mock
    PubSubClientWrapper mockPubSubClientWrapper;

    @Captor
    ArgumentCaptor<RejectRequest> rejectRequestCaptor;
    @Captor
    ArgumentCaptor<AcceptRequest> acceptRequestCaptor;

    @BeforeEach
    void setup () {
        when(mockContext.getContinuation()).thenReturn(mock(ServerConnectionContinuation.class));
        when(mockContext.getAuthenticationData()).thenReturn(mockAuthenticationData);
        when(mockAuthenticationData.getIdentityLabel()).thenReturn(TEST_SERVICE);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {SHADOW_NAME})
    void GIVEN_only_reported_state_WHEN_handle_request_THEN_get_response_with_reported(String shadowName) throws IOException, URISyntaxException {
        File f = new File(getClass().getResource("json_shadow_examples/good_initial_document.json").toURI());
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(shadowName);
        byte[] allByteData = Files.readAllBytes(f.toPath());
        Optional<JsonNode> payloadJson = JsonUtil.getPayloadJson(allByteData);
        assertThat("Found payloadJson", payloadJson.isPresent(), is(true));

        GetThingShadowResponse expectedResponse = new GetThingShadowResponse();
        expectedResponse.setPayload(allByteData);

        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(allByteData));
        GetThingShadowResponse actualResponse = getThingShadowIPCHandler.handleRequest(request);
        Optional<JsonNode> retrievedDocument = JsonUtil.getPayloadJson(actualResponse.getPayload());
        assertThat("Retrieved document", retrievedDocument.isPresent(), is(true));
        assertThat(payloadJson.get(), is(equalTo(retrievedDocument.get())));
        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());

        assertThat(acceptRequestCaptor.getValue(), is(notNullValue()));

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getValue().getPayload());
        assertThat("Accepted json", acceptedJson.isPresent(), is(true));

        // IPCRequest does not accept null value for shadowName
        if (shadowName != null) {
            assertThat(acceptRequestCaptor.getValue().getShadowName(), is(equalTo(shadowName)));
        }
        assertThat(acceptRequestCaptor.getValue().getThingName(), is(equalTo(THING_NAME)));
        assertThat("Expected operation", acceptRequestCaptor.getValue().getPublishOperation(), is(Operation.GET_SHADOW));
        assertThat("Expected log code", acceptRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(IPCUtil.LogEvents.GET_THING_SHADOW.code()));

        assertThat(acceptedJson.get(), is(equalTo(payloadJson.get())));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {SHADOW_NAME})
    void GIVEN_reported_and_desired_state_shadow_WHEN_handle_request_THEN_get_response_with_desired_and_reported(String shadowName) throws IOException, URISyntaxException {
        File documentFile = new File(getClass().getResource("json_shadow_examples/good_new_document.json").toURI());
        File deltaFile = new File(getClass().getResource("json_shadow_examples/good_delta_node.json").toURI());
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(shadowName);
        byte[] documentByteData = Files.readAllBytes(documentFile.toPath());
        byte[] deltaFileByteData = Files.readAllBytes(deltaFile.toPath());
        Optional<JsonNode> documentJson = JsonUtil.getPayloadJson(documentByteData);
        Optional<JsonNode> deltaJson = JsonUtil.getPayloadJson(deltaFileByteData);
        assertThat("Retrieved documentJson", documentJson.isPresent(), is(true));
        assertThat("Retrieved documentJson", deltaJson.isPresent(), is(true));
        JsonNode deltaColorNode = deltaJson.get().get(Constants.SHADOW_DOCUMENT_STATE);
        ((ObjectNode)documentJson.get().get(Constants.SHADOW_DOCUMENT_STATE))
                .set(Constants.SHADOW_DOCUMENT_STATE_DELTA, deltaColorNode);

        GetThingShadowResponse expectedResponse = new GetThingShadowResponse();
        expectedResponse.setPayload(documentByteData);

        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(documentByteData));
        GetThingShadowResponse actualResponse = getThingShadowIPCHandler.handleRequest(request);
        Optional<JsonNode> retrievedDocument = JsonUtil.getPayloadJson(actualResponse.getPayload());
        assertThat("Retrieved document", retrievedDocument.isPresent(), is(true));
        assertThat(documentJson.get(), is(equalTo(retrievedDocument.get())));

        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());

        assertThat(acceptRequestCaptor.getValue(), is(notNullValue()));

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getValue().getPayload());
        assertThat("Accepted json", acceptedJson.isPresent(), is(true));

        // IPCRequest does not accept null value for shadowName
        if (shadowName != null) {
            assertThat(acceptRequestCaptor.getValue().getShadowName(), is(equalTo(shadowName)));
        }
        assertThat(acceptRequestCaptor.getValue().getThingName(), is(equalTo(THING_NAME)));
        assertThat("Expected operation", acceptRequestCaptor.getValue().getPublishOperation(), is(Operation.GET_SHADOW));
        assertThat("Expected log code", acceptRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(IPCUtil.LogEvents.GET_THING_SHADOW.code()));

        assertThat(acceptedJson.get(), is(equalTo(documentJson.get())));
    }

    @Test
    void GIVEN_no_shadow_document_found_WHEN_handle_request_THEN_throw_resource_not_found_error(ExtensionContext context) {
        ignoreExceptionOfType(context, ResourceNotFoundError.class);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.empty());
        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        ResourceNotFoundError thrown = assertThrows(ResourceNotFoundError.class, () -> getThingShadowIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), is(equalTo("No shadow found")));

        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertThat(rejectRequestCaptor.getValue(), is(not(nullValue())));
        assertThat(rejectRequestCaptor.getValue().getShadowName(), is(equalTo(SHADOW_NAME)));
        assertThat("Expected operation", rejectRequestCaptor.getValue().getPublishOperation(), is(Operation.GET_SHADOW));
        assertThat("Expected log code", rejectRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(IPCUtil.LogEvents.GET_THING_SHADOW.code()));

        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertThat(errorMessage.getTimestamp(), is(not(equalTo(Instant.EPOCH.toEpochMilli()))));
        assertThat(errorMessage.getErrorCode(), is(404));
        assertThat(errorMessage.getMessage(), startsWith("No shadow exists"));
    }

    @Test
    void GIVEN_shadow_manager_data_exception_from_query_WHEN_handle_request_THEN_throw_service_error(ExtensionContext context) {
        ignoreExceptionOfType(context, ShadowManagerDataException.class);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        doThrow(new ShadowManagerDataException(new Exception("sample exception message"))).when(mockDao).getShadowThing(any(), any());
        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        ServiceError thrown = assertThrows(ServiceError.class, () -> getThingShadowIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), containsString("sample"));

        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertThat(rejectRequestCaptor.getValue(), is(not(nullValue())));
        assertThat(rejectRequestCaptor.getValue().getShadowName(), is(equalTo(SHADOW_NAME)));
        assertThat("Expected operation", rejectRequestCaptor.getValue().getPublishOperation(), is(Operation.GET_SHADOW));
        assertThat("Expected log code", rejectRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(IPCUtil.LogEvents.GET_THING_SHADOW.code()));

        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertThat(errorMessage.getTimestamp(), is(not(equalTo(Instant.EPOCH.toEpochMilli()))));
        assertThat(errorMessage.getErrorCode(), is(500));
        assertThat(errorMessage.getMessage(), startsWith("Internal service failure"));
    }

    @Test
    void GIVEN_unauthorized_service_WHEN_handle_request_THEN_throw_unauthorized_error(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, AuthorizationException.class);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class)))
                .thenThrow(new AuthorizationException("sample authorization error"));

        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        UnauthorizedError thrown = assertThrows(UnauthorizedError.class, () -> getThingShadowIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(), startsWith("sample"));

        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertThat(rejectRequestCaptor.getValue(), is(not(nullValue())));
        assertThat(rejectRequestCaptor.getValue().getShadowName(), is(equalTo(SHADOW_NAME)));
        assertThat("Expected operation", rejectRequestCaptor.getValue().getPublishOperation(), is(Operation.GET_SHADOW));
        assertThat("Expected log code", rejectRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(IPCUtil.LogEvents.GET_THING_SHADOW.code()));

        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertThat(errorMessage.getErrorCode(), is(401));
        assertThat(errorMessage.getMessage(), Matchers.startsWith("Unauthorized"));
    }

    @ParameterizedTest
    @MethodSource("com.aws.greengrass.shadowmanager.TestUtils#invalidThingAndShadowName")
    void GIVEN_invalid_request_input_WHEN_handle_request_THEN_throw_invalid_arguments_error(String thingName, String shadowName, ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(thingName);
        request.setShadowName(shadowName);

        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> getThingShadowIPCHandler.handleRequest(request));
        assertThat(thrown.getMessage(),either(startsWith("ShadowName")).or(startsWith("ThingName")));
        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertThat(rejectRequestCaptor.getValue(), is(not(nullValue())));
        assertThat(rejectRequestCaptor.getValue().getShadowName(), is(equalTo(shadowName)));
        assertThat("Expected operation found", rejectRequestCaptor.getValue().getPublishOperation(), is(Operation.GET_SHADOW));
        assertThat("Expected log code", rejectRequestCaptor.getValue().getPublishOperation().getLogEventType(), is(IPCUtil.LogEvents.GET_THING_SHADOW.code()));

        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertThat(errorMessage.getTimestamp(), is(not(equalTo(Instant.EPOCH.toEpochMilli()))));
        assertThat(errorMessage.getErrorCode(), is(400));
        assertThat(errorMessage.getMessage(),either(startsWith("ShadowName")).or(startsWith("ThingName")));
    }

    @Test
    void GIVEN_get_thing_shadow_ipc_handler_WHEN_handle_stream_event_THEN_nothing_happens() {
        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertDoesNotThrow(() -> getThingShadowIPCHandler.handleStreamEvent(mock(EventStreamJsonMessage.class)));
    }

    @Test
    void GIVEN_get_thing_shadow_ipc_handler_WHEN_stream_closes_THEN_nothing_happens() {
        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertDoesNotThrow(getThingShadowIPCHandler::onStreamClosed);
    }
}
