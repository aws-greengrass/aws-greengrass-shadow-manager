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
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_TIMESTAMP;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    private static final String TEST_SERVICE = "TestService";
    private static final String THING_NAME = "testThingName";
    private static final String SHADOW_NAME = "testShadowName";

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
    @ValueSource(strings = {SHADOW_NAME, ""})
    void GIVEN_get_thing_shadow_ipc_handler_with_only_reported_state_shadow_WHEN_handle_request_THEN_get_thing_shadow(String shadowName) throws IOException, URISyntaxException {
        File f = new File(getClass().getResource("json_shadow_examples/good_initial_document.json").toURI());
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(shadowName);
        byte[] allByteData = Files.readAllBytes(f.toPath());
        Optional<JsonNode> payloadJson = JsonUtil.getPayloadJson(allByteData);
        assertTrue(payloadJson.isPresent());

        GetThingShadowResponse expectedResponse = new GetThingShadowResponse();
        expectedResponse.setPayload(allByteData);

        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(allByteData));
        GetThingShadowResponse actualResponse = getThingShadowIPCHandler.handleRequest(request);

        Optional<JsonNode> retrievedDocument = JsonUtil.getPayloadJson(actualResponse.getPayload());
        assertTrue(retrievedDocument.isPresent());
        assertTrue(retrievedDocument.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) retrievedDocument.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        assertThat(retrievedDocument.get(), Matchers.is(payloadJson.get()));
        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());

        assertNotNull(acceptRequestCaptor.getValue());

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getValue().getPayload());
        assertTrue(acceptedJson.isPresent());
        assertTrue(acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        assertEquals(shadowName, acceptRequestCaptor.getValue().getShadowName());
        assertEquals(THING_NAME, acceptRequestCaptor.getValue().getThingName());
        assertEquals(Operation.GET_SHADOW, acceptRequestCaptor.getValue().getPublishOperation());
        assertEquals(IPCUtil.LogEvents.GET_THING_SHADOW.code(), acceptRequestCaptor.getValue().getPublishOperation().getLogEventType());
        assertThat(acceptedJson.get(), Matchers.is(payloadJson.get()));
    }

    @ParameterizedTest
    @ValueSource(strings = {SHADOW_NAME, ""})
    void GIVEN_get_thing_shadow_ipc_handler_with_reported_and_desired_state_shadow_WHEN_handle_request_THEN_get_thing_shadow(String shadowName) throws IOException, URISyntaxException {
        File documentFile = new File(getClass().getResource("json_shadow_examples/good_new_document.json").toURI());
        File deltaFile = new File(getClass().getResource("json_shadow_examples/good_delta_node.json").toURI());
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(shadowName);
        byte[] documentByteData = Files.readAllBytes(documentFile.toPath());
        byte[] deltaFileByteData = Files.readAllBytes(deltaFile.toPath());
        Optional<JsonNode> documentJson = JsonUtil.getPayloadJson(documentByteData);
        Optional<JsonNode> deltaJson = JsonUtil.getPayloadJson(deltaFileByteData);
        assertTrue(documentJson.isPresent());
        assertTrue(deltaJson.isPresent());
        JsonNode deltaColorNode = deltaJson.get().get(Constants.SHADOW_DOCUMENT_STATE);
        ((ObjectNode)documentJson.get().get(Constants.SHADOW_DOCUMENT_STATE))
                .set(Constants.SHADOW_DOCUMENT_STATE_DELTA, deltaColorNode);

        GetThingShadowResponse expectedResponse = new GetThingShadowResponse();
        expectedResponse.setPayload(documentByteData);

        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(documentByteData));
        GetThingShadowResponse actualResponse = getThingShadowIPCHandler.handleRequest(request);

        Optional<JsonNode> retrievedDocument = JsonUtil.getPayloadJson(actualResponse.getPayload());
        assertTrue(retrievedDocument.isPresent());
        assertTrue(retrievedDocument.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) retrievedDocument.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        assertThat(retrievedDocument.get(), Matchers.is(documentJson.get()));

        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());

        assertNotNull(acceptRequestCaptor.getValue());

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getValue().getPayload());
        assertTrue(acceptedJson.isPresent());
        assertTrue(acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);
        assertEquals(shadowName, acceptRequestCaptor.getValue().getShadowName());
        assertEquals(THING_NAME, acceptRequestCaptor.getValue().getThingName());
        assertEquals(Operation.GET_SHADOW, acceptRequestCaptor.getValue().getPublishOperation());
        assertEquals(IPCUtil.LogEvents.GET_THING_SHADOW.code(), acceptRequestCaptor.getValue().getPublishOperation().getLogEventType());
        assertThat(acceptedJson.get(), Matchers.is(documentJson.get()));
    }

    @Test
    void GIVEN_get_thing_shadow_ipc_handler_WHEN_document_not_found_THEN_throw_resource_not_found_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ResourceNotFoundError.class);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.empty());
        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(ResourceNotFoundError.class, () -> getThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.GET_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(404, errorMessage.getErrorCode());
        assertEquals("No shadow exists with name: testShadowName", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.GET_THING_SHADOW.code(), rejectRequestCaptor.getValue().getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_get_thing_shadow_ipc_handler_WHEN_dao_sends_data_exception_THEN_throw_service_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ShadowManagerDataException.class);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        doThrow(ShadowManagerDataException.class).when(mockDao).getShadowThing(any(), any());
        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(ServiceError.class, () -> getThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.GET_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(500, errorMessage.getErrorCode());
        assertEquals("Internal service failure", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.GET_THING_SHADOW.code(), rejectRequestCaptor.getValue().getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_get_thing_shadow_ipc_handler_WHEN_ipc_request_unauthorized_THEN_throw_unauthorized_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, AuthorizationException.class);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class)))
                .thenThrow(AuthorizationException.class);

        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(UnauthorizedError.class, () -> getThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.GET_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(401, errorMessage.getErrorCode());
        assertEquals("Unauthorized", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.GET_THING_SHADOW.code(), rejectRequestCaptor.getValue().getPublishOperation().getLogEventType());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void GIVEN_missing_thing_name_WHEN_handle_request_THEN_throw_invalid_arguments_error(String thingName, ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(thingName);
        request.setShadowName(SHADOW_NAME);

        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(InvalidArgumentsError.class, () -> getThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.GET_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(404, errorMessage.getErrorCode());
        assertEquals("Thing not found", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.GET_THING_SHADOW.code(), rejectRequestCaptor.getValue().getPublishOperation().getLogEventType());
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
