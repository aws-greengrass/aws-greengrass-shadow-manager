/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.ipc.model.AcceptRequest;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.aws.greengrass.shadowmanager.ipc.model.RejectRequest;
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
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_TIMESTAMP;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
class DeleteThingShadowIPCHandlerTest {

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
    void setup() {
        when(mockContext.getContinuation()).thenReturn(mock(ServerConnectionContinuation.class));
        when(mockContext.getAuthenticationData()).thenReturn(mockAuthenticationData);
        when(mockAuthenticationData.getIdentityLabel()).thenReturn(TEST_SERVICE);
    }

    @ParameterizedTest
    @ValueSource(strings = {SHADOW_NAME, ""})
    void GIVEN_delete_thing_shadow_ipc_handler_with_named_shadow_WHEN_handle_request_THEN_delete_thing_shadow(String shadowName) throws URISyntaxException, IOException {
        File f = new File(getClass().getResource("json_shadow_examples/good_new_document.json").toURI());
        byte[] allByteData = Files.readAllBytes(f.toPath());
        Optional<JsonNode> shadowDocumentJson = JsonUtil.getPayloadJson(allByteData);
        assertTrue(shadowDocumentJson.isPresent());
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(shadowName);

        DeleteThingShadowResponse expectedResponse = new DeleteThingShadowResponse();
        expectedResponse.setPayload(new byte[0]);

        DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.deleteShadowThing(any(), any())).thenReturn(Optional.of(allByteData));

        DeleteThingShadowResponse actualResponse = deleteThingShadowIPCHandler.handleRequest(request);
        assertEquals(expectedResponse, actualResponse);
        verify(mockPubSubClientWrapper, times(1)).accept(acceptRequestCaptor.capture());
        assertNotNull(acceptRequestCaptor.getValue());

        Optional<JsonNode> acceptedJson = JsonUtil.getPayloadJson(acceptRequestCaptor.getValue().getPayload());
        assertTrue(acceptedJson.isPresent());
        assertTrue(acceptedJson.get().has(SHADOW_DOCUMENT_TIMESTAMP));
        ((ObjectNode) acceptedJson.get()).remove(SHADOW_DOCUMENT_TIMESTAMP);

        assertEquals(shadowName, acceptRequestCaptor.getValue().getShadowName());
        assertThat(acceptedJson.get().get(SHADOW_DOCUMENT_VERSION), Matchers.is(shadowDocumentJson.get().get(SHADOW_DOCUMENT_VERSION)));
        assertEquals(THING_NAME, acceptRequestCaptor.getValue().getThingName());
        assertEquals(Operation.DELETE_SHADOW, acceptRequestCaptor.getValue().getPublishOperation());
        assertEquals(IPCUtil.LogEvents.DELETE_THING_SHADOW.code(), acceptRequestCaptor.getValue().getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_document_not_found_THEN_throw_resource_not_found_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ResourceNotFoundError.class);
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        when(mockDao.deleteShadowThing(any(), any())).thenReturn(Optional.empty());
        DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(ResourceNotFoundError.class, () -> deleteThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertNotEquals(Instant.EPOCH.toEpochMilli(), errorMessage.getTimestamp());
        assertEquals(404, errorMessage.getErrorCode());
        assertEquals("No shadow exists with name: testShadowName", errorMessage.getMessage());
        assertEquals(Operation.DELETE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_dao_sends_data_exception_THEN_throw_service_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ShadowManagerDataException.class);
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        doThrow(ShadowManagerDataException.class).when(mockDao).deleteShadowThing(any(), any());
        DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(ServiceError.class, () -> deleteThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertNotEquals(Instant.EPOCH.toEpochMilli(), errorMessage.getTimestamp());
        assertEquals(Operation.DELETE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(500, errorMessage.getErrorCode());
        assertEquals("Internal service failure", errorMessage.getMessage());
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_ipc_request_unauthorized_THEN_throw_unauthorized_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, AuthorizationException.class);
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class)))
                .thenThrow(AuthorizationException.class);

        DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(UnauthorizedError.class, () -> deleteThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.DELETE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(401, errorMessage.getErrorCode());
        assertEquals("Unauthorized", errorMessage.getMessage());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void GIVEN_missing_thing_name_WHEN_handle_request_THEN_throw_invalid_arguments_error(String thingName, ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidRequestParametersException.class);
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(thingName);
        request.setShadowName(SHADOW_NAME);

        DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(InvalidArgumentsError.class, () -> deleteThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.DELETE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(404, errorMessage.getErrorCode());
        assertEquals("Thing not found", errorMessage.getMessage());
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_handle_stream_event_THEN_nothing_happens() {
        DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertDoesNotThrow(() -> deleteThingShadowIPCHandler.handleStreamEvent(mock(EventStreamJsonMessage.class)));
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_stream_closes_THEN_nothing_happens() {
        DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertDoesNotThrow(deleteThingShadowIPCHandler::onStreamClosed);
    }
}
