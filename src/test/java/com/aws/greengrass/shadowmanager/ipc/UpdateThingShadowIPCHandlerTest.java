/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.ipc.model.AcceptRequest;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.aws.greengrass.shadowmanager.ipc.model.RejectRequest;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.*;
import software.amazon.awssdk.crt.eventstream.ServerConnectionContinuation;
import software.amazon.awssdk.eventstreamrpc.AuthenticationData;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;

import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
class UpdateThingShadowIPCHandlerTest {

    private static final String TEST_SERVICE = "TestService";
    private static final String THING_NAME = "testThingName";
    private static final String SHADOW_NAME = "testShadowName";
    private static final byte[] UPDATE_DOCUMENT = "{\"id\": 1, \"name\": \"The Beatles\"}".getBytes();

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

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_with_named_shadow_WHEN_handle_request_THEN_update_thing_shadow() {
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowResponse expectedResponse = new UpdateThingShadowResponse();
        expectedResponse.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(UPDATE_DOCUMENT));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(UPDATE_DOCUMENT));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        assertEquals(expectedResponse, actualResponse);
        verify(mockPubSubClientWrapper, times(1))
                .accept(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(0))
                .delta(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1))
                .documents(acceptRequestCaptor.capture());
        assertEquals(2, acceptRequestCaptor.getAllValues().size());


        assertEquals(SHADOW_NAME, acceptRequestCaptor.getAllValues().get(0).getShadowName());
        assertArrayEquals(UPDATE_DOCUMENT, acceptRequestCaptor.getAllValues().get(0).getPayload());
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(0).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(0).getPublishOperation());

        assertEquals(SHADOW_NAME, acceptRequestCaptor.getAllValues().get(1).getShadowName());
        assertArrayEquals(new byte[0], acceptRequestCaptor.getAllValues().get(1).getPayload());
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(1).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(1).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(1).getPublishOperation());
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_with_empty_shadow_name_WHEN_handle_request_THEN_update_thing_shadow() {
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName("");
        request.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowResponse expectedResponse = new UpdateThingShadowResponse();
        expectedResponse.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(UPDATE_DOCUMENT));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(UPDATE_DOCUMENT));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        assertEquals(expectedResponse, actualResponse);
        verify(mockPubSubClientWrapper, times(1))
                .accept(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(0))
                .delta(acceptRequestCaptor.capture());
        verify(mockPubSubClientWrapper, times(1))
                .documents(acceptRequestCaptor.capture());
        assertEquals(2, acceptRequestCaptor.getAllValues().size());


        assertEquals("", acceptRequestCaptor.getAllValues().get(0).getShadowName());
        assertArrayEquals(UPDATE_DOCUMENT, acceptRequestCaptor.getAllValues().get(0).getPayload());
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(0).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(0).getPublishOperation());

        assertEquals("", acceptRequestCaptor.getAllValues().get(1).getShadowName());
        assertArrayEquals(new byte[0], acceptRequestCaptor.getAllValues().get(1).getPayload());
        assertEquals(THING_NAME, acceptRequestCaptor.getAllValues().get(1).getThingName());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, acceptRequestCaptor.getAllValues().get(1).getPublishOperation().getLogEventType());
        assertEquals(Operation.UPDATE_SHADOW, acceptRequestCaptor.getAllValues().get(1).getPublishOperation());
    }

    @Test
    void GIVEN_update_thing_shadow_WHEN_dao_update_sends_data_exception_THEN_update_thing_shadow_fails(ExtensionContext context) {
        ignoreExceptionOfType(context, ShadowManagerDataException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowResponse expectedResponse = new UpdateThingShadowResponse();
        expectedResponse.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        doThrow(ShadowManagerDataException.class).when(mockDao).getShadowThing(any(), any());

        assertThrows(ServiceError.class, () -> updateThingShadowIPCHandler.handleRequest(request));

        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertNotEquals(Instant.EPOCH.toEpochMilli(), errorMessage.getTimestamp());
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(500, errorMessage.getErrorCode());
        assertEquals("Internal service failure", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_missing_payload_THEN_update_thing_shadow(ExtensionContext context) {
        ignoreExceptionOfType(context, IllegalArgumentException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);

        assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(400, errorMessage.getErrorCode());
        assertEquals("Invalid clientToken", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_ipc_request_unauthorized_THEN_throw_unauthorized_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, AuthorizationException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(UPDATE_DOCUMENT);
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class)))
                .thenThrow(AuthorizationException.class);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(UnauthorizedError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubClientWrapper, times(1)).reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(401, errorMessage.getErrorCode());
        assertEquals("Unauthorized", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void GIVEN_missing_thing_name_WHEN_handle_request_THEN_throw_invalid_arguments_error(String thingName, ExtensionContext context) {
        ignoreExceptionOfType(context, IllegalArgumentException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(thingName);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(400, errorMessage.getErrorCode());
        assertEquals("Invalid clientToken", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_missing_payload_THEN_throw_invalid_arguments_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, IllegalArgumentException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(new byte[0]);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));

        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(400, errorMessage.getErrorCode());
        assertEquals("Invalid clientToken", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }


    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_unexpected_empty_return_during_update_THEN_throw_service_error_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ServiceError.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubClientWrapper);
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.empty());

        ServiceError thrown = assertThrows(ServiceError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertTrue(thrown.getMessage().contains("Unexpected error"));

        verify(mockPubSubClientWrapper, times(1))
                .reject(rejectRequestCaptor.capture());

        assertNotNull(rejectRequestCaptor.getValue());

        assertEquals(SHADOW_NAME, rejectRequestCaptor.getValue().getShadowName());
        ErrorMessage errorMessage = rejectRequestCaptor.getValue().getErrorMessage();
        assertNotEquals(Instant.EPOCH.toEpochMilli(), errorMessage.getTimestamp());
        assertEquals(Operation.UPDATE_SHADOW, rejectRequestCaptor.getValue().getPublishOperation());
        assertEquals(500, errorMessage.getErrorCode());
        assertEquals("Internal service failure", errorMessage.getMessage());
        assertEquals(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code, rejectRequestCaptor.getAllValues().get(0).getPublishOperation().getLogEventType());
    }
}
