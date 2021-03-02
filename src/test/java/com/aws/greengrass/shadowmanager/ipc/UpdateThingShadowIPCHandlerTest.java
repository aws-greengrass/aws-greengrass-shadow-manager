/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.*;
import software.amazon.awssdk.crt.eventstream.ServerConnectionContinuation;
import software.amazon.awssdk.eventstreamrpc.AuthenticationData;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;
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

@ExtendWith({MockitoExtension.class, GGExtension.class})
class UpdateThingShadowIPCHandlerTest {

    private static final String TEST_SERVICE = "TestService";
    private static final String THING_NAME = "testThingName";
    private static final String SHADOW_NAME = "testShadowName";
    private static final byte[] UPDATE_DOCUMENT =  "{\"id\": 1, \"name\": \"The Beatles\"}".getBytes();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new JsonFactory());

    @Mock
    OperationContinuationHandlerContext mockContext;

    @Mock
    AuthenticationData mockAuthenticationData;

    @Mock
    AuthorizationHandler mockAuthorizationHandler;

    @Mock
    ShadowManagerDAO mockDao;

    @Mock
    PubSubIPCEventStreamAgent mockPubSubIPCEventStreamAgent;

    @Captor
    ArgumentCaptor<String> serviceNameCaptor;
    @Captor
    ArgumentCaptor<String> topicCaptor;
    @Captor
    ArgumentCaptor<byte[]> payloadCaptor;

    @BeforeEach
    void setup () {
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

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(UPDATE_DOCUMENT));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(UPDATE_DOCUMENT));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        assertEquals(expectedResponse, actualResponse);
        verify(mockPubSubIPCEventStreamAgent, times(2)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertEquals(2, serviceNameCaptor.getAllValues().size());
        assertEquals(2, payloadCaptor.getAllValues().size());
        assertEquals(2, topicCaptor.getAllValues().size());

        assertEquals(SERVICE_NAME, serviceNameCaptor.getAllValues().get(0));
        assertArrayEquals(UPDATE_DOCUMENT, payloadCaptor.getAllValues().get(0));
        assertEquals("$aws/things/testThingName/shadow/name/testShadowName/update/accepted", topicCaptor.getAllValues().get(0));

        assertEquals(SERVICE_NAME, serviceNameCaptor.getAllValues().get(1));
        assertArrayEquals(new byte[0], payloadCaptor.getAllValues().get(1));
        assertEquals("$aws/things/testThingName/shadow/name/testShadowName/update/delta", topicCaptor.getAllValues().get(1));
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_with_empty_shadow_name_WHEN_handle_request_THEN_update_thing_shadow() {
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName("");
        request.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowResponse expectedResponse = new UpdateThingShadowResponse();
        expectedResponse.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(UPDATE_DOCUMENT));
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(UPDATE_DOCUMENT));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        assertEquals(expectedResponse, actualResponse);
        verify(mockPubSubIPCEventStreamAgent, times(2)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertEquals(2, serviceNameCaptor.getAllValues().size());
        assertEquals(2, payloadCaptor.getAllValues().size());
        assertEquals(2, topicCaptor.getAllValues().size());

        assertEquals(SERVICE_NAME, serviceNameCaptor.getAllValues().get(0));
        assertArrayEquals(UPDATE_DOCUMENT, payloadCaptor.getAllValues().get(0));
        assertEquals("$aws/things/testThingName/shadow/update/accepted", topicCaptor.getAllValues().get(0));

        assertEquals(SERVICE_NAME, serviceNameCaptor.getAllValues().get(1));
        assertArrayEquals(new byte[0], payloadCaptor.getAllValues().get(1));
        assertEquals("$aws/things/testThingName/shadow/update/delta", topicCaptor.getAllValues().get(1));
    }

    @Test
    void GIVEN_update_thing_shadow_WHEN_dao_update_sends_data_exception_THEN_update_thing_shadow_fails(ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, ShadowManagerDataException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowResponse expectedResponse = new UpdateThingShadowResponse();
        expectedResponse.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);
        doThrow(ShadowManagerDataException.class).when(mockDao).getShadowThing(any(), any());

        assertThrows(ServiceError.class, () -> updateThingShadowIPCHandler.handleRequest(request));

        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertNotNull(serviceNameCaptor.getValue());
        assertNotNull(payloadCaptor.getValue());
        assertNotNull(topicCaptor.getValue());
        assertEquals(SERVICE_NAME, serviceNameCaptor.getValue());
        ErrorMessage errorMessage = OBJECT_MAPPER.readValue(payloadCaptor.getValue(), ErrorMessage.class);
        assertNotEquals(Instant.EPOCH.toEpochMilli(), errorMessage.getTimestamp());
        assertEquals(500, errorMessage.getErrorCode());
        assertEquals("Internal service failure", errorMessage.getMessage());
        assertEquals("$aws/things/testThingName/shadow/name/testShadowName/update/rejected", topicCaptor.getValue());
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_missing_payload_THEN_update_thing_shadow(ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);

        assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertNotNull(serviceNameCaptor.getValue());
        assertNotNull(payloadCaptor.getValue());
        assertNotNull(topicCaptor.getValue());
        assertEquals(SERVICE_NAME, serviceNameCaptor.getValue());
        ErrorMessage errorMessage = OBJECT_MAPPER.readValue(payloadCaptor.getValue(), ErrorMessage.class);
        assertEquals(400, errorMessage.getErrorCode());
        assertEquals("Invalid clientToken", errorMessage.getMessage());
        assertEquals("$aws/things/testThingName/shadow/name/testShadowName/update/rejected", topicCaptor.getValue());
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

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);
        assertThrows(UnauthorizedError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertNotNull(serviceNameCaptor.getValue());
        assertNotNull(payloadCaptor.getValue());
        assertNotNull(topicCaptor.getValue());
        assertEquals(SERVICE_NAME, serviceNameCaptor.getValue());
        ErrorMessage errorMessage = OBJECT_MAPPER.readValue(payloadCaptor.getValue(), ErrorMessage.class);
        assertEquals(401, errorMessage.getErrorCode());
        assertEquals("Unauthorized", errorMessage.getMessage());
        assertEquals("$aws/things/testThingName/shadow/name/testShadowName/update/rejected", topicCaptor.getValue());
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_missing_thing_name_THEN_throw_invalid_arguments_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName("");
        request.setShadowName(SHADOW_NAME);
        request.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);
        assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubIPCEventStreamAgent, times(0)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_missing_payload_THEN_throw_invalid_arguments_exception(ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(new byte[0]);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);
        assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertNotNull(serviceNameCaptor.getValue());
        assertNotNull(payloadCaptor.getValue());
        assertNotNull(topicCaptor.getValue());
        assertEquals(SERVICE_NAME, serviceNameCaptor.getValue());
        ErrorMessage errorMessage = OBJECT_MAPPER.readValue(payloadCaptor.getValue(), ErrorMessage.class);
        assertEquals(400, errorMessage.getErrorCode());
        assertEquals("Invalid clientToken", errorMessage.getMessage());
        assertEquals("$aws/things/testThingName/shadow/name/testShadowName/update/rejected", topicCaptor.getValue());
    }


    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_unexpected_empty_return_during_update_THEN_throw_service_error_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ServiceError.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.empty());

        ServiceError thrown = assertThrows(ServiceError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
        assertTrue(thrown.getMessage().contains("Unexpected error"));
    }
}
