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

import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class GetThingShadowIPCHandlerTest {

    private static final String TEST_SERVICE = "TestService";
    private static final String THING_NAME = "testThingName";
    private static final String SHADOW_NAME = "testShadowName";
    private static final byte[] BASE_DOCUMENT =  "{\"id\": 1, \"name\": \"The Beatles\"}".getBytes();
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
    void GIVEN_get_thing_shadow_ipc_handler_with_named_shadow_WHEN_handle_request_THEN_get_thing_shadow() {
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        GetThingShadowResponse expectedResponse = new GetThingShadowResponse();
        expectedResponse.setPayload(BASE_DOCUMENT);

        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(BASE_DOCUMENT));
        GetThingShadowResponse actualResponse = getThingShadowIPCHandler.handleRequest(request);
        assertEquals(expectedResponse, actualResponse);
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertNotNull(serviceNameCaptor.getValue());
        assertNotNull(payloadCaptor.getValue());
        assertNotNull(topicCaptor.getValue());
        assertEquals(SERVICE_NAME, serviceNameCaptor.getValue());
        assertArrayEquals(BASE_DOCUMENT, payloadCaptor.getValue());
        assertEquals("$aws/things/testThingName/shadow/name/testShadowName/get/accepted", topicCaptor.getValue());
    }

    @Test
    void GIVEN_get_thing_shadow_ipc_handler_with_empty_shadow_name_WHEN_handle_request_THEN_get_thing_shadow() {
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName("");

        GetThingShadowResponse expectedResponse = new GetThingShadowResponse();
        expectedResponse.setPayload(BASE_DOCUMENT);

        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(BASE_DOCUMENT));
        GetThingShadowResponse actualResponse = getThingShadowIPCHandler.handleRequest(request);
        assertEquals(expectedResponse, actualResponse);
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertNotNull(serviceNameCaptor.getValue());
        assertNotNull(payloadCaptor.getValue());
        assertNotNull(topicCaptor.getValue());
        assertEquals(SERVICE_NAME, serviceNameCaptor.getValue());
        assertArrayEquals(BASE_DOCUMENT, payloadCaptor.getValue());
        assertEquals("$aws/things/testThingName/shadow/get/accepted", topicCaptor.getValue());
    }

    @Test
    void GIVEN_get_thing_shadow_ipc_handler_WHEN_document_not_found_THEN_throw_resource_not_found_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ResourceNotFoundError.class);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.empty());
        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);
        assertThrows(ResourceNotFoundError.class, () -> getThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertNotNull(serviceNameCaptor.getValue());
        assertNotNull(payloadCaptor.getValue());
        assertNotNull(topicCaptor.getValue());
        assertEquals(SERVICE_NAME, serviceNameCaptor.getValue());
        ErrorMessage errorMessage = OBJECT_MAPPER.readValue(payloadCaptor.getValue(), ErrorMessage.class);
        assertNotEquals(Instant.EPOCH.toEpochMilli(), errorMessage.getTimestamp());
        assertEquals(404, errorMessage.getErrorCode());
        assertEquals("No shadow exists with name: testShadowName", errorMessage.getMessage());
        assertEquals("$aws/things/testThingName/shadow/name/testShadowName/get/rejected", topicCaptor.getValue());
    }

    @Test
    void GIVEN_get_thing_shadow_ipc_handler_WHEN_dao_sends_data_exception_THEN_throw_service_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ShadowManagerDataException.class);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        doThrow(ShadowManagerDataException.class).when(mockDao).getShadowThing(any(), any());
        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);
        assertThrows(ServiceError.class, () -> getThingShadowIPCHandler.handleRequest(request));
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
        assertEquals("$aws/things/testThingName/shadow/name/testShadowName/get/rejected", topicCaptor.getValue());
    }

    @Test
    void GIVEN_get_thing_shadow_ipc_handler_WHEN_ipc_request_unauthorized_THEN_throw_unauthorized_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, AuthorizationException.class);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class)))
                .thenThrow(AuthorizationException.class);

        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);
        assertThrows(UnauthorizedError.class, () -> getThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
        assertNotNull(serviceNameCaptor.getValue());
        assertNotNull(payloadCaptor.getValue());
        assertNotNull(topicCaptor.getValue());
        assertEquals(SERVICE_NAME, serviceNameCaptor.getValue());
        ErrorMessage errorMessage = OBJECT_MAPPER.readValue(payloadCaptor.getValue(), ErrorMessage.class);
        assertEquals(401, errorMessage.getErrorCode());
        assertEquals("Unauthorized", errorMessage.getMessage());
        assertEquals("$aws/things/testThingName/shadow/name/testShadowName/get/rejected", topicCaptor.getValue());
    }

    @Test
    void GIVEN_get_thing_shadow_ipc_handler_WHEN_missing_thing_name_THEN_throw_invalid_arguments_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName("");
        request.setShadowName(SHADOW_NAME);

        GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler, mockPubSubIPCEventStreamAgent);
        assertThrows(InvalidArgumentsError.class, () -> getThingShadowIPCHandler.handleRequest(request));
        verify(mockPubSubIPCEventStreamAgent, times(0)).publish(topicCaptor.capture(),
                payloadCaptor.capture(), serviceNameCaptor.capture());
    }
}
