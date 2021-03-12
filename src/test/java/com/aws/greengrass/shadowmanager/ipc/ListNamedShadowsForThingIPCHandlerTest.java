/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.ListNamedShadowsForThingRequest;
import software.amazon.awssdk.aws.greengrass.model.ListNamedShadowsForThingResponse;
import software.amazon.awssdk.crt.eventstream.ServerConnectionContinuation;
import software.amazon.awssdk.eventstreamrpc.AuthenticationData;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class ListNamedShadowsForThingIPCHandlerTest {
    private static final String TEST_SERVICE = "TestService";
    private static final String THING_NAME = "testThingName";
    private static final List<String> NAMED_SHADOW_LIST = Arrays.asList("one", "two", "three");

    private static final String EXPECTED_TOKEN_WITH_OFFSET = "o8Zz1puGZZ/aNy+OKKAN8A==";
    private static final int DECODED_OFFSET_VALUE = 3;
    private static final int PAGE_SIZE_MATCHING_SHADOW_LIST = 3;
    private static final int DEFAULT_OFFSET = 0;
    private static final int DEFAULT_PAGE_SIZE = 25;

    @Mock
    OperationContinuationHandlerContext mockContext;

    @Mock
    AuthenticationData mockAuthenticationData;

    @Mock
    AuthorizationHandler mockAuthorizationHandler;

    @Mock
    ShadowManagerDAO mockDao;

    @Captor
    ArgumentCaptor<Integer> offsetCaptor;
    @Captor
    ArgumentCaptor<Integer> pageSizeCaptor;

    @BeforeEach
    void setup () {
        when(mockContext.getContinuation()).thenReturn(mock(ServerConnectionContinuation.class));
        when(mockContext.getAuthenticationData()).thenReturn(mockAuthenticationData);
        when(mockAuthenticationData.getIdentityLabel()).thenReturn(TEST_SERVICE);
    }

    @Test
    void GIVEN_list_named_shadows_ipc_handler_WHEN_handle_request_THEN_list_named_shadows() {
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);

        ListNamedShadowsForThingResponse expectedResponse = new ListNamedShadowsForThingResponse();
        expectedResponse.setResults(NAMED_SHADOW_LIST);

        ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        when(mockDao.listNamedShadowsForThing(any(), anyInt(), anyInt())).thenReturn(Optional.of(NAMED_SHADOW_LIST));
        ListNamedShadowsForThingResponse actualResponse = listNamedShadowsForThingIPCHandler.handleRequest(request);
        assertEquals(expectedResponse.getResults(), actualResponse.getResults());
        assertNull(actualResponse.getNextToken());
        assertNotNull(actualResponse.getTimestamp());

        verify(mockDao, times(1)).listNamedShadowsForThing(any(),
                offsetCaptor.capture(), pageSizeCaptor.capture());
        assertEquals(DEFAULT_OFFSET, offsetCaptor.getValue());
        assertEquals(DEFAULT_PAGE_SIZE, pageSizeCaptor.getValue());
    }

    @Test
    void GIVEN_list_named_shadows_ipc_handler_WHEN_page_size_reached_THEN_return_next_token_with_response() {
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);
        request.setPageSize(PAGE_SIZE_MATCHING_SHADOW_LIST);

        ListNamedShadowsForThingResponse expectedResponse = new ListNamedShadowsForThingResponse();
        expectedResponse.setResults(NAMED_SHADOW_LIST);

        ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        when(mockDao.listNamedShadowsForThing(any(), anyInt(), anyInt())).thenReturn(Optional.of(NAMED_SHADOW_LIST));
        ListNamedShadowsForThingResponse actualResponse = listNamedShadowsForThingIPCHandler.handleRequest(request);

        assertEquals(expectedResponse.getResults(), actualResponse.getResults());
        assertEquals(EXPECTED_TOKEN_WITH_OFFSET, actualResponse.getNextToken());
        assertNotNull(actualResponse.getTimestamp());

        verify(mockDao, times(1)).listNamedShadowsForThing(any(),
                offsetCaptor.capture(), pageSizeCaptor.capture());
        assertEquals(DEFAULT_OFFSET, offsetCaptor.getValue());
        assertEquals(PAGE_SIZE_MATCHING_SHADOW_LIST, pageSizeCaptor.getValue());
    }

    @Test
    void GIVEN_list_named_shadows_ipc_handler_WHEN_next_token_passed_in_THEN_return_next_set_of_values() {
        ListNamedShadowsForThingRequest request = new ListNamedShadowsForThingRequest();
        request.setThingName(THING_NAME);
        request.setNextToken(EXPECTED_TOKEN_WITH_OFFSET);

        ListNamedShadowsForThingResponse expectedResponse = new ListNamedShadowsForThingResponse();
        expectedResponse.setResults(NAMED_SHADOW_LIST);

        ListNamedShadowsForThingIPCHandler listNamedShadowsForThingIPCHandler = new ListNamedShadowsForThingIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        when(mockDao.listNamedShadowsForThing(any(), anyInt(), anyInt())).thenReturn(Optional.of(NAMED_SHADOW_LIST));
        ListNamedShadowsForThingResponse actualResponse = listNamedShadowsForThingIPCHandler.handleRequest(request);

        assertEquals(expectedResponse.getResults(), actualResponse.getResults());
        assertNull(actualResponse.getNextToken());
        assertNotNull(actualResponse.getTimestamp());

        verify(mockDao, times(1)).listNamedShadowsForThing(any(),
                offsetCaptor.capture(), pageSizeCaptor.capture());
        assertEquals(DECODED_OFFSET_VALUE, offsetCaptor.getValue());
        assertEquals(DEFAULT_PAGE_SIZE, pageSizeCaptor.getValue());
    }

}
