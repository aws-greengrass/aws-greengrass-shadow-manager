package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.crt.eventstream.ServerConnectionContinuation;
import software.amazon.awssdk.eventstreamrpc.AuthenticationData;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class ShadowManagerIPCAgentTest {
    private static final String TEST_SERVICE = "TestService";
    private static final String THING_NAME = "testThingName";
    private static final String SHADOW_NAME = "testShadowName";
    private static final byte[] BASE_DOCUMENT =  "{\"id\": 1, \"name\": \"The Beatles\"}".getBytes();

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

    @Mock
    OperationContinuationHandlerContext mockContext;

    @Mock
    ShadowManager mockShadowManager;

    @Mock
    AuthenticationData mockAuthenticationData;

    @Captor
    ArgumentCaptor<GetThingShadowRequest> getThingShadowRequestArgumentCaptor;

    @Captor
    ArgumentCaptor<UpdateThingShadowRequest> updateThingShadowRequestArgumentCaptor;

    @Captor
    ArgumentCaptor<String> stringArgumentCaptor;

    private ShadowManagerIPCAgent shadowManagerIPCAgent;

    @BeforeEach
    void setup () {
        when(mockContext.getContinuation()).thenReturn(mock(ServerConnectionContinuation.class));
        when(mockContext.getAuthenticationData()).thenReturn(mockAuthenticationData);
        when(mockAuthenticationData.getIdentityLabel()).thenReturn(TEST_SERVICE);
        this.shadowManagerIPCAgent = new ShadowManagerIPCAgent();
        shadowManagerIPCAgent.setShadowManager(mockShadowManager);
    }

    @Test
    void GIVEN_ShadowManagerIPCAgent_WHEN_handle_request_THEN_get_thing_shadow() {
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        GetThingShadowResponse expectedResponse = new GetThingShadowResponse();
        expectedResponse.setPayload(convertBytesToPayload(BASE_DOCUMENT));

        when(mockShadowManager.handleGetThingShadowIPCRequest(any(), any())).thenReturn(expectedResponse);
        GetThingShadowResponse actualResponse =
                shadowManagerIPCAgent.getGetThingShadowOperationHandler(mockContext).handleRequest(request);
        verify(mockShadowManager).handleGetThingShadowIPCRequest(getThingShadowRequestArgumentCaptor.capture(),
                stringArgumentCaptor.capture());
        assertEquals(request, getThingShadowRequestArgumentCaptor.getValue());
        assertEquals(expectedResponse, actualResponse);
    }

    /*
    @Test
    void GIVEN_ShadowManagerIPCAgent_WHEN_handle_request_THEN_update_thing_shadow() {
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(convertBytesToPayload(BASE_DOCUMENT));

        UpdateThingShadowResponse expectedResponse = new UpdateThingShadowResponse();
        expectedResponse.setPayload(convertBytesToPayload(BASE_DOCUMENT));

        when(mockShadowManager.handleUpdateThingShadowIPCRequest(any(), any())).thenReturn(expectedResponse);
        UpdateThingShadowResponse actualResponse =
                shadowManagerIPCAgent.getUpdateThingShadowOperationHandler(mockContext).handleRequest(request);
        verify(mockShadowManager).handleUpdateThingShadowIPCRequest(updateThingShadowRequestArgumentCaptor.capture(),
                stringArgumentCaptor.capture());
        assertEquals(request, updateThingShadowRequestArgumentCaptor.getValue());
        assertEquals(expectedResponse, actualResponse);
    }
    */

    private Map<String, Object> convertBytesToPayload(byte[] doc) {
        try{
            return OBJECT_MAPPER.readValue(doc, new TypeReference<Map<String, Object>>() {});
        } catch (IOException e) {
            throw new ShadowManagerDataException(e);
        }
    }
}
