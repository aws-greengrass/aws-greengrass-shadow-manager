package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
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

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class ShadowManagerIPCAgentTest {
    private static final String TEST_SERVICE = "TestService";
    private static final String THING_NAME = "testThingName";
    private static final String SHADOW_NAME = "testShadowName";
    private static final byte[] BASE_DOCUMENT =  "{\"id\": 1, \"name\": \"The Beatles\"}".getBytes();
    private static final Map<String, Object> BASE_DOCUMENT_PAYLOAD = ShadowManager.convertBytesToPayload(BASE_DOCUMENT);

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
        expectedResponse.setPayload(BASE_DOCUMENT_PAYLOAD);

        when(mockShadowManager.handleGetThingShadowIPCRequest(any(), any())).thenReturn(expectedResponse);
        GetThingShadowResponse actualResponse =
                shadowManagerIPCAgent.getGetThingShadowOperationHandler(mockContext).handleRequest(request);
        verify(mockShadowManager).handleGetThingShadowIPCRequest(getThingShadowRequestArgumentCaptor.capture(),
                stringArgumentCaptor.capture());
        assertEquals(request, getThingShadowRequestArgumentCaptor.getValue());
        assertEquals(expectedResponse, actualResponse);
    }
}
