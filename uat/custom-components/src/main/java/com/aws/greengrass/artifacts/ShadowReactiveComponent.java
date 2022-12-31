/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.artifacts;

import com.aws.greengrass.utils.IPCTestUtils;
import com.aws.greengrass.utils.JsonMerger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPC;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClientV2;
import software.amazon.awssdk.aws.greengrass.model.ReportedLifecycleState;
import software.amazon.awssdk.aws.greengrass.model.SubscribeToTopicRequest;
import software.amazon.awssdk.aws.greengrass.model.SubscribeToTopicResponse;
import software.amazon.awssdk.aws.greengrass.model.SubscriptionResponseMessage;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.aws.greengrass.model.UpdateStateRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;
import software.amazon.awssdk.eventstreamrpc.StreamResponseHandler;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ShadowReactiveComponent implements Consumer<String[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShadowReactiveComponent.class);
    private GreengrassCoreIPCClientV2 eventStreamRpcConnection = null;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final CountDownLatch LATCH = new CountDownLatch(1);

    @Override
    public void accept(String[] args) {
        try {
            if (args.length > 6) {
                LOGGER.error("Need more arguments. Expected arguments: <ThingName> <ShadowName> <UpdatedShadowRequest1> <ExpectedShadowDocument1> <SubscribeTopic> <ExpectedShadowDocument2>");
                return;
            }

            String thingName = args[0];
            String shadowName = "CLASSIC".equals(args[1]) ? "" : args[1];
            String updateShadowDocumentRequest1 = args[2];
            byte[] updateShadowDocumentPayload1 = null;
            JsonNode expectedUpdateShadowDocumentJson = null;
            JsonNode expectedUpdateShadowDocumentJson2 = null;
            if (!isNullOrEmpty(updateShadowDocumentRequest1)) {
                updateShadowDocumentPayload1 = updateShadowDocumentRequest1.getBytes(UTF_8);
            }
            String expectedUpdateShadowDocumentRequest1 = args[3];
            if (!isNullOrEmpty(expectedUpdateShadowDocumentRequest1)) {
                byte[] expectedUpdateShadowDocumentPayload1 = expectedUpdateShadowDocumentRequest1.getBytes(UTF_8);
                expectedUpdateShadowDocumentJson = MAPPER.readTree(expectedUpdateShadowDocumentPayload1);
            }
            if (updateShadowDocumentPayload1 != null) {
                handleUpdateThingShadowOperation(thingName, shadowName, updateShadowDocumentPayload1, expectedUpdateShadowDocumentJson);
            }

            String subscribeTopic = System.getenv("SubscribeTopic");
            if (subscribeTopic != null) {
                String expectedUpdateShadowDocumentRequest2 = args[4];
                if (!isNullOrEmpty(expectedUpdateShadowDocumentRequest2)) {
                    byte[] expectedUpdateShadowDocumentPayload2 = expectedUpdateShadowDocumentRequest2.getBytes(UTF_8);
                    if (expectedUpdateShadowDocumentPayload2.length > 0) {
                        expectedUpdateShadowDocumentJson2 = MAPPER.readTree(expectedUpdateShadowDocumentPayload2);
                    }
                }
                subscribeToShadowUpdates(thingName, shadowName, subscribeTopic, expectedUpdateShadowDocumentJson, expectedUpdateShadowDocumentJson2);
            }
        } catch (InterruptedException | ExecutionException | IOException e) {
            LOGGER.error("Error", e);
            System.exit(1);
        } finally {
            if (eventStreamRpcConnection != null) {
                try {
                    eventStreamRpcConnection.close();
                } catch (Exception ex) {
                    LOGGER.error("Unexpected error occurred while closing IPC connection", ex);
                }
            }
        }
    }

    private boolean isNullOrEmpty(String s) {
        return s == null || "".equals(s) || "null".equals(s);
    }

    private void updateComponentState(GreengrassCoreIPC greengrassCoreIPCClient, ReportedLifecycleState state) throws InterruptedException, ExecutionException {
        UpdateStateRequest updateStateRequest = new UpdateStateRequest();
        updateStateRequest.setState(state);
        greengrassCoreIPCClient.updateState(updateStateRequest, Optional.empty()).getResponse().get();
    }

    private void subscribeToShadowUpdates(String thingName, String shadowName, String topic, JsonNode expectedUpdateShadowDocumentJson, JsonNode expectedUpdateShadowDocumentJson2) {
        try {
            SubscribeToTopicRequest subscribe = new SubscribeToTopicRequest();
            subscribe.setTopic(topic);
            eventStreamRpcConnection = IPCTestUtils.getGreengrassClient();
            GreengrassCoreIPC greengrassCoreIPCClient = eventStreamRpcConnection.getClient();
            CompletableFuture<SubscribeToTopicResponse> fut = greengrassCoreIPCClient.subscribeToTopic(subscribe, Optional.of(new StreamResponseHandler<SubscriptionResponseMessage>() {
                @Override
                public void onStreamEvent(SubscriptionResponseMessage message) {
                    String payload = new String(message.getBinaryMessage().getMessage(), UTF_8);
                    LOGGER.info("Received new message: {}", payload);
                    try {
                        LOGGER.info("Received new message on {}", topic);
                        if (topic.contains("delta")) {
                            JsonNode retrievedDeltaPayloadJson = MAPPER.readTree(message.getBinaryMessage().getMessage());
                            JsonNode newUpdateDocument = calculateNewDocument(retrievedDeltaPayloadJson, expectedUpdateShadowDocumentJson);
                            handleUpdateThingShadowOperation(thingName, shadowName, MAPPER.writeValueAsBytes(newUpdateDocument), expectedUpdateShadowDocumentJson2);
                        }
                    } catch (IOException | ExecutionException | InterruptedException e) {
                        LOGGER.error("Unable to send assertion for receive message", e);
                    }

                    LATCH.countDown();
                    LOGGER.info("Assertion posted");
                }

                @Override
                public boolean onStreamError(Throwable error) {
                    LOGGER.error("Received a stream error", error);
                    return false;
                }

                @Override
                public void onStreamClosed() {
                    LOGGER.info("Subscribe to topic stream closed.");
                }
            })).getResponse();

            try {
                fut.get(50, TimeUnit.SECONDS);
                LOGGER.info("Successfully subscribed to {}", topic);

                if (!LATCH.await(180, TimeUnit.SECONDS)) {
                    LOGGER.error("Timed out waiting for the message");
                    updateComponentState(greengrassCoreIPCClient, ReportedLifecycleState.ERRORED);
                }
            } catch (TimeoutException e) {
                LOGGER.error("Timeout occurred while subscribing to {}", topic, e);
                updateComponentState(greengrassCoreIPCClient, ReportedLifecycleState.ERRORED);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnauthorizedError) {
                    UnauthorizedError unauthorizedError = (UnauthorizedError) e.getCause();
                    LOGGER.error("Unauthorized error while subscribing to {}", topic, e);
                    updateComponentState(greengrassCoreIPCClient, ReportedLifecycleState.ERRORED);
                } else {
                    LOGGER.error("Execution error while subscribing to {}", topic, e);
                    updateComponentState(greengrassCoreIPCClient, ReportedLifecycleState.ERRORED);
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (IOException | ExecutionException e) {
            LOGGER.error("Exception occurred while running subscribe", e);
        } finally {
            if (eventStreamRpcConnection != null) {
                try {
                    eventStreamRpcConnection.close();
                } catch (Exception ex) {
                    LOGGER.error("Unexpected error occurred while closing IPC connection", ex);
                }
            }
        }
    }

    void removeTimeStamp(JsonNode node) {
        ((ObjectNode) node).remove("timestamp");
    }

    void removeMetadata(JsonNode node) {
        ((ObjectNode) node).remove("metadata");
    }

    private JsonNode calculateNewDocument(JsonNode retrievedDeltaPayloadJson, JsonNode expectedUpdateShadowDocumentJson) {
        JsonNode newUpdateDocument = expectedUpdateShadowDocumentJson.deepCopy();
        JsonMerger.merge(newUpdateDocument.get("state").get("reported"), retrievedDeltaPayloadJson.get("state"));
        ((ObjectNode) newUpdateDocument).set("version", new IntNode(retrievedDeltaPayloadJson.get("version").asInt()));
        return newUpdateDocument;
    }

    void handleUpdateThingShadowOperation(String thingName, String shadowName, byte[] updateDocument, JsonNode expectedShadowDocumentJson)
            throws ExecutionException, InterruptedException, IOException {
        GreengrassCoreIPCClientV2 eventStreamRpcConnection = null;
        try {
            LOGGER.info("Updating shadow for {} with name {}", thingName, shadowName);

            eventStreamRpcConnection = IPCTestUtils.getGreengrassClient();
            GreengrassCoreIPC greengrassCoreIPCClient = eventStreamRpcConnection.getClient();
            UpdateThingShadowRequest updateThingShadowRequest = new UpdateThingShadowRequest();
            updateThingShadowRequest.setThingName(thingName);
            updateThingShadowRequest.setShadowName(shadowName);
            updateThingShadowRequest.setPayload(updateDocument);

            CompletableFuture<UpdateThingShadowResponse> fut =
                    greengrassCoreIPCClient.updateThingShadow(updateThingShadowRequest, Optional.empty()).getResponse();

            UpdateThingShadowResponse updateThingShadowResponse = fut.get(90, TimeUnit.SECONDS);
            JsonNode receivedShadowDocumentJson = MAPPER.readTree(updateThingShadowResponse.getPayload());
            removeTimeStamp(receivedShadowDocumentJson);
            removeMetadata(receivedShadowDocumentJson);
            if (receivedShadowDocumentJson.equals(expectedShadowDocumentJson)) {
                LOGGER.info("Updated shadow for {} with name {} successfully", thingName, shadowName);
            } else {
                LOGGER.info("Not Updated shadow for {} with name {} successfully", thingName, shadowName);
            }
        } catch (TimeoutException e) {
            LOGGER.error("Timeout occurred while updating shadow to {}:{}", thingName, shadowName, e);
        } finally {
            if (eventStreamRpcConnection != null) {
                try {
                    eventStreamRpcConnection.close();
                } catch (Exception ex) {
                    LOGGER.error("Unexpected error occurred while closing IPC connection", ex);
                }
            }
        }
    }

}
