/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.artifacts;

import com.aws.greengrass.utils.IPCTestUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPC;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClientV2;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.ListNamedShadowsForThingRequest;
import software.amazon.awssdk.aws.greengrass.model.ListNamedShadowsForThingResponse;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ShadowComponent implements Consumer<String[]> {
    private String thingName;
    private String operation;
    private String shadowName;
    private String shadowDocument;
    private byte[] shadowDocumentPayload;
    private Integer pageSize;
    private String nextToken;
    private int timeoutSeconds;
    private static final Logger LOGGER = LoggerFactory.getLogger(ShadowComponent.class);
    private static final List<String> NAMED_SHADOWS_LIST = Arrays.asList("alpha", "bravo", "charlie", "delta");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private GreengrassCoreIPC greengrassCoreIPCClient = null;

    @Override
    public void accept(String[] args) {
        if (args.length != 7) {
            LOGGER.error(
                    "Wrong number of arguments. Expected arguments: <Operation> <ThingName> <ShadowName> <ShadowDocument> <PageSize> <NextToken> <Expected Document> <timeout>");
            for (String arg : args) {
                LOGGER.error(arg);
            }
            return;
        }
        parseArgs(args);
        try (GreengrassCoreIPCClientV2 eventStreamRpcConnection = IPCTestUtils.getGreengrassClient()) {
            greengrassCoreIPCClient = eventStreamRpcConnection.getClient();
            handleIPCOperation();
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Exception while handling the IPC operation", e);
        } catch (Exception e) {
            LOGGER.error("Exception occurred while getting the ipc client", e);
        }
    }

    private void parseArgs(String[] args) {
        operation = args[0];
        thingName = args[1];
        shadowName = "CLASSIC".equals(args[2]) ? "" : args[2];
        shadowDocument = args[3];
        shadowDocumentPayload = shadowDocument.getBytes();
        pageSize = (!args[4].isEmpty()) ? Integer.parseInt(args[4]) : null;
        nextToken = args[5];
        timeoutSeconds = (!args[6].isEmpty()) ? Integer.parseInt(args[6]) : 0;

    }

    private void handleIPCOperation() throws InterruptedException {
        boolean isSuccessful = false;
        Instant finalTime = Instant.now().plusSeconds(timeoutSeconds);
        do {
            try {
                switch (operation) {
                    case "GetThingShadow":
                        handleGetThingShadowOperation(thingName, shadowName);
                        break;
                    case "UpdateThingShadow":
                        handleUpdateThingShadowOperation(thingName, shadowName, shadowDocumentPayload);
                        break;
                    case "DeleteThingShadow":
                        handleDeleteThingShadowOperation(thingName, shadowName);
                        break;
                    case "SetupListNamedShadowTest":
                        handleSetupForListNamedShadows(thingName, shadowDocumentPayload);
                        break;
                    case "ListNamedShadowsForThing":
                        handleListNamedShadowsForThingOperation(thingName, pageSize, nextToken);
                        break;
                    case "NoOp":
                        // Do nothing
                        break;
                    default:
                        LOGGER.error("No matching operation found for: {}", operation);
                        throw new ExecutionException(new UnsupportedOperationException("No matching operation found for: " + operation));
                }
                isSuccessful = true;
            } catch (ExecutionException | IOException | InterruptedException e) {
                String errorClassName = e.getCause().getClass().getSimpleName();
                String errorMessage = String.format("%s error in the %s operation", errorClassName, operation);
                LOGGER.error(errorMessage, e);
                TimeUnit.SECONDS.sleep(1);
            }
        } while (!isSuccessful && finalTime.isAfter(Instant.now()));
    }

    private void removeTimeStamp(JsonNode node) {
        ((ObjectNode) node).remove("timestamp");
    }

    private void removeMetadata(JsonNode node) {
        ((ObjectNode) node).remove("metadata");
    }

    // Basic Operation handlers. Execute operation and verify results with expected payload/update document
    private void handleGetThingShadowOperation(String thingName, String shadowName)
            throws ExecutionException, InterruptedException, IOException {

        GetThingShadowRequest getThingShadowRequest = new GetThingShadowRequest();
        getThingShadowRequest.setThingName(thingName);
        getThingShadowRequest.setShadowName(shadowName);

        GetThingShadowResponse getThingShadowResponse =
                greengrassCoreIPCClient.getThingShadow(getThingShadowRequest, Optional.empty()).getResponse().get();

        JsonNode receivedShadowDocumentJson = MAPPER.readTree(getThingShadowResponse.getPayload());
        removeTimeStamp(receivedShadowDocumentJson);
        removeMetadata(receivedShadowDocumentJson);
        LOGGER.info(MAPPER.writeValueAsString(receivedShadowDocumentJson));
        LOGGER.error(MAPPER.writeValueAsString(receivedShadowDocumentJson));
    }

    private void handleUpdateThingShadowOperation(String thingName, String shadowName, byte[] updateDocument
    )
            throws ExecutionException, InterruptedException, IOException {
        UpdateThingShadowRequest updateThingShadowRequest = new UpdateThingShadowRequest();
        updateThingShadowRequest.setThingName(thingName);
        updateThingShadowRequest.setShadowName(shadowName);
        updateThingShadowRequest.setPayload(updateDocument);

        UpdateThingShadowResponse updateThingShadowResponse =
                greengrassCoreIPCClient.updateThingShadow(updateThingShadowRequest, Optional.empty()).getResponse().get();

        JsonNode receivedShadowDocumentJson = MAPPER.readTree(updateThingShadowResponse.getPayload());
        removeTimeStamp(receivedShadowDocumentJson);
        removeMetadata(receivedShadowDocumentJson);
    }

    private void handleDeleteThingShadowOperation(String thingName, String shadowName)
            throws ExecutionException, InterruptedException, IOException {
        DeleteThingShadowRequest deleteThingShadowRequest = new DeleteThingShadowRequest();
        deleteThingShadowRequest.setThingName(thingName);
        deleteThingShadowRequest.setShadowName(shadowName);

        DeleteThingShadowResponse deleteThingShadowResponse =
                greengrassCoreIPCClient.deleteThingShadow(deleteThingShadowRequest, Optional.empty()).getResponse().get();
    }

    private void handleListNamedShadowsForThingOperation(String thingName, Integer pageSize, String nextToken)
            throws ExecutionException, InterruptedException, IOException {
        ListNamedShadowsForThingRequest listNamedShadowsForThingRequest = new ListNamedShadowsForThingRequest();
        listNamedShadowsForThingRequest.setThingName(thingName);
        listNamedShadowsForThingRequest.setPageSize(pageSize);
        listNamedShadowsForThingRequest.setNextToken(nextToken);

        ListNamedShadowsForThingResponse listNamedShadowsForThingResponse =
                greengrassCoreIPCClient.listNamedShadowsForThing(listNamedShadowsForThingRequest, Optional.empty()).getResponse().get();

        List<String> resultList = listNamedShadowsForThingResponse.getResults();

        Integer namedShadowCount = resultList.size();
        LOGGER.info("Returned {} from list named shadow for thingName: {}", namedShadowCount, thingName);
        // expected to get token if returned list is equal to pageSize
        if (namedShadowCount.equals(pageSize)) {
            String returnToken = listNamedShadowsForThingResponse.getNextToken();
            if (returnToken != null && !returnToken.isEmpty()) {
                LOGGER.info("Retrieved token {} from list named shadow for thingName: {}", returnToken, thingName);
            } else {
                LOGGER.info("Expected token not returned in response");
            }
        }

        for (String namedShadowResult : resultList) {
            LOGGER.info("Retrieved named shadow {} from list named shadow for thingName:{}", namedShadowResult, thingName);
        }
    }

    // Setup helper function which creates four named shadows for a particular thing
    private void handleSetupForListNamedShadows(String thingName, byte[] updateDocument)
            throws ExecutionException, InterruptedException, IOException {
        for (String namedShadow : NAMED_SHADOWS_LIST) {
            handleUpdateThingShadowOperation(thingName, namedShadow, updateDocument);
        }
    }
}

