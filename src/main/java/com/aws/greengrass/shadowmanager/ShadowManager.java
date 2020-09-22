/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.dependency.State;
import com.aws.iot.evergreen.ipc.ConnectionContext;
import com.aws.iot.evergreen.ipc.IPCRouter;
import com.aws.iot.evergreen.ipc.common.BuiltInServiceDestinationCode;
import com.aws.iot.evergreen.ipc.common.FrameReader;
import com.aws.iot.evergreen.ipc.exceptions.IPCException;
import com.aws.iot.evergreen.ipc.services.common.ApplicationMessage;
import com.aws.iot.evergreen.ipc.services.shadow.ShadowClientOpCodes;
import com.aws.iot.evergreen.ipc.services.shadow.models.DeleteThingShadowRequest;
import com.aws.iot.evergreen.ipc.services.shadow.models.DeleteThingShadowResult;
import com.aws.iot.evergreen.ipc.services.shadow.models.GetThingShadowRequest;
import com.aws.iot.evergreen.ipc.services.shadow.models.GetThingShadowResult;
import com.aws.iot.evergreen.ipc.services.shadow.models.ShadowGenericResponse;
import com.aws.iot.evergreen.ipc.services.shadow.models.ShadowResponseStatus;
import com.aws.iot.evergreen.ipc.services.shadow.models.UpdateThingShadowRequest;
import com.aws.iot.evergreen.ipc.services.shadow.models.UpdateThingShadowResult;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.iot.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import org.flywaydb.core.api.FlywayException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import javax.inject.Inject;

@ImplementsService(name = ShadowManager.SERVICE_NAME)
public class ShadowManager extends PluginService {
    enum LogEvents {
        IPC_REGISTRATION("shadow-ipc-registration"),
        IPC_ERROR("shadow-ipc-error"),
        DATABASE_OPERATION_ERROR("shadow-database-operation-error"),
        DATABASE_CLOSE_ERROR("shadow-database-close-error");

        String code;
        LogEvents(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }
    }


    public static final String SERVICE_NAME = "aws.greengrass.ShadowManager";
    // Should make this injected?
    private static final ObjectMapper CBOR_MAPPER = new CBORMapper();
    private final IPCRouter ipcRouter;
    private final ShadowManagerDAO dao;
    private final ShadowManagerDatabase database;

    /**
     * Ctr for ShadowManager.
     * @param topics topics passed by by the kernel
     * @param ipcRouter IPC router for handling local shadow Request / Reply
     * @param dao Local shadow repository for managing documents
     * @param database Local shadow database management
     */
    @Inject
    public ShadowManager(
            Topics topics,
            IPCRouter ipcRouter,
            ShadowManagerDAOImpl dao,
            ShadowManagerDatabase database) {
        super(topics);
        this.ipcRouter = ipcRouter;
        this.database = database;
        this.dao = dao;
    }

    private void registerHandler() throws IPCException {
        BuiltInServiceDestinationCode destinationCode = BuiltInServiceDestinationCode.SHADOW;
        ipcRouter.registerServiceCallback(destinationCode.getValue(), this::handleMessage);
        logger.atInfo()
                .setEventType(LogEvents.IPC_REGISTRATION.code())
                .addKeyValue("destination", destinationCode.name())
                .log();
    }

    @Override
    protected void install() throws InterruptedException {
        try {
            database.install();
        } catch (SQLException | FlywayException e) {
            serviceErrored(e);
        }
    }

    @Override
    public void startup() {
        try {
            // Register IPC
            registerHandler();
            reportState(State.RUNNING);
        } catch (IPCException e) {
            serviceErrored(e);
        }
    }

    @Override
    protected void shutdown() throws InterruptedException {
        super.shutdown();
        try {
            database.close();
        } catch (IOException e) {
            logger.atError()
                    .setEventType(LogEvents.DATABASE_CLOSE_ERROR.name())
                    .setCause(e)
                    .log("Failed to close out shadow database");
        }
    }

    /**
     * Handles local shadow service IPC calls.
     * @param message API message received from a client.
     * @param context connection context received from a client.
     * @return
     */
    private Future<FrameReader.Message> handleMessage(FrameReader.Message message, ConnectionContext context) {
        CompletableFuture<FrameReader.Message> future = new CompletableFuture<>();
        ShadowGenericResponse response = new ShadowGenericResponse();
        Optional<byte[]> result;
        final String thingName;
        ApplicationMessage applicationMessage = ApplicationMessage.fromBytes(message.getPayload());
        try {
            ShadowClientOpCodes opCode = ShadowClientOpCodes.values()[applicationMessage.getOpCode()];
            logger.atInfo().log("Received message with OpCode: {}", opCode);
            switch (opCode) {
                // Let's break this up into a map->router
                case GET_THING_SHADOW:
                    GetThingShadowRequest getThingShadowRequest = CBOR_MAPPER.readValue(
                            applicationMessage.getPayload(),
                            GetThingShadowRequest.class);
                    thingName = getThingShadowRequest.getThingName();
                    logger.atInfo().log("Getting Thing Shadow for Thing Name: {}", thingName);
                    result = dao.getShadowThing(thingName);
                    response = new GetThingShadowResult();
                    if (result.isPresent()) {
                        ((GetThingShadowResult) response).setPayload(result.get());
                        response.setStatus(ShadowResponseStatus.Success);
                    } else {
                        response.setStatus(ShadowResponseStatus.ResourceNotFoundError);
                        response.setErrorMessage("Shadow for " + thingName + " could not be found.");
                    }
                    break;
                case DELETE_THING_SHADOW:
                    DeleteThingShadowRequest deleteThingShadowRequest = CBOR_MAPPER.readValue(
                            applicationMessage.getPayload(),
                            DeleteThingShadowRequest.class);
                    thingName = deleteThingShadowRequest.getThingName();
                    logger.atInfo().log("Deleting Thing Shadow for Thing Name: {}", thingName);
                    result = dao.deleteShadowThing(thingName);
                    response = new DeleteThingShadowResult();
                    if (result.isPresent()) {
                        ((DeleteThingShadowResult) response).setPayload(result.get());
                        response.setStatus(ShadowResponseStatus.Success);
                    } else {
                        response.setStatus(ShadowResponseStatus.ResourceNotFoundError);
                        response.setErrorMessage("Shadow for " + thingName + " could not be found.");
                    }
                    break;
                case UPDATE_THING_SHADOW:
                    UpdateThingShadowRequest updateThingShadowRequest = CBOR_MAPPER.readValue(
                            applicationMessage.getPayload(),
                            UpdateThingShadowRequest.class);
                    thingName = updateThingShadowRequest.getThingName();
                    logger.atInfo().log("Updating Thing Shadow for Thing Name: {}", thingName);
                    result = dao.updateShadowThing(thingName, updateThingShadowRequest.getPayload());
                    response = new UpdateThingShadowResult();
                    if (result.isPresent()) {
                        ((UpdateThingShadowResult) response).setPayload(result.get());
                        response.setStatus(ShadowResponseStatus.Success);
                    } else {
                        response.setStatus(ShadowResponseStatus.ResourceNotFoundError);
                        response.setErrorMessage("Shadow for " + thingName + " could not be found.");
                    }
                    break;
                default:
                    response.setStatus(ShadowResponseStatus.InvalidRequest);
                    response.setErrorMessage("Unknown request type " + opCode);
                    break;
            }
        } catch (IOException e) {
            response.setStatus(ShadowResponseStatus.InternalError);
            response.setErrorMessage(e.getMessage());
            logger.atError()
                    .setEventType(LogEvents.IPC_ERROR.code()).setCause(e)
                    .log("Failed to parse IPC message from {}", context.getClientId());
        }  catch (ShadowManagerDataException e) {
            response.setStatus(ShadowResponseStatus.InternalError);
            response.setErrorMessage(e.getMessage());
            logger.atError()
                    .setEventType(LogEvents.DATABASE_OPERATION_ERROR.code()).setCause(e)
                    .log("A database error occurred");
        } finally {
            try {
                ApplicationMessage responseMessage = ApplicationMessage.builder()
                        .version(applicationMessage.getVersion())
                        .payload(CBOR_MAPPER.writeValueAsBytes(response))
                        .build();
                future.complete(new FrameReader.Message(responseMessage.toByteArray()));
            } catch (IOException e) {
                logger.atError()
                        .setEventType(LogEvents.IPC_ERROR.code()).setCause(e)
                        .log("Failed to send application message response");
            }
        }
        if (!future.isDone()) {
            future.completeExceptionally(new IPCException("Unable to serialize any response"));
        }
        return future;
    }

}
