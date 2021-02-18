/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.GetThingShadowIPCHandler;
import org.flywaydb.core.api.FlywayException;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import javax.inject.Inject;

import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.DELETE_THING_SHADOW;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.GET_THING_SHADOW;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.UPDATE_THING_SHADOW;

@ImplementsService(name = ShadowManager.SERVICE_NAME)
public class ShadowManager extends PluginService {
    enum LogEvents {
        AUTHORIZATION_ERROR("shadow-authorization-error"),
        IPC_REGISTRATION("shadow-ipc-registration"),
        IPC_ERROR("shadow-ipc-error"),
        DCM_ERROR("shadow-dcm-error"),
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
    public static final List<String> SHADOW_AUTHORIZATION_OPCODES = Arrays.asList(GET_THING_SHADOW,
            UPDATE_THING_SHADOW, DELETE_THING_SHADOW, "*");

    private final ShadowManagerDAO dao;
    private final ShadowManagerDatabase database;
    private final AuthorizationHandler authorizationHandler;
    private final Kernel kernel;

    @Inject
    private GreengrassCoreIPCService greengrassCoreIPCService;

    /**
     * Ctr for ShadowManager.
     *
     * @param topics               topics passed by the Nucleus
     * @param database             Local shadow database management
     * @param dao                  Local shadow database management
     * @param authorizationHandler The authorization handler
     * @param kernel               greengrass kernel
     */
    @Inject
    public ShadowManager(
            Topics topics,
            ShadowManagerDatabase database,
            ShadowManagerDAOImpl dao,
            AuthorizationHandler authorizationHandler,
            Kernel kernel) {
        super(topics);
        this.database = database;
        this.authorizationHandler = authorizationHandler;
        this.kernel = kernel;
        this.dao = dao;
    }

    private void registerHandlers() {
        try {
            authorizationHandler.registerComponent(this.getName(), new HashSet<>(SHADOW_AUTHORIZATION_OPCODES));
        } catch (AuthorizationException e) {
            logger.atError()
                    .setEventType(LogEvents.AUTHORIZATION_ERROR.code)
                    .setCause(e)
                    .log("Failed to initialize the ShadowManager service with the Authorization module.");
        }

        greengrassCoreIPCService.setGetThingShadowHandler(context -> new GetThingShadowIPCHandler(context,
                dao, authorizationHandler));
        greengrassCoreIPCService.setDeleteThingShadowHandler(context -> new DeleteThingShadowIPCHandler(context,
                dao, authorizationHandler));
    }

    @Override
    protected void install() {
        try {
            database.install();
        } catch (SQLException | FlywayException e) {
            serviceErrored(e);
        }
    }

    @Override
    public void startup() {
        try {
            // Register IPC and Authorization
            registerHandlers();

            reportState(State.RUNNING);
        } catch (Exception e) {
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
                    .setEventType(LogEvents.DATABASE_CLOSE_ERROR.code())
                    .setCause(e)
                    .log("Failed to close out shadow database");
        }
    }
}
