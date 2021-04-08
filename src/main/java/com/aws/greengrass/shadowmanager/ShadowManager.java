/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.config.ChildChanged;
import com.aws.greengrass.config.Node;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.GetThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.ListNamedShadowsForThingIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.PubSubClientWrapper;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.configuration.ShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.model.configuration.ThingShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.shadowmanager.util.Validator;
import com.aws.greengrass.util.Coerce;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import lombok.AccessLevel;
import lombok.Getter;
import org.flywaydb.core.api.FlywayException;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SYNCHRONIZATION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_DISK_UTILIZATION_SIZE_B;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_DOCUMENT_SIZE;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.DELETE_THING_SHADOW;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.GET_THING_SHADOW;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.LIST_NAMED_SHADOWS_FOR_THING;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.UPDATE_THING_SHADOW;

@ImplementsService(name = ShadowManager.SERVICE_NAME)
public class ShadowManager extends PluginService {
    public static final String SERVICE_NAME = "aws.greengrass.ShadowManager";
    private static final List<String> SHADOW_AUTHORIZATION_OPCODES = Arrays.asList(GET_THING_SHADOW,
            UPDATE_THING_SHADOW, LIST_NAMED_SHADOWS_FOR_THING, DELETE_THING_SHADOW, "*");

    private final ShadowManagerDAO dao;
    private final ShadowManagerDatabase database;
    private final AuthorizationHandlerWrapper authorizationHandlerWrapper;
    private final PubSubClientWrapper pubSubClientWrapper;
    private final DeviceConfiguration deviceConfiguration;
    //TODO: Move this to sync handler?
    @Getter(AccessLevel.PACKAGE)
    private ShadowSyncConfiguration syncConfiguration;

    @Inject
    private GreengrassCoreIPCService greengrassCoreIPCService;
    private final ChildChanged deviceThingNameWatcher;

    /**
     * Ctr for ShadowManager.
     *
     * @param topics                      topics passed by the Nucleus
     * @param database                    Local shadow database management
     * @param dao                         Local shadow database management
     * @param authorizationHandlerWrapper The authorization handler wrapper
     * @param pubSubClientWrapper         The PubSub client wrapper
     * @param deviceConfiguration         {@link DeviceConfiguration}
     */
    @Inject
    public ShadowManager(
            Topics topics,
            ShadowManagerDatabase database,
            ShadowManagerDAOImpl dao,
            AuthorizationHandlerWrapper authorizationHandlerWrapper,
            PubSubClientWrapper pubSubClientWrapper,
            DeviceConfiguration deviceConfiguration) {
        super(topics);
        this.database = database;
        this.authorizationHandlerWrapper = authorizationHandlerWrapper;
        this.dao = dao;
        this.pubSubClientWrapper = pubSubClientWrapper;
        this.deviceConfiguration = deviceConfiguration;
        this.deviceThingNameWatcher = this::handleDeviceThingNameChange;
    }

    private void registerHandlers() {
        try {
            authorizationHandlerWrapper.registerComponent(this.getName(), new HashSet<>(SHADOW_AUTHORIZATION_OPCODES));
        } catch (AuthorizationException e) {
            logger.atError()
                    .setEventType(LogEvents.AUTHORIZATION_ERROR.code())
                    .setCause(e)
                    .log("Failed to initialize the ShadowManager service with the Authorization module.");
        }

        greengrassCoreIPCService.setGetThingShadowHandler(context -> new GetThingShadowIPCHandler(context,
                dao, authorizationHandlerWrapper, pubSubClientWrapper));
        greengrassCoreIPCService.setDeleteThingShadowHandler(context -> new DeleteThingShadowIPCHandler(context,
                dao, authorizationHandlerWrapper, pubSubClientWrapper));
        greengrassCoreIPCService.setUpdateThingShadowHandler(context -> new UpdateThingShadowIPCHandler(context,
                dao, authorizationHandlerWrapper, pubSubClientWrapper));
        greengrassCoreIPCService.setListNamedShadowsForThingHandler(context -> new ListNamedShadowsForThingIPCHandler(
                context, dao, authorizationHandlerWrapper));
    }

    void handleDeviceThingNameChange(Object whatHappened, Node changedNode) {
        if (!(changedNode instanceof Topic)) {
            return;
        }
        String newThingName = Coerce.toString(this.deviceConfiguration.getThingName());
        getNucleusThingShadowSyncConfiguration().ifPresent(thingShadowSyncConfiguration ->
                thingShadowSyncConfiguration.setThingName(newThingName));
    }

    @Override
    protected void install() {
        Topics configTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC);
        configTopics.subscribe((why, newv) -> {
            Map<String, Object> configTopicsPojo = configTopics.toPOJO();
            try {
                Topic thingNameTopic = this.deviceConfiguration.getThingName();
                String thingName = Coerce.toString(thingNameTopic);
                syncConfiguration = ShadowSyncConfiguration.processConfiguration(configTopicsPojo, thingName);
                Optional<ThingShadowSyncConfiguration> nucleusThingConfig = getNucleusThingShadowSyncConfiguration();
                if (nucleusThingConfig.isPresent()) {
                    thingNameTopic.subscribeGeneric(this.deviceThingNameWatcher);
                } else {
                    thingNameTopic.remove(this.deviceThingNameWatcher);
                }
            } catch (InvalidConfigurationException e) {
                serviceErrored(e);
            }
        });

        config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC)
                .dflt(DEFAULT_DOCUMENT_SIZE)
                .subscribe((why, newv) -> {
                    int newMaxShadowSize = Coerce.toInt(newv);
                    try {
                        Validator.validateMaxShadowSize(newMaxShadowSize);
                        Validator.setMaxShadowDocumentSize(newMaxShadowSize);
                    } catch (InvalidConfigurationException e) {
                        serviceErrored(e);
                    }
                });

        config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC)
                .dflt(DEFAULT_DISK_UTILIZATION_SIZE_B)
                .subscribe((why, newv) -> {
                    try {
                        int newMaxDiskUtilization = Coerce.toInt(newv);
                        Validator.validateMaxDiskUtilization(newMaxDiskUtilization);
                        this.database.setMaxDiskUtilization(Coerce.toInt(newv));
                    } catch (InvalidConfigurationException e) {
                        serviceErrored(e);
                    }
                });
        try {
            database.install();
            JsonUtil.setUpdateShadowJsonSchema();
        } catch (SQLException | FlywayException | IOException | ProcessingException e) {
            serviceErrored(e);
        }
    }

    private Optional<ThingShadowSyncConfiguration> getNucleusThingShadowSyncConfiguration() {
        return syncConfiguration.getSyncConfigurationList()
                .stream()
                .filter(ThingShadowSyncConfiguration::isNucleusThing)
                .findAny();
    }

    @Override
    @SuppressWarnings({"PMD.AvoidCatchingGenericException"})
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
