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
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.GetThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.ListNamedShadowsForThingIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.PubSubClientWrapper;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.configuration.ShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.model.configuration.ThingShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.CloudDataClient;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.shadowmanager.util.ShadowWriteSynchronizeHelper;
import com.aws.greengrass.shadowmanager.util.Validator;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Pair;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import lombok.AccessLevel;
import lombok.Getter;
import org.flywaydb.core.api.FlywayException;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
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
    @Getter
    private final DeleteThingShadowRequestHandler deleteThingShadowRequestHandler;
    @Getter
    private final UpdateThingShadowRequestHandler updateThingShadowRequestHandler;
    private final IotDataPlaneClientFactory iotDataPlaneClientFactory;
    private final SyncHandler syncHandler;
    private final CloudDataClient cloudDataClient;
    private final MqttClient mqttClient;

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
     * @param database                    local shadow database management
     * @param dao                         local shadow database management
     * @param authorizationHandlerWrapper the authorization handler wrapper
     * @param pubSubClientWrapper         The PubSub client wrapper
     * @param deviceConfiguration         the device configuration
     * @param synchronizeHelper           the shadow write operation synchronizer helper
     * @param iotDataPlaneClientFactory   factory for the IoT data plane client
     * @param syncHandler                 a synchronization handler
     * @param cloudDataClient             the data client subscribing to cloud shadow topics
     * @param mqttClient                  the mqtt client connected to IoT Core
     */
    @SuppressWarnings("PMD.ExcessiveParameterList")
    @Inject
    public ShadowManager(
            Topics topics,
            ShadowManagerDatabase database,
            ShadowManagerDAOImpl dao,
            AuthorizationHandlerWrapper authorizationHandlerWrapper,
            PubSubClientWrapper pubSubClientWrapper,
            DeviceConfiguration deviceConfiguration,
            ShadowWriteSynchronizeHelper synchronizeHelper,
            IotDataPlaneClientFactory iotDataPlaneClientFactory,
            SyncHandler syncHandler,
            CloudDataClient cloudDataClient,
            MqttClient mqttClient) {
        super(topics);
        this.database = database;
        this.authorizationHandlerWrapper = authorizationHandlerWrapper;
        this.dao = dao;
        this.pubSubClientWrapper = pubSubClientWrapper;
        this.deviceConfiguration = deviceConfiguration;
        this.iotDataPlaneClientFactory = iotDataPlaneClientFactory;
        this.syncHandler = syncHandler;
        this.cloudDataClient = cloudDataClient;
        this.mqttClient = mqttClient;
        this.deleteThingShadowRequestHandler = new DeleteThingShadowRequestHandler(dao, authorizationHandlerWrapper,
                pubSubClientWrapper, synchronizeHelper, this.syncHandler);
        this.updateThingShadowRequestHandler = new UpdateThingShadowRequestHandler(dao, authorizationHandlerWrapper,
                pubSubClientWrapper, synchronizeHelper, this.syncHandler);
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

        greengrassCoreIPCService.setOperationHandler(GET_THING_SHADOW, context -> new GetThingShadowIPCHandler(context,
                dao, authorizationHandlerWrapper, pubSubClientWrapper));
        greengrassCoreIPCService.setOperationHandler(DELETE_THING_SHADOW, context ->
                new DeleteThingShadowIPCHandler(context, deleteThingShadowRequestHandler));
        greengrassCoreIPCService.setOperationHandler(UPDATE_THING_SHADOW, context ->
                new UpdateThingShadowIPCHandler(context, updateThingShadowRequestHandler));
        greengrassCoreIPCService.setOperationHandler(LIST_NAMED_SHADOWS_FOR_THING, context ->
                new ListNamedShadowsForThingIPCHandler(context, dao, authorizationHandlerWrapper));
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
        try {
            database.install();
            JsonUtil.setUpdateShadowJsonSchema();
        } catch (SQLException | FlywayException | IOException | ProcessingException e) {
            serviceErrored(e);
        }

        Topics configTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC);
        configTopics.subscribe((what, newv) -> {
            Map<String, Object> configTopicsPojo = configTopics.toPOJO();
            try {
                Topic thingNameTopic = this.deviceConfiguration.getThingName();
                String thingName = Coerce.toString(thingNameTopic);
                syncConfiguration = ShadowSyncConfiguration.processConfiguration(configTopicsPojo, thingName);

                // Subscribe to the thing name topic if the Nucleus thing shadows have been synced.
                Optional<ThingShadowSyncConfiguration> nucleusThingConfig = getNucleusThingShadowSyncConfiguration();
                if (nucleusThingConfig.isPresent()) {
                    thingNameTopic.subscribeGeneric(this.deviceThingNameWatcher);
                } else {
                    thingNameTopic.remove(this.deviceThingNameWatcher);
                }

                cloudDataClient.stopSubscribing();
                syncHandler.stop();
                // Remove sync information of shadows that are no longer being synced.
                deleteRemovedSyncInformation();

                // Initialize the sync information if the sync information does not exist.
                initializeSyncInfo();
                startSyncHandler();
                cloudDataClient.updateSubscriptions(syncConfiguration.getSyncShadows());
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
    }

    private void deleteRemovedSyncInformation() {
        List<Pair<String, String>> removedShadowList = new ArrayList<>();
        this.dao.listSyncedShadows().forEach(shadowThingPair -> {
            // Let's assume the shadow was removed from being synced.
            boolean present = false;

            // Iterate over the configuration to check if that particular shadow is still being synced.
            for (ThingShadowSyncConfiguration configuration : syncConfiguration.getSyncConfigurationList()) {
                if (configuration.getThingName().equals(shadowThingPair.getLeft())) {
                    for (String shadowName : configuration.getSyncNamedShadows()) {
                        // If the shadow is present, don't need to iterate over other shadows.
                        if (shadowName.equals(shadowThingPair.getRight())) {
                            present = true;
                            break;
                        }
                    }
                }

                // If the shadow is present, don't need to iterate over other things.
                if (present) {
                    break;
                }
            }
            if (!present) {
                removedShadowList.add(shadowThingPair);
            }
        });

        removedShadowList.forEach(shadowThingPair ->
                dao.deleteSyncInformation(shadowThingPair.getLeft(), shadowThingPair.getRight()));
    }

    private void initializeSyncInfo() {
        long epochSeconds = Instant.EPOCH.getEpochSecond();
        for (ThingShadowSyncConfiguration configuration : syncConfiguration.getSyncConfigurationList()) {
            if (configuration.isSyncClassicShadow()) {
                insertSyncInfoIfNotPresent(epochSeconds, configuration, CLASSIC_SHADOW_IDENTIFIER);
            }
            for (String shadowName : configuration.getSyncNamedShadows()) {
                insertSyncInfoIfNotPresent(epochSeconds, configuration, shadowName);
            }
        }
    }

    private void insertSyncInfoIfNotPresent(long epochSeconds, ThingShadowSyncConfiguration configuration,
                                            String shadowName) {
        this.dao.insertSyncInfoIfNotExists(SyncInformation.builder()
                .thingName(configuration.getThingName())
                .shadowName(shadowName)
                .cloudDeleted(false)
                .cloudVersion(0)
                .cloudUpdateTime(epochSeconds)
                .lastSyncedDocument(null)
                .localVersion(0)
                .lastSyncTime(epochSeconds)
                .build());
    }

    private Optional<ThingShadowSyncConfiguration> getNucleusThingShadowSyncConfiguration() {
        return syncConfiguration.getSyncConfigurationList()
                .stream()
                .filter(ThingShadowSyncConfiguration::isNucleusThing)
                .findAny();
    }

    @Override
    public void postInject() {
        super.postInject();

        // Register IPC and Authorization
        registerHandlers();

        mqttClient.addToCallbackEvents(new MqttClientConnectionEvents() {
            @Override
            public void onConnectionInterrupted(int errorCode) {
                syncHandler.stop();
            }

            @Override
            public void onConnectionResumed(boolean sessionPresent) {
                if (getState() == State.RUNNING) {
                    startSyncHandler();
                }
            }
        });
    }

    @Override
    @SuppressWarnings({"PMD.AvoidCatchingGenericException"})
    public void startup() {
        try {
            reportState(State.RUNNING);

            if (mqttClient.connected()) {
                startSyncHandler();
                cloudDataClient.updateSubscriptions(syncConfiguration.getSyncShadows());
            }
        } catch (Exception e) {
            serviceErrored(e);
        }
    }

    @Override
    protected void shutdown() throws InterruptedException {
        super.shutdown();
        try {
            cloudDataClient.stopSubscribing();
            syncHandler.stop();
            database.close();
        } catch (IOException e) {
            logger.atError()
                    .setEventType(LogEvents.DATABASE_CLOSE_ERROR.code())
                    .setCause(e)
                    .log("Failed to close out shadow database");
        }
    }

    private void startSyncHandler() {
        if (!syncConfiguration.getSyncConfigurationList().isEmpty()) {
            final SyncContext syncContext = new SyncContext(dao, getUpdateThingShadowRequestHandler(),
                    getDeleteThingShadowRequestHandler(),
                    iotDataPlaneClientFactory);
            syncHandler.start(syncContext, SyncHandler.DEFAULT_PARALLELISM);
        }
    }
}
