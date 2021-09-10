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
import com.aws.greengrass.config.WhatHappened;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.mqttclient.CallbackEventManager;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.GetThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.GetThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.InboundRateLimiter;
import com.aws.greengrass.shadowmanager.ipc.ListNamedShadowsForThingIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.PubSubClientWrapper;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.configuration.ShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.model.configuration.ThingShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.CloudDataClient;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientWrapper;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.strategy.model.Strategy;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.shadowmanager.util.ShadowWriteSynchronizeHelper;
import com.aws.greengrass.shadowmanager.util.Validator;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Pair;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.Synchronized;
import org.flywaydb.core.api.FlywayException;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_LOCAL_REQUESTS_RATE_PER_THING_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_TOTAL_LOCAL_REQUESTS_RATE;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_RATE_LIMITS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_STRATEGY_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SYNCHRONIZATION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_DOCUMENT_SIZE;
import static com.aws.greengrass.shadowmanager.sync.strategy.model.Strategy.DEFAULT_STRATEGY;
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
    private final InboundRateLimiter inboundRateLimiter;
    private final DeviceConfiguration deviceConfiguration;
    @Getter
    private final DeleteThingShadowRequestHandler deleteThingShadowRequestHandler;
    @Getter
    private final UpdateThingShadowRequestHandler updateThingShadowRequestHandler;
    private final GetThingShadowRequestHandler getThingShadowRequestHandler;
    private final IotDataPlaneClientWrapper iotDataPlaneClientWrapper;
    @Getter(AccessLevel.PACKAGE)
    private final SyncHandler syncHandler;
    private final CloudDataClient cloudDataClient;
    private final MqttClient mqttClient;
    public final MqttClientConnectionEvents callbacks = new MqttClientConnectionEvents() {
        @Override
        public void onConnectionInterrupted(int errorCode) {
            stopSyncingShadows(true);
        }

        @Override
        public void onConnectionResumed(boolean sessionPresent) {
            if (inState(State.RUNNING)) {
                startSyncingShadows(StartSyncInfo.builder().updateCloudSubscriptions(true).build());

            }
        }
    };
    private final CallbackEventManager.OnConnectCallback onConnect = callbacks::onConnectionResumed;

    @Getter(AccessLevel.PUBLIC)
    @Setter(AccessLevel.PACKAGE)
    private ShadowSyncConfiguration syncConfiguration;

    @Inject
    @Setter
    private GreengrassCoreIPCService greengrassCoreIPCService;
    private final ChildChanged deviceThingNameWatcher;
    private String thingName;

    /**
     * Ctr for ShadowManager.
     *
     * @param topics                      topics passed by the Nucleus
     * @param database                    local shadow database management
     * @param dao                         local shadow database management
     * @param authorizationHandlerWrapper the authorization handler wrapper
     * @param pubSubClientWrapper         The PubSub client wrapper
     * @param inboundRateLimiter          The inbound rate limiter class for throttling local requests
     * @param deviceConfiguration         the device configuration
     * @param synchronizeHelper           the shadow write operation synchronizer helper
     * @param iotDataPlaneClientWrapper   the iot data plane client
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
            InboundRateLimiter inboundRateLimiter,
            DeviceConfiguration deviceConfiguration,
            ShadowWriteSynchronizeHelper synchronizeHelper,
            IotDataPlaneClientWrapper iotDataPlaneClientWrapper,
            SyncHandler syncHandler,
            CloudDataClient cloudDataClient,
            MqttClient mqttClient) {
        super(topics);
        this.database = database;
        this.authorizationHandlerWrapper = authorizationHandlerWrapper;
        this.inboundRateLimiter = inboundRateLimiter;
        this.dao = dao;
        this.deviceConfiguration = deviceConfiguration;
        this.iotDataPlaneClientWrapper = iotDataPlaneClientWrapper;
        this.syncHandler = syncHandler;
        this.cloudDataClient = cloudDataClient;
        this.mqttClient = mqttClient;
        this.deleteThingShadowRequestHandler = new DeleteThingShadowRequestHandler(dao, authorizationHandlerWrapper,
                pubSubClientWrapper, synchronizeHelper, this.syncHandler);
        this.updateThingShadowRequestHandler = new UpdateThingShadowRequestHandler(dao, authorizationHandlerWrapper,
                pubSubClientWrapper, synchronizeHelper, this.syncHandler);
        this.getThingShadowRequestHandler = new GetThingShadowRequestHandler(dao, authorizationHandlerWrapper,
                pubSubClientWrapper);
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
                inboundRateLimiter, getThingShadowRequestHandler));
        greengrassCoreIPCService.setOperationHandler(DELETE_THING_SHADOW, context ->
                new DeleteThingShadowIPCHandler(context, inboundRateLimiter, deleteThingShadowRequestHandler));
        greengrassCoreIPCService.setOperationHandler(UPDATE_THING_SHADOW, context ->
                new UpdateThingShadowIPCHandler(context, inboundRateLimiter, updateThingShadowRequestHandler));
        greengrassCoreIPCService.setOperationHandler(LIST_NAMED_SHADOWS_FOR_THING, context ->
                new ListNamedShadowsForThingIPCHandler(context, dao, authorizationHandlerWrapper, inboundRateLimiter));
    }

    void handleDeviceThingNameChange(WhatHappened whatHappened, Node changedNode) {
        if (!(changedNode instanceof Topic)) {
            return;
        }
        String oldThingName = thingName;
        thingName = Coerce.toString(this.deviceConfiguration.getThingName());
        if (oldThingName == null) {
            // First time.
            return;
        }
        getCoreThingShadowSyncConfiguration(oldThingName).ifPresent(thingShadowSyncConfiguration ->
                thingShadowSyncConfiguration.setThingName(thingName));
    }

    @Override
    protected void install() {
        try {
            database.install();
            JsonUtil.loadSchema();
        } catch (FlywayException | IOException e) {
            serviceErrored(e);
            return;
        }

        inboundRateLimiter.clear();

        Topics configTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC);
        configTopics.subscribe((what, newv) -> {
            Map<String, Object> configTopicsPojo = configTopics.toPOJO();
            try {
                Topic thingNameTopic = this.deviceConfiguration.getThingName();
                String thingName = Coerce.toString(thingNameTopic);
                ShadowSyncConfiguration newSyncConfiguration =
                        ShadowSyncConfiguration.processConfiguration(configTopicsPojo, thingName);
                if (this.syncConfiguration != null && this.syncConfiguration.equals(newSyncConfiguration)) {
                    return;
                }
                this.syncConfiguration = newSyncConfiguration;
                this.syncHandler.setSyncConfigurations(this.syncConfiguration.getSyncConfigurations());

                // Subscribe to the thing name topic if the Nucleus thing shadows have been synced.
                Optional<ThingShadowSyncConfiguration> coreThingConfig =
                        getCoreThingShadowSyncConfiguration(thingName);
                if (coreThingConfig.isPresent()) {
                    thingNameTopic.subscribeGeneric(this.deviceThingNameWatcher);
                } else {
                    thingNameTopic.remove(this.deviceThingNameWatcher);
                }

                // only stop / start syncing if we are not in install - it will otherwise be started by lifecycle
                if (inState(State.RUNNING)) {
                    stopSyncingShadows(true);
                    startSyncingShadows(StartSyncInfo.builder().reInitializeSyncInfo(true)
                            .updateCloudSubscriptions(true).build());
                }
            } catch (InvalidConfigurationException e) {
                serviceErrored(e);
            }
        });

        Topics rateLimitTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_RATE_LIMITS_TOPIC);
        rateLimitTopics.subscribe((what, newv) -> {
            Map<String, Object> rateLimitsPojo = rateLimitTopics.toPOJO();
            try {
                if (rateLimitsPojo.containsKey(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC)) {
                    int maxOutboundSyncUpdatesPerSecond = Coerce.toInt(rateLimitsPojo
                            .get(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC));
                    Validator.validateOutboundSyncUpdatesPerSecond(maxOutboundSyncUpdatesPerSecond);
                    iotDataPlaneClientWrapper.setRate(maxOutboundSyncUpdatesPerSecond);
                }

                if (rateLimitsPojo.containsKey(CONFIGURATION_MAX_TOTAL_LOCAL_REQUESTS_RATE)) {
                    int maxTotalLocalRequestRate = Coerce.toInt(rateLimitsPojo
                            .get(CONFIGURATION_MAX_TOTAL_LOCAL_REQUESTS_RATE));
                    Validator.validateTotalLocalRequestRate(maxTotalLocalRequestRate);
                    inboundRateLimiter.setTotalRate(maxTotalLocalRequestRate);
                }

                if (rateLimitsPojo.containsKey(CONFIGURATION_MAX_LOCAL_REQUESTS_RATE_PER_THING_TOPIC)) {
                    int maxLocalShadowUpdatesPerThingPerSecond = Coerce.toInt(rateLimitsPojo
                            .get(CONFIGURATION_MAX_LOCAL_REQUESTS_RATE_PER_THING_TOPIC));
                    Validator.validateLocalShadowRequestsPerThingPerSecond(maxLocalShadowUpdatesPerThingPerSecond);
                    inboundRateLimiter.setRate(maxLocalShadowUpdatesPerThingPerSecond);
                }
            } catch (InvalidConfigurationException e) {
                serviceErrored(e);
            }
        });

        config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC)
                .dflt(DEFAULT_DOCUMENT_SIZE)
                .subscribe((why, newv) -> {
                    int newMaxShadowSize;
                    if (WhatHappened.removed.equals(why)) {
                        newMaxShadowSize = DEFAULT_DOCUMENT_SIZE;
                    } else {
                        newMaxShadowSize = Coerce.toInt(newv);
                    }
                    try {
                        Validator.validateMaxShadowSize(newMaxShadowSize);
                        Validator.setMaxShadowDocumentSize(newMaxShadowSize);
                        updateThingShadowRequestHandler.setMaxShadowSize(newMaxShadowSize);
                    } catch (InvalidConfigurationException e) {
                        serviceErrored(e);
                    }
                });

        Topics strategyTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_STRATEGY_TOPIC);
        strategyTopics.subscribe((why, newv) -> {
                    Strategy strategy;
                    Map<String, Object> strategyPojo = strategyTopics.toPOJO();
                    if (WhatHappened.removed.equals(why) || strategyPojo == null || strategyPojo.isEmpty()) {
                        strategy = DEFAULT_STRATEGY;
                    } else {
                        // Get the correct sync strategy configuration from the POJO.
                        try {
                            strategy = Strategy.fromPojo(strategyPojo);
                        } catch (InvalidConfigurationException e) {
                            serviceErrored(e);
                            return;
                        }
                    }
                    stopSyncingShadows(false);
                    syncHandler.setSyncStrategy(strategy);
                    startSyncingShadows(StartSyncInfo.builder().build());
                });
    }



    private void deleteRemovedSyncInformation() {
        Set<Pair<String, String>> removedShadows = new HashSet<>(this.dao.listSyncedShadows());
        removedShadows.removeAll(syncConfiguration.getSyncShadows());
        removedShadows.forEach(shadowThingPair ->
                dao.deleteSyncInformation(shadowThingPair.getLeft(), shadowThingPair.getRight()));
    }

    private void initializeSyncInfo() {
        long epochSeconds = Instant.EPOCH.getEpochSecond();
        for (ThingShadowSyncConfiguration configuration : syncConfiguration.getSyncConfigurations()) {
            insertSyncInfoIfNotPresent(epochSeconds, configuration);
        }
    }

    private void insertSyncInfoIfNotPresent(long epochSeconds, ThingShadowSyncConfiguration configuration) {
        this.dao.insertSyncInfoIfNotExists(SyncInformation.builder()
                .thingName(configuration.getThingName())
                .shadowName(configuration.getShadowName())
                .cloudDeleted(false)
                .cloudVersion(0)
                .cloudUpdateTime(epochSeconds)
                .lastSyncedDocument(null)
                .localVersion(0)
                .lastSyncTime(epochSeconds)
                .build());
    }

    private Optional<ThingShadowSyncConfiguration> getCoreThingShadowSyncConfiguration(String thingName) {
        return syncConfiguration.getSyncConfigurations()
                .stream()
                .filter(thingShadowSyncConfiguration -> thingName.equals(thingShadowSyncConfiguration.getThingName()))
                .findAny();
    }

    @Override
    public void postInject() {
        super.postInject();

        // Register IPC and Authorization
        registerHandlers();

        if (!deviceConfiguration.isDeviceConfiguredToTalkToCloud()) {
            logger.atWarn().log("Device not configured to talk to AWS Iot cloud. Not syncing shadows to the cloud");
            // Right now the connection cannot be brought online without a restart.
            // Skip setting up scheduled upload and event triggered upload because they won't work
            return;
        }
        mqttClient.addToCallbackEvents(onConnect, callbacks);
    }

    @Override
    @SuppressWarnings({"PMD.AvoidCatchingGenericException"})
    public void startup() {
        try {
            database.open();

            startSyncingShadows(StartSyncInfo.builder().reInitializeSyncInfo(true)
                    .updateCloudSubscriptions(true).build());

            reportState(State.RUNNING);
        } catch (Exception e) {
            serviceErrored(e);
        }
    }

    @Override
    protected void shutdown() throws InterruptedException {
        try {
            stopSyncingShadows(true);
            database.close();
            inboundRateLimiter.clear();
        } catch (IOException e) {
            logger.atError()
                    .setEventType(LogEvents.DATABASE_CLOSE_ERROR.code())
                    .setCause(e)
                    .log("Failed to close out shadow database");
        }
        super.shutdown();
    }

    /**
     * Stop the sync handler and stop subscription thread on cloud client.
     *
     * @param stopCloudSubscriptions whether or not to stop MQTT subscriptions.
     */
    @Synchronized
    void stopSyncingShadows(boolean stopCloudSubscriptions) {
        // Only stop subscribing to cloud shadows on reconnection or startup.
        if (stopCloudSubscriptions) {
            cloudDataClient.stopSubscribing();
        }
        syncHandler.stop();
    }

    /**
     * Starts the Sync handler and update the subscriptions for Cloud Data Client.
     *
     * @param startSyncInfo information on what to do to start syncing shadows.
     * @implNote Making this package-private for unit tests.
     */
    @Synchronized
    public void startSyncingShadows(StartSyncInfo startSyncInfo) {
        // Only reinitialize the sync info if the synchronize configuration has been updated.
        if (startSyncInfo.reInitializeSyncInfo) {
            // Remove sync information of shadows that are no longer being synced.
            deleteRemovedSyncInformation();

            // Initialize the sync information if the sync information does not exist.
            initializeSyncInfo();
        }

        if (mqttClient.connected() && !syncConfiguration.getSyncConfigurations().isEmpty()) {
            final SyncContext syncContext = new SyncContext(dao, getUpdateThingShadowRequestHandler(),
                    getDeleteThingShadowRequestHandler(),
                    iotDataPlaneClientWrapper);
            syncHandler.start(syncContext, SyncHandler.DEFAULT_PARALLELISM);

            // Only update the MQTT subscriptions to cloud shadows at startup or reconnection.
            if (startSyncInfo.updateCloudSubscriptions) {
                cloudDataClient.updateSubscriptions(syncConfiguration.getSyncShadows());
            }
        } else {
            logger.atTrace()
                    .kv("connected", mqttClient.connected())
                    .kv("sync configuration list count", syncConfiguration.getSyncConfigurations().size())
                    .log("Not starting sync loop");
        }
    }

    /**
     * Class to handle different configurations to start syncing shadows.
     */
    @Builder
    @Data
    public static class StartSyncInfo {
        /**
         * Whether or not to reinitialize the sync info. We should only remove the deleted or add the new sync info if
         * the synchronize configuration has changed or at startup.
         */
        boolean reInitializeSyncInfo;

        /**
         * Whether or not to update the MQTT subscriptions for cloud shadows.
         */
        boolean updateCloudSubscriptions;
    }
}
