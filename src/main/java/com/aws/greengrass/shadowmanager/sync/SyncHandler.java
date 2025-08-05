/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.model.configuration.ThingShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.sync.model.BaseSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.Direction;
import com.aws.greengrass.shadowmanager.sync.model.DirectionWrapper;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.OverwriteCloudShadowRequest;
import com.aws.greengrass.shadowmanager.sync.model.OverwriteLocalShadowRequest;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.strategy.SyncStrategy;
import com.aws.greengrass.shadowmanager.sync.strategy.SyncStrategyFactory;
import com.aws.greengrass.shadowmanager.sync.strategy.model.Strategy;
import com.aws.greengrass.shadowmanager.sync.strategy.model.StrategyType;
import com.aws.greengrass.util.Pair;
import com.aws.greengrass.util.RetryUtils;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import lombok.Setter;
import lombok.Synchronized;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;
import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.model.LogEvents.SYNC;

/**
 * Handle syncing shadows between AWS IoT Device Shadow Service and the local shadow service.
 */
public class SyncHandler {
    private static final Logger logger = LogManager.getLogger(SyncHandler.class);

    private static final String SYNC_EVENT_TYPE = SYNC.code();

    /**
     * Default number of threads to use for executing sync requests.
     */
    public static final int DEFAULT_PARALLELISM = 1;

    /**
     * Context object containing handlers useful for sync requests.
     */
    private SyncContext context;

    /**
     * Context object containing sync configurations.
     */
    // TODO: [GG-36231]: Figure out a better way to set this configuration in only one place.
    @Setter
    private Set<ThingShadowSyncConfiguration> syncConfigurations;

    /**
     * The sync strategy for all shadows.
     *
     * @implNote The Getter and Setter are only used in integration tests.
     */
    @Getter
    @Setter
    private SyncStrategy overallSyncStrategy;

    /**
     * The direction of syncing shadows to/from the cloud.
     */
    @Getter
    private final DirectionWrapper direction;

    /**
     * The sync strategy factory object to generate.
     */
    private final SyncStrategyFactory syncStrategyFactory;

    /**
     * Request queue.
     */
    private final RequestQueue syncQueue;
    // retry wrapper so that requests can be mocked
    // setter is used in integ tests only
    @Setter
    private static Retryer retryer = (config, request, context) ->
            RetryUtils.runWithRetry(config,
                    () -> {
                        request.execute(context);
                        return null;
                    },
                    SYNC_EVENT_TYPE, logger);


    /**
     * Construct a new instance.
     *
     * @param executorService              provider of threads for real time syncing
     * @param syncScheduledExecutorService provider of thread for periodic syncing
     * @param syncQueue                    a request queue
     * @param direction                    The sync direction
     */
    @Inject
    public SyncHandler(ExecutorService executorService, ScheduledExecutorService syncScheduledExecutorService,
                       RequestQueue syncQueue, DirectionWrapper direction) {
        this(executorService, syncScheduledExecutorService, retryer, syncQueue, direction);
    }

    /**
     * Constructor.
     *
     * @param executorService              provider of threads for syncing
     * @param syncScheduledExecutorService provider of thread for periodic syncing
     * @param retryer                      The retryer object.
     * @param syncQueue                    a request queue.
     * @param direction                    The sync direction
     */
    private SyncHandler(ExecutorService executorService, ScheduledExecutorService syncScheduledExecutorService,
                        Retryer retryer, RequestQueue syncQueue, DirectionWrapper direction) {
        this(new SyncStrategyFactory(retryer, executorService, syncScheduledExecutorService, direction),
                syncQueue, direction);
    }

    /**
     * Constructor for testing.
     *
     * @param syncStrategyFactory The sync strategy factory object to generate.
     * @param syncQueue           a request queue.
     * @param direction           The sync direction
     */
    SyncHandler(SyncStrategyFactory syncStrategyFactory, RequestQueue syncQueue, DirectionWrapper direction) {
        this.syncStrategyFactory = syncStrategyFactory;
        this.syncQueue = syncQueue;
        this.direction = direction;
        setSyncStrategy(Strategy.builder().type(StrategyType.REALTIME).build());
    }

    /**
     * Sets the sync strategy based on the Strategy object provided.
     *
     * @param syncStrategy The sync strategy.
     */
    public void setSyncStrategy(Strategy syncStrategy) {
        this.overallSyncStrategy = this.syncStrategyFactory.createSyncStrategy(syncStrategy, syncQueue);
    }

    /**
     * Performs a full sync on all shadows. Clears any existing sync requests and will create full shadow sync requests
     * for all shadows.
     */
    private void fullSyncOnAllShadows() {
        overallSyncStrategy.clearSyncQueue();

        List<Pair<String, String>> shadows = context.getDao().listSyncedShadows();

        Stream<BaseSyncRequest> requestStream = null;
        switch (direction.get()) {
            case BETWEEN_DEVICE_AND_CLOUD:
                requestStream = shadows.stream().map(p -> new FullShadowSyncRequest(p.getLeft(), p.getRight()));
                break;
            case DEVICE_TO_CLOUD:
                requestStream = shadows.stream().map(p -> new OverwriteCloudShadowRequest(p.getLeft(), p.getRight()));
                break;
            case CLOUD_TO_DEVICE:
                requestStream = shadows.stream().map(p -> new OverwriteLocalShadowRequest(p.getLeft(), p.getRight()));
                break;
            default:
                break;
        }
        if (requestStream != null && !requestStream.equals(Stream.empty())) {
            Iterator<BaseSyncRequest> it = requestStream.iterator();
            while (it.hasNext() && !Thread.currentThread().isInterrupted()) {
                overallSyncStrategy.putSyncRequest(it.next());
            }
        }
    }

    /**
     * Start sync threads to process sync requests. This automatically starts a full sync for all shadows.
     *
     * @param context         an context object for syncing
     * @param syncParallelism number of threads to use for syncing
     */
    @Synchronized
    public void start(SyncContext context, int syncParallelism) {
        overallSyncStrategy.start(context, syncParallelism);
        this.context = context;
        fullSyncOnAllShadows();
    }

    /**
     * Stops sync threads and clear syncing queue.
     */
    @Synchronized
    public void stop() {
        overallSyncStrategy.stop();
    }

    /**
     * Checks if the shadow is supposed to be synced or not.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     * @return true if the shadow is supposed to be synced; Else false.
     */
    private boolean isShadowSynced(String thingName, String shadowName) {
        return this.syncConfigurations != null && this.syncConfigurations
                .contains(ThingShadowSyncConfiguration.builder().shadowName(shadowName).thingName(thingName).build());
    }

    /**
     * Pushes an update sync request to the request queue to update shadow in the cloud after a local shadow has
     * been successfully updated.
     *
     * @param thingName      The thing name associated with the sync shadow update
     * @param shadowName     The shadow name associated with the sync shadow update
     * @param updateDocument The update shadow request
     */
    public void pushCloudUpdateSyncRequest(String thingName, String shadowName, JsonNode updateDocument) {
        if (isShadowSynced(thingName, shadowName) && !Direction.CLOUD_TO_DEVICE.equals(direction.get())) {
            overallSyncStrategy.putSyncRequest(new CloudUpdateSyncRequest(thingName, shadowName, updateDocument));
        }
    }

    /**
     * Pushes an update sync request to the request queue to update local shadow after a cloud shadow has
     * been successfully updated.
     *
     * @param thingName      The thing name associated with the sync shadow update
     * @param shadowName     The shadow name associated with the sync shadow update
     * @param updateDocument Update document to be applied to local shadow
     */
    public void pushLocalUpdateSyncRequest(String thingName, String shadowName, byte[] updateDocument) {
        if (isShadowSynced(thingName, shadowName) && !Direction.DEVICE_TO_CLOUD.equals(direction.get())) {
            overallSyncStrategy.putSyncRequest(new LocalUpdateSyncRequest(thingName, shadowName, updateDocument));
        }
    }

    /**
     * Pushes a delete sync request in the request queue to delete a shadow in the cloud after a local shadow has
     * been successfully deleted.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public void pushCloudDeleteSyncRequest(String thingName, String shadowName) {
        if (isShadowSynced(thingName, shadowName) && !Direction.CLOUD_TO_DEVICE.equals(direction.get())) {
            overallSyncStrategy.putSyncRequest(new CloudDeleteSyncRequest(thingName, shadowName));
        }
    }

    /**
     * Pushes a delete sync request in the request queue to delete a local shadow after a cloud shadow has
     * been successfully deleted.
     *
     * @param thingName     The thing name associated with the sync shadow update
     * @param shadowName    The shadow name associated with the sync shadow update
     * @param deletePayload Delete response payload containing the deleted shadow version
     */
    public void pushLocalDeleteSyncRequest(String thingName, String shadowName, byte[] deletePayload) {
        if (isShadowSynced(thingName, shadowName) && !Direction.DEVICE_TO_CLOUD.equals(direction.get())) {
            overallSyncStrategy.putSyncRequest(new LocalDeleteSyncRequest(thingName, shadowName, deletePayload));
        }
    }
}
