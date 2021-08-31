/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.model.configuration.ThingShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.sync.model.CloudDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalUpdateSyncRequest;
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

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
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
     * The sync strategy factory object to generate.
     */
    private final SyncStrategyFactory syncStrategyFactory;

    /**
     * Construct a new instance.
     *
     * @param executorService provider of threads for syncing
     */
    @Inject
    public SyncHandler(ExecutorService executorService) {
        this(executorService,
                // retry wrapper so that requests can be mocked
                (config, request, context) ->
                        RetryUtils.runWithRetry(config,
                                () -> {
                                    request.execute(context);
                                    return null;
                                },
                                SYNC_EVENT_TYPE, logger));
    }

    /**
     * Constructor.
     *
     * @param executorService provider of threads for syncing
     * @param retryer         The retryer object.
     */
    private SyncHandler(ExecutorService executorService, Retryer retryer) {
        this(new SyncStrategyFactory(retryer, executorService));
    }

    /**
     * Constructor for testing.
     *
     * @param syncStrategyFactory The sync strategy factory object to generate.
     */
    SyncHandler(SyncStrategyFactory syncStrategyFactory) {
        this.syncStrategyFactory = syncStrategyFactory;
        setSyncStrategy(Strategy.builder().type(StrategyType.REALTIME).build());
    }

    /**
     * Sets the sync strategy based on the Strategy object provided.
     *
     * @param syncStrategy The sync strategy.
     */
    public void setSyncStrategy(Strategy syncStrategy) {
        this.overallSyncStrategy = this.syncStrategyFactory.getSyncStrategy(syncStrategy);
    }

    /**
     * Performs a full sync on all shadows. Clears any existing sync requests and will create full shadow sync requests
     * for all shadows.
     *
     * @throws InterruptedException if the thread is interrupted while enqueuing data
     */
    private void fullSyncOnAllShadows() throws InterruptedException {
        overallSyncStrategy.clearSyncQueue();

        List<Pair<String, String>> shadows = context.getDao().listSyncedShadows();

        if (shadows.size() > overallSyncStrategy.getRemainingCapacity()) {
            logger.atWarn(SYNC_EVENT_TYPE)
                    .addKeyValue("syncedShadows", shadows.size())
                    .addKeyValue("syncQueueCapacity", overallSyncStrategy.getRemainingCapacity())
                    .log("There are more shadows than space in the sync queue. Syncing will block");
        }
        Iterator<FullShadowSyncRequest> it =
                shadows.stream().map(p -> new FullShadowSyncRequest(p.getLeft(), p.getRight())).iterator();
        while (it.hasNext()) {
            overallSyncStrategy.putSyncRequest(it.next());
        }
    }

    /**
     * Start sync threads to process sync requests. This automatically starts a full sync for all shadows.
     *
     * @param context         an context object for syncing
     * @param syncParallelism number of threads to use for syncing
     */
    public void start(SyncContext context, int syncParallelism) {
        overallSyncStrategy.startSync(context, syncParallelism);
        this.context = context;
        try {
            fullSyncOnAllShadows();
        } catch (InterruptedException e) {
            logger.atWarn(SYNC_EVENT_TYPE)
                    .log("Interrupted while queuing full sync requests at startup. Syncing will stop");
            stop();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Start sync threads to process sync requests. This automatically starts a full sync for all shadows.
     *
     * @param syncParallelism number of threads to use for syncing
     * @implNote Required for integration test.
     */
    public void startSyncStrategy(int syncParallelism) {
        overallSyncStrategy.startSync(context, syncParallelism);
    }

    /**
     * Stops sync threads and clear syncing queue.
     */
    public void stop() {
        overallSyncStrategy.stopSync();
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
     * @throws InterruptedException if the thread is interrupted while enqueuing data
     */
    public void pushCloudUpdateSyncRequest(String thingName, String shadowName, JsonNode updateDocument)
            throws InterruptedException {
        if (isShadowSynced(thingName, shadowName)) {
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
     * @throws InterruptedException if the thread is interrupted while enqueuing data
     */
    public void pushLocalUpdateSyncRequest(String thingName, String shadowName, byte[] updateDocument)
            throws InterruptedException {
        if (isShadowSynced(thingName, shadowName)) {
            overallSyncStrategy.putSyncRequest(new LocalUpdateSyncRequest(thingName, shadowName, updateDocument));
        }
    }

    /**
     * Pushes a delete sync request in the request queue to delete a shadow in the cloud after a local shadow has
     * been successfully deleted.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     * @throws InterruptedException if the thread is interrupted while enqueuing data
     */
    public void pushCloudDeleteSyncRequest(String thingName, String shadowName)
            throws InterruptedException {
        if (isShadowSynced(thingName, shadowName)) {
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
     * @throws InterruptedException if the thread is interrupted while enqueuing data
     */
    public void pushLocalDeleteSyncRequest(String thingName, String shadowName, byte[] deletePayload)
            throws InterruptedException {
        if (isShadowSynced(thingName, shadowName)) {
            overallSyncStrategy.putSyncRequest(new LocalDeleteSyncRequest(thingName, shadowName, deletePayload));
        }
    }
}
