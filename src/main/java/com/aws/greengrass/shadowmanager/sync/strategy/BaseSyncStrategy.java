/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.sync.RequestBlockingQueue;
import com.aws.greengrass.shadowmanager.sync.RequestMerger;
import com.aws.greengrass.shadowmanager.sync.Retryer;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.util.RetryUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

import static com.aws.greengrass.shadowmanager.model.LogEvents.SYNC;

public class BaseSyncStrategy {
    static final String SYNC_EVENT_TYPE = SYNC.code();

    /**
     * Lock used to synchronize start and stop of the sync strategy.
     */
    final Object lifecycleLock = new Object();

    /**
     * The threads running the sync loop.
     */
    final List<Future<?>> syncThreads = new ArrayList<>();

    /**
     * The request blocking queue holding all the sync requests.
     * @implNote The Setter is only used in unit tests. The Getter is used in integration tests.
     */
    @Getter
    @Setter(AccessLevel.PACKAGE)
    RequestBlockingQueue syncQueue;

    /**
     * Interface for executing sync requests.
     */
    @Getter
    final Retryer retryer;

    /**
     * Context object containing handlers useful for sync requests.
     */
    @Setter(AccessLevel.PACKAGE)
    SyncContext context;

    /**
     * Configuration for retrying a sync request.
     */
    final RetryUtils.RetryConfig retryConfig;

    /**
     * Configuration for retrying sync requests.
     */
    static final RetryUtils.RetryConfig DEFAULT_RETRY_CONFIG =
            RetryUtils.RetryConfig.builder()
                    .maxAttempt(5)
                    .initialRetryInterval(Duration.of(3, ChronoUnit.SECONDS))
                    .maxRetryInterval(Duration.of(1, ChronoUnit.MINUTES))
                    .retryableExceptions(Collections.singletonList(RetryableException.class)).build();

    /**
     * Configuration for retrying a sync request immediately after failing with the {@link #DEFAULT_RETRY_CONFIG}.
     */
    static final RetryUtils.RetryConfig FAILED_RETRY_CONFIG =
            RetryUtils.RetryConfig.builder()
                    .maxAttempt(3)
                    .initialRetryInterval(Duration.of(30, ChronoUnit.SECONDS))
                    .maxRetryInterval(Duration.of(2, ChronoUnit.MINUTES))
                    .retryableExceptions(Collections.singletonList(RetryableException.class))
                    .build();


    /**
     * Constructor.
     *
     * @param retryer The retryer object.
     */
    public BaseSyncStrategy(Retryer retryer) {
        this.retryer = retryer;
        this.retryConfig = DEFAULT_RETRY_CONFIG;
        RequestMerger requestMerger = new RequestMerger();
        this.syncQueue = new RequestBlockingQueue(requestMerger);
    }

    /**
     * Constructor for testing.
     *
     * @param retryer     The retryer object.
     * @param retryConfig The config to be used by the retryer.
     */
    public BaseSyncStrategy(Retryer retryer, RetryUtils.RetryConfig retryConfig) {
        this.retryer = retryer;
        this.retryConfig = retryConfig;
        RequestMerger requestMerger = new RequestMerger();
        this.syncQueue = new RequestBlockingQueue(requestMerger);
    }
}
