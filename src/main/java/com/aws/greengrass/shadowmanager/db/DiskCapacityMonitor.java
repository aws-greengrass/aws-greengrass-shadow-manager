/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.db;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import lombok.Synchronized;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitor for disk bases database. This allows for watching disk usage to determine if space has been exceeded.
 */
public class DiskCapacityMonitor implements CapacityMonitor {
    private static final Logger logger = LogManager.getLogger(DiskCapacityMonitor.class);

    private final ExecutorService executorService;

    private final DiskSpaceNotifier diskSpaceNotifier;

    private final AtomicBoolean overDiskCapacity = new AtomicBoolean(false);
    private final AtomicLong maxDiskUsage = new AtomicLong(0);
    private final AtomicLong diskUsage = new AtomicLong(0);

    private Future<?> notifierTask;
    private boolean isStarted = false;

    /**
     * Construct a new instance.
     * @param executorService the executor service.
     * @param dbPath path for the database.
     */
    public DiskCapacityMonitor(ExecutorService executorService, Path dbPath) {
        this(executorService, new DiskSpaceNotifier(dbPath));
    }

    DiskCapacityMonitor(ExecutorService executorService, DiskSpaceNotifier notifier) {
        this.executorService = executorService;
        this.diskSpaceNotifier = notifier;

        registerListener();
    }

    /**
     * Add a listener which will check if the disk usage has crossed the threshold.
     */
    private void registerListener() {
        diskSpaceNotifier.registerListener((path, size) -> {
            logger.atTrace()
                    .kv("path", path)
                    .kv("currentUsage", size)
                    .log("Size changed");
            checkSize(size);
        });
    }

    /**
     * Check the disk usage against the threshold.
     * <p/>
     * If it has crossed the threshold (either under or over), then the overDiskCapacity field is updated.
     * The field is only updated if the threshold is crossed. If it is over the threshold, and the usage is updated
     * again to remain above the threshold, the state is not changed.
     *
     * @param size usage in bytes
     */
    @Synchronized
    void checkSize(long size) {
        long maxUsage = maxDiskUsage.get();

        long prev = diskUsage.getAndSet(size);

        if (prev < maxUsage && size >= maxUsage
            && overDiskCapacity.compareAndSet(false, true)) {
            // monitored size has gone from under to over limit
            logger.atInfo()
                    .kv("currentUsage", size)
                    .kv("threshold", maxUsage)
                    .log("Disk capacity now exceeds threshold");
        } else if (prev >= maxUsage && size < maxUsage
            && overDiskCapacity.compareAndSet(true, false)) {
            // monitored size has gone from over to under limit
            logger.atInfo()
                    .kv("currentUsage", size)
                    .kv("threshold", maxUsage)
                    .log("Disk capacity is now under threshold");
        }
    }

    /**
     * Set the maximum disk usage threshold.
     * @param maxDiskUsage number of bytes
     */
    @Synchronized
    public void setMaxDiskUsage(long maxDiskUsage) {
        long oldValue = this.maxDiskUsage.getAndSet(maxDiskUsage);
        if (maxDiskUsage <= 0) {
            logger.atInfo().log("No maximum disk usage threshold is set");
            stopNotifier();
            // reset usage and capacity flag
            overDiskCapacity.set(false);
            diskUsage.set(0);
        } else if (isStarted && oldValue <= 0) {
            startNotifier();
        } else {
            long prev = diskUsage.getAndSet(0);
            checkSize(prev); // recheck the size to see if we are now over / under
        }
    }

    @Override
    public boolean isCapacityExceeded() {
        return overDiskCapacity.get();
    }

    @Synchronized
    public void start() {
        startNotifier();
        isStarted = true;
    }

    @Synchronized
    public void stop() {
        stopNotifier();
        isStarted = false;
    }

    private void startNotifier() {
        if (this.notifierTask == null && this.maxDiskUsage.get() > 0) {
            this.notifierTask = executorService.submit(diskSpaceNotifier);
        }
    }

    private void stopNotifier() {
        if (this.notifierTask != null && !this.notifierTask.isDone()) {
            this.notifierTask.cancel(true);
            this.notifierTask = null; // NOPMD - don't keep a reference if the usage is set to 0 and task stopped
        }
    }
}
