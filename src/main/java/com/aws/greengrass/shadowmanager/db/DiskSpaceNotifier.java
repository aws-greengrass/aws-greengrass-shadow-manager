/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.db;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Monitor a disk path and notify listeners when disk space changes.
 */
public class DiskSpaceNotifier implements Runnable {
    private static final Logger logger = LogManager.getLogger(DiskSpaceNotifier.class);
    private static final String PATH_LOG_KEY = "path";

    private final Path path;
    private final AtomicLong size = new AtomicLong(-1);
    private final Set<PathSizeListener> listeners = new LinkedHashSet<>();
    private final long pollPeriod;
    private static final long DEFAULT_POLL_PERIOD_SECONDS = 30;

    /**
     * Listener for size updates to a path.
     */
    public interface PathSizeListener {
        /**
         * Fired when path size is updated.
         * @param path the path
         * @param size the size in bytes, or -1
         */
        void onSizeUpdated(Path path, long size);
    }

    /**
     * Create a new instance that polls every {@value #DEFAULT_POLL_PERIOD_SECONDS} seconds for file system changes.
     *
     * @param path the path to monitor
     */
    public DiskSpaceNotifier(Path path) {
        this(path, DEFAULT_POLL_PERIOD_SECONDS, TimeUnit.SECONDS);

    }

    /**
     * Create a new instance that checks for file system changes at the specified polling interval.
     *
     * @param path the path to monitor
     * @param pollPeriod the polling period length
     * @param unit the unit of the polling period
     */
    public DiskSpaceNotifier(Path path, long pollPeriod, TimeUnit unit) {
        this.path = path;
        this.pollPeriod = TimeUnit.SECONDS.convert(pollPeriod, unit);
    }

    /**
     * Add a listener.
     * @param listener the listener to add
     */
    public void registerListener(PathSizeListener listener) {
        this.listeners.add(listener);
    }

    /**
     * Start monitoring a path.
     */
    @Override
    public void run() {
        logger.atInfo().kv(PATH_LOG_KEY, path).log("Start disk space task");

        // WatchService in JDK 8 does not always return a notification for every delete on every OS/filesystem.
        // In lieu of keeping our own tree representation of the tracked filesystem, we just poll
        // periodically to determine if space has changed
        final Supplier<Long> sizeCalc = () -> {
            long[] calc = new long[1];

            try {
                Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
                            calc[0] += Files.size(file);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                logger.atError().kv(PATH_LOG_KEY, path).setCause(e)
                        .kv("previousSize", size.get())
                        .log("Error while processing files to calculate updated size. Previous value will be used");
                return size.get();
            }
            return calc[0];
        };

        updateSize(sizeCalc.get());

        while (!Thread.interrupted()) {
            try {
                updateSize(sizeCalc.get());
                TimeUnit.SECONDS.sleep(pollPeriod);
            } catch (InterruptedException e) {
                logger.atDebug().kv(PATH_LOG_KEY, path).log("Interrupted while monitoring path");
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Update the current tracked size and notify all listeners if it has changed.
     * @param value the size in bytes
     */
    private void updateSize(long value) {
        logger.atTrace().kv(PATH_LOG_KEY, path).kv("size", value).log();
        long old = size.getAndSet(value);
        if (value != old) {
            notifyListeners(value);
        }
    }

    /**
     * Notify each listener of the size in bytes. Any {@link RuntimeException} is logged.
     *
     * @param size the size in bytes.
     */
    private void notifyListeners(long size) {
        listeners.forEach(l -> l.onSizeUpdated(path, size));
    }
}
