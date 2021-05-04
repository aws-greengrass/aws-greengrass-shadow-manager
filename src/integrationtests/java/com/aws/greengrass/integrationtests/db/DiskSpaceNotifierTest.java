/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.db;

import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.db.DiskSpaceNotifier;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class DiskSpaceNotifierTest {
    @TempDir
    Path testPath;

    DiskSpaceNotifier monitor;

    ExecutorService executorService;

    static final Duration MAX_WAIT = Duration.ofSeconds(5);
    private AtomicLong actualSize;

    @BeforeEach
    void setup() {
        LogManager.getRootLogConfiguration().setLevel(Level.TRACE);
        monitor = new DiskSpaceNotifier(testPath, 1, TimeUnit.SECONDS);
        executorService = Executors.newSingleThreadExecutor();
        actualSize = new AtomicLong(-1);

        monitor.registerListener((path, size) -> {
            actualSize.set(size);
        });
    }

    @AfterEach
    void stop() {
        executorService.shutdownNow();
    }

    void assertSize(long size) {
        assertThat(actualSize::get, eventuallyEval(is(size), MAX_WAIT));
    }

    void writePath(Path path, int length, OpenOption... openOptions) throws IOException {
        Files.createDirectories(path.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(path, openOptions)) {
            for (int i = 0; i < length; i++) {
                writer.write('x');
            }
            writer.flush();
        }
    }

    @Test
    void GIVEN_empty_path_WHEN_files_change_THEN_size_updated() throws IOException {

        executorService.submit(monitor::run);
        assertSize(0);

        Path test1 = testPath.resolve("test1");
        writePath(test1, 50, CREATE_NEW, WRITE);
        assertSize(50);

        Path test2 = testPath.resolve("child")  .resolve("test2");
        writePath(test2, 50, CREATE_NEW, WRITE);
        assertSize(100);

        Files.delete(test2);
        assertSize(50);

        writePath(test1, 1, APPEND);
        assertSize(51);

        Files.delete(test1);
        assertSize(0);
    }


    @Test
    void GIVEN_non_empty_path_WHEN_files_change_THEN_size_updated() throws IOException {
        Path initialFile = testPath.resolve("initial");
        writePath(initialFile, 500);

        executorService.submit(monitor::run);
        assertSize(500);

        writePath(initialFile, 32, TRUNCATE_EXISTING, WRITE);
        assertSize(32);

        Path test1 = testPath.resolve("test1");
        writePath(test1, 32, CREATE_NEW, WRITE);
        assertSize(64);
    }

    @Test
    void GIVEN_nested_path_WHEN_files_change_THEN_size_updated() throws IOException {
        Path initialFile = testPath.resolve("a").resolve("b").resolve("file");
        writePath(initialFile, 500);

        executorService.submit(monitor::run);
        assertSize(500);

        writePath(initialFile, 32, TRUNCATE_EXISTING, WRITE);
        assertSize(32);

        Path test1 = testPath.resolve("a").resolve("b").resolve("c").resolve("file");
        writePath(test1, 32, CREATE_NEW, WRITE);
        assertSize(64);

        Path test2 = testPath.resolve("foo").resolve("file1");
        writePath(test2, 32, CREATE_NEW, WRITE);
        assertSize(96);

        Files.walkFileTree(testPath.resolve("a"), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
        assertSize(32);
    }
}
