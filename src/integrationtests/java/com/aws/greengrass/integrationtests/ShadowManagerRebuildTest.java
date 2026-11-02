/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.shadowmanager.ShadowManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDatabaseRecoveryException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Runs the local-database rebuild ({@code ShadowManager.rebuildDatabase()}) on a real Nucleus and its real
 * executor, rather than a synchronous mock.
 */
class ShadowManagerRebuildTest extends NucleusLaunchUtils {
    private static final String THING = "RebuildThing";
    private final List<String> transitions = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setup() {
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");
        kernel = new Kernel();
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    private void recordShadowManagerTransitions() {
        kernel.getContext().addGlobalStateChangeListener((service, was, now) -> {
            if (ShadowManager.SERVICE_NAME.equals(service.getName())) {
                transitions.add(was + "->" + now);
            }
        });
    }

    private void assertReinstalledAndRunning() {
        assertThat("component was reinstalled", () -> transitions, eventuallyEval(hasItem("STOPPING->NEW"),
                Duration.ofSeconds(30)));
        assertThat("component is running again", () -> transitions.get(transitions.size() - 1),
                eventuallyEval(is("STARTING->RUNNING"), Duration.ofSeconds(30)));
    }

    // Trigger the rebuild on the running instance, as the DAO's corruption callback does.
    private void triggerRebuild() throws Exception {
        Method rebuild = ShadowManager.class.getDeclaredMethod("rebuildDatabase");
        rebuild.setAccessible(true);
        rebuild.invoke(shadowManager);
    }

    @Test
    void GIVEN_rebuilt_database_WHEN_one_local_pubsub_update_THEN_it_is_applied_once() throws Exception {
        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder().configFile("config.yaml").build());

        triggerRebuild();
        Field inProgress = ShadowManager.class.getDeclaredField("rebuildInProgress");
        inProgress.setAccessible(true);
        AtomicBoolean rebuildInProgress = (AtomicBoolean) inProgress.get(shadowManager);
        assertThat("rebuild finished", rebuildInProgress::get, eventuallyEval(is(false), Duration.ofSeconds(30)));

        kernel.getContext().get(PubSubIPCEventStreamAgent.class).publish("$aws/things/" + THING + "/shadow/update",
                "{\"state\":{\"reported\":{\"k\":1}}}".getBytes(UTF_8), "test");

        // one local pub/sub listener -> one update handled -> version 1 (a duplicate listener would give 2)
        ShadowManagerDAOImpl realDao = kernel.getContext().get(ShadowManagerDAOImpl.class);
        assertThat("update applied", () -> realDao.getShadowThing(THING, "").isPresent(),
                eventuallyEval(is(true), Duration.ofSeconds(10)));
        Thread.sleep(1000);
        Optional<ShadowDocument> doc = realDao.getShadowThing(THING, "");
        assertThat(doc.get().getVersion(), is(1L));
    }

    @Test
    void GIVEN_resuming_sync_fails_WHEN_rebuild_THEN_component_is_reinstalled(ExtensionContext context)
            throws Exception {
        ignoreExceptionOfType(context, IllegalStateException.class);
        AtomicBoolean failNextListSyncedShadows = new AtomicBoolean(false);
        when(dao.listSyncedShadows()).thenAnswer(invocation -> {
            if (failNextListSyncedShadows.getAndSet(false)) {
                throw new IllegalStateException("simulated failure while resuming sync");
            }
            return Collections.emptyList();
        });
        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder().configFile("config.yaml").mockDao(true).build());
        recordShadowManagerTransitions();

        // startup() reports RUNNING before setupSync(); wait for its sync setup so the failure below is
        // consumed by the rebuild's sync restart, not by startup
        verify(dao, timeout(10_000).atLeastOnce()).listSyncedShadows();

        // recovery succeeds, then the sync restart (reInitializeSyncInfo) throws an unchecked exception
        failNextListSyncedShadows.set(true);
        triggerRebuild();

        assertReinstalledAndRunning();
    }

    @Test
    void GIVEN_recovery_fails_WHEN_rebuild_THEN_component_is_reinstalled(ExtensionContext context)
            throws Exception {
        ignoreExceptionOfType(context, ShadowManagerDatabaseRecoveryException.class);
        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder().configFile("config.yaml").mockDao(true).build());
        recordShadowManagerTransitions();
        doThrow(new ShadowManagerDatabaseRecoveryException(new IOException("simulated: cannot recreate database")))
                .doNothing().when(dao).recoverDatabase(any());

        triggerRebuild();

        assertReinstalledAndRunning();
    }

    @Test
    void GIVEN_component_reinstalled_WHEN_one_local_pubsub_update_THEN_it_is_applied_once() throws Exception {
        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder().configFile("config.yaml").build());
        recordShadowManagerTransitions();

        // shutdown() unsubscribes and startup() subscribes again: exactly one listener must remain
        shadowManager.requestReinstall();
        assertReinstalledAndRunning();

        kernel.getContext().get(PubSubIPCEventStreamAgent.class).publish("$aws/things/" + THING + "/shadow/update",
                "{\"state\":{\"reported\":{\"k\":1}}}".getBytes(UTF_8), "test");

        ShadowManagerDAOImpl realDao = kernel.getContext().get(ShadowManagerDAOImpl.class);
        assertThat("update applied after reinstall", () -> realDao.getShadowThing(THING, "").isPresent(),
                eventuallyEval(is(true), Duration.ofSeconds(10)));
        Thread.sleep(1000);
        assertThat(realDao.getShadowThing(THING, "").get().getVersion(), is(1L));
    }
}
