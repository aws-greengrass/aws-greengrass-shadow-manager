package com.aws.greengrass;

//import com.aws.greengrass.platforms.PlatformResolver;

import com.aws.greengrass.testing.platform.PlatformResolver;

import java.util.concurrent.TimeUnit;

public final class Constants {
    public static final String JVM_OPTIONS = "jvmOptions";
    public static final String CREDENTIALS_PATH = "credentialsPath";
    public static final String CLI_PATH = "cliPath";
    public static final String CLI_SERVER_PATH = "cliServerPath";
    public static final String KERNEL_ZIP_PATH = "kernelZipPath";
    public static final String PKCS11_PROVIDER_PATH = "pkcs11ProviderPath";
    public static final String FLEET_PROVISIONING_BY_CLAIM_PATH = "fleetProvisionByClaimPath";
    public static final String TEST_RESULTS_PATH = "testResDir";
    public static final String ROOT_CA = "rootCA";
    public static final String CLI_EXECUTABLE_NO_EXTENSION = "greengrass-cli";
//    public static final String CLI_EXECUTABLE = PlatformResolver.IS_WINDOWS ? "greengrass-cli.cmd" : "greengrass-cli";
    public static final String TEST_RUN_ID = "testId";
    public static final String TEST_RUN_PATH = "testRunDir";
    public static final String TEST_INSTALL_PATH = "testInstallDir";
    public static final String TEST_JVM_OPTIONS = "UAT_SYSTEM_PROP_JVM_OPTIONS";
    public static final String TEST_TEMP_LOG_DIRECTORIES_MAP = "testTempLogDirectoriesMap";
    public static final String THING_NAME = "thingName";
    public static final String CLEANUP_TEST_ARTIFACTS = "cleanupTestArtifacts";
    public static final String COMPONENT_BUCKET_NAME = "releaseBucketName";
    public static final String E2E_TEST_PKG_STORE = "eteTestPkgStore";
    public static final String NUCLEUS_VERSION = "nucleusVersion";
    public static final String GGC_USER = "ggc_user";
    public static final String GGC_GROUP = "ggc_group";
    public static final String RELEASE_ACCOUNT = "releaseAccount";
    public static final String GGC_USER_AND_GROUP = String.format("%s:%s", GGC_USER, GGC_GROUP);
    public static final String HSM_REFERENCE = "hsm";
    public static final String SOFTHSM_DEFAULT_TOKEN = "HSM_default_token";
    // TODO: Find a better place for these SoftHSM config constants
    public static final String SOFTHSM_TOKEN_PIN = "a1B2c3";
    public static final String SOFTHSM_SO_PIN = "54321";
    public static final String SOFTHSM_PKCS11_CONFIG_FILE_PATH = "softHSMPKCS11ConfigFilePath";
    public static final String PKCS11_CONFIG_FILE_NAME = "pkcs11.conf";
    public static final String PRIVATE_KEY_NAME = "privKey.key";
    public static final String CERTIFICATE_NAME = "thingCert.crt";
    public static final String TPM_KEY_LABEL = "rsa-privkey";
    public static final String TOKEN_ID = "0000";

    /**
     * Property name for the platform the test is running on.
     */
    public static final String PLATFORM_PROP_NAME = "platform";
    /**
     * Property name for the test case id passed in.
     * testCaseId is being used for the purpose of adding Dimension to monitor metrics
     * EGRunner can override this for human readable string
     * When running UAT directly, testCaseId defaults to testRunId.
     */
    public static final String TEST_CASE_ID_OVERRIDE_PROP_NAME = "testCaseId";
    /**
     * Property name for the device pool the test is running on (If running on a DUT).
     */
    public static final String DEVICE_POOL_PROP_NAME = "devicePoolName";
    /**
     * Property name for the device instance id the test is running on (If running on a DUT).
     */
    public static final String DEVICE_INSTANCE_ID_PROP_NAME = "deviceInstanceId";
    /**
     * Property name for the max duration for a long running test.
     * Being used for waiting before exiting
     */
    public static final String TEST_MAX_DURATION_PROP_NAME = "testMaxDurationSeconds";
    /**
     * Property name prefix for all the system properties used in tests.
     */
    public static final String UAT_SYSTEM_PROP_PREFIX = "UAT_SYSTEM_PROP_";
    /**
     * Enabled debug logs for monitor. Since the prop name starts with @UAT_SYSTEM_PROP_PREFIX, egrunner will pass it
     * along to the UAT.
     */
    public static final String ENABLED_DEBUG_LOGS_FOR_MONITOR_PROP_NAME = "UAT_SYSTEM_PROP_ENABLE_MONITOR_DEBUG_LOGS";
    /**
     * Prop for Kinesis stream name for monitor metrics data
     */
    public static final String KINESIS_METRIC_STREAM_NAME_FOR_MONITOR_PROP_NAME =
            "UAT_SYSTEM_PROP_KINESIS_METRIC_STREAM_NAME_FOR_MONITOR";
    /**
     * Prop for Kinesis stream name for monitor logs data
     */
    public static final String KINESIS_LOG_STREAM_NAME_FOR_MONITOR_PROP_NAME =
            "UAT_SYSTEM_PROP_KINESIS_LOG_STREAM_NAME_FOR_MONITOR";
    /**
     * Prop for TestRunId override. If this prop is not provided, a random string is used.
     */
    public static final String TEST_RUN_ID_OVERRIDE_PROP_NAME =
            "UAT_SYSTEM_PROP_TEST_RUN_ID";

    public static final String COMP_STORE_DIR = "packages";
    public static final String RECIPES_DIR = "recipes";
    public static final String ARTIFACTS_DIR = "artifacts";
    public static final String ARTIFACTS_DECOMPRESSED_DIR = "artifacts-unarchived";
    public static final String COMMON_TEMP_FOLDER_PREFIX = "EGUAT_TEMP";

    public static final String TEMP_DUMMY_FILE_NAME = "delete.me";

    public static final long DEFAULT_GENERIC_POLLING_TIMEOUT_SECONDS =
        (long) Math.ceil(30.0 * TestUtils.getTimeOutMultiplier());
    public static final long DEFAULT_GENERIC_POLLING_TIMEOUT_MILLIS =
            TimeUnit.SECONDS.toMillis(DEFAULT_GENERIC_POLLING_TIMEOUT_SECONDS);

    public static final String LATEST_VERSION_PLACEHOLDER = "LATEST";

    public static final String TEST_RUN_DIR_VAR = String.format("${%s}", TEST_RUN_PATH);
    public static final String GREENGRASS = "greengrass";
    public static final String EVERGREEN_LOG_FILE_NAME = "greengrass.log";
    public static final String EVERGREEN_LOG_DIRECTORY = String.format("%s/logs/", TEST_RUN_DIR_VAR);
    public static final String EVERGREEN_LOG_FILE_PATH = String.format("%s/logs/%s", TEST_RUN_DIR_VAR,
            EVERGREEN_LOG_FILE_NAME);
    public static final String EVERGREEN_TLOG_FILE_PATH = String.format("%s/config/config.tlog", TEST_RUN_DIR_VAR);
    public static final String RELEASE_BUCKET_NAME = "RELEASE_BUCKET_NAME";

    private Constants() {
    }
}
