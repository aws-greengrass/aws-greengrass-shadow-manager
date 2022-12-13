@ShadowManager
Feature: Greengrass V2 ShadowManager

    This is a dummy test

    Background:
        Given my device is registered as a Thing
        And my device is running Greengrass

    Scenario: I can install the shadow manage
        Given I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST |
            | aws.greengrass.ShadowManager | LATEST |
        And I deploy the Greengrass deployment configuration
        Then the Greengrass deployment is COMPLETED on the device after 2 minutes
        Then I verify the aws.greengrass.ShadowManager component is RUNNING using the greengrass-cli