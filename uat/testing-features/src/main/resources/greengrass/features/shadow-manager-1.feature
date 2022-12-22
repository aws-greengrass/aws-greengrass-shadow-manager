@ShadowManager
Feature: Greengrass V2 ShadowManager

    As a device owner I can install the aws.greengrass.ShadowManager component

    Background:
        Given my device is registered as a Thing
        And my device is running Greengrass

    Scenario: I can install the shadow manager component
        Given I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST |
            | aws.greengrass.ShadowManager | LATEST |
        And I deploy the Greengrass deployment configuration
        Then the Greengrass deployment is COMPLETED on the device after 2 minutes
        Then I verify the aws.greengrass.ShadowManager component is RUNNING using the greengrass-cli

    Scenario: HelloWorld-1-T1: I can install the shadow manager component
        When I create a Greengrass deployment with components
            | aws.greengrass.Cli | GG_CLI_VERSION |
        And I deploy the Greengrass deployment configuration
        Then the Greengrass deployment is COMPLETED on the device after 180 seconds
        Then I verify greengrass-cli is available in greengrass root
        When I create a local deployment with components
            | HelloWorld | local:/local-store/recipes/HelloWorld.yaml |
        Then the local Greengrass deployment is SUCCEEDED on the device after 120 seconds
