@ShadowManager @Shadow1
Feature: Greengrass V2 ShadowManager

    As a developer, I can access the Greengrass local shadow service from my component.

    Background:
        Given my device is registered as a Thing
        And my device is running Greengrass
        Given I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST |
            | aws.greengrass.ShadowManager | LATEST |
        And I deploy the Greengrass deployment configuration
        Then the Greengrass deployment is COMPLETED on the device after 2 minutes

    Scenario: I can install the shadow manager component
        Given I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST |
            | aws.greengrass.ShadowManager | LATEST |
        And I deploy the Greengrass deployment configuration
        Then the Greengrass deployment is COMPLETED on the device after 2 minutes
        Then I verify the aws.greengrass.ShadowManager component is RUNNING using the greengrass-cli

    Scenario: Shadow-1-T1: As a developer, I can use the Greengrass local shadow service to UPDATE and GET a shadow from my component.
        When I install the component ShadowComponentPing from local store with configuration
            | key                    | value             |
            | Operation              | UpdateThingShadow |
            | ThingName              | testThing         |
            | ShadowName             | testShadow        |
            | ShadowDocument         | {\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}         |
        Then the local Greengrass deployment is SUCCEEDED on the device after 120 seconds
        When I install the component ShadowComponentPong from local store with configuration
            | key                    | value             |
            | Operation              | GetThingShadow |
            | ThingName              | testThing         |
            | ShadowName             | testShadow        |
            | ShadowDocument         | {\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}         |
        Then the local Greengrass deployment is SUCCEEDED on the device after 120 seconds

    Scenario: Shadow-1-T2: As a developer, I can use the Greengrass local shadow service to UPDATE a shadow from my component.
        When I install the component ShadowComponentPing from local store with configuration
            | key                    | value              |
            | Operation              | UpdateThingShadow  |
            | ThingName              | testThing          |
            | ShadowName             | testShadow         |
            | ShadowDocument         | {\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}         |
        Then the local Greengrass deployment is SUCCEEDED on the device after 120 seconds
        When I install the component ShadowComponentPong from local store with configuration
            | key                    | value             |
            | Operation              | UpdateThingShadow |
            | ThingName              | testThing         |
            | ShadowName             | testShadow        |
            | ShadowDocument         | {\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}         |
        Then the local Greengrass deployment is SUCCEEDED on the device after 120 seconds
        And I update the component ShadowComponentPing with configuration
            | key            | value                    |
            | Operation      | GetThingShadow           |
            | ThingName      | testThing                |
            | ShadowName     | testShadow               |
            | ShadowDocument | {\"version\":2,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":0,\"b\":0},\"SomeKey\":\"SomeOtherValue\"}}}  |
        Then the Greengrass deployment is COMPLETED on the device after 2 minutes