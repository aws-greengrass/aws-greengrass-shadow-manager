@ShadowManager @Shadow2
Feature: Shadow-2

  As a developer, I can synchronize my local and cloud shadows.

  Background:
    Given my device is registered as a Thing
    And my device is running Greengrass
    When I add random shadow for MyThing with name MyThingNamedShadow in context
    When I add random shadow for MyThing2 with name MyThingNamedShadow2 in context

  @stable @functional @JavaSDK @smoke @RunWithHSM
  Scenario Outline: Shadow-2-T1-<strategy>: As a developer, I can sync a local named shadow to the cloud.
    When I create a Greengrass deployment with components
      | aws.greengrass.Nucleus       | LATEST |
      | aws.greengrass.ShadowManager | LATEST |
    And I update my Greengrass deployment configuration, setting the component aws.greengrass.ShadowManager configuration to:
        """
        {
           "MERGE":{
              "strategy":{
                 "type":"<strategy>",
                 "delay":5
              },
              "synchronize":{
                 "shadowDocuments":[
                    {
                       "thingName":"${MyThing}",
                       "classic":false,
                       "namedShadows":[
                          "${MyThingNamedShadow}"
                       ]
                    },
                    {
                       "thingName":"${MyThing2}",
                       "classic":true
                    }
                 ],
                 "provideSyncStatus":true,
                 "maxOutboundSyncUpdatesPerSecond":50
              },
              "shadowDocumentSizeLimitBytes":8192,
              "maxDiskUtilizationMegaBytes":16
           }
        }
        """
    And I update my Greengrass deployment configuration, setting the component aws.greengrass.Nucleus configuration to:
            """
        {"MERGE":{"logging": {"level": "DEBUG"}}}
        """
    And I deploy the Greengrass deployment configuration
    Then the Greengrass deployment is COMPLETED on the device after 2 minutes
    And I install the component ShadowComponentPing from local store with configuration
      | key                    | value                                                                                                          |
      | assertionServerPort    | ${assertionServerPort}                                                                                         |
      | Operation              | UpdateThingShadow                                                                                              |
      | ThingName              | MyThing                                                                                                        |
      | ShadowName             | MyThingNamedShadow                                                                                             |
      | ShadowDocument         | {\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}               |
      | OperationTimeout       | <timeout>                                                                                                      |
#    Then I get 1 assertions with context "Updated shadow document" within 15 seconds
    Then I can get cloud shadow for MyThing with name MyThingNamedShadow with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}} within 30 seconds
    When I install the component ShadowComponentPong from local store with configuration
      | key              | value              |
      | Operation        | DeleteThingShadow  |
      | ThingName        | MyThing            |
      | ShadowName       | MyThingNamedShadow |
      | ShadowDocument   |                    |
      | OperationTimeout | <timeout>          |
#    Then I get 1 assertions with context "Deleted shadow document" within 15 seconds
    And I can not get cloud shadow for MyThing with name MyThingNamedShadow within 30 seconds
    
    Examples:
      | strategy | timeout |
      | realTime | 5       |
    
    @gamma
    Examples:
      | strategy | timeout |
      | periodic | 30      |

#  @stable @functional @JavaSDK @smoke @RunWithHSM
#  Scenario Outline: Shadow-2-T2-<strategy>: As a developer, I can sync a cloud named shadow to the local.
#    When I create a deployment configuration for deployment DeploymentShadow-1 with components
#      | aws.greengrass.Nucleus       | LATEST |
#      | aws.greengrass.ShadowManager | LATEST |
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.ShadowManager" version "LATEST" configuration:
#        """
#        {
#           "MERGE":{
#              "strategy":{
#                 "type":"<strategy>",
#                 "delay":5
#              },
#              "synchronize":{
#                 "shadowDocuments":[
#                    {
#                       "thingName":"${MyThing}",
#                       "classic":false,
#                       "namedShadows":[
#                          "${MyThingNamedShadow}"
#                       ]
#                    },
#                    {
#                       "thingName":"${MyThing2}",
#                       "classic":true
#                    }
#                 ],
#                 "provideSyncStatus":true,
#                 "maxOutboundSyncUpdatesPerSecond":50
#              },
#              "shadowDocumentSizeLimitBytes":8192,
#              "maxDiskUtilizationMegaBytes":16
#           }
#        }
#        """
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.Nucleus" version "LATEST" configuration:
#        """
#        {"MERGE":{"logging": {"level": "DEBUG"}}}
#        """
#    And I deploy the configuration for deployment DeploymentShadow-1
#    Then the deployment DeploymentShadow-1 completes with SUCCEEDED within 240 seconds
#    When the greengrass log eventually contains the following patterns within 30 seconds
#      | Subscribed to \$aws\/things\/MyThinge2e* | 1 | Should have subscribed to the shadow topics for thing |
#    When I can create cloud shadow for MyThing with name MyThingNamedShadow with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}}
#    Then I can get cloud shadow for MyThing with name MyThingNamedShadow with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}} within 30 seconds
#    When I install the component ShadowComponentPing version 0.0.0 from local store within 30 seconds with configuration using the variables
#      | key              | value                                                                                                          |
#      | Operation        | GetThingShadow                                                                                                 |
#      | ThingName        | MyThing                                                                                                        |
#      | ShadowName       | MyThingNamedShadow                                                                                             |
#      | ShadowDocument   | {\"version\":1,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}} |
#      | OperationTimeout | <timeout>                                                                                                      |
#    Then I get 1 assertions with context "Retrieved matching shadow document" within 15 seconds
#    When I delete cloud shadow for MyThing with name MyThingNamedShadow
#    Then I can not get cloud shadow for MyThing with name MyThingNamedShadow within 30 seconds
#    Then I update the component ShadowComponentPing with parameter configuration
#      | key              | value              |
#      | Operation        | GetThingShadow     |
#      | ThingName        | MyThing            |
#      | ShadowName       | MyThingNamedShadow |
#      | ShadowDocument   |                    |
#      | OperationTimeout | <timeout>          |
#    And I get at least 1 assertions with context "No shadow found" within 15 seconds
#
#    Examples:
#      | strategy | timeout |
#      | realTime | 5       |
#
#    @gamma
#    Examples:
#      | strategy | timeout |
#      | periodic | 30      |
#
#  @stable @functional @JavaSDK
#  Scenario Outline: Shadow-2-T3-<strategy>: As a developer, I do not sync a local named shadow to the cloud that is not in the sync configuration.
#    When I create a deployment configuration for deployment DeploymentShadow-1 with components
#      | aws.greengrass.Nucleus       | LATEST |
#      | aws.greengrass.ShadowManager | LATEST |
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.ShadowManager" version "LATEST" configuration:
#        """
#        {
#           "MERGE":{
#              "strategy":{
#                 "type":"<strategy>",
#                 "delay":5
#              },
#              "synchronize":{
#                 "shadowDocuments":[
#                    {
#                       "thingName":"${MyThing}",
#                       "classic":false,
#                       "namedShadows":[
#                          "${MyThingNamedShadow}"
#                       ]
#                    },
#                    {
#                       "thingName":"${MyThing2}",
#                       "classic":true
#                    }
#                 ],
#                 "provideSyncStatus":true,
#                 "maxOutboundSyncUpdatesPerSecond":50
#              },
#              "shadowDocumentSizeLimitBytes":8192,
#              "maxDiskUtilizationMegaBytes":16
#           }
#        }
#        """
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.Nucleus" version "LATEST" configuration:
#        """
#        {"MERGE":{"logging": {"level": "DEBUG"}}}
#        """
#    And I deploy the configuration for deployment DeploymentShadow-1
#    Then the deployment DeploymentShadow-1 completes with SUCCEEDED within 240 seconds
#    When I install the component ShadowComponentPing version 0.0.0 from local store within 30 seconds with configuration using the variables
#      | key                    | value                                                                                                          |
#      | assertionServerPort    | ${assertionServerPort}                                                                                         |
#      | Operation              | UpdateThingShadow                                                                                              |
#      | ThingName              | MyThing                                                                                                        |
#      | ShadowName             | NotSyncedShadow                                                                                                |
#      | ShadowDocument         | {\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}               |
#      | ExpectedShadowDocument | {\"version\":1,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}} |
#      | OperationTimeout       | <timeout>                                                                                                      |
#    Then I get 1 assertions with context "Updated shadow document" within 15 seconds
#    And I can not get cloud shadow for MyThing with name NotSyncedShadow within 30 seconds
#
#    Examples:
#      | strategy | timeout |
#      | realTime | 5       |
#
#    @gamma
#    Examples:
#      | strategy | timeout |
#      | periodic | 30      |
#
#  @stable @functional @JavaSDK
#  Scenario Outline: Shadow-2-T4-<strategy>: As a developer, I do not sync a cloud named shadow to the cloud that is not in the sync configuration.
#    When I create a deployment configuration for deployment DeploymentShadow-1 with components
#      | aws.greengrass.Nucleus       | LATEST |
#      | aws.greengrass.ShadowManager | LATEST |
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.ShadowManager" version "LATEST" configuration:
#        """
#        {
#           "MERGE":{
#              "strategy":{
#                 "type":"<strategy>",
#                 "delay":5
#              },
#              "synchronize":{
#                 "shadowDocuments":[
#                    {
#                       "thingName":"${MyThing}",
#                       "classic":false,
#                       "namedShadows":[
#                          "${MyThingNamedShadow}"
#                       ]
#                    },
#                    {
#                       "thingName":"${MyThing2}",
#                       "classic":true
#                    }
#                 ],
#                 "provideSyncStatus":true,
#                 "maxOutboundSyncUpdatesPerSecond":50
#              },
#              "shadowDocumentSizeLimitBytes":8192,
#              "maxDiskUtilizationMegaBytes":16
#           }
#        }
#        """
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.Nucleus" version "LATEST" configuration:
#        """
#        {"MERGE":{"logging": {"level": "DEBUG"}}}
#        """
#    And I deploy the configuration for deployment DeploymentShadow-1
#    Then the deployment DeploymentShadow-1 completes with SUCCEEDED within 240 seconds
#    When the greengrass log eventually contains the following patterns within 30 seconds
#      | Subscribed to \$aws\/things\/MyThinge2e* | 1 | Should have subscribed to the shadow topics for thing |
#    When I can create cloud shadow for MyThing with name NotSyncedShadow with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}}
#    Then I can get cloud shadow for MyThing with name NotSyncedShadow with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}} within 30 seconds
#    When I install the component ShadowComponentPing version 0.0.0 from local store within 30 seconds with configuration using the variables
#      | key                 | value                  |
#      | assertionServerPort | ${assertionServerPort} |
#      | Operation           | GetThingShadow         |
#      | ThingName           | MyThing                |
#      | ShadowName          | NotSyncedShadow        |
#      | ShadowDocument      |                        |
#      | OperationTimeout    | <timeout>              |
#    Then I get at least 1 assertions with context "No shadow found" within 15 seconds
#
#    Examples:
#      | strategy | timeout |
#      | realTime | 5       |
#
#    @gamma
#    Examples:
#      | strategy | timeout |
#      | periodic | 30      |
#
#  @stable @functional @JavaSDK
#  Scenario Outline: Shadow-2-T5-<strategy>: As a developer, I can sync a local classic shadow to the cloud.
#    When I create a deployment configuration for deployment DeploymentShadow-1 with components
#      | aws.greengrass.Nucleus       | LATEST |
#      | aws.greengrass.ShadowManager | LATEST |
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.ShadowManager" version "LATEST" configuration:
#        """
#        {
#           "MERGE":{
#              "strategy":{
#                 "type":"<strategy>",
#                 "delay":5
#              },
#              "synchronize":{
#                 "shadowDocuments":[
#                    {
#                       "thingName":"${MyThing}",
#                       "classic":false,
#                       "namedShadows":[
#                          "${MyThingNamedShadow}"
#                       ]
#                    },
#                    {
#                       "thingName":"${MyThing2}",
#                       "classic":true
#                    }
#                 ],
#                 "provideSyncStatus":true,
#                 "maxOutboundSyncUpdatesPerSecond":50
#              },
#              "shadowDocumentSizeLimitBytes":8192,
#              "maxDiskUtilizationMegaBytes":16
#           }
#        }
#        """
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.Nucleus" version "LATEST" configuration:
#        """
#        {"MERGE":{"logging": {"level": "DEBUG"}}}
#        """
#    And I deploy the configuration for deployment DeploymentShadow-1
#    Then the deployment DeploymentShadow-1 completes with SUCCEEDED within 240 seconds
#    And I install the component ShadowComponentPing version 0.0.0 from local store within 30 seconds with configuration using the variables
#      | key                    | value                                                                                                          |
#      | assertionServerPort    | ${assertionServerPort}                                                                                         |
#      | Operation              | UpdateThingShadow                                                                                              |
#      | ThingName              | MyThing2                                                                                                       |
#      | ShadowName             | CLASSIC                                                                                                        |
#      | ShadowDocument         | {\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}               |
#      | ExpectedShadowDocument | {\"version\":1,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}} |
#      | OperationTimeout       | <timeout>                                                                                                      |
#    Then I get 1 assertions with context "Updated shadow document" within 15 seconds
#    Then I can get cloud shadow for MyThing2 with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}} within 30 seconds
#    When I install the component ShadowComponentPong version 0.0.0 from local store within 30 seconds with configuration using the variables
#      | key              | value             |
#      | Operation        | DeleteThingShadow |
#      | ThingName        | MyThing2          |
#      | ShadowName       | CLASSIC           |
#      | ShadowDocument   |                   |
#      | OperationTimeout | <timeout>         |
#    Then I get 1 assertions with context "Deleted shadow document" within 15 seconds
#    And I can not get cloud shadow for MyThing2 within 30 seconds
#
#    Examples:
#      | strategy | timeout |
#      | realTime | 5       |
#
#    @gamma
#    Examples:
#      | strategy | timeout |
#      | periodic | 30      |
#
#  @stable @functional @JavaSDK
#  Scenario Outline: Shadow-2-T6-<strategy>: As a developer, I can sync a cloud named shadow to the local.
#    When I create a deployment configuration for deployment DeploymentShadow-1 with components
#      | aws.greengrass.Nucleus       | LATEST |
#      | aws.greengrass.ShadowManager | LATEST |
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.ShadowManager" version "LATEST" configuration:
#        """
#        {
#           "MERGE":{
#              "strategy":{
#                 "type":"<strategy>",
#                 "delay":5
#              },
#              "synchronize":{
#                 "shadowDocuments":[
#                    {
#                       "thingName":"${MyThing}",
#                       "classic":false,
#                       "namedShadows":[
#                          "${MyThingNamedShadow}"
#                       ]
#                    },
#                    {
#                       "thingName":"${MyThing2}",
#                       "classic":true
#                    }
#                 ],
#                 "provideSyncStatus":true,
#                 "maxOutboundSyncUpdatesPerSecond":50
#              },
#              "shadowDocumentSizeLimitBytes":8192,
#              "maxDiskUtilizationMegaBytes":16
#           }
#        }
#        """
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.Nucleus" version "LATEST" configuration:
#        """
#        {"MERGE":{"logging": {"level": "DEBUG"}}}
#        """
#    And I deploy the configuration for deployment DeploymentShadow-1
#    Then the deployment DeploymentShadow-1 completes with SUCCEEDED within 240 seconds
#    When the greengrass log eventually contains the following patterns within 30 seconds
#      | Subscribed to \$aws\/things\/MyThinge2e* | 1 | Should have subscribed to the shadow topics for thing |
#    When I can create cloud shadow for MyThing2 with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}}
#    Then I can get cloud shadow for MyThing2 with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}} within 30 seconds
#    When I install the component ShadowComponentPing version 0.0.0 from local store within 30 seconds with configuration using the variables
#      | key              | value                                                                                                          |
#      | Operation        | GetThingShadow                                                                                                 |
#      | ThingName        | MyThing2                                                                                                       |
#      | ShadowName       | CLASSIC                                                                                                        |
#      | ShadowDocument   | {\"version\":1,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}} |
#      | OperationTimeout | <timeout>                                                                                                      |
#    Then I get 1 assertions with context "Retrieved matching shadow document" within 15 seconds
#    When I delete cloud shadow for MyThing2
#    Then I can not get cloud shadow for MyThing2 within 30 seconds
#    When I update the component ShadowComponentPing with parameter configuration
#      | key              | value          |
#      | Operation        | GetThingShadow |
#      | ThingName        | MyThing2       |
#      | ShadowName       | CLASSIC        |
#      | ShadowDocument   |                |
#      | OperationTimeout | <timeout>      |
#    Then I get at least 1 assertions with context "No shadow found" within 15 seconds
#
#    Examples:
#      | strategy | timeout |
#      | realTime | 5       |
#
#    @gamma
#    Examples:
#      | strategy | timeout |
#      | periodic | 30      |
#
#  @stable @functional @JavaSDK
#  Scenario Outline: Shadow-2-T7-<strategy>: As a developer, I do not sync a local named shadow to the cloud that is not in the sync configuration.
#    When I create a deployment configuration for deployment DeploymentShadow-1 with components
#      | aws.greengrass.Nucleus       | LATEST |
#      | aws.greengrass.ShadowManager | LATEST |
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.ShadowManager" version "LATEST" configuration:
#        """
#        {
#           "MERGE":{
#              "strategy":{
#                 "type":"<strategy>",
#                 "delay":5
#              },
#              "synchronize":{
#                 "shadowDocuments":[
#                    {
#                       "thingName":"${MyThing}",
#                       "classic":false,
#                       "namedShadows":[
#                          "${MyThingNamedShadow}"
#                       ]
#                    },
#                    {
#                       "thingName":"${MyThing2}",
#                       "classic":true
#                    }
#                 ],
#                 "provideSyncStatus":true,
#                 "maxOutboundSyncUpdatesPerSecond":50
#              },
#              "shadowDocumentSizeLimitBytes":8192,
#              "maxDiskUtilizationMegaBytes":16
#           }
#        }
#        """
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.Nucleus" version "LATEST" configuration:
#        """
#        {"MERGE":{"logging": {"level": "DEBUG"}}}
#        """
#    And I deploy the configuration for deployment DeploymentShadow-1
#    Then the deployment DeploymentShadow-1 completes with SUCCEEDED within 240 seconds
#    When I install the component ShadowComponentPing version 0.0.0 from local store within 30 seconds with configuration using the variables
#      | key                    | value                                                                                                          |
#      | assertionServerPort    | ${assertionServerPort}                                                                                         |
#      | Operation              | UpdateThingShadow                                                                                              |
#      | ThingName              | MyThing                                                                                                        |
#      | ShadowName             | CLASSIC                                                                                                        |
#      | ShadowDocument         | {\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}               |
#      | ExpectedShadowDocument | {\"version\":1,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}} |
#      | OperationTimeout       | <timeout>                                                                                                      |
#    Then I get 1 assertions with context "Updated shadow document" within 15 seconds
#    And I can not get cloud shadow for MyThing within 30 seconds
#
#    Examples:
#      | strategy | timeout |
#      | realTime | 5       |
#
#    @gamma
#    Examples:
#      | strategy | timeout |
#      | periodic | 30      |
#
#  @stable @functional @JavaSDK
#  Scenario Outline: Shadow-2-T8-<strategy>: As a developer, I do not sync a cloud named shadow to the cloud that is not in the sync configuration.
#    When I create a deployment configuration for deployment DeploymentShadow-1 with components
#      | aws.greengrass.Nucleus       | LATEST |
#      | aws.greengrass.ShadowManager | LATEST |
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.ShadowManager" version "LATEST" configuration:
#        """
#        {
#           "MERGE":{
#              "strategy":{
#                 "type":"<strategy>",
#                 "delay":5
#              },
#              "synchronize":{
#                 "shadowDocuments":[
#                    {
#                       "thingName":"${MyThing}",
#                       "classic":false,
#                       "namedShadows":[
#                          "${MyThingNamedShadow}"
#                       ]
#                    },
#                    {
#                       "thingName":"${MyThing2}",
#                       "classic":true
#                    }
#                 ],
#                 "provideSyncStatus":true,
#                 "maxOutboundSyncUpdatesPerSecond":50
#              },
#              "shadowDocumentSizeLimitBytes":8192,
#              "maxDiskUtilizationMegaBytes":16
#           }
#        }
#        """
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.Nucleus" version "LATEST" configuration:
#        """
#        {"MERGE":{"logging": {"level": "DEBUG"}}}
#        """
#    And I deploy the configuration for deployment DeploymentShadow-1
#    Then the deployment DeploymentShadow-1 completes with SUCCEEDED within 240 seconds
#    When the greengrass log eventually contains the following patterns within 30 seconds
#      | Subscribed to \$aws\/things\/MyThinge2e* | 1 | Should have subscribed to the shadow topics for thing |
#    When I can create cloud shadow for MyThing with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}}
#    Then I can get cloud shadow for MyThing with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}} within 30 seconds
#    When I install the component ShadowComponentPing version 0.0.0 from local store within 30 seconds with configuration using the variables
#      | key                 | value                  |
#      | assertionServerPort | ${assertionServerPort} |
#      | Operation           | GetThingShadow         |
#      | ThingName           | MyThing                |
#      | ShadowName          | CLASSIC                |
#      | ShadowDocument      |                        |
#      | OperationTimeout    | <timeout>              |
#    Then I get at least 1 assertions with context "No shadow found" within 15 seconds
#
#    Examples:
#      | strategy | timeout |
#      | realTime | 5       |
#
#    @gamma
#    Examples:
#      | strategy | timeout |
#      | periodic | 30      |
#
#  @stable @functional @JavaSDK
#  Scenario Outline: Shadow-2-T9-<strategy>: As a developer, I can sync a cloud named shadow to the local which was previously deleted.
#    When I create a deployment configuration for deployment DeploymentShadow-1 with components
#      | aws.greengrass.Nucleus       | LATEST |
#      | aws.greengrass.ShadowManager | LATEST |
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.ShadowManager" version "LATEST" configuration:
#        """
#        {
#           "MERGE":{
#              "strategy":{
#                 "type":"<strategy>",
#                 "delay":5
#              },
#              "synchronize":{
#                 "shadowDocuments":[
#                    {
#                       "thingName":"${MyThing}",
#                       "classic":false,
#                       "namedShadows":[
#                          "${MyThingNamedShadow}"
#                       ]
#                    },
#                    {
#                       "thingName":"${MyThing2}",
#                       "classic":true
#                    }
#                 ],
#                 "provideSyncStatus":true,
#                 "maxOutboundSyncUpdatesPerSecond":50
#              },
#              "shadowDocumentSizeLimitBytes":8192,
#              "maxDiskUtilizationMegaBytes":16
#           }
#        }
#        """
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.Nucleus" version "LATEST" configuration:
#        """
#        {"MERGE":{"logging": {"level": "DEBUG"}}}
#        """
#    And I deploy the configuration for deployment DeploymentShadow-1
#    Then the deployment DeploymentShadow-1 completes with SUCCEEDED within 240 seconds
#    When the greengrass log eventually contains the following patterns within 30 seconds
#      | Subscribed to \$aws\/things\/MyThinge2e* | 1 | Should have subscribed to the shadow topics for thing |
#    When I can create cloud shadow for MyThing2 with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}}
#    Then I can get cloud shadow for MyThing2 with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}} within 30 seconds
#    When I install the component ShadowComponentPing version 0.0.0 from local store within 30 seconds with configuration using the variables
#      | key              | value                                                                                                          |
#      | Operation        | GetThingShadow                                                                                                 |
#      | ThingName        | MyThing2                                                                                                       |
#      | ShadowName       | CLASSIC                                                                                                        |
#      | ShadowDocument   | {\"version\":1,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}} |
#      | OperationTimeout | <timeout>                                                                                                      |
#    Then I get 1 assertions with context "Retrieved matching shadow document" within 15 seconds
#    When I delete cloud shadow for MyThing2
#    And I can not get cloud shadow for MyThing2 within 30 seconds
#    Then I update the component ShadowComponentPing with parameter configuration
#      | key            | value          |
#      | Operation      | GetThingShadow |
#      | ThingName      | MyThing2       |
#      | ShadowName     | CLASSIC        |
#      | ShadowDocument |                |
#    And I get at least 1 assertions with context "No shadow found" within 15 seconds
#    And I clear the assertions
#    When I can create cloud shadow for MyThing2 with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}}
#    # Make sure that the cloud shadow exists after the create/update shadow operation is completed.
#    Then I can get cloud shadow for MyThing2 with version 3 and state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}} within 30 seconds
#    When I install the component ShadowComponentPing version 0.0.0 from local store within 30 seconds with configuration using the variables
#      | key              | value                                                                                                          |
#      | Operation        | GetThingShadow                                                                                                 |
#      | ThingName        | MyThing2                                                                                                       |
#      | ShadowName       | CLASSIC                                                                                                        |
#      | ShadowDocument   | {\"version\":1,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}} |
#      | OperationTimeout | <timeout>                                                                                                      |
#    Then I get 1 assertions with context "Retrieved matching shadow document" within 15 seconds
#    # Check to make sure that the cloud shadow exists after the sync as well.
#    And I can get cloud shadow for MyThing2 with version 3 and state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}} within 30 seconds
#
#    Examples:
#      | strategy | timeout |
#      | realTime | 5       |
#
#    @gamma
#    Examples:
#      | strategy | timeout |
#      | periodic | 30      |
#
#    @gamma @functional @JavaSDK @smoke
#    Scenario: Shadow-2-T10: As a customer, I can configure shadow manager to subscribe to more than 25 shadows to sync.
#    When I add random shadow for MyThing with name MyThingNamedShadow in context
#    And I create an empty deployment configuration for deployment DeploymentShadow-1
#    And I update the deployment configuration DeploymentShadow-1, setting the shadow component version "LATEST" configuration with 30 named shadows with prefix MyThingNamedShadow per 1 things with prefix MyThing:
#        """
#        {
#          "MERGE": {
#            "synchronize":{
#              "shadowDocuments": processedShadowDocuments
#            }
#          }
#        }
#        """
#    And I update the deployment configuration DeploymentShadow-1, setting the component "aws.greengrass.Nucleus" version "LATEST" configuration:
#      """
#      {
#        "MERGE": {
#          "logging": {
#             "level": "DEBUG"
#           }
#        }
#      }
#      """
#    And I deploy the configuration for deployment DeploymentShadow-1
#    Then the deployment DeploymentShadow-1 completes with SUCCEEDED within 180 seconds
#    And the greengrass log eventually contains the following patterns within 60 seconds
#      | Successfully subscribed to topic(.*?)\$aws\/things\/(MyThing(.*?))\/shadow(\/name\/(MyThingNamedShadow(.*?)))?\/(update\|delete\|get)\/(accepted\|rejected\|delta\|documents) | 60 | should subscribe to 60 MQTT subscriptions |
