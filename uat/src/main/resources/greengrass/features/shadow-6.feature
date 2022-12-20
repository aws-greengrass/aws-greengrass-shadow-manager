@ShadowManager @Shadow6
Feature: Shadow-6

  As a customer I can reset the default fields and merge it back again and it should work correctly.

  Background:
    Given my device is registered as a Thing
    And my device is running Greengrass

  @stable @functional
  Scenario: Shadow-6-T1: As a customer I can reset the strategy field and merge it back again and it should work correctly.
    When I create a Greengrass deployment with components
      | aws.greengrass.ShadowManager | LATEST |
      | aws.greengrass.Cli           | LATEST |
    And I update my Greengrass deployment configuration, setting the component aws.greengrass.ShadowManager configuration to:
      """
      {
         "MERGE":{
            "strategy":{
               "type":"periodic",
               "delay":30
            },
            "synchronize":{
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
    When I update my Greengrass deployment configuration, setting the component aws.greengrass.ShadowManager configuration to:
      """
      {"MERGE":{"strategy":{"type":"realtime"}}}
      """
    And I deploy the Greengrass deployment configuration
    Then the Greengrass deployment is COMPLETED on the device after 2 minutes
    Then I verify the aws.greengrass.ShadowManager component is RUNNING using the greengrass-cli
    When I update my Greengrass deployment configuration, setting the component aws.greengrass.ShadowManager configuration to:
      """
      {
        "MERGE":{
          "strategy":{
            "type":"periodic",
            "delay":30
          },
          "synchronize":{
            "maxOutboundSyncUpdatesPerSecond":50
          },
          "shadowDocumentSizeLimitBytes":8192,
          "maxDiskUtilizationMegaBytes":16
        }
      }
      """
    And I deploy the Greengrass deployment configuration
    Then the Greengrass deployment is COMPLETED on the device after 2 minutes
    Then I verify the aws.greengrass.ShadowManager component is RUNNING using the greengrass-cli