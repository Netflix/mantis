# Dynamic Job Scaling Rules Implementation

## Problem Statement

In order to effectively manage job scaling in various scenarios, particularly during periods of bursty traffic, there is a need to dynamically update job auto scaler settings such as minimum, maximum, step-size, and burst-change. The goal is to consolidate the APIs required to modify a job's auto scaler and to allow changes to the job scaler without necessitating job resubmission.

## Solution

This pull request introduces support for dynamic job scaling rules that override the default job scaler configuration. It allows multiple rules to coexist using different modes, providing flexibility in handling diverse scaling requirements.

### Key Features:

- **Dynamic Scaling Rules**: Introduce the ability to define and manage multiple scaling rules that can override the default job scaler settings.
- **Rule Types**: Support for three types of rules:
    - **Perpetual**: Continuous scaling based on predefined conditions.
    - **Schedule**: Time-based scaling using cron expressions. This schedule can a one time trigger or recurring event.
    - **Custom**: User-defined rules where users can implement and inject the `JobScalingRuleCustomTrigger` interface to customize the triggering conditions and actions.
- **Coexistence of Rules**: Multiple rules can coexist, allowing for complex scaling strategies tailored to specific needs.

## Internals

- **Actor System**: The Job Manager (JM) will now host an actor system to represent different rules and control the job scaler state.
- **Rule Priority**: Rules are prioritized based on their `ruleId`, with newer rules taking precedence over older ones.
- **Subscription to Rule Changes**: The JM subscribes to a new `/jobScalerRules/` stream API to listen for rule changes, ensuring that the system is responsive to updates.
- **Rule Activation/Deactivation**: Rules have the capability to activate or deactivate themselves based on the defined conditions.
- **Default Rule Handling**: The default job configuration's `schedulingInfo` scaler config is treated as the default rule with an ID of "-1".

This implementation provides a robust framework for managing job scaling dynamically, enhancing the system's ability to handle varying traffic patterns efficiently. The introduction of multiple rule types and the ability to customize scaling behavior offers significant flexibility and control to users.
# API Reference for Job Cluster Scaler Rules

This document provides details on the APIs available under `api/v1/jobClusters/{}/scalerRules`. These APIs allow you to manage scaler rules for job clusters, including creating, retrieving, and deleting scaler rules.

## Endpoints

### 1. Create Scaler Rule

- **Endpoint:** `POST /api/v1/jobClusters/{clusterName}/scalerRules`
- **Description:** Creates a new scaler rule for the specified job cluster.

#### Request Schema

- **jobClusterName** (String): The name of the job cluster.
- **scalerConfig** (ScalerConfig): Configuration for the scaler.
    - **type** (String): Type of scaling policy. Only supports "standard" at the moment.
    - **scalingPolicies** (List of StageScalingPolicy): List of scaling policies. See StageScalingPolicy reference for details.
    - **stageDesireSize** (Map<Integer, Integer>): Desired size for each stage when the rule gets activated.
- **triggerConfig** (TriggerConfig): Configuration for the trigger.
    - **triggerType** (String): Type of trigger (e.g., "schedule", "perpetual", "custom").
    - **scheduleCron** (String): Cron expression for scheduling. It can be a recurring schedule or a one-time trigger.
    - **scheduleDuration** (String): [Optional] Duration for the schedule to be effective. Will trigger deactivation when the duration expires.
    - **customTrigger** (String): Custom trigger configuration.
- **metadata** (Map<String, String>): Additional metadata for the rule.

#### Sample Request Payload

```json
{
  "jobClusterName": "exampleCluster",
  "scalerConfig": {
    "type": "standard",
    "scalingPolicies": [
      {
        "stage": 1,
        "min": 1,
        "max": 5,
        "increment": 1,
        "decrement": 1,
        "coolDownSecs": 300,
        "strategies": {
          "CPU": {
            "reason": "CPU",
            "scaleDownBelowPct": 20.0,
            "scaleUpAbovePct": 80.0,
            "rollingCount": {
              "count": 3,
              "of": 5
            }
          }
        },
        "allowAutoScaleManager": true
      }
    ],
    "stageDesireSize": {
      "1": 3
    }
  },
  "triggerConfig": {
    "triggerType": "schedule",
    "scheduleCron": "0 0 * * *",
    "scheduleDuration": "1h",
    "customTrigger": "customLogic"
  },
  "metadata": {
    "createdBy": "admin"
  }
}
```

#### Response Schema

- **ruleId** (String): The unique identifier for the created scaler rule.

### 2. Get Scaler Rules

- **Endpoint:** `GET /api/v1/jobClusters/{clusterName}/scalerRules`
- **Description:** Retrieves all scaler rules for the specified job cluster.

#### Request Schema

- **jobClusterName** (String): The name of the job cluster.

#### Response Schema

- **rules** (List of JobScalingRule): List of scaler rules.
  - **ruleId** (String): Unique identifier for the rule.
  - **scalerConfig** (ScalerConfig): Configuration for the scaler.
  - **triggerConfig** (TriggerConfig): Configuration for the trigger.
  - **metadata** (Map<String, String>): Additional metadata for the rule.

### 3. Delete Scaler Rule

- **Endpoint:** `DELETE /api/v1/jobClusters/{clusterName}/scalerRules/{ruleId}`
- **Description:** Deletes a specific scaler rule from the specified job cluster.

#### Request Schema

- **jobClusterName** (String): The name of the job cluster.
- **ruleId** (String): The unique identifier of the scaler rule to be deleted.

#### Response Schema

- No content is returned upon successful deletion.

This documentation provides a comprehensive overview of the available APIs for managing scaler rules in job clusters. For further details or assistance, please refer to the official API documentation or contact support.
