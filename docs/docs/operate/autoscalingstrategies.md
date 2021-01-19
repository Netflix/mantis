There are various strategies for autoscaling. These strategies affect when an autoscale activity will occur and also how
many workers to scale up/down a stage. They fall into 2 main categories. Rule based strategies monitor a specific resource
and scale up/down when a certain threshold is reached. PID control based strategies pick a resource utilization level
and scale up/down dynamically to maintain that level.

## Rule Based Strategy

Rule based strategy can be defined for the following resources:

| Resource        | Metric  |
| --------------- | ------- |
| `CPU`           | group: `ResourceUsage` name: `cpuPctUsageCurr` aggregation: `AVG`    |
| `Memory`        | group: `ResourceUsage` name: `totMemUsageCurr` aggregation: `AVG`    |
| `Network`       | group: `ResourceUsage` name: `nwBytesUsageCurr` aggregation: `AVG`   |
| `JVMMemory`     | group: `ResourceUsage` name: `jvmMemoryUsedBytes` aggregation: `AVG` |
| `DataDrop`      | group: `DataDrop` name: `dropCount` aggregation: `AVG` |
| `KafkaLag`      | group: `consumer-fetch-manager-metrics` name: `records-lag-max` aggregation: `MAX` |
| `KafkaProcessed` | group: `consumer-fetch-manager-metrics` name: `records-consumed-rate` aggregation: `AVG` |
| `UserDefined`   | Metric is defined by user with job parameter `mantis.jobmaster.autoscale.metric` in this format `{group}::{name}::{aggregation}`. |

Each strategy has the following parameters:

| Name            | Description  |
| --------------- | ------------ |
| `Scale down below percentage` | When the aggregated value for all workers falls below this value, the stage will scale down. It will scale down by the decrement value specified in the policy. For data drop, this is calculated as the number of data items dropped divided by the total number of data items, dropped+processed. For CPU, Memory, etc., it is calculated as a percentage of allocated resource when you defined the worker. |
| `Scale up above percentage`   | When the aggregated value for all workers rises above this value, the stage will scale up. |
| `Rolling count`               | This value helps to keep jitter out of the autoscaling process. Instead of scaling immediately the first time values fall outside of the scale-down and scale-up percentage thresholds you define, Mantis will wait until the thresholds are exceeded a certain number of times within a certain window. For example, a rolling count of “6 of 10” means that only if in ten consecutive observations six or more of the observations fall below the scale-down threshold will the stage be scaled down. |

It is possible to employ multiple rule based strategies for a stage. In this case, as soon as 1 strategy triggers a
scaling action, the cooldown will prevent subsequent strategies from scaling for that duration.

!!! note
    Ideally, there should be zero data drop, so there isn’t an elegant way to express “scale down
    below percentage” for data drop. Specifying “0%” as the “scale down below percentage”
    effectively means the data drop percentage never trigger a scale down. For this reason, it is
    best to use the data drop strategy in conjunction with another strategy that provides the
    scale-down trigger.

## PID Control Based Strategy

PID control system uses a continuous feedback loop to maintain a signal at a target level (set point). Mantis offers variations of
this strategy that operates on different signals. Additionally, they try to learn the appropriate target over time
without the need for user input. The PID controller computes the magnitude of scale up/down based on the drift between
the observed signal and the target. Thus, this strategy can react quicker to big changes compared to rule based strategies,
since rule based strategies use fixed step size. Cooldown still applies between scaling activities.

### Clutch

The strategy operates on CPU, Memory, Network and UserDefined. Every 24 hours, it will pick 1 dominant resource and use the
P99 value as the target set point. For the next 24 hours, it will monitor that resource metric and scale the stage to
keep the metric close to the target set point. In the initial 24 hours after the job is first launched, this strategy will
scale the stage to max in order to learn the first dominant resource and set point. This also happens if the job is restarted.

### Clutch with User Defined Configs

With this strategy, the user defines the target for each resource without relying on the system to learn it. There is
no need for an initial 24 hour pin high period, the PID controller can start working right away. You can supply the configuration as
JSON in the job parameter `mantis.jobmaster.clutch.config`. Example:

```json
{
  "minSize": 3,
  "maxSize": 25,
  "cooldownSeconds": 300,
  "rps": 8000,
  "cpu": {
      "setPoint": 60.0,
      "rope": [25.0, 0.0],
      "kp": 0.01,
      "kd": 0.01
  },
  "memory": {
      "setPoint": 100.0,
      "rope": [0.0, 0.0],
      "kp": 0.01,
      "kd": 0.01
  },
  "network": {
      "setPoint": 60.0,
      "rope": [25.0, 0.0],
      "kp": 0.01,
      "kd": 0.01
  }
}
```

| Field           | Description  |
| --------------- | ------------ |
| `minSize`       | Minimum number of workers in the stage. It will not scale down below this number. |
| `maxSize`       | Maximum number of workers in the stage. It will not scale up above this number. |
| `cooldownSeconds` | This indicates how many seconds to wait after a scaling operation has been completed before beginning another scaling operation. |
| `maxAdjustment` | Optional. The maximum number of workers to scale up/down in a single operation. |
| `rps`           | Expected RPS per worker. Must be > 0. |
| `cpu`, `memory`, `network` | Configure PID controller for each resource. |
| `setPoint`      | Target set point for the resource. This is expressed as a percentage of allocated resource to the worker. For example, `60.0` on `network` means network bytes should be 60% of the network limit on machine definition. |
| `rope`          | Lower and upper buffer around the set point. Metric values within this buffer are assumed to be at set point, and thus contributes an error of 0 to the PID controller. |
| `kp`            | Multiplier for the proportional term of the PID controller. This will affect the size of scaling actions. |
| `kd`            | Multiplier for the derivative term of the PID controller. This will affect the size of scaling actions. |

### Clutch RPS

This strategy scales the stage base on number of events processed. The target set point is a percentile of RPS. The signal
is the sum of RPS, inbound drops, Kafka lag, and outbound drops from source jobs. Therefore, it effectively tries to keep
drops and lag at 0. It takes the first 10 minutes after job launch to learn the first RPS set point. This also applies if
the job is restarted, the set point does not carry over. Afterwards, it may adjust the set point once every hour. Set point
should become stable the longer a job runs, since it simply takes a percentile of historical RPS metric.

The source job drop metric is not enabled by default. It is only applicable if your job connects to an upstream job as
input. You can enable this metric by setting the job parameter `mantis.jobmaster.autoscale.sourcejob.metric.enabled` to true.
Further, you need to specify the source job targets in the job parameter `mantis.jobmaster.autoscale.sourcejob.target`.
You can omit this if your job already has a `target` parameter for connecting to source jobs, the auto scaler will pick
that up automatically. Example:

```json
{
  "targets": [
    {
      "sourceJobName": "ConsolidatedLoggingEventKafkaSource"
    }
  ]
}
```

Optionally, it is possible to further customize the behavior of the PID controller. You can supply the configuration as
JSON in the job parameter `mantis.jobmaster.clutch.config`. Example:

```json
{
  "rpsConfig": {
    "setPointPercentile": 50.0,
    "rope": [30.0, 0.0],
    "scaleDownBelowPct": 40.0,
    "scaleUpAbovePct": 10.0,
    "scaleDownMultiplier": 0.5,
    "scaleDownMultiplier": 3.0
  }
}
```

| Field                | Description  |
| -------------------- | ------------ |
| `setPointPercentile` | Percentile of historical RPS metric to use as the set point. Valid input is between `[1.0, 100.0]` Default is `75.0`. |
| `rope`               | Lower and upper buffer around the set point. The value is interpreted as percentage of set point. For example, `[30.0, 30.0]` means values within 30% of set point is considered to have 0 error. Valid input is between `[0.0, 100.0]` Default is `[30.0, 0.0]`. |
| `scaleDownBelowPct`  | Only scale down if the PID controller output is below this number. It can be used to delay a scaling action. Valid input is between `[0.0, 100.0]`. Default is `0.0`. |
| `scaleUpAbovePct`    | Only scale up if the PID controller output is above this number. It can be used to delay a scaling action. Valid input is between `[0.0, 100.0]`. Default is `0.0`. |
| `scaleDownMultiplier` | Artificially increase/decrease the size of scale down by this factor. Default is `1.0`. |
| `scaleUpMultiplier`  | Artificially increase/decrease the size of scale up by this factor. Default is `1.0`. |

### Clutch Experimental (Developmental Use Only)

This strategy is internally used for testing new Clutch implementations. It should not be used for production jobs.
