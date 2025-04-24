# Mantis Runtime Autoscaler API

This module contains the API interfaces and classes for the Mantis autoscaling system. It was created to break circular dependencies between the `mantis-runtime-executor` module and lower-level modules like `mantis-runtime` and `mantis-runtime-loader`.

## Key Components

- `JobScalerContext`: Context object that holds all the necessary information for job autoscaling
- `JobAutoScalerService`: Interface for job autoscaler services
- `JobAutoscalerManager`: Interface for job autoscaler managers
- `AutoScaleMetricsConfig`: Configuration for autoscaling metrics
- `RuleUtils`: Utility methods for autoscaling rules

## Purpose

This module allows lower-level modules to reference autoscaling interfaces and context objects without creating circular dependencies. It serves as a bridge between the runtime modules and the executor module.
