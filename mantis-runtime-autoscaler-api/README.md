# Mantis Runtime Autoscaler API

This module contains the API interfaces and classes for the Mantis autoscaling system. It was created to break circular dependencies between the `mantis-runtime-executor` module and lower-level modules like `mantis-runtime` and `mantis-runtime-loader`.

'mantis-jm-akka' module reference this api as 'compileOnly' while it's loaded in separate loader to isolate akka/scala dependencies.

