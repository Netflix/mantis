# mantis-rxcontrol

## Design

### PID

### Clutch
Clutch is an autoscaling domain specific implementation for scaling
stateless systems. Initially Clutch was designed to autoscale Mantis
Kafka source jobs

#### Metric
`Metric` is an enum contained within the `Clutch` class allowing various
systems to map their resource measurements into something Clutch understands.

#### Event
A pair of `Metric` and `Double` representing a measurement taken from the
target system. For example `new Event(Metric.CPU, 78.2)` represents a measurement
of 78.2% CPU usage. The Clutch systems will operate on an `Observable<Event>`
stream.

#### ClutchConfigurator
The ClutchConfigurator class within the `io.mantisrx.control.clutch` namespace is
responsible for consuming an `Observable<Event>` and producing an appropriate
`ClutchConfiguration` instance for the taget system. Currently the configurator
performs the following tasks;

* Determines the appropriate metric for autoscaling from the set CPU, Memory, Network.
* Determines the actual range of resource usage and autoscales using those as min/max.

In the near future this class will also be responsible for;

* Adjusting configuration based on lag/drops.
* Adjusting configuration based on oscillation.

#### Clutch
`Clutch` contained within the `io.mantisrx.control.Clutch` namespace is a wrapper
around a `PID` controller containing domain specific knowledge / implementations
for the task of autoscaling stateless systems. This class is currently responsible
for;

* Instantiating a ClutchConfigurator.
* Attaching a ControlLoop which executes the autoscaling.

