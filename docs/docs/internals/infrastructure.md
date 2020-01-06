#Mantis Infrastructure Overview
![High Level Architecture](../images/MantisArchitecture.png)

At a high level the Mantis Architecture consists of the Control plane (Mantis Master) and the
Mantis runtime. The Master is responsible for managing the life cycle of Jobs, including
creation, scheduling and deletion of jobs. The Mantis runtime is responsible for the actual execution 
of Mantis jobs.
 
## Major components
Here is a brief description of the major components of the Mantis Architecture

### Mantis control plane (Master)

The [Control plane](https://github.com/Netflix/mantis-control-plane) plane is responsible for managing the lifecycle of Mantis Jobs. 
Metadata related to the jobs is stored in the Metadata store. The Open source version of Mantis
ships with a simple file based store. For production use of a highly available store is recommended.

The default resource manager for scheduling Job tasks is Mesos. The control plane registers itself
as a framework into Mesos. The control plane uses [Fenzo](https://github.com/Netflix/Fenzo/) to match the job
to the available Mesos offers in an optimal manner.

### Mantis Runtime

Mantis jobs are executed on Mesos agent(s) as one or more tasks (Job Workers) based on the job
topology. The [Mantis runtime](https://github.com/Netflix/mantis) is responsible for the actual execution of the job. This includes
bootstrapping user code, exchanging control messages with the Master and establishing/maintaining upstream and downstream 
network connections per the Job execution DAG. 
One of the unique features of Mantis is that it allows Mantis Jobs to discover other Mantis Jobs and 
stream results of one job as input to another. The Mantis runtime does the work of discovering, 
connecting and maintaining these intra-job network flows.

### Mantis API

[Mantis API](https://github.com/Netflix/mantis-api) provides an easy REST interface for interacting 
with the Mantis system. It allows users to create/submit/kill Mantis Jobs and Mantis Job Clusters.
Additionally, it also allows users to stream the output of any running jobs via WebSocket or SSE.

### Zookeeper

Mantis usage of [Zookeeper](https://zookeeper.apache.org/) is fairly minimum. The zookeeper is used primarily for Master leader election
both by the Mantis Control Plane and the Mesos Master. 

## External Dependencies

***[Zookeeper](https://zookeeper.apache.org/)*** : Used for leader election of Mantis and Mesos
Masters.

***[RxJava 1.x](https://github.com/ReactiveX/RxJava)*** : Provides the fluent, functional programming
model for Mantis Jobs.

***[Mesos 1.x](https://mesos.apache.org/)*** : Powers the underlying resource management system.

***[RxNetty 0.4](https://github.com/ReactiveX/RxNetty)*** : Used for the intra-task network communication.
