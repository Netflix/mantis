# Project ideas for Google Summer of Code 2023

Here is a list of project ideas you can contribute to as part of the Google Summer of Code (GSoC) 2023 program!

## Mantis in a process for local testing

### Abstract
Running Mantis jobs locally currently requires running multiple core components like Zookeeper,
Mantis Master, and Mantis Task Executors, which involves complex configurations and multiple
independent processes. This makes it challenging for developers to test and debug their Mantis jobs
before submitting them to production clusters. By bringing all core components of Mantis within the
same JVM, developers can test and debug their applications more easily without the need to set up a
full cluster.

### Deliverables
- Mantis MiniCluster pluggable and usable in tests.
- Documentation on how to setup/use MiniCluster.
- Migrating N mantis examples to use the MiniCluster and validate the functionality.

### Required SkillSet
- Java

### Skills that you will gain
- You will learn about building distributed systems as part of this project.
- You will learn about you can leverage actor frameworks such as Akka to write safe, high performance code.
- You will also learn a lot about stream processing and how Mantis leverages stream processing to analyze operational data.

### Mentors
- [Sundaram Ananthanarayanan](https://www.linkedin.com/in/sundaram-ananthanarayanan-97b8b545/)

### Difficulty rating
Medium

## Mantis Connection Monitor

### Abstract
Mantis is a highly reliable and scalable distributed systems platform, but there are instances where
the callbacks on life cycle events such as channel inactive and exception are not triggered.
This can lead to Mantis still thinking a connection is live even after it has been terminated by
the client. To address this challenge, the aim of this project is to create a connection monitor
within Mantis that tracks the connection status. This monitor will run in the background and
periodically check the state of each connection. If it detects that a connection is no longer
writable but Mantis has not triggered an inactive callback, it will force close the connection,
ensuring the stability and reliability of the system.

### Expected outcomes
- The connection monitor will be implemented using the Singleton design pattern, ensuring only one instance of the monitor is created in the system.
- The monitor will use a periodic checking mechanism to evaluate the state of each connection.
- If the monitor detects a connection that is no longer writable but Mantis has not triggered an inactive callback, it will force close the connection.
- The monitor will log extensive metrics to observe the severity and trends of the issue, providing valuable insights for future improvements.
- The monitor will undergo extensive testing to verify its reliability and robustness within the Mantis system.

### Recommended skills
Java, RxJava, RxNetty

### Mentors
- [Calvin Cheung](https://www.linkedin.com/in/cal681/)
- [Sundaram Ananthanarayanan](https://www.linkedin.com/in/sundaram-ananthanarayanan-97b8b545/)
- [Harshit Mittal](https://www.linkedin.com/in/harshitmittal/)

### Difficulty rating
Hard

## Capability to publish to Mantis streams from non-JVM languages

### Abstract
Develop a new specification for selecting Mantis-Realtime-Events (MRE) from applications using a JSON or protobuf format with native support in Python and Node.js. Provide webassembly support for other languages as a fallback option.

### Context
Applications and web services use the mantis-publish library to create Mantis Real-time Events (MRE) data as requests and responses. These events are triggered based on user-defined criteria specified using the Mantis Query Language (MQL), a SQL-like interface implemented in Clojure. However, the use of a JVM-based language makes it challenging to port MQL to non-JVM languages like Python and Go.

The mantis-publish library only requires a simplified version of MQL, known as Simplified-MQL, which includes projections, sampling, and filters. To address the language compatibility issue, we want to define a language-agnostic interface using either JSON or Protocol Buffers for this Simplified-MQL. This will enable support for other programming languages.


### Goals for project
1. Define a new spec to specify filter, projection, and sample criteria currently supported by MQL. Document the new spec.
2. Implement the spec in python, golang, or rust with unit-tests.
3. Write a parser to convert simplified-MQL to the spec.
4. Replicate mantis-publish java library in that programming language

### Mentors
- [Sundaram Ananthanarayanan](https://www.linkedin.com/in/sundaram-ananthanarayanan-97b8b545/)
- [Harshit Mittal](https://www.linkedin.com/in/harshitmittal/)

### Difficulty rating
Hard

## Resource Cluster Management User Interface

### Abstract
Develop a user interface to allow Mantis admins to manage resource clusters and allow end-users to select resource clusters to run their jobs on.

### Deliverables
- Full support for CRUD operations on resource clusters
- Support for specifying scaling operations and upgrading clusters
- Allow users to select a resource cluster before submitting a job and a container SKU per job stage

### Required SkillSet
-   Javascript, HTML, CSS
-   Experience using a front end framework such as React, Angular, Vue

### Skills that you will gain
-   You will learn about how to create a UI feature from design, implementation, and testing

### Mentors
- [Santosh Kalidindi](https://www.linkedin.com/in/santosh-kalidindi/)

### Difficulty rating
Medium

## User Interface for Querying Real-time Streaming Data on Mantis

### Abstract
Mantis provides a real-time streaming platform for processing data at scale. However, there is currently no way for a user to query their data and show the results in a user friendly way. This project aims to develop a UI that allows users to query their data in Mantis and visualize the results.

### Deliverables
- New Query page featuring a query editor to allow users to write queries
- Page to list of recent queries that the user has previously submitted
- Page to list and create recipes which are saved templates for queries

### Required SkillSet
- Javascript, HTML, CSS
- Experience using a front end framework such as React, Angular, Vue

### Skills that you will gain
- You will learn about how to create a UI feature from design, implementation, and testing

### Mentors
- [Santosh Kalidindi](https://www.linkedin.com/in/santosh-kalidindi/)

### Difficulty rating
Hard

## Support custom serializers Mantis DSL (mantis-runtime/io.mantisrx.runtime.core)

### Abstract
Mantis is adopting a new interface for writing mantis jobs. We are calling it mantis-dsl and code can be [found here](https://github.com/Netflix/mantis/tree/master/mantis-runtime/src/main/java/io/mantisrx/runtime/core). Because of it's nascency, the new interface only supports (de)serializing objects using the default java serializer. It'd be nice to add support for more serialization techniques.

A starting point could be using kryo which is a popular (de)serializer library that supports dynamic discovery of many encoding formats like protobufs, json, custom.

### Deliverables
- Add support for efficient serialization libraries in mantis-dsl
- Support encoding java objects using json, protobuf, and custom

### Required SkillSet
- Java, Familiarity with object encoding, json and protobuf formats.

### Skills that you will gain
- Work on an open-source software used in production at scale
- Learn about serializations, object encoding

### Mentors
- [Harshit Mittal](https://www.linkedin.com/in/harshitmittal/)

### Difficulty rating
Medium

## Refactor Metrics Interface/Implementation in Mantis

### Abstract
Mantis currently utilizes its own metrics interfaces and implementation in various components
such as the Runtime, Control Plane, and Task Executor, with the relevant classes residing under the
`io.mantisrx.common.metrics` package. While this has been effective in the past, it may be advisable
to replace this library with specialized OSS libraries for metrics, such as micrometer, moving
forward.

Making such a transition can help address several issues that exist in the current metrics library.
Firstly, it is challenging to support other time-series databases without significant effort, as
one must create a MetricsPublisher implementation. Additionally, the current implementation is
dependent on spectator-api, further complicating the system. Finally, maintaining a metrics
implementation that is not integral to Mantis itself can add unnecessary complexity.

By adopting a more standardized and OSS option like micrometer, we can eliminate all these issues.

### Deliverables
- Proposal that evaluates various options in this space and suggests a solution based on our design goals.
- Getting rid of the existing implementation and cleaning up the spectator usage in various components.

### Required SkillSet
- Java

### Skills that you will gain
- You will learn about how metrics is implemented in Java microservices.
- You will learn about how to evaluate different solutions within a given design space.

### Mentors
- [Sundaram Ananthanarayanan](https://www.linkedin.com/in/sundaram-ananthanarayanan-97b8b545/)

### Difficulty rating
Medium

[//]: # (contributors:)
[//]: # (    hmittal: [Harshit Mittal]&#40;https://www.linkedin.com/in/harshitmittal/&#41;)
[//]: # (    sundaram: [Sundaram Ananthanarayanan]&#40;https://www.linkedin.com/in/sundaram-ananthanarayanan-97b8b545/&#41;)
[//]: # (    ccheung: [Calvin Cheung]&#40;https://www.linkedin.com/in/cal681/&#41;)
[//]: # (    santosh: [Santosh Kalidindi]&#40;https://www.linkedin.com/in/santosh-kalidindi/&#41;)
