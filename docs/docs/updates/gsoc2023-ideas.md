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
- Calvin Cheung
- Sundaram Ananthanarayanan
- Harshit Mittal

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

### Difficulty rating
Hard
