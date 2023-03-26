# Mantis Use Cases

Here is a non-comprehensive list of use-cases powered by Mantis to give you an idea of the type of
applications that can be built on Mantis.

## Realtime monitoring of Netflix streaming health

At Netflix Stream Starts per Second (SPS) is a key metric used to track the health of the Netflix streaming service.
Streaming starts per second tracks the number of people successfully hitting `play` on their streaming devices.
Any abnormal change in the trend of this metric signifies a negative impact on user's viewing experience.

This Mantis application monitors the SPS trend by processing data sourced directly from thousands of Netflix servers (using
the mantis-publish library) in realtime. Using a version of Double Exponential Smoothing (DES) it can detect abnormal deviations in seconds and
alerts key teams at Netflix.

## Contextual Alerting 

As Netflix has grown over the years so has the number of microservices. Getting alerted for outages for your own service
is often not sufficient to root-cause issues. Engineers need to understand what is happening with downstream and upstream services
as well to be able to quickly narrow down the root of the issue.

The [Contextual alerting application](https://www.youtube.com/watch?v=6UwcqiNsZ8U) analyzes millions of interactions between dozens of Netflix microservices in realtime to 
identify anomalies and provide operators with rich and relevant context. 
The realtime nature of these Mantis-backed aggregations allows the Mean-Time-To-Detect to be cut down from tens of minutes to a few seconds. 
Given the scale of Netflix this makes a huge impact.

## Raven

In large distributed systems, often there are cases where a user reports a problem but the overall health of the system
is green. In such cases there is a need to explore events associated with the user/device/service in realtime to find
a smoking gun. With the potential of a user request landing across thousands of servers it is often a laborious task
to find the right servers and inspect their logs.

The [Raven](https://www.youtube.com/watch?v=uODxUJ5Jwis) applications makes this task trivial, The raven jobs work with the mantis-publish library to _look_ for events
matching a certain criterion (user-id/device-id etc) right at the server and stream matching results in realtime.
It provides an intuitive UI that allows SREs to construct and submit simple MQL queries. 

## Cassandra and Elastic Search Health Monitoring

Netflix maintains hundreds of Cassandra and Elastic Search clusters. These clusters are critical for the day to day
operation of Netflix.  

The [Cassandra](https://www.youtube.com/watch?v=w3WbVMavy2I) and Elastic Search health application analyzes rich operational events sent by the Priam side car in realtime to generate a holistic picture 
of the health of every Cassandra cluster at Netflix. Since this system has gone into operation the number of _false pages_ 
has dropped down significantly. 

## Alerting on Logs

The Alerting on Logs application allows users to create alerts which page when a certain pattern is detected within
the application logs. This application analyzes logs from thousands of servers in realtime. 


## Chaos Experimentation monitoring

Chaos Testing is one of the pillars of resilience at Netflix. Dozens of chaos experiments are run daily to test
the resilience of variety of applications. 

The Chaos experimentation application tracks user experience by analyzing
client and server side events during a Chaos exercise in realtime and triggers an abort of the chaos exercise in case of an adverse impact.

## Realtime detection of Personally Identifiable Information (PII) data detection

With trillions of events flowing through Netflix data systems daily it is critical to ensure no sensitive data is
accidentally passed along. 

This application samples data across all streaming sources and applies custom pattern
detection algorithms to identify presence of such data.

## Canary Analysis

Mantis supports on-demand request/response capture which are forwarded to a canary application instance to test out new changes OR even perform scale test.
