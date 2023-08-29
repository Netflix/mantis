# Worker Heartbeats
Mantis job workers send heartbeats to Mantis master periodically. Missed heartbeats after a certain interval causes worker resubmit.

By default, workers send heartbeats at a fixed interval configured in master — `mantis.worker.heartbeat.intervalv2.secs`. It defaults to 20s. [sourcecode](https://github.com/search?q=repo%3ANetflix%2Fmantis%20mantis.worker.heartbeat.intervalv2.secs&type=code)

Along with it, there's a default configuration for worker timeout in master — `mantis.worker.heartbeat.interval.secs`. It defaults to 60s. [sourcecode](https://github.com/search?q=repo%3ANetflix%2Fmantis+mantis.worker.heartbeat.interval.secs&type=code)

If you want to customize these for your job, you could configure following system parameters during job cluster and/or job update — [sourcecode](https://github.com/Netflix/mantis/blob/master/mantis-common/src/main/java/io/mantisrx/common/SystemParameters.java#L27-L28)
```
    mantis.job.worker.heartbeat.interval.secs
    mantis.job.worker.timeout.secs
```