In addition to the Mantis UI, there is a REST API with which you can maintain
[Mantis Jobs] and [Job Clusters]. This page describes the open source version of the Mantis REST
API.

You can use the Mantis REST API to submit Jobs based on existing Job Cluster or to connect to the
output of running Jobs. It is easier to set up or update new Job Clusters by using the Mantis UI,
but you can also do this with the Mantis REST API.

!!!note "Response Content Type"
    Mantis API endpoints always return JSON, and do not respect content-type headers in API requests.

## Summary of REST API

### Cluster APIs
| endpoint                                                                                                                         | verb     | purpose |
| -------------------------------------------------------------------------------------------------------------------------------- | -------- | ------- |
| [<code>/api/v1/jobClusters</code>](#get-a-list-of-clusters)                                                                      | `GET`    | Return a list of Mantis [clusters]. |
| [<code>/api/v1/jobClusters</code>](#create-a-new-cluster)                                                                        | `POST`   | Create a new cluster. |
| [<code>/api/v1/jobClusters/<var>clusterName</var></code>](#get-information-about-a-cluster)                                      | `GET`    | Return information about a single cluster by name. |
| [<code>/api/v1/jobClusters/<var>clusterName</var></code>](#change-information-about-a-cluster)                                   | `PUT`    | Update the information about a particular cluster. |
| [<code>/api/v1/jobClusters/<var>clusterName</var></code>](#delete-a-cluster)                                                     | `DELETE` | Permanently delete a paticular cluster. |
| [<code>/api/v1/jobClusters/<var>clusterName</var>/actions/updateArtifact</code>](#update-a-clusters-artifacts)                   | `POST`   | Update the job cluster [artifact] and optionally resubmit the [job]. |
| [<code>/api/v1/jobClusters/<var>clusterName</var>/actions/updateSla</code>](#update-a-clusters-sla)                              | `POST`   | Update cluster [SLA] information. |
| [<code>/api/v1/jobClusters/<var>clusterName</var>/actions/updateMigrationStrategy</code>](#update-a-clusters-migration-strategy) | `POST`   | Update the cluster [migration strategy]. |
| [<code>/api/v1/jobClusters/<var>clusterName</var>/actions/updateLabel</code>](#update-a-clusters-labels)                         | `POST`   | Update cluster [labels]. |
| [<code>/api/v1/jobClusters/<var>clusterName</var>/actions/enableCluster</code>](#enable-a-cluster)                               | `POST`   | Enable a disabled cluster. |
| [<code>/api/v1/jobClusters/<var>clusterName</var>/actions/disableCluster</code>](#disable-a-cluster)                             | `POST`   | Disable a cluster. |
| [`/api/v1/mantis/publish/streamJobClusterMap`](#get-a-map-of-mantis-publish-push-based--streams-to-clusters)                     | `GET`    | Return a mapping of Mantis Publish push-based streams to clusters. |

### Job APIs
| endpoint                                                                                                                 | verb     | purpose |
| ------------------------------------------------------------------------------------------------------------------------ | -------- | ------- |
| [<code>/api/v1/jobs</code>](#get-a-list-of-jobs)                                                                         | `GET`    | Return a list of jobs. |
| [<code>/api/v1/jobClusters/<var>clusterName</var>/jobs</code>](#get-a-list-of-jobs-for-a-particular-cluster)             | `GET`    | Return a list of jobs for a particular cluster. |
| [<code>/api/v1/jobs/<var>jobID</var></code>](#get-information-about-a-particular-job)                                    | `GET`    | Return information about a particular job. |
| [<code>/api/v1/jobClusters/<var>clusterName</var>/jobs/<var>jobID</var></code>](#get-information-about-a-particular-job) | `GET`    | Return information about a particular job. |
| [<code>/api/v1/jobs/<var>jobID</var></code>](#kill-a-job)                                                                | `DELETE` | Permanently kill a particular job. |
| [<code>/api/v1/jobClusters/<var>clusterName</var>/jobs</code>](#submit-a-new-job-for-a-particular-cluster)               | `POST`   | Submit a new job. |
| [<code>/api/v1/jobs/actions/quickSubmit</code>](#update-a-job-cluster-and-submit-a-new-job-at-the-same-time)             | `POST`   | Update a job cluster and submit a new job at the same time. |
| [<code>/api/v1/jobs/<var>jobID</var>/actions/postJobStatus</code>](#post-job-heartbeat-status)                           | `POST`   | Post job heartbeat status. |
| [<code>/api/v1/jobs/<var>jobID</var>/actions/scaleStage</code>](#horizontally-scale-a-stage)                             | `POST`   | Horizontally scale a stage. |
| [<code>/api/v1/jobs/<var>jobID</var>/actions/resubmitWorker</code>](#resubmit-a-worker)                                  | `POST`   | Resubmit a [worker]. |

### Admin APIs
| endpoint                                                                                         | verb   | purpose |
| ------------------------------------------------------------------------------------------------ | ------ | ------- |
| [<code>/api/v1/masterInfo</code>](#return-job-master-information)                                | `GET`  | Return [Job Master] information. |
| [<code>/api/v1/masterConfigs</code>](#return-job-master-configs)                                 | `GET`  | Return Job Master configs. |
| [<code>/api/v1/agentClusters/</code>](#get-information-about-agent-clusters)                     | `GET`  | Get information about active agent clusters. |
| [<code>/api/v1/agentClusters/</code>](##activate-or-deactivate-an-agent-cluster)                 | `POST` | Activate or deactivate an agent cluster. |
| [<code>/api/v1/agentClusters/jobs</code>](#get-jobs-and-host-information-for-an-agent-cluster)   | `GET`  | Get jobs and host information for an agent cluster. |
| [<code>/api/v1/agentClusters/autoScalePolicy</code>](#retrieve-the-agent-cluster-scaling-policy) | `GET`  | Retrieve the Agent Cluster Scaling Policy. |

### Streaming WebSocket/SSE APIs
| endpoint                                                                                                                             | verb   | purpose |
| ------------------------------------------------------------------------------------------------------------------------------------ | ------ | ------- |
| [<code>ws://<var>masterHost</var>:7101/api/v1/jobStatusStream/<var>jobID</var></code>](#stream-job-status-changes)                   | n/a    | Stream Job Status Changes. |
| [<code>/api/v1/jobDiscoveryStream/<var>jobID</var></code>](#stream-scheduling-information-for-a-job) ([SSE])                         | `GET`  | Return streaming (SSE) scheduling information for a particular job. |
| [<code>/api/v1/jobs/schedulingInfo/<var>jobID</var></code>](#stream-scheduling-information-for-a-job) (SSE)                          | `GET`  | Return streaming (SSE) scheduling information for a particular job. |
| [<code>/api/v1/jobClusters/discoveryInfoStream/<var>clusterName</var></code>](#get-streaming-sse-discovery-info-for-a-cluster) (SSE) | `GET`  | Return streaming (SSE) discovery info for the given job cluster. |
| [<code>/api/v1/lastSubmittedJobIdStream/<var>clusterName</var></code>](#stream-job-information-for-a-cluster) (SSE)                  | `GET`  | Return streaming (SSE) job information for a particular cluster. |
| [<code>/api/v1/jobConnectbyid/<var>jobID</var></code>](#stream-job-sink-messages) (SSE)                                              | n/a    | Collect messages from the job sinks and merge them into a single websocket or SSE stream. |
| [<code>/api/v1/jobConnectbyname/<var>jobName</var></code>](#stream-job-sink-messages) (SSE)                                          | n/a    | Collect messages from the job sinks and merge them into a single websocket or SSE stream. |
| [<code>/api/v1/jobsubmitandconnect</code>](#submit-job-and-stream-job-sink-messages) (SSE)                                           | `POST` | Submit a job, collect messages from the job sinks, and merge them into a single websocket or SSE stream. |


-----

## Cluster Tasks

<p class="tbd">TBD</p>
### Get a List of Clusters
* **<code>/api/v1/jobClusters</code> (`GET`)**

To retrieve a list of JSON objects that include details about the available Job Clusters, issue a `GET` command to the Mantis REST API endpoint `/api/v1/jobClusters/`.

???question "Query Parameters"
    | Query Parameter        | Purpose |
    | ---------------------- | ------- |
    | `ascending` (optional) | You can use this to indicate whether or not to sort the records in ascending order (`true` | `false`). |
    | `fields` (optional)    | By default this endpoint will return all of the fields in the payload. You can set `fields` to a comma-delimited series of payload fields, in which case this endpoint will return only those fields of the payload. For example `?fields=name`. |
    | `offset` (optional)    | The record number to begin with in the set of records to return in this request (use this with `pageSize` to get records by the page). See [Pagination](#pagination) for more details. |
    | `pageSize` (optional)  | The maximum number of records to return in this request (default = 0, which means all records). See [Pagination](#pagination) for more details. |
    | `sortBy` (optional)    | You can set this to the name of any payload field whose values are `Comparable` and this endpoint will return its results sorted by that field. |
    | `matching` (optional)  | You can set this to a regular expression, and Mantis will filter the list of Job Clusters on the server side, and will return only those that match this expression. |

???abstract "Example Response Format"
    **`GET /api/v1/jobClusters?pageSize=5&fields=name&sortBy=name&ascending=false`**

        {
          "list":
            [{"name":"jschmoeSLATest"},
             {"name":"sinefn"},
             {"name":"Validation_ZV93BAWR2"},
             {"name":"Validation_Z8NB06L1A"},
             {"name":"Validation_Y828QAI01"}],
          "prev":null,
          "next":"/api/v1/jobClusters?pageSize=5&fields=name&sortBy=name&ascending=false&offset=5"
        }

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

### Create a New Cluster
* **<code>/api/v1/jobClusters</code> (`POST`)**

Before you submit a [Mantis Job], you must first have set up a [Job Cluster]. A Job Cluster contains
a unique name for the Job, a URL of the Job’s `.jar` or `.zip` artifact file, resource requirements
to run your Job, and other optional information such as [SLA] values for minimum and maximum Jobs to
keep active for this Cluster, or a cron-based schedule to launch a Job for this Cluster. Each new
Job can be considered as an instance of the Job Cluster, and is given a unique ID by appending a
number suffix to the Cluster name. The Job inherits the resource requirements from the Cluster
unless whoever submits the Job overrides these at submit time.

A Job Cluster name must match this regular expression: `^[A-Za-z]+[A-Za-z0-9+-_=:;]*`

To create a new Job Cluster, issue a POST command to the Mantis REST API endpoint
`/api/v1/jobClusters` with a request body that matches format of the following example:

???abstract "Example Request Body"
        {
          "jobDefinition": {
            "name": "jschmoe_validation",
            "user": "jschmoe",
            "jobJarFileLocation": "https://some.host/mantis-artifacts/mantis-examples-sine-function-0.2.9.zip",
            "version": "0.2.9 2019-03-19 17:01:36",
            "schedulingInfo": {
              "stages": {
                "1": {
                  "numberOfInstances": "1",
                  "machineDefinition": {
                    "cpuCores": "1",
                    "memoryMB": "1024",
                    "diskMB": "1024",
                    "networkMbps": "128",
                    "numPorts": "1"
                  },
                  "scalable": false,
                  "softConstraints": [],
                  "hardConstraints": []
                }
              }
            },
            "parameters": [],
            "labels": [
              {
                "name": "_mantis.user",
                "value": "jschmoe"
              },
              {
                "name": "_mantis.ownerEmail",
                "value": "jschmoe@netflix.com"
              },
              {
                "name": "_mantis.artifact",
                "value": "mantis-examples-sine-function"
              },
              {
                "name": "_mantis.artifact.version",
                "value": "0.2.9"
              }
            ],
            "migrationConfig": {
              "strategy": "PERCENTAGE",
              "configString": "{\"percentToMove\":25,\"intervalMs\":60000}"
            },
            "slaMin": "0",
            "slaMax": "0",
            "cronSpec": null,
            "cronPolicy": "KEEP_EXISTING"
          },
          "owner": {
            "contactEmail": "jschmoe@netflix.com",
            "description": "",
            "name": "Joe Schmoe",
            "repo": "",
            "teamName": ""
          }
        }

???abstract "Example Response Format"
        {
          "name": "jschmoe_validation1",
          "jars": [
            {
              "url": "https://mantis.us-east-1.prod.netflix.net/mantis-artifacts/mantis-examples-sine-function-0.2.9.zip",
              "uploadedAt": 1553040262171,
              "version": "0.2.9 2019-03-19 17:01:36",
              "schedulingInfo": {
                "stages": {
                  "1": {
                    "numberOfInstances": 1,
                    "machineDefinition": {
                      "cpuCores": 1,
                      "memoryMB": 1024,
                      "networkMbps": 128,
                      "diskMB": 1024,
                      "numPorts": 1
                    },
                    "hardConstraints": [],
                    "softConstraints": [],
                    "scalingPolicy": null,
                    "scalable": false
                  }
                }
              }
            }
          ],
          "sla": {
            "min": 0,
            "max": 0,
            "cronSpec": null,
            "cronPolicy": null
          },
          "parameters": [],
          "owner": {
            "name": "Joe Schmoe",
            "teamName": "",
            "description": "",
            "contactEmail": "jschmoe@netflix.com",
            "repo": ""
          },
          "lastJobCount": 0,
          "disabled": false,
          "isReadyForJobMaster": false,
          "migrationConfig": {
            "strategy": "PERCENTAGE",
            "configString": "{\"percentToMove\":25,\"intervalMs\":60000}"
          },
          "labels": [
            {
              "name": "_mantis.user",
              "value": "jschmoe"
            },
            {
              "name": "_mantis.ownerEmail",
              "value": "jschmoe@netflix.com"
            },
            {
              "name": "_mantis.artifact",
              "value": "mantis-examples-sine-function"
            },
            {
              "name": "_mantis.artifact.version",
              "value": "0.2.9"
            }
          ],
          "cronActive": false,
          "latestVersion": "0.2.9 2019-03-19 17:01:36"
        }

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `201`         | normal response |
    | `405`         | incorrect HTTP verb (use `POST` instead) |
    | `409`         | cluster name already exists |
    | `500`         | unknown server error |

#### Setting Jobs to Launch at Particular Times
You can use the `cronSpec` field in the body of this request to specify when to launch the Jobs in
the Cluster. By default this is blank (`""`).

If you set `cronSpec` to a non-blank value, this also sets the `min` and `max` values for the Job
Cluster to 0 and 1 respectively. That is to say, you can have no more than one Job running at any
one time for that Cluster.

Optionally, you can provide a policy (`cronpolicy`) to use when a cron trigger fires while a
previosuly submitted Job for the Job Cluster is still running. The possible policy values are
`KEEP_EXISTING` (do not replace the current Job) and `KEEP_NEW` (replace the current Job with a new
one). The default policy is `KEEP_EXISTING`.

!!!note
    If the [Mantis Master] is down during a time window when cron would normally have fired, that
    cron trigger time window is lost. Mantis does not check for this upon restart. The next cron
    trigger will resume normally.

-----

### Get Information about a Cluster
* **<code>/api/v1/jobClusters/<var>clusterName</var></code> (`GET`)**

To retrieve a JSON object that includes details about a Job Cluster, issue a `GET` command to the Mantis REST API endpoint <code>/api/v1/jobClusters/<var>clusterName</var></code>.

???question "Query Parameters"
    | Query Parameter      | Purpose |
    | -------------------- | ------- |
    | `fields` (optional)  | By default this endpoint will return all of the fields in the payload. You can set `fields` to a comma-delimited series of payload fields, in which case this endpoint will return only those fields of the payload. |

???abstract "Example Response Format"
        {
          "name": "jschmoe_validation1",
          "jars": [
            {
              "url": "https://mantis.us-east-1.prod.netflix.net/mantis-artifacts/mantis-examples-sine-function-0.2.9.zip",
              "uploadedAt": 1553040262171,
              "version": "0.2.9 2019-03-19 17:01:36",
              "schedulingInfo": {
                "stages": {
                  "1": {
                    "numberOfInstances": 1,
                    "machineDefinition": {
                      "cpuCores": 1,
                      "memoryMB": 1024,
                      "networkMbps": 128,
                      "diskMB": 1024,
                      "numPorts": 1
                    },
                    "hardConstraints": [],
                    "softConstraints": [],
                    "scalingPolicy": null,
                    "scalable": false
                  }
                }
              }
            }
          ],
          "sla": {
            "min": 0,
            "max": 0,
            "cronSpec": null,
            "cronPolicy": null
          },
          "parameters": [],
          "owner": {
            "name": "Joe Schmoe",
            "teamName": "",
            "description": "",
            "contactEmail": "jschmoe@netflix.com",
            "repo": ""
          },
          "lastJobCount": 0,
          "disabled": false,
          "isReadyForJobMaster": false,
          "migrationConfig": {
            "strategy": "PERCENTAGE",
            "configString": "{\"percentToMove\":25,\"intervalMs\":60000}"
          },
          "labels": [
            {
              "name": "_mantis.user",
              "value": "jschmoe"
            },
            {
              "name": "_mantis.ownerEmail",
              "value": "jschmoe@netflix.com"
            },
            {
              "name": "_mantis.artifact",
              "value": "mantis-examples-sine-function"
            },
            {
              "name": "_mantis.artifact.version",
              "value": "0.2.9"
            }
          ],
          "cronActive": false,
          "latestVersion": "0.2.9 2019-03-19 17:01:36"
        }

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `404`         | no cluster with that cluster name was found |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

### Change Information about a Cluster
* **<code>/api/v1/jobClusters/<var>clusterName</var></code> (`PUT`)**

To update an existing [Job Cluster], send a `PUT` command to the Mantis REST API endpoint
<code>/api/v1/jobClusters/<var>clusterName</var></code> with the same sort of body described above
in the case of a Job Cluster create operation. Increment the version number so as to differentiate
your new Cluster from the previous Job [artifacts]. If you try to update an existing Job Cluster by
reusing the version number of an existing one, the operation will fail.

???abstract "Example Request Body"
        {
          "jobDefinition": {
            "name": "Validation_jschmoe",
            "user": "validator",
            "jobJarFileLocation": "https://some.host/mantis-artifacts/mantis-examples-sine-function-0.2.9.zip",
            "parameters": [
              {
                "name": "useRandom",
                "value": false
              }
            ],
            "schedulingInfo": {
              "stages": {
                "0": {
                  "numberOfInstances": 1,
                  "machineDefinition": {
                    "cpuCores": 2,
                    "memoryMB": 4096,
                    "diskMB": 10,
                    "numPorts": 1
                  },
                  "hardConstraints": null,
                  "softConstraints": null,
                  "scalable": false
                },
                "1": {
                  "numberOfInstances": 1,
                  "machineDefinition": {
                    "cpuCores": 2,
                    "memoryMB": 4096,
                    "diskMB": 10,
                    "numPorts": 1
                  },
                  "hardConstraints": null,
                  "softConstraints": null,
                  "scalable": false
                }
              }
            },
            "slaMin": 0,
            "slaMax": 0,
            "cronSpec": null,
            "cronPolicy": "KEEP_EXISTING",
            "migrationConfig": {
              "configString": "{\"percentToMove\":60, \"intervalMs\":30000}",
              "strategy": "PERCENTAGE"
            }
          },
          "owner": {
            "name": "validator",
            "teamName": "Mantis",
            "description": "integration validator",
            "contactEmail": "mantisteam@netflix.com"
          }
        }

???abstract "Example Response Format"
    sine-function Job cluster updated

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `400`         | client failure |
    | `404`         | no existing cluster with that cluster name was found |
    | `405`         | incorrect HTTP verb (use `PUT` instead) |
    | `500`         | unknown server error |

-----

### Delete a Cluster
* **<code>/api/v1/jobClusters/<var>clusterName</var></code> (`DELETE`)**

To permanently delete an existing [Job Cluster], send a `DELETE` command to the Mantis REST API
endpoint <code>/api/v1/jobClusters/<var>clusterName</var></code>.

???question "Query Parameters"
    | Query Parameter   | Purpose |
    | ----------------- | ------- |
    | `user` (required) | Must match the original user in the cluster payload. |

???abstract "Example Response Format"
    <p class="tbd">TBD</p>

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `202`         | normal response: asynchronous delete has been scheduled |
    | `405`         | incorrect HTTP verb (use `DELETE` instead) |
    | `500`         | unknown server error |

-----

### Update a Cluster’s Artifacts
* **<code>/api/v1/jobClusters/<var>clusterName</var>/actions/updateArtifact</code> (`POST`)**

You can make a “quick update” of an existing [Job Cluster] and also submit a new [Job] with the
updated cluster [artifacts]. This lets you update the Job Cluster with minimal information, without
having to specify the scheduling info, as long as at least one Job was previously submitted for this
Job Cluster. Mantis copies the scheduling information, Job [parameters], and so forth, from the last
Job submitted.

To do this, send a `POST` command to the Mantis REST API endpoint
<code>/api/v1/jobClusters/<var>clusterName</var>/actions/updateArtifact</code> with a body that
matches the format of the following example:

???abstract "Example Request Body"
        {
          "name": "ValidatorDemo",
          "version": "0.0.1 2019-02-06 11:30:49",
          "url": "mantis-artifacts/demo-0.0.1-dev201901231434.zip",
          "skipsubmit": false,
          "user": "jschmoe"
        }

???abstract "Example Response Format"
    sine-function Job cluster updated

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `204`         | normal response |
    | `404`         | no cluster with the given cluster name was found |
    | `405`         | incorrect HTTP verb (use `POST` instead) |
    | `500`         | unknown server error |

You will receive in the response the Job ID of the newly submitted Job (unless you set `skipsubmit`
to `true` in the request body, in which case no such Job will be created).

-----

### Update a Cluster’s SLA
* **<code>/api/v1/jobClusters/<var>clusterName</var>/actions/updateSla</code> (`POST`)**

To update the [SLA] of a [Job Cluster] without having to submit a new version, send a `POST` command
to the Mantis REST API endpoint
<code>/api/v1/jobClusters/<var>clusterName</var>/actions/updateSla</code> with a body that matches
the format of the following example:

???abstract "Example Request Body"
        { 
          "user": "YourUserName",
          "name": "Foo",
          "min": 0,
          "max": 2,
          "cronspec": "5 * * * * ?",
          "cronpolicy": "KEEP_EXISTING",
          "forceenable": true
        }

The fields of this body are as follows:

| SLA Field     | Purpose |
| ------------- | ------- |
| `user`        | (required) name of the user calling this endpoint |
| `name`        | (required) name of the Job Cluster |
| `min`         | minimum number of [Jobs] to keep running |
| `max`         | maximum number of Jobs to allow running simultaneously |
| `cronspec`    | cron specification, see below for format and examples |
| `cronpolicy`  | either `KEEP_EXISTING` or `KEEP_NEW` (see above for details) |
| `forceenable` | either `true` or `false`; reenable the Job Cluster if it is in the disabled state |

!!! note
    While `min`, `max`, `cronspec`, `cronpolicy`, and `forceenable` are all optional fields, you
    should provide at minimum either `cronspec` or the combination of `min` & `max`.

The cron specification string is defined by [Quartz CronTrigger](http://www.quartz-scheduler.org/documentation/quartz-1.x/tutorials/crontrigger). Here are some examples:

???example "Examples of `cronspec` Values"
    | example `cronspec` value                                 | resulting job trigger time |
    | -------------------------------------------------------- | -------------------------- |
    | <code style="white-space:nowrap">"0 0 12 * * ?"</code>   | Fire at 12&nbsp;p.m. (noon) every day. |
    | <code style="white-space:nowrap">"0 15 10 ? * *"</code>  | Fire at 10:15&nbsp;a.m. every day. |
    | <code style="white-space:nowrap">"0 15 10 * * ?"</code>  | Fire at 10:15&nbsp;a.m. every day. |
    | <code style="white-space:nowrap">"0 0-5 14 * * ?"</code> | Fire every minute starting at 2&nbsp;p.m. and ending at 2:05&nbsp;p.m., every day. |

Scheduling information for Jobs launched by means of cron triggers is inherited from the scheduling
information for the Job Cluster.

!!! warning
    If a Job takes required [parameters], the Job will not launch successfully if the Job Cluster
    does not establish defaults for those parameters. A Job launched by means of a cron trigger
    always uses these default parameters to launch the Job.

If you provide an invalid cron specification, this will disable the Job Cluster. To fix this, when
you reformulate your cron specification, also set `forceenable` to `"true"` in the body that you
send via `POST` to <code>/api/v2/jobClusters/<var>clusterName</var>/actions/updateSla</code>.

???abstract "Example Response Format"
    sine-function SLA updated

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `204`         | normal response |
    | `404`         | no cluster with the given cluster name was found |
    | `405`         | incorrect HTTP verb (use `POST` instead) |
    | `500`         | unknown server error |

???example "Example: Creating a Job Cluster with a cron Specification"

    The following `POST` body to
    <code>/api/v2/jobClusters/<var>clusterName</var>/actions/updateSla</code> would specify Jobs
    that are launched based on a timed schedule:

        {
          "jobDefinition": {
            "name": "Foo",
            "user": "YourUserName",
            "version": "1.0",
            "parameters": [
              {
                "name": "param1",
                "value": "value1"
              },
              {
                "name": "param2",
                "value": "value2"
              }
            ],
            "schedulingInfo": {
              "stages": {
                "1": {
                  "numberOfInstances": 1,
                  "machineDefinition": {
                    "cpuCores": 2,
                    "memoryMB": 4096,
                    "diskMB": 10
                  },
                  "hardConstraints": null,
                  "softConstraints": null,
                  "scalable": false
                }
              }
            },
            "slaMin": 0,
            "slaMax": 0,
            "cronSpec": "2 * * * * ?",
            "cronPolicy":"KEEP_EXISTING",
            "jobJarFileLocation": "http://www.jobjars.com/foo"
          },
          "owner": {
            "name": "MyName",
            "teamName": "myTeam",
            "description": "description",
            "contactEmail": "email@company.com",
            "repo": "http://repos.com/myproject.git"
          }
        }


-----

### Update a Cluster’s Migration Strategy
* **<code>/api/v1/jobClusters/<var>clusterName</var>/actions/updateMigrationStrategy</code> (`POST`)**

You can quickly update the [migration strategy] of an existing [Job Cluster] without having to
update the entirety of the Cluster definition. To do this, send a `POST` command to the Mantis REST
API endpoint <code>/api/v1/jobClusters/<var>clusterName</var>/actions/updateMigrationStrategy</code>
with a body that matches the format of the following example:

???abstract "Example Request Body"
        {
          "name": "NameOfJobCluster",
          "migrationConfig": {
            "strategy": "PERCENTAGE",
            "configString": "{\"percentToMove\":10, \"intervalMs\":1000}"
          },
          "user": "YourUserName"
        }

You will receive in the response the migration strategy config that you have updated the Job Cluster
to.

???abstract "Example Response Format"
    sine-function worker migration config updated

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `204`         | normal response |
    | `404`         | no cluster with the given cluster name was found |
    | `405`         | incorrect HTTP verb (use `POST` instead) |
    | `500`         | unknown server error |

-----

### Update a Cluster’s Labels
* **<code>/api/v1/jobClusters/<var>clusterName</var>/actions/updateLabel</code> (`POST`)**

You can quickly update the [labels] of an existing [Job Cluster] without having to update the
entirety of the Cluster definition. To do this, send a `POST` command to the Mantis REST
API endpoint <code>/api/v1/jobClusters/<var>clusterName</var>/actions/updateLabel</code> with a body
that matches the format of the following example:

???abstract "Example Request Body"
        {
          "name": "SPaaSBackpressureDemp",
          "labels": [
            {
              "name": "_mantis.user",
              "value": "jschmoe"
            },
            {
              "name": "_mantis.ownerEmail",
              "value": "jschmoe@netflix.com"
            },
            {
              "name": "_mantis.artifact",
              "value": "backpressure-demo-aggregator-0.0.1"
            },
            {
              "name": "_mantis.artifact.version",
              "value": "dev201901231434"
            },
            {
              "name": "_mantis.jobType",
              "value": "aggregator"
            },
            {
              "name": "_mantis.criticality",
              "value": "medium"
            },
            {
              "name": "myTestLabel",
              "value": "bingo"
            }
          ],
          "user": "jschmoe"
        }

???abstract "Example Response Format"
    sine-function labels updated

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `204`         | normal response |
    | `404`         | no cluster with the given cluster name was found |
    | `405`         | incorrect HTTP verb (use `POST` instead) |
    | `500`         | unknown server error |

-----

### Enable a Cluster
* **<code>/api/v1/jobClusters/<var>clusterName</var>/actions/enableCluster</code> (`POST`)**

You can quickly change the state of an existing [Job Cluster] to `enabled=true` without having to
update the entirety of the Cluster definition.

To do this, send a `POST` command to the Mantis REST API endpoint
<code>/api/v1/jobClusters/<var>clusterName</var>/actions/enableCluster</code> with a body that
matches the format of the following example:

???abstract "Example Request Body"
        {
          "name": "SPaaSBackpressureDemp",
          "user": "jschmoe"
        }

???abstract "Example Response Format"
    sine-function enabled

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `204`         | normal response |
    | `404`         | no cluster with the given cluster name was found |
    | `405`         | incorrect HTTP verb (use `POST` instead) |
    | `500`         | unknown server error |

-----

### Disable a Cluster
* **<code>/api/v1/jobClusters/<var>clusterName</var>/actions/disableCluster</code> (`POST`)**

You can quickly change the state of an existing [Job Cluster] to `enabled=false` without having to
update the entirety of the Cluster definition.

When you disable a Job Cluster Mantis will not allow new [Job] submissions under that Cluster and
it will terminate any Jobs from that Cluster that are currently running. Mantis will also stop
enforcing the [SLA] requirements for the Cluster, including any cron setup that would otherwise
launch new Jobs.

This is useful when a Job Cluster must be temporarily made inactive, for instance if you have
determined that there is a problem with it.

To do this, send a `POST` command to the Mantis REST API endpoint
<code>/api/v1/jobClusters/<var>clusterName</var>/actions/disableCluster</code> with a body that
matches the format of the following example:

???abstract "Example Request Body"
        {
          "name": "SPaaSBackpressureDemp",
          "user": "jschmoe"
        }

???abstract "Example Response Format"
    sine-function disabled

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `204`         | normal response |
    | `404`         | no cluster with the given cluster name was found |
    | `405`         | incorrect HTTP verb (use `POST` instead) |
    | `500`         | unknown server error |

-----

### Get a Map of Mantis Publish Push-Based Streams to Clusters
* **`/api/v1/mantis/publish/streamJobClusterMap` (`GET`)**

<p class="tbd">describe</p>

???abstract "Example Response Format"
        {
          "version": "1",
          "timestamp": 2,
          "mappings": {
            "__default__": {
              "requestEventStream": "SharedMantisPublishEventSource",
              "__default__": "SharedMantisPublishEventSource"
            }
          }
        }

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

## Job Tasks

<p class="tbd">TBD</p>
### Get a List of Jobs
* **<code>/api/v1/jobs</code> (`GET`)**

To retrieve a JSON array of IDs of the active [Jobs], issue a `GET` command to the Mantis REST API
endpoint <code>/api/v1/jobs</code>.

???question "Query Parameters"
    | Query Parameter        | Purpose |
    | ---------------------- | ------- |
    | `ascending` (optional) | You can use this to indicate whether or not to sort the records in ascending order (`true` | `false`; default=`false`). |
    | `compact` (optional)   | Ask the server to return compact responses (`true` | `false`; default=`false`). |
    | `fields` (optional)    | By default this endpoint will return all of the fields in the payload. You can set `fields` to a comma-delimited series of payload fields, in which case this endpoint will return only those fields in the response. |
    | `limit` (optional)     | The maximum record size to return (default = no limit). |
    | `offset` (optional)    | The record number to begin with in the set of records to return in this request (use this with `pageSize` to get records by the page). See [Pagination](#pagination) for more details. |
    | `pageSize` (optional)  | The maximum number of records to return in this request (default = 0, which means all records). See [Pagination](#pagination) for more details. |
    | `sortBy` (optional)    | You can set this to the name of any payload field whose values are `Comparable` and this endpoint will return its results sorted by that field. |

    There is also a series of query parameters that you can use to set server-side filters that will restrict the jobs represented in the resulting list of jobs to only those jobs that match the filters:

    | Query Parameter            | What It Filters |
    | -------------------------- | --------------- | 
    | `activeOnly` (optional)    | The `activeOnly` field (boolean). By default, this is `true`. |
    | `jobState` (optional)      | Job state, `Active` or `Terminal`. By default, this endpoint filters on `jobState=Active`. This query parameter has precedence over the `activeOnly` parameter. |
    | `labels` (optional)        | Labels in the `labels` array. You can express this by setting this parameter to a comma-delimited list of label strings. |
    | `labels.op` (optional)     | Use this parameter to tell the server whether to treat the list of `labels` you have provided as an `or` (default: a job that contains *any* of the labels will be returned in the list), or an `and` (only jobs that contain *all* of the labels will be returned). Set this to `or` or `and`. |
    | `matching` (optional)      | The cluster name. Set this to a regex string. You can also use the <code>/api/v1/jobClusters/<var>clusterName</var>/jobs</code> endpoint if you mean to match a specific cluster name and do not need the flexibility of a regex filter. |
    | `stageNumber` (optional)   | Stage number (integer). This filters the list to contain only those jobs corresponding to workers that are relevant to the specified stage. |
    | `workerIndex` (optional)   | The `workerIndex` field (integer). |
    | `workerNumber` (optional)  | The `workerNumber` field (integer). |
    | `workerState` (optional)   | The `workerState` field (`Noop`, `Active` (default), or `Terminal`) |

<p class="tbd">describe</p>

???abstract "Example Response Format"
        {
                "list": [
                    {
                        "jobMetadata": {
                            "jobId": "sine-test-4",
                            "name": "sine-test",
                            "user": "someuser",
                            "submittedAt": 1574188324276,
                            "startedAt": 1574188354708,
                            "jarUrl": "http://mantis-examples-sine-function-0.2.9.zip",
                            "numStages": 1,
                            "sla": {
                                "runtimeLimitSecs": 0,
                                "minRuntimeSecs": 0,
                                "slaType": "Lossy",
                                "durationType": "Perpetual",
                                "userProvidedType": ""
                            },
                            "state": "Launched",
                            "subscriptionTimeoutSecs": 0,
                            "parameters": [
                                {
                                    "name": "useRandom",
                                    "value": "True"
                                }
                            ],
                            "nextWorkerNumberToUse": 40,
                            "migrationConfig": {
                                "strategy": "PERCENTAGE",
                                "configString": "{\"percentToMove\":25,\"intervalMs\":60000}"
                            },
                            "labels": [
                                {
                                    "name": "_mantis.user",
                                    "value": "zxu"
                                },
                                {
                                    "name": "_mantis.ownerEmail",
                                    "value": "zxu@netflix.com"
                                },
                                {
                                    "name": "_mantis.artifact.version",
                                    "value": "0.2.9"
                                },
                                {
                                    "name": "_mantis.artifact",
                                    "value": "mantis-examples-sine-function-0.2.9.zip"
                                },
                                {
                                    "name": "_mantis.version",
                                    "value": "0.2.9 2019-03-19 17:01:36"
                                }
                            ]
                        },
                        "stageMetadataList": [
                            {
                                "jobId": "sine-test-4",
                                "stageNum": 1,
                                "numStages": 1,
                                "machineDefinition": {
                                    "cpuCores": 1.0,
                                    "memoryMB": 1024.0,
                                    "networkMbps": 128.0,
                                    "diskMB": 1024.0,
                                    "numPorts": 1
                                },
                                "numWorkers": 1,
                                "hardConstraints": [],
                                "softConstraints": [],
                                "scalingPolicy": null,
                                "scalable": false
                            }
                        ],
                        "workerMetadataList": [
                            {
                                "workerIndex": 0,
                                "workerNumber": 31,
                                "jobId": "sine-test-4",
                                "stageNum": 1,
                                "numberOfPorts": 5,
                                "metricsPort": 7150,
                                "consolePort": 7152,
                                "debugPort": 7151,
                                "customPort": 7153,
                                "ports": [
                                    7154
                                ],
                                "state": "Started",
                                "slave": "100.82.168.140",
                                "slaveID": "f39108b0-da43-45df-8b12-c132d85de7c0-S1",
                                "cluster": "mantisagent-staging-m5.2xlarge-1",
                                "acceptedAt": 1575675900626,
                                "launchedAt": 1575675996506,
                                "startingAt": 1575676025493,
                                "startedAt": 1575676026661,
                                "completedAt": -1,
                                "reason": "Normal",
                                "resubmitOf": 22,
                                "totalResubmitCount": 4
                            }
                        ],
                        "version": null
                    }
                ],
                "prev": null,
                "next": null,
                "total": 1
            }



    
???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

### Get Information About a Particular Job
* **<code>/api/v1/jobs/<var>jobID</var></code> (`GET`)**
* **<code>/api/v1/jobClusters/<var>clusterName</var>/jobs/<var>jobID</var></code> (`GET`)**

To retrieve a JSON record of a particular [Job], issue a `GET` command to the Mantis REST API
endpoint <code>/api/v1/jobs/<var>jobID</var></code>
or <code>/api/v1/jobClusters/<var>clusterName</var>/jobs/<var>jobID</var></code>.

???question "Query Parameters"
    | Query Parameter       | Purpose |
    | --------------------- | ------- |
    | `fields` (optional)   | By default this endpoint will return all of the fields in the payload. You can set `fields` to a comma-delimited series of payload fields, in which case this endpoint will return only those fields in the response. |
    | `archived` (optional) | By default only information about an active job will be returned. Set this to `true` if you want information about the job returned even if it is an archived inactive job. |

<p class="tbd">describe</p>

???abstract "Example Response Format"
        {
                    "jobMetadata": {
                        "jobId": "sine-function-7531",
                        "name": "sine-function",
                        "user": "someuser",
                        "submittedAt": 1576002266997,
                        "startedAt": 0,
                        "jarUrl": "http://mantis-examples-sine-function-0.2.9.zip",
                        "numStages": 2,
                        "sla": {
                            "runtimeLimitSecs": 0,
                            "minRuntimeSecs": 0,
                            "slaType": "Lossy",
                            "durationType": "Perpetual",
                            "userProvidedType": ""
                        },
                        "state": "Accepted",
                        "subscriptionTimeoutSecs": 0,
                        "parameters": [
                            {
                                "name": "useRandom",
                                "value": "False"
                            }
                        ],
                        "nextWorkerNumberToUse": 10,
                        "migrationConfig": {
                            "strategy": "PERCENTAGE",
                            "configString": "{\"percentToMove\":25,\"intervalMs\":60000}"
                        },
                        "labels": [
                            {
                                "name": "_mantis.artifact",
                                "value": "mantis-examples-sine-function-0.2.9.zip"
                            },
                            {
                                "name": "_mantis.version",
                                "value": "0.2.9 2018-04-23 13:22:02"
                            }
                        ]
                    },
                    "stageMetadataList": [
                        {
                            "jobId": "sine-function-7531",
                            "stageNum": 0,
                            "numStages": 2,
                            "machineDefinition": {
                                "cpuCores": 2.0,
                                "memoryMB": 4096.0,
                                "networkMbps": 128.0,
                                "diskMB": 1024.0,
                                "numPorts": 1
                            },
                            "numWorkers": 1,
                            "hardConstraints": [],
                            "softConstraints": [],
                            "scalingPolicy": null,
                            "scalable": false
                        },
                        {
                            "jobId": "sine-function-7531",
                            "stageNum": 1,
                            "numStages": 2,
                            "machineDefinition": {
                                "cpuCores": 1.0,
                                "memoryMB": 1024.0,
                                "networkMbps": 128.0,
                                "diskMB": 1024.0,
                                "numPorts": 1
                            },
                            "numWorkers": 1,
                            "hardConstraints": [],
                            "softConstraints": [],
                            "scalingPolicy": null,
                            "scalable": false
                        }
                    ],
                    "workerMetadataList": [
                        {
                            "workerIndex": 0,
                            "workerNumber": 1,
                            "jobId": "sine-function-7531",
                            "stageNum": 0,
                            "numberOfPorts": 5,
                            "metricsPort": -1,
                            "consolePort": -1,
                            "debugPort": -1,
                            "customPort": -1,
                            "ports": [],
                            "state": "Accepted",
                            "slave": null,
                            "slaveID": null,
                            "cluster": null,
                            "acceptedAt": 1576002267005,
                            "launchedAt": -1,
                            "startingAt": -1,
                            "startedAt": -1,
                            "completedAt": -1,
                            "reason": "Normal",
                            "resubmitOf": 0,
                            "totalResubmitCount": 0
                        },
                        {
                            "workerIndex": 0,
                            "workerNumber": 2,
                            "jobId": "sine-function-7531",
                            "stageNum": 1,
                            "numberOfPorts": 5,
                            "metricsPort": -1,
                            "consolePort": -1,
                            "debugPort": -1,
                            "customPort": -1,
                            "ports": [],
                            "state": "Accepted",
                            "slave": null,
                            "slaveID": null,
                            "cluster": null,
                            "acceptedAt": 1576002267007,
                            "launchedAt": -1,
                            "startingAt": -1,
                            "startedAt": -1,
                            "completedAt": -1,
                            "reason": "Normal",
                            "resubmitOf": 0,
                            "totalResubmitCount": 0
                        }
                    ],
                    "version": "0.2.9 2018-04-23 13:22:02"
                }


    

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `404`         | no job with that ID was found |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

### Kill a Job
* **<code>/api/v1/jobs/<var>jobID</var></code> (`DELETE`)**

To permanently kill a particular [Job], issue a `DELETE` command to the Mantis REST API endpoint
endpoint <code>/api/v1/jobs/<var>jobID</var></code>.

???question "Query Parameters"
    | Query Parameter       | Purpose |
    | --------------------- | ------- |
    | `reason` (required)   | Specify why you are killing this job. |
    | `user` (required)     | Specify which user is initiating this request. |

???abstract "Example Response Format"
    <p class="tbd">empty</p>
    No response

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `202`         | the kill request has been accepted and is being processed asynchronously |
    | `404`         | no job with that ID was found |
    | `405`         | incorrect HTTP verb (use `DELETE` instead) |
    | `500`         | unknown server error |

-----

### List the Archived Workers for a Job
* **<code>/api/v1/jobs/<var>jobID</var>/archivedWorkers</code> (`GET`)**

To list all of the archived workers for a particular [Job], issue a `GET` command to the Mantis REST
API endpoint endpoint <code>/api/v1/jobs/<var>jobID</var>/archivedWorkers</code>.

???question "Query Parameters"
    | Query Parameter        | Purpose |
    | ---------------------- | ------- |
    | `ascending` (optional) | You can use this to indicate whether or not to sort the records in ascending order (`true` | `false`; default=`false`). |
    | `fields` (optional)    | By default this endpoint will return all of the fields in the payload. You can set `fields` to a comma-delimited series of payload fields, in which case this endpoint will return only those fields in the response. |
    | `limit` (optional)     | The maximum record size to return (default = no limit). |
    | `offset` (optional)    | The record number to begin with in the set of records to return in this request (use this with `pageSize` to get records by the page). See [Pagination](#pagination) for more details. |
    | `pageSize` (optional)  | The maximum number of records to return in this request (default = 0, which means all records). See [Pagination](#pagination) for more details. |
    | `sortBy` (optional)    | You can set this to the name of any payload field whose values are `Comparable` and this endpoint will return its results sorted by that field. |

<p class="tbd">describe</p>

???abstract "Example Response Format"
        {
                "list": [
                    {
                        "workerIndex": 0,
                        "workerNumber": 2,
                        "jobId": "sine-function-7532",
                        "stageNum": 1,
                        "numberOfPorts": 5,
                        "metricsPort": 7155,
                        "consolePort": 7157,
                        "debugPort": 7156,
                        "customPort": 7158,
                        "ports": [
                            7159
                        ],
                        "state": "Failed",
                        "slave": "100.85.130.224",
                        "slaveID": "079f4fa6-f910-4247-b5d0-f5574f36cace-S5274",
                        "cluster": "mantisagent-main-m5.2xlarge-1",
                        "acceptedAt": 1576003739758,
                        "launchedAt": 1576003739861,
                        "startingAt": 1576003748544,
                        "startedAt": 1576003750069,
                        "completedAt": 1576003959366,
                        "reason": "Relaunched",
                        "resubmitOf": 0,
                        "totalResubmitCount": 0
                    }
                ],
                "prev": null,
                "next": null,
                "total": 1
            }

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `404`         | no job with that ID was found |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

### Get a List of Jobs for a Particular Cluster
* **<code>/api/v1/jobClusters/<var>clusterName</var>/jobs</code> (`GET`)**

To retrieve a JSON array of IDs of the active [Jobs] in a particular cluster, issue a `GET` command
to the Mantis REST API endpoint <code>/api/v1/jobClusters/<var>clusterName</var>/jobs</code>.

???question "Query Parameters"
    | Query Parameter      | Purpose |
    | -------------------- | ------- |
    | `ascending` (optional) | You can use this to indicate whether or not to sort the records in ascending order (`true` | `false`; default=`false`). |
    | `compact` (optional)   | Ask the server to return compact responses (`true` | `false`; default=`false`). |
    | `fields` (optional)    | By default this endpoint will return all of the fields in the payload. You can set `fields` to a comma-delimited series of payload fields, in which case this endpoint will return only those fields in the response. |
    | `limit` (optional)     | The maximum record size to return (default = no limit). |
    | `offset` (optional)    | The record number to begin with in the set of records to return in this request (use this with `pageSize` to get records by the page). See [Pagination](#pagination) for more details. |
    | `pageSize` (optional)  | The maximum number of records to return in this request (default = 0, which means all records). See [Pagination](#pagination) for more details. |
    | `sortBy` (optional)    | You can set this to the name of any payload field whose values are `Comparable` and this endpoint will return its results sorted by that field. |

    There is also a series of query parameters that you can use to set server-side filters that will restrict the jobs represented in the resulting list of jobs to only those jobs that match the filters:

    | Query Parameter            | What It Filters |
    | -------------------------- | --------------- | 
    | `activeOnly` (optional)    | The `activeOnly` field (boolean). By default, this is `true`. |
    | `jobState` (optional)      | Job state, `Active` or `Terminal`. By default, this endpoint filters on `jobState=Active`. This query parameter has precedence over the `activeOnly` parameter. |
    | `labels` (optional)        | Labels in the `labels` array. You can express this by setting this parameter to a comma-delimited list of label strings. |
    | `labels.op` (optional)     | Use this parameter to tell the server whether to treat the list of `labels` you have provided as an `or` (default: a job that contains *any* of the labels will be returned in the list), or an `and` (only jobs that contain *all* of the labels will be returned). Set this to `or` or `and`. |
    | `stageNumber` (optional)   | Stage number (integer). This filters the list to contain only those jobs corresponding to workers that are relevant to the specified stage. |
    | `workerIndex` (optional)   | The `workerIndex` field (integer). |
    | `workerNumber` (optional)  | The `workerNumber` field (integer). |
    | `workerState` (optional)   | The `workerState` field (`Noop`, `Active` (default), or `Terminal`) |

<p class="tbd">describe</p>

???abstract "Example Response Format"
        {
                "list": [
                    {
                        "jobMetadata": {
                            "jobId": "sine-function-7532",
                            "name": "sine-function",
                            "user": "someuser",
                            "submittedAt": 1576003739750,
                            "startedAt": 1576003750076,
                            "jarUrl": "http://mantis-examples-sine-function-0.2.9.zip",
                            "numStages": 2,
                            "sla": {
                                "runtimeLimitSecs": 0,
                                "minRuntimeSecs": 0,
                                "slaType": "Lossy",
                                "durationType": "Perpetual",
                                "userProvidedType": ""
                            },
                            "state": "Launched",
                            "subscriptionTimeoutSecs": 0,
                            "parameters": [
                                {
                                    "name": "useRandom",
                                    "value": "False"
                                }
                            ],
                            "nextWorkerNumberToUse": 10,
                            "migrationConfig": {
                                "strategy": "PERCENTAGE",
                                "configString": "{\"percentToMove\":25,\"intervalMs\":60000}"
                            },
                            "labels": [
                                {
                                    "name": "_mantis.isResubmit",
                                    "value": "true"
                                },
                                {
                                    "name": "_mantis.artifact",
                                    "value": "mantis-examples-sine-function-0.2.9.zip"
                                },
                                {
                                    "name": "_mantis.version",
                                    "value": "0.2.9 2018-04-23 13:22:02"
                                }
                            ]
                        },
                        "stageMetadataList": [
                            {
                                "jobId": "sine-function-7532",
                                "stageNum": 0,
                                "numStages": 2,
                                "machineDefinition": {
                                    "cpuCores": 2.0,
                                    "memoryMB": 4096.0,
                                    "networkMbps": 128.0,
                                    "diskMB": 1024.0,
                                    "numPorts": 1
                                },
                                "numWorkers": 1,
                                "hardConstraints": [],
                                "softConstraints": [],
                                "scalingPolicy": null,
                                "scalable": false
                            },
                            {
                                "jobId": "sine-function-7532",
                                "stageNum": 1,
                                "numStages": 2,
                                "machineDefinition": {
                                    "cpuCores": 1.0,
                                    "memoryMB": 1024.0,
                                    "networkMbps": 128.0,
                                    "diskMB": 1024.0,
                                    "numPorts": 1
                                },
                                "numWorkers": 1,
                                "hardConstraints": [],
                                "softConstraints": [],
                                "scalingPolicy": null,
                                "scalable": false
                            }
                        ],
                        "workerMetadataList": [
                            {
                                "workerIndex": 0,
                                "workerNumber": 1,
                                "jobId": "sine-function-7532",
                                "stageNum": 0,
                                "numberOfPorts": 5,
                                "metricsPort": 7150,
                                "consolePort": 7152,
                                "debugPort": 7151,
                                "customPort": 7153,
                                "ports": [
                                    7154
                                ],
                                "state": "Started",
                                "slave": "100.85.130.224",
                                "slaveID": "079f4fa6-f910-4247-b5d0-f5574f36cace-S5274",
                                "cluster": "mantisagent-main-m5.2xlarge-1",
                                "acceptedAt": 1576003739756,
                                "launchedAt": 1576003739861,
                                "startingAt": 1576003748558,
                                "startedAt": 1576003749967,
                                "completedAt": -1,
                                "reason": "Normal",
                                "resubmitOf": 0,
                                "totalResubmitCount": 0
                            },
                            {
                                "workerIndex": 0,
                                "workerNumber": 3,
                                "jobId": "sine-function-7532",
                                "stageNum": 1,
                                "numberOfPorts": 5,
                                "metricsPort": 7165,
                                "consolePort": 7167,
                                "debugPort": 7166,
                                "customPort": 7168,
                                "ports": [
                                    7169
                                ],
                                "state": "Started",
                                "slave": "100.85.130.224",
                                "slaveID": "079f4fa6-f910-4247-b5d0-f5574f36cace-S5274",
                                "cluster": "mantisagent-main-m5.2xlarge-1",
                                "acceptedAt": 1576003959366,
                                "launchedAt": 1576003959407,
                                "startingAt": 1576003961542,
                                "startedAt": 1576003962822,
                                "completedAt": -1,
                                "reason": "Normal",
                                "resubmitOf": 2,
                                "totalResubmitCount": 1
                            }
                        ],
                        "version": "0.2.9 2018-04-23 13:22:02"
                    }
                ],
                "prev": null,
                "next": null,
                "total": 1
            }

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `404`         | no cluster with the given cluster name was found |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

### Submit a New Job for a Particular Cluster
* **<code>/api/v1/jobClusters/<var>clusterName</var>/jobs</code> (`POST`)**

To submit a new [Job] based on a [Job Cluster], issue a `POST` command to the
<code>/api/v1/jobClusters/<var>clusterName</var>/jobs</code> endpoint with a
request body like the following:

???abstract "Example Request Body"
        {
          "name": "myClusterName",
          "user": "jschmoe",
          "jobJarFileLocation": null,
          "version": "0.0.1 2019-02-06 13:30:07",
          "subscriptionTimeoutSecs": 0,
          "jobSla": {
            "runtimeLimitSecs": "0",
            "slaType": "Lossy",
            "durationType": "Perpetual",
            "userProvidedType": ""
          },
          "schedulingInfo": {
            "stages": {
              "0": {
                "numberOfInstances": 1,
                "machineDefinition": {
                  "cpuCores": 0.35,
                  "memoryMB": 600,
                  "networkMbps": 30,
                  "diskMB": 100,
                  "numPorts": 1
                },
                "hardConstraints": null,
                "softConstraints": null,
                "scalable": false
              },
              "1": {
                "numberOfInstances": 1,
                "machineDefinition": {
                  "cpuCores": 0.35,
                  "memoryMB": 600,
                  "networkMbps": 30,
                  "diskMB": 100,
                  "numPorts": 1
                },
                "hardConstraints": null,
                "softConstraints": null,
                "scalable": true
              }
            }
          },
          "parameters": [
            {
              "name": "criterion",
              "value": "mock"
            },
            {
              "name": "sourceJobName",
              "value": "RequestSource"
            },
            {
              "name": "spaasJobId",
              "value": "spaasjschmoe-clsessionizer_backpressuretest"
            }
          ],
          "isReadyForJobMaster": false
        }

???abstract "Example Response Format"
    <span class="tbd">(Same as "Get Information about a Job" below)</span>
    (Same as "Get Information about a Job" above)
???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `201`         | normal response |
    | `404`         | no cluster with the given cluster name was found |
    | `405`         | incorrect HTTP verb (use `POST` instead) |
    | `500`         | unknown server error |

-----

### Update a Job Cluster and Submit a New Job at the Same Time
* **<code>/api/v1/jobs/actions/quickSubmit</code> (`POST`)**

You can make a “quick update” of an existing [Job Cluster] and also submit a new [Job] with the
updated cluster [artifacts]. This lets you update the Job Cluster with minimal information, without
having to specify the scheduling info, as long as at least one Job was previously submitted for this
Job Cluster.

To do this, send a `POST` command to the Mantis REST API endpoint
<code>/api/v1/jobs/actions/quickSubmit</code> with a body that matches the format
of the following example:

???abstract "Example Request Body"
        {
          "name": "NameOfJobCluster",
          "user": "myusername",
          "jobSla": {
            "durationType": "Perpetual",
            "runtimeLimitSecs": "0",
            "minRuntimeSecs": "0",
            "userProvidedType": ""
          }
        }

???abstract "Example Response Format"
    You will receive in the response the Job ID of the newly submitted Job. E.g sine-test-5  <span class="tbd">sample response body?</span>

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `201`         | normal response |
    | `404`         | no cluster was found with a cluster name matching the value of `name` from the request body |
    | `405`         | incorrect HTTP verb (use `POST` instead) |
    | `500`         | unknown server error |


-----

### Post Job Heartbeat Status
* **<code>/api/v1/jobs/actions/postJobStatus</code> (`POST`)**

???question "Query Parameters"
    <p class="tbd">TBD</p>

???abstract "Example Request Body"
        {
          "jobId": "sine-function-1",
          "status": {
            "jobId": "sine-function-1",
            "stageNum": 1,
            "workerIndex": 0,
            "workerNumber": 2,
            "type": "HEARTBEAT",
            "message": "heartbeat",
            "state": "Noop",
            "hostname": null,
            "timestamp": 1525813363585,
            "reason": "Normal",
            "payloads": [
              {
                "type": "SubscriptionState",
                "data": "false"
              },
              {
                "type": "IncomingDataDrop",
                "data": "{\"onNextCount\":0,\"droppedCount\":0}"
              }
            ]
          }
        }

???abstract "Example Response Format"
    <p class="tbd">TBD</p>

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `204`         | normal response |
    | `405`         | incorrect HTTP verb (use `POST` instead) |
    | `500`         | unknown server error |

-----

### Horizontally Scale a Stage
* **<code>/api/v1/jobs/<var>jobID</var>/actions/scaleStage</code> (`POST`)**

To manually scale a [Job]  [Processing Stage], that is, to alter the number of [workers] assigned to
that stage, send a `POST` command to the Mantis REST API endpoint
<code>/api/v1/jobs/<var>jobID</var>/actions/scaleStage</code> with a request body in the following
format:

!!! note
    You can only manually scale a Processing Stage if the Job was submitted with the `scalable` flag
    turned on.

???abstract "Example Request Body"
        {
          "JobId": "ValidatorDemo-33",
          "StageNumber": 1,
          "NumWorkers": 3
        }

    `NumWorkers` here is the *number* of workers you want to be assigned to the stage after the
    scaling action takes place (that is, it is *not* the *delta* by which you want to change the
    number of workers in the stage).

???abstract "Example Response Format"
    <p class="tbd">TBD</p>

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `204`         | normal response |
    | `404`         | no Job with that Job ID was found |
    | `405`         | incorrect HTTP verb (use `POST` instead) |
    | `500`         | unknown server error |

-----

### Resubmit a Worker
* **<code>/api/v1/jobs/<var>jobID</var>/actions/resubmitWorker</code> (`POST`)**

To resubmit a particular [worker] for a particular [Job], send a `POST` command to the Mantis REST
API endpoint <code>/api/v1/jobs/<var>jobID</var>/actions/resubmitWorker</code> with a request body
resembling the following:

???abstract "Example Request Body"
        {
          "user": "jschmoe",
          "workerNumber": 5,
          "reason": "test worker resubmit"
        }

    !!! note
        `workerNumber` is the worker *number* not the worker *index*.

???abstract "Example Response Format"
    <p class="tbd">TBD</p>

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `404`         | no Job with that Job ID was found |
    | `405`         | incorrect HTTP verb (use `POST` instead) |
    | `500`         | unknown server error |


-----

## Administrative Tasks

<p class="tbd">TBD</p>
### Return Job Master Information
* **<code>/api/v1/masterInfo</code> (`GET`)**

<p class="tbd">TBD</p>

???abstract "Example Response Body"
        {
          "hostname": "100.86.121.198",
          "hostIP": "100.86.121.198",
          "apiPort": 7101,
          "schedInfoPort": 7101,
          "apiPortV2": 7075,
          "apiStatusUri": "api/v1/jobs/actions/postJobStatus",
          "consolePort": 7101,
          "createTime": 1548803881867,
          "fullApiStatusUri": "http://100.86.121.198:7101/api/v1/jobs/actions/postJobStatus"
        }

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

### Return Job Master Configs
* **<code>/api/v1/masterConfigs</code> (`GET`)**

<p class="tbd">TBD</p>

???abstract "Example Response Body"
        [
          {
            "name": "JobConstraints",
            "value": "[\"UniqueHost\",\"ExclusiveHost\",\"ZoneBalance\",\"M4Cluster\",\"M3Cluster\",\"M5Cluster\"]"
          },
          {
            "name": "ScalingReason",
            "value": "[\"CPU\",\"Memory\",\"Network\",\"DataDrop\",\"KafkaLag\",\"UserDefined\",\"KafkaProcessed\"]"
          },
          {
            "name": "MigrationStrategyEnum",
            "value": "[\"ONE_WORKER\",\"PERCENTAGE\"]"
          },
          {
            "name": "WorkerResourceLimits",
            "value": "{\"maxCpuCores\":8,\"maxMemoryMB\":28000,\"maxNetworkMbps\":1024}"
          }
        ]

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

### Get Information about Agent Clusters
* **<code>/api/v1/agentClusters/</code> (`GET`)**

Returns information about active agent clusters (agent clusters are physical [AWS] resources that
Mantis connects to).

???abstract "Example Response Body"
    <span class="tbd">response body?</span>

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

### Activate or Deactivate an Agent Cluster
* **<code>/api/v1/agentClusters/</code> (`POST`)**

<p class="tbd">TBD</p>

???abstract "Example Request Body"
        ["mantisagent-staging-cl1-m5.2xlarge-1-v00"]
    
    (an array of active clusters)

???abstract "Example Response Body"
    <span class="tbd">response body?</span>

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `400`         | client failure |
    | `405`         | incorrect HTTP verb (use `POST` instead) |
    | `500`         | unknown server error |

-----

### Retrieve the Agent Cluster Scaling Policy
* **<code>/api/v1/agentClusters/autoScalePolicy</code> (`GET`)**

<p class="tbd">TBD</p>

???abstract "Example Response Body"
    <span class="tbd">response body?</span>

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

### Get Jobs and Host Information for an Agent Cluster
* **<code>/api/v1/agentClusters/jobs</code> (`GET`)**

<p class="tbd">TBD</p>

???abstract "Example Response Body"
    <span class="tbd">response body?</span>

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |


-----

## Streaming WebSocket/SSE Tasks

<p class="tbd">TBD: introduction</p>

## Stream Job Status Changes

* **<code>ws://<var>masterHost</var>:7101/api/v1/jobStatusStream/<var>jobID</var></code>**

<p class="tbd">TBD: description</p>

<p class="tbd">See also: mantisapi/websocket/</p>

???abstract "Example Response Stream Excerpt"
        {
          "status": {
            "jobId": "SPaaSBackpressureDemp-26",
            "stageNum": 1,
            "workerIndex": 0,
            "workerNumber": 7,
            "type": "INFO",
            "message": "SPaaSBackpressureDemp-26-worker-0-7 worker status update",
            "state": "StartInitiated",
            "hostname": null,
            "timestamp": 1549404217065,
            "reason": "Normal",
            "payloads": []
          }
        }

-----

### Stream Scheduling Information for a Job
* **<code>/api/v1/jobDiscoveryStream/<var>jobID</var></code> (`GET`)**
* **<code>/api/v1/jobs/schedulingInfo/<var>jobID</var></code> (`GET`)**

To retrieve an SSE stream of scheduling information for a particular job, send an HTTP `GET` command
to either the Mantis REST API endpoint <code>/api/v1/jobDiscoveryStream/<var>jobID</var></code> or <code>/api/v1/jobs/schedulingInfo/<var>jobID</var></code>.

???question "Query Parameters"
    | `jobDiscoveryStream` Query Parameter | Purpose |
    | ------------------------------------ | ------- |
    | `sendHB` (optional)                  | Indicate whether or not to send heartbeats (default=`false`). |

    | `jobs/schedulingInfo` Query Parameter | Purpose |
    | ------------------------------------- | ------- |
    | `jobId` (required)                   | The job ID of the job for which scheduling information is to be streamed. |

???abstract "Example Response Stream Excerpt"
    <span class="tbd">response body?</span>

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `404`         | no Job with that Job ID was found |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

### Get Streaming (SSE) Discovery Info for a Cluster
* **<code>/api/v1/jobClusters/discoveryInfoStream/<var>clusterName</var></code> (`GET`)**

<p class="tbd">describe</p>

???abstract "Example Response Format"
    <p class="tbd">TBD</p>

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

### Stream Job Information for a Cluster
* **<code>/api/v1/lastSubmittedJobIdStream/<var>clusterName</var></code> (`GET`)**

To retrieve an SSE stream of job information for a particular cluster, send an HTTP `GET` command
to the Mantis REST API endpoint
<code>/api/v1/lastSubmittedJobIdStream/<var>clusterName</var></code>.

???question "Query Parameters"
    | Query Parameter      | Purpose |
    | -------------------- | ------- |
    | `sendHB`             | Indicate whether or not to send heartbeats. |

???abstract "Example Response Stream Excerpt"
    <span class="tbd">response body?</span>

???info "Possible Response Codes"
    | Response Code | Reason |
    | ------------- | ------ |
    | `200`         | normal response |
    | `404`         | no Cluster with that Cluster name was found |
    | `405`         | incorrect HTTP verb (use `GET` instead) |
    | `500`         | unknown server error |

-----

### Stream Job Sink Messages
* **<code>/api/v1/jobConnectbyid/<var>jobID</var></code> (SSE)**
* **<code>/api/v1/jobConnectbyname/<var>jobName</var></code> (SSE)**

Collect messages from the job sinks and merge them into a single websocket or SSE stream.

???abstract "Example Response Stream Excerpt"
        data: {"x": 928400.000000, "y": 3.139934}
        data: {"x": 928402.000000, "y": -9.939772}
        data: {"x": 928404.000000, "y": 5.132876}
        data: {"x": 928406.000000, "y": 5.667712}

-----

### Submit Job and Stream Job Sink Messages
* **`/api/v1/jobsubmitandconnect` (`POST`)**

To submit a job and then collect messages from the job sinks and merge them into a single websocket or SSE stream, send a `POST` request to `/api/v1/jobsubmitandconnect` with a request body that describes the job.

???abstract "Example Request Body"
        {
          "name": "ValidatorDemo",
          "user": "jschmoe",
          "jobSla": {
            "durationType": "Perpetual",
            "runtimeLimitSecs": "0",
            "minRuntimeSecs": "0",
            "userProvidedType": ""
          }
        }

???abstract "Example Response Stream Excerpt"
        data: {"x": 928400.000000, "y": 3.139934}
        data: {"x": 928402.000000, "y": -9.939772}
        data: {"x": 928404.000000, "y": 5.132876}
        data: {"x": 928406.000000, "y": 5.667712}

-----

## Pagination

You can configure some endpoints to return their responses in *pages*. This is to say that rather
than returning all of the data responsive to a request all at once, the endpoint can return a
specific subset of the data at a time.

An endpoint that supports pagination will typically do so by means of two query parameters:

| Query Parameter | Purpose |
| --------------- | ------- |
| `pageSize`      | the maximum number of records to return in this request (default = 0, which means all records) |
| `offset`        | the record number to begin with in the set of records to return in this request |

So, for example, you might make consecutive requests for…

1. <code><var>endpoint</var>?pageSize=10&offset=0</code>
1. <code><var>endpoint</var>?pageSize=10&offset=10</code>
1. <code><var>endpoint</var>?pageSize=10&offset=20</code>

…and so forth, until you reach the final page of records.

When you request records in a paginated fashion like this, the Mantis API will enclose them in a
JSON structure like the following:

<div class="codeblock">
{
  "list": [
    {
      "Id": <var>originalOffset</var>,
      ⋮
    },
    {
      "Id": <var>originalOffset+1</var>,
      ⋮
    },
    ⋮
  ],
  "prev": "/api/v1/<var>endpoint</var>?pageSize=<var>pageSize</var>&offset=<var>originalOffset+pageSize</var>",
  "next": "/api/v1/<var>endpoint</var>?pageSize=<var>pageSize</var>&offset=<var>originalOffset−pageSize</var>"
}
</div>

For example:

```json
{
  "list": [
    {
      "Id": 101,
      ⋮
    },
    {
      "Id": 102,
      ⋮
    }
  ],
  "next": "/api/v1/someResource?pageSize=20&offset=120",
  "prev": "/api/v1/someResource?pageSize=20&offset=80"
}
```

`prev` and/or `next` will be `null` if there is no previous or next page, that is, if you are at the
first and/or last page in the pagination.

<!-- Do not edit below this line -->
<!-- START -->
<!-- This section comes from the file "reference_links". It is automagically inserted into other files by means of the "refgen" script, also in the "docs/" directory. Edit this section only in the "reference_links" file, not in any of the other files in which it is included, or your edits will be overwritten. -->
[artifact]:                ../glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[artifacts]:               ../glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[artifact file]:           ../glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[artifact files]:          ../glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[autoscale]:               ../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[autoscaled]:              ../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[autoscales]:              ../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[autoscaling]:             ../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[scalable]:                ../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[AWS]:                     javascript:void(0)          "Amazon Web Services"
[backpressure]:            ../glossary#backpressure      "Backpressure refers to a set of possible strategies for coping with ReactiveX Observables that produce items more rapidly than their observers consume them."
[Binary compression]:      ../glossary#binarycompression
[broadcast]:               ../glossary#broadcast         "In broadcast mode, each worker of your job gets all the data from all workers of the Source Job rather than having that data distributed equally among the workers of your job."
[broadcast mode]:          ../glossary#broadcast         "In broadcast mode, each worker of your job gets all the data from all workers of the Source Job rather than having that data distributed equally among the workers of your job."
[Cassandra]:               ../glossary#cassandra         "Apache Cassandra is an open source, distributed database management system."
[cluster]:                 ../glossary#cluster           "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[clusters]:                ../glossary#cluster           "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[cold]:                    ../glossary#cold              "A cold ReactiveX Observable waits until an observer subscribes to it before it begins to emit items. This means the observer is guaranteed to see the whole Observable sequence from the beginning. This is in contrast to a hot Observable, which may begin emitting items as soon as it is created, even before observers have subscribed to it."
[cold Observable]:         ../glossary#cold              "A cold ReactiveX Observable waits until an observer subscribes to it before it begins to emit items. This means the observer is guaranteed to see the whole Observable sequence from the beginning. This is in contrast to a hot Observable, which may begin emitting items as soon as it is created, even before observers have subscribed to it."
[cold Observables]:        ../glossary#cold              "A cold ReactiveX Observable waits until an observer subscribes to it before it begins to emit items. This means the observer is guaranteed to see the whole Observable sequence from the beginning. This is in contrast to a hot Observable, which may begin emitting items as soon as it is created, even before observers have subscribed to it."
[component]:               ../glossary#component         "A Mantis Job is composed of three types of component: a Source, one or more Processing Stages, and a Sink."
[components]:              ../glossary#component         "A Mantis Job is composed of three types of component: a Source, one or more Processing Stages, and a Sink."
[custom source]:           ../glossary#customsource      "In contrast to a Source Job, which is a built-in variety of Source component designed to pull data from a common sort of data source, a custom source typically accesses data from less-common sources or has unusual delivery guarantee semantics."
[custom sources]:          ../glossary#customsource      "In contrast to a Source Job, which is a built-in variety of Source component designed to pull data from a common sort of data source, a custom source typically accesses data from less-common sources or has unusual delivery guarantee semantics."
[executor]:                ../glossary#executor          "The stage executor is responsible for loading the bytecode for a Mantis Job and then executing its stages and workers in a coordinated fashion. In the Mesos UI, workers are also referred to as executors."
[executors]:               ../glossary#executor          "The stage executor is responsible for loading the bytecode for a Mantis Job and then executing its stages and workers in a coordinated fashion. In the Mesos UI, workers are also referred to as executors."
[fast property]: ../glossary#fastproperties "Fast properties allow you to change the behavior of Netflix services without recompiling and redeploying them."
[fast properties]: ../glossary#fastproperties "Fast properties allow you to change the behavior of Netflix services without recompiling and redeploying them."
[Fenzo]:                   ../glossary#fenzo             "Fenzo is a Java library that implements a generic task scheduler for Mesos frameworks."
[grouped]:                 ../glossary#grouped           "Grouped data is distinguished from scalar data in that each datum is accompanied by a key that indicates what group it belongs to. Grouped data can be processed by a RxJava GroupedObservable or by a MantisGroup."
[grouped data]:            ../glossary#grouped           "Grouped data is distinguished from scalar data in that each datum is accompanied by a key that indicates what group it belongs to. Grouped data can be processed by a RxJava GroupedObservable or by a MantisGroup."
[GRPC]:                    ../glossary#grpc              "gRPC is an open-source RPC framework using Protocol Buffers."
[hot]:                     ../glossary#hot               "A hot ReactiveX Observable may begin emitting items as soon as it is created, even before observers have subscribed to it. This means the observer may miss items that were emitted before the observer subscribed. This is in contrast to a cold Observable, which waits until an observer subscribes to it before it begins to emit items."
[hot Observable]:          ../glossary#hot               "A hot ReactiveX Observable may begin emitting items as soon as it is created, even before observers have subscribed to it. This means the observer may miss items that were emitted before the observer subscribed. This is in contrast to a cold Observable, which waits until an observer subscribes to it before it begins to emit items."
[hot Observables]:         ../glossary#hot               "A hot ReactiveX Observable may begin emitting items as soon as it is created, even before observers have subscribed to it. This means the observer may miss items that were emitted before the observer subscribed. This is in contrast to a cold Observable, which waits until an observer subscribes to it before it begins to emit items."
[JMC]:                     ../glossary#jmc               "Java Mission Control is a tool from Oracle with which developers can monitor and manage Java applications."
[job]:                     ../glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[jobs]:                    ../glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[Mantis job]:              ../glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[Mantis jobs]:             ../glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[job cluster]:             ../glossary#jobcluster        "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[job clusters]:            ../glossary#jobcluster        "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[Job Master]:              ../glossary#jobmaster         "If a job is configured with autoscaling, Mantis will add a Job Master component to it as its initial component. This component will send metrics back to Mantis to help it govern the autoscaling process."
[Mantis Master]:           ../glossary#mantismaster      "The Mantis Master coordinates the execution of [Mantis Jobs] and starts the services on each Worker."
[Kafka]:                   ../glossary#kafka             "Apache Kafka is a large-scale, distributed streaming platform."
[keyed data]:              ../glossary#keyed             "Grouped (or keyed) data is distinguished from scalar data in that each datum is accompanied by a key that indicates what group it belongs to. Grouped data can be processed by a RxJava GroupedObservable or by a MantisGroup."
[Keystone]:                ../glossary#keystone          "Keystone is Netflix’s data backbone, a stream processing platform that focuses on data analytics."
[label]:                   ../glossary#label             "A label is a text key/value pair that you can add to a Job Cluster or to an individual Job to make it easier to search for or group."
[labels]:                  ../glossary#label             "A label is a text key/value pair that you can add to a Job Cluster or to an individual Job to make it easier to search for or group."
[Log4j]:                   ../glossary#log4j             "Log4j is a Java-based logging framework."
[Apache Mesos]:            ../glossary#mesos             "Apache Mesos is an open-source technique for balancing resources across frameworks in clusters."
[Mesos]:                   ../glossary#mesos             "Apache Mesos is an open-source technique for balancing resources across frameworks in clusters."
[metadata]:                ../glossary#metadata          "Mantis inserts metadata into its Job payload. This may include information about where the data came from, for instance. You can define additional metadata to include in the payload when you establish the Job Cluster."
[meta message]:            ../glossary#metamessage       "A Source Job may occasionally inject meta messages into its data stream that indicate things like data drops."
[meta messages]:           ../glossary#metamessage       "A Source Job may occasionally inject meta messages into its data stream that indicate things like data drops."
[migration strategy]:      ../glossary#migration
[migration strategies]:    ../glossary#migration
[MRE]:                     ../glossary#mre               "Mantis Publish (a.k.a. Mantis Realtime Events, or MRE) is a library that your application can use to stream events into Mantis while respecting MQL filters."
[Mantis Publish]:          ../glossary#mantispublish     "Mantis Publish is a library that your application can use to stream events into Mantis while respecting MQL filters."
[Mantis Query Language]:   ../glossary#mql               "You use Mantis Query Language to define filters and other data processing that Mantis applies to a Source data stream at its point of origin, so as to reduce the amount of data going over the wire."
[MQL]:                     ../glossary#mql               "You use Mantis Query Language to define filters and other data processing that Mantis applies to a Source data stream at its point of origin, so as to reduce the amount of data going over the wire."
[Observable]:              ../glossary#observable        "In ReactiveX an Observable is the method of processing a stream of data in a way that facilitates its transformation and consumption by observers. Observables come in hot and cold varieties. There is also a GroupedObservable that is specialized to grouped data."
[Observables]:             ../glossary#observable        "In ReactiveX an Observable is the method of processing a stream of data in a way that facilitates its transformation and consumption by observers. Observables come in hot and cold varieties. There is also a GroupedObservable that is specialized to grouped data."
[parameter]:               ../glossary#parameter         "A Mantis Job may accept parameters that modify its behavior. You can define these in your Job Cluster definition, and set their values on a per-Job basis."
[parameters]:              ../glossary#parameter         "A Mantis Job may accept parameters that modify its behavior. You can define these in your Job Cluster definition, and set their values on a per-Job basis."
[Processing Stage]:        ../glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[Processing Stages]:       ../glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[stage]:                   ../glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[stages]:                  ../glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[property]:                ../glossary#property          "A property is a particular named data value found within events in an event stream."
[properties]:              ../glossary#property          "A property is a particular named data value found within events in an event stream."
[Reactive Stream]:         ../glossary#reactivestreams   "Reactive Streams is the latest advance of the ReactiveX project. It is an API for manipulating streams of asynchronous data in a non-blocking fashion, with backpressure."
[Reactive Streams]:        ../glossary#reactivestreams   "Reactive Streams is the latest advance of the ReactiveX project. It is an API for manipulating streams of asynchronous data in a non-blocking fashion, with backpressure."
[ReactiveX]:               ../glossary#reactivex         "ReactiveX is a software technique for transforming, combining, reacting to, and managing streams of data. RxJava is an example of a library that implements this technique."
[RxJava]:                  ../glossary#rxjava            "RxJava is the Java implementation of ReactiveX, a software technique for transforming, combining, reacting to, and managing streams of data."
[downsample]:              ../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[sample]:                  ../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[sampled]:                 ../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[samples]:                 ../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[sampling]:                ../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[scalar]:                  ../glossary#scalar            "Scalar data is distinguished from keyed or grouped data in that it is not categorized into groups by key. Scalar data can be processed by an ordinary ReactiveX Observable."
[scalar data]:             ../glossary#scalar            "Scalar data is distinguished from keyed or grouped data in that it is not categorized into groups by key. Scalar data can be processed by an ordinary ReactiveX Observable."
[Sink]:                    ../glossary#sink              "The Sink is the final component of a Mantis Job. It takes the Observable that has been transformed by the Processing Stage and outputs it in the form of a new data stream."
[Sinks]:                   ../glossary#sink              "The Sink is the final component of a Mantis Job. It takes the Observable that has been transformed by the Processing Stage and outputs it in the form of a new data stream."
[Sink component]:          ../glossary#sink              "The Sink is the final component of a Mantis Job. It takes the Observable that has been transformed by the Processing Stage and outputs it in the form of a new data stream."
[service-level agreement]:  ../glossary#sla               "A service-level agreement, in the Mantis context, is defined on a per-Cluster basis. You use it to configure how many Jobs in the cluster will be in operation at any time, among other things."
[service-level agreements]: ../glossary#sla               "A service-level agreement, in the Mantis context, is defined on a per-Cluster basis. You use it to configure how many Jobs in the cluster will be in operation at any time, among other things."
[SLA]:                     ../glossary#sla               "A service-level agreement, in the Mantis context, is defined on a per-Cluster basis. You use it to configure how many Jobs in the cluster will be in operation at any time, among other things."
[Source]:                  ../glossary#source            "The Source component of a Mantis Job fetches data from a source outside of Mantis and makes it available to the Processing Stage component in the form of an RxJava Observable. There are two varieties of Source: a Source Job and a custom source."
[Sources]:                 ../glossary#source            "The Source component of a Mantis Job fetches data from a source outside of Mantis and makes it available to the Processing Stage component in the form of an RxJava Observable. There are two varieties of Source: a Source Job and a custom source."
[Source Job]:              ../glossary#sourcejob         "A Source Job is a Mantis Job that you can use as a Source, which wraps a data source external to Mantis and makes it easier for you to create a job that observes its data."
[Source Jobs]:             ../glossary#sourcejob         "A Source Job is a Mantis Job that you can use as a Source, which wraps a data source external to Mantis and makes it easier for you to create a job that observes its data."
[Spinnaker]: ../glossary#spinnaker "Spinnaker is a set of resources that help you deploy and manage resources in the cloud."
[SSE]:                     ../glossary#sse               "Server-sent events (SSE) are a way for a browser to receive automatic updates from a server through an HTTP connection. Mantis includes an SSE Sink."
[server-sent event]:       ../glossary#sse               "Server-sent events (SSE) are a way for a browser to receive automatic updates from a server through an HTTP connection. Mantis includes an SSE Sink."
[server-sent events]:      ../glossary#sse               "Server-sent events (SSE) are a way for a browser to receive automatic updates from a server through an HTTP connection. Mantis includes an SSE Sink."
[transform]:               ../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transformed]:             ../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transforms]:              ../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transformation]:          ../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transformations]:         ../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transient]:               ../glossary#transient         "A transient (or ephemeral) Mantis Job is automatically killed by Mantis after a certain amount of time has passed since the last subscriber to the job disconnects."
[transient job]:           ../glossary#transient         "A transient (or ephemeral) Mantis Job is automatically killed by Mantis after a certain amount of time has passed since the last subscriber to the job disconnects."
[transient jobs]:          ../glossary#transient         "A transient (or ephemeral) Mantis Job is automatically killed by Mantis after a certain amount of time has passed since the last subscriber to the job disconnects."
[WebSocket]:               ../glossary#websocket         "WebSocket is a two-way, interactive communication channel that works over HTTP. In the Mantis context, it is an alternative to SSE."
[Worker]:                  ../glossary#worker            "A worker is the smallest unit of work that is scheduled within a Mantis component. You can configure how many resources Mantis allocates to each worker, and Mantis will adjust the number of workers your Mantis component needs based on its autoscaling policy."
[Workers]:                 ../glossary#worker            "A worker is the smallest unit of work that is scheduled within a Mantis component. You can configure how many resources Mantis allocates to each worker, and Mantis will adjust the number of workers your Mantis component needs based on its autoscaling policy."
[Zookeeper]:               ../glossary#zookeeper         "Apache Zookeeper is an open-source server that maintains configuration information and other services required by distributed applications."
<!-- END -->
