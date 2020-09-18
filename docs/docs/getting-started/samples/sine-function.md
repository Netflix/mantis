The Sine function sample is a very simple job that generates a set of x and y coordinates of a sine
wave. 

## Prerequisites

A SineFunction Job Cluster exists.

> **Note** If you are following the [Mantis Cluster using Docker](../tutorials/docker.md) instructions this
>will be already set up. 

## Running the sample

1. Go to the [clusters page](https://netflix.github.io/mantis-ui/#/clusters) page and 
Click on `SineFunction`

2. On the Job Cluster detail page. Click the `Submit` green button on the top right.

    This will open up a submit screen that will 
    allow you to override Resource configurations as well as parameter values.
 
![Submit Job1](../../images/submit_job1.png)

Let us skip all that and scroll directly to the bottom and hit the `Submit` button on the bottom left.

![Submit Job](../../images/submit_job.png)

View output of the job

If all goes well your job would go into `Launched` state.

![Job Launched](../../images/job_launched.png)

Scroll to the bottom and in the `Job Output` section click on `Start`

You should see output of the Sine function job being streamed below

```
Oct 4 2019, 03:55:39.338 PM - {"x": 26.000000, "y": 7.625585}
```

![Running Job](../../images/running_job.png)

## Terminate the job
To stop the job click on the red `Kill Job` button on the top right corner.

## Next Steps

* Explore the [code](https://github.com/Netflix/mantis-examples/tree/master/sine-function)

* Checkout out the other samples
