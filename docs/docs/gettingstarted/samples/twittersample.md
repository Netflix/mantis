# Twitter sample
The Twitter sample demonstrates sourcing data from an external source (twitter in this case) and 
calculates the number of occurrences of a word within a hopping window. 

## Prerequisites

A `TwitterSample` Job Cluster exists.

> **Note** If you are following the [Mantis Cluster using Docker](../docker.md) instructions this
>should be already setup. 

Twitter credentials to be used to connect to Twitter.

>>If you don't already have a twitter application
You can create one here [here](https://developer.twitter.com/en/apps)
The *Keys and Tokens* section should list the credentials needed for this application. 

## Running the sample
Let us try submitting this job.

1. Click on TwitterSample from the clusters page.

2. Click the `Submit` green button on the top right.

    This will open up a submit screen that will 
    allow you to override Resource configurations as well as parameter values.
 
Let us scroll down to the parameters section.  

![Submit Job](../../images/twitter-submit.png)

Here we fill in the required parameters for this job. These include

* Twitter consumer key
* Twitter consumer secret
* Twitter token 
* Twitter token secret 

### View output of the job

If all goes well your job would go into `Launched` state.
Scroll to the bottom and click on `Start`
You should see output of the Twitter job being streamed below

![Job Launched](../../images/twitter-running.png)


## Terminate the job
To stop the job click on the red `Kill Job` button on the top right corner.

* Explore the [code](https://github.com/Netflix/mantis-examples/tree/master/twitter-sample)

* Checkout out the other samples