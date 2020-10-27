/*
 *
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.mantisrx.runtime.api;

import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.runtime.api.lifecycle.LifecycleNoOp;
import io.mantisrx.runtime.api.lifecycle.ServiceLocator;
import io.mantisrx.runtime.api.parameter.Parameters;
import io.mantisrx.runtime.common.WorkerInfo;
import io.mantisrx.runtime.common.WorkerMap;
import org.reactivestreams.Publisher;


/**
 * <p>
 * An instance of this class is made available to the user job during the <code>init()</code> and <code>call</code> methods
 * of a stage. This object provides information related to the current job
 * <p>
 * </p>
 */
public class Context {


    // Callback invoked during job shutdown
    private final Runnable completeAndExitCallback;
    private MetricsRegistry metricsRegistry;
    // Parameters associated with this job
    private Parameters parameters = new Parameters();
    // Service Locator allows the user to access guice modules loaded with this job
    private ServiceLocator serviceLocator = new LifecycleNoOp().getServiceLocator();
    // Information related to the current worker
    private WorkerInfo workerInfo;
    // details of all workers of the current job
    private Publisher<WorkerMap> workerMapPublisher;

    /**
     * For testing only
     */
    public Context() {
        completeAndExitCallback = () -> { };
    }

//    public Context(Parameters parameters, ServiceLocator serviceLocator, WorkerInfo workerInfo,
//                   MetricsRegistry metricsRegistry, Runnable completeAndExitAction) {
//        this(parameters, serviceLocator, workerInfo, metricsRegistry, completeAndExitAction, Observable.empty());
//    }

    public Context(Parameters parameters, ServiceLocator serviceLocator, WorkerInfo workerInfo,
                   MetricsRegistry metricsRegistry, Runnable completeAndExitCallback,
                   Publisher<WorkerMap> workerMapPublisher) {
        this.parameters = parameters;
        this.serviceLocator = serviceLocator;
        this.workerInfo = workerInfo;
        this.metricsRegistry = metricsRegistry;
        if (completeAndExitCallback == null)
            throw new IllegalArgumentException("Null complete action provided in Context contructor");
        this.completeAndExitCallback = completeAndExitCallback;
        this.workerMapPublisher = workerMapPublisher;
    }


    public MetricsRegistry getMetricsRegistry() {
        return metricsRegistry;
    }

    /**
     * Returns the Job Parameters associated with the current job
     *
     * @return
     */

    public Parameters getParameters() {
        return parameters;
    }

    public ServiceLocator getServiceLocator() {
        return serviceLocator;
    }

    /**
     * Returns the JobId of the current job.
     *
     * @return
     */
    public String getJobId() {
        return workerInfo.getJobId();
    }

    /**
     * Returns information related to the current worker
     *
     * @return
     */
    public WorkerInfo getWorkerInfo() {
        return workerInfo;
    }

    /**
     * Used to convey to the Mantis Master that the job has finished its work and wishes to be shutdown.
     */
    public void completeAndExit() {
        completeAndExitCallback.run();
    }


    /**
     * Get details of all worker locations
     *
     * @return Publisher of WorkerMap for the current Job. The user can use this Observable
     * to track the location information of all the workers of the current job.
     */
    public Publisher<WorkerMap> getWorkerMapPublisher() {
        return this.workerMapPublisher;
    }


}
