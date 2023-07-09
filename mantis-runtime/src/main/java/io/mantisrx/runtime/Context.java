/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.runtime;

import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.runtime.lifecycle.LifecycleNoOp;
import io.mantisrx.runtime.lifecycle.ServiceLocator;
import io.mantisrx.runtime.parameter.Parameters;
import javax.annotation.Nullable;
import rx.Observable;
import rx.functions.Action0;
import rx.subjects.BehaviorSubject;


/**
 * <p>
 * An instance of this class is made available to the user job during the <code>init()</code> and <code>call</code> methods
 * of a stage. This object provides information related to the current job
 * <p>
 * </p>
 */
public class Context {


    // Callback invoked during job shutdown
    private final Action0 completeAndExitAction;
    private MetricsRegistry metricsRegistry;
    // Parameters associated with this job
    private Parameters parameters = new Parameters();
    // Service Locator allows the user to access guice modules loaded with this job
    private ServiceLocator serviceLocator = new LifecycleNoOp().getServiceLocator();
    // Information related to the current worker
    private WorkerInfo workerInfo;
    // No longer used.
    private Observable<Boolean> prevStageCompletedObservable = BehaviorSubject.create(false);
    // An Observable providing details of all workers of the current job
    private Observable<WorkerMap> workerMapObservable = Observable.empty();
    // Custom class loader
    @Nullable
    private final ClassLoader classLoader;

    /**
     * For testing only
     */
    public Context() {
        this.classLoader = null;
        completeAndExitAction = () -> {
            // no-op
        };
    }

    public Context(Parameters parameters, ServiceLocator serviceLocator, WorkerInfo workerInfo,
                   MetricsRegistry metricsRegistry, Action0 completeAndExitAction) {
        this(parameters, serviceLocator, workerInfo, metricsRegistry, completeAndExitAction, Observable.empty(), null);
    }

    public Context(Parameters parameters, ServiceLocator serviceLocator, WorkerInfo workerInfo,
                   MetricsRegistry metricsRegistry, Action0 completeAndExitAction, Observable<WorkerMap> workerMapObservable, @Nullable ClassLoader classLoader) {
        this.parameters = parameters;
        this.serviceLocator = serviceLocator;
        this.workerInfo = workerInfo;
        this.metricsRegistry = metricsRegistry;
        if (completeAndExitAction == null)
            throw new IllegalArgumentException("Null complete action provided in Context contructor");
        this.completeAndExitAction = completeAndExitAction;
        this.workerMapObservable = workerMapObservable;
        this.classLoader = classLoader;
    }


    public MetricsRegistry getMetricsRegistry() {
        return metricsRegistry;
    }

    /**
     * Returns the Job Parameters associated with the current job
     *
     * @return Parameters
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
     * @return String
     */
    public String getJobId() {
        return workerInfo.getJobId();
    }

    /**
     * Returns information related to the current worker
     *
     * @return WorkerInfo
     */
    public WorkerInfo getWorkerInfo() {
        return workerInfo;
    }

    /**
     * <P> Note: This method is deprecated as it is not expected for workers of a stage to complete. If the task
     * of the Job is complete the workers should invoke <code>context.completeAndExit()</code> api.
     * <p>
     * Get a boolean observable that will emit the state of whether or not all workers in the previous
     * stage have completed. Workers in the previous stage that have failed and are going to be replaced by new
     * workers will not trigger this event.
     * <p>
     * The observable will be backed by an rx.subjects.BehaviorSubject so you can expect to
     * always get the latest state upon subscription. They are may be multiple emissions of <code>false</code> before
     * an emission of <code>true</code> when the previous stage completes all of its workers.
     * <p>
     * In workers for first stage of the job, this observable will never emit a value of <code>true</code>.
     * <p>
     * Generally, this is useful only for batch style jobs (those with their sla of job duration type set to
     * {@link MantisJobDurationType#Transient}).
     *
     * @return A boolean observable to indicate whether or not the previous stage has completed all of its workers.
     */
    @Deprecated
    public Observable<Boolean> getPrevStageCompletedObservable() {
        return prevStageCompletedObservable;
    }

    @Deprecated
    public void setPrevStageCompletedObservable(Observable<Boolean> prevStageCompletedObservable) {
        this.prevStageCompletedObservable = prevStageCompletedObservable;
    }

    /**
     * Used to convey to the Mantis Master that the job has finished its work and wishes to be shutdown.
     */
    public void completeAndExit() {
        completeAndExitAction.call();
    }


    /**
     * Returns an Observable of WorkerMap for the current Job. The user can use this Observable
     * to track the location information of all the workers of the current job.
     *
     * @return Observable
     */
    public Observable<WorkerMap> getWorkerMapObservable() {
        return this.workerMapObservable;
    }

    @Nullable
    public ClassLoader getClassLoader() { return this.classLoader; }
}
