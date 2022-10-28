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

package io.mantisrx.server.worker;

import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.WrappedExecuteStageRequest;
import java.io.IOException;
import java.util.Optional;
import java.util.ServiceLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

public class ExecuteStageRequestService extends BaseService {

    private static final Logger logger = LoggerFactory.getLogger(ExecuteStageRequestService.class);
    private final Observable<WrappedExecuteStageRequest> executeStageRequestObservable;
    private final Observer<Observable<Status>> tasksStatusObserver;
    private final WorkerExecutionOperations executionOperations;
    private final Optional<String> jobProviderClass;

    private final Optional<Job> mantisJob;

    /**
     * This class loader should be set as the context class loader for threads that may dynamically
     * load user code.
     */
    private final UserCodeClassLoader userCodeClassLoader;
    private Subscription subscription;

    public ExecuteStageRequestService(
        Observable<WrappedExecuteStageRequest> executeStageRequestObservable,
        Observer<Observable<Status>> tasksStatusObserver,
        WorkerExecutionOperations executionOperations,
        Optional<String> jobProviderClass,
        UserCodeClassLoader userCodeClassLoader,
        Optional<Job> mantisJob) {
        this.executeStageRequestObservable = executeStageRequestObservable;
        this.tasksStatusObserver = tasksStatusObserver;
        this.executionOperations = executionOperations;
        this.jobProviderClass = jobProviderClass;
        this.userCodeClassLoader = userCodeClassLoader;
        this.mantisJob = mantisJob;
    }

    @Override
    public void start() {
        subscription = executeStageRequestObservable
                // map to request with status observer
                .map(new Func1<WrappedExecuteStageRequest, TrackedExecuteStageRequest>() {
                    @Override
                    public TrackedExecuteStageRequest call(
                            WrappedExecuteStageRequest executeRequest) {
                        PublishSubject<Status> statusSubject = PublishSubject.create();
                        tasksStatusObserver.onNext(statusSubject);
                        return new TrackedExecuteStageRequest(executeRequest, statusSubject);
                    }
                })
                // get provider from jar, return tracked MantisJob
                .flatMap(new Func1<TrackedExecuteStageRequest, Observable<ExecutionDetails>>() {
                    @SuppressWarnings("rawtypes") // raw type due to unknown type for mantis job
                    @Override
                    public Observable<ExecutionDetails> call(TrackedExecuteStageRequest executeRequest) {

                        ExecuteStageRequest executeStageRequest =
                                executeRequest.getExecuteRequest().getRequest();

                        Job mantisJob;
                        ClassLoader cl = null;
                        try {
                            if (!ExecuteStageRequestService.this.mantisJob.isPresent()) {
                                // first of all, get a user-code classloader
                                // this may involve downloading the job's JAR files and/or classes
                                logger.info("Loading JAR files for task {}.", this);

                                cl = userCodeClassLoader.asClassLoader();
                                if (jobProviderClass.isPresent()) {
                                    logger.info("loading job main class " + jobProviderClass.get());
                                    final MantisJobProvider jobProvider = InstantiationUtil.instantiate(
                                        jobProviderClass.get(), MantisJobProvider.class, cl);
                                    mantisJob = jobProvider.getJobInstance();
                                } else {
                                    logger.info("using serviceLoader to get job instance");
                                    ServiceLoader<MantisJobProvider> provider = ServiceLoader.load(
                                        MantisJobProvider.class, cl);
                                    // should only be a single provider, check is made in master
                                    MantisJobProvider mantisJobProvider = provider.iterator()
                                        .next();
                                    mantisJob = mantisJobProvider.getJobInstance();
                                }
                            } else {
                                cl = userCodeClassLoader.asClassLoader();
                                mantisJob = ExecuteStageRequestService.this.mantisJob.get();
                            }
                        } catch (Throwable e) {
                            logger.error("Failed to load job class", e);
                            executeRequest.getStatus().onError(e);
                            return Observable.empty();
                        }
                        logger.info("Executing job {}", mantisJob);
                        return Observable.just(new ExecutionDetails(executeRequest.getExecuteRequest(),
                                executeRequest.getStatus(), mantisJob, cl, executeStageRequest.getParameters()));
                    }
                })
                .subscribe(new Observer<ExecutionDetails>() {
                    @Override
                    public void onCompleted() {
                        logger.error("Execute stage observable completed"); // should never occur
                        try {
                            executionOperations.shutdownStage();
                        } catch (IOException e) {
                            logger.error("Failed to close stage cleanly", e);
                        }
                    }

                    @Override
                    public void onError(Throwable e) {
                        logger.error("Execute stage observable threw exception", e);
                    }

                    @Override
                    public void onNext(final ExecutionDetails executionDetails) {
                        logger.info("Executing stage for job ID: " + executionDetails.getExecuteStageRequest().getRequest().getJobId());
                        Thread t = new Thread("mantis-worker-thread-" + executionDetails.getExecuteStageRequest().getRequest().getJobId()) {
                            @Override
                            public void run() {
                                // Add ports here
                                try {
                                    executionOperations.executeStage(executionDetails);
                                } catch (Throwable t) {
                                    logger.error("Failed to execute job stage", t);
                                }
                            }
                        };
                        // rebuild class path, job jar + parent class loader
                        // job jar to reference third party libraries and resources
                        // parent to reference worker code
                        ClassLoader cl = executionDetails.getClassLoader();
                        t.setContextClassLoader(cl);
                        t.setDaemon(true);
                        t.start();
                    }
                });
    }

    @Override
    public void shutdown() {
        subscription.unsubscribe();
        try {
            logger.info("Shutting down execution operations");
            executionOperations.shutdownStage();
        } catch (IOException e) {
            logger.error("Failed to close cleanly", e);
        }
    }

    @Override
    public void enterActiveMode() {}
}
