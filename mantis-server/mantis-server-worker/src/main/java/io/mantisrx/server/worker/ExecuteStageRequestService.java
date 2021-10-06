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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.functions.Func1;
import rx.subjects.PublishSubject;


public class ExecuteStageRequestService extends BaseService {

    private static final Logger logger = LoggerFactory.getLogger(ExecuteStageRequestService.class);
    private Observable<WrappedExecuteStageRequest> executeStageRequestObservable;
    private Observer<Observable<Status>> tasksStatusObserver;
    private WorkerExecutionOperations executionOperations;
    private Optional<String> jobProviderClass;
    private Job mantisJob;
    private Subscription subscription;

    public ExecuteStageRequestService(Observable<WrappedExecuteStageRequest> executeStageRequestObservable,
                                      Observer<Observable<Status>> tasksStatusObserver,
                                      WorkerExecutionOperations executionOperations,
                                      Optional<String> jobProviderClass,
                                      Job mantisJob) {
        this.executeStageRequestObservable = executeStageRequestObservable;
        this.tasksStatusObserver = tasksStatusObserver;
        this.executionOperations = executionOperations;
        this.jobProviderClass = jobProviderClass;
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
                        URL jobJarUrl = executeStageRequest.getJobJarUrl();
                        // pull out file name from URL
                        String jobJarFile = jobJarUrl.getFile();
                        String jarName = jobJarFile.substring(jobJarFile.lastIndexOf('/') + 1);

                        // path used to store job on local disk
                        Path path = Paths.get("/tmp", "mantis-jobs", executeStageRequest.getJobId(),
                                Integer.toString(executeStageRequest.getWorkerNumber()),
                                "libs");
                        URL pathLocation = null;
                        try {
                            pathLocation = Paths.get(path.toString(), "*").toUri().toURL();
                        } catch (MalformedURLException e1) {
                            logger.error("Failed to convert path location to URL", e1);
                            executeRequest.getStatus().onError(e1);
                            return Observable.empty();
                        }
                        logger.info("Creating job classpath with pathLocation " + pathLocation);
                        ClassLoader cl = URLClassLoader.newInstance(new URL[] {pathLocation});
                        try {
                            if (mantisJob == null) {
                                if (jobProviderClass.isPresent()) {
                                    logger.info("loading job main class " + jobProviderClass.get());
                                    final Class clazz = Class.forName(jobProviderClass.get());
                                    final MantisJobProvider jobProvider = (MantisJobProvider) clazz.newInstance();
                                    mantisJob = jobProvider.getJobInstance();
                                } else {
                                    logger.info("using serviceLoader to get job instance");
                                    ServiceLoader<MantisJobProvider> provider = ServiceLoader.load(MantisJobProvider.class, cl);
                                    // should only be a single provider, check is made in master
                                    MantisJobProvider mantisJobProvider = provider.iterator().next();
                                    mantisJob = mantisJobProvider.getJobInstance();
                                }
                            }
                        } catch (Throwable e) {
                            logger.error("Failed to load job class", e);
                            executeRequest.getStatus().onError(e);
                            return Observable.empty();
                        }
                        logger.info("Executing job");
                        return Observable.just(new ExecutionDetails(executeRequest.getExecuteRequest(),
                                executeRequest.getStatus(), mantisJob, cl, executeStageRequest.getParameters()));
                    }
                })
                .subscribe(new Observer<ExecutionDetails>() {
                    @Override
                    public void onCompleted() {
                        logger.error("Execute stage observable completed"); // should never occur
                        executionOperations.shutdownStage();
                    }

                    @Override
                    public void onError(Throwable e) {
                        logger.error("Execute stage observable threw exception", e);
                    }

                    @Override
                    public void onNext(final ExecutionDetails executionDetails) {
                        logger.info("Executing stage for job ID: " + executionDetails.getExecuteStageRequest().getRequest().getJobId());
                        Thread t = new Thread("Mantis Worker Thread for " + executionDetails.getExecuteStageRequest().getRequest().getJobId()) {
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
    }

    @Override
    public void enterActiveMode() {}

}
