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
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Optional;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.UserCodeClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

@Slf4j
public class ExecuteStageRequestService extends BaseService {

    private static final Logger logger = LoggerFactory.getLogger(ExecuteStageRequestService.class);
    private final Observable<WrappedExecuteStageRequest> executeStageRequestObservable;
    private final Observer<Observable<Status>> tasksStatusObserver;
    private final WorkerExecutionOperations executionOperations;
    private final Optional<String> jobProviderClass;

    private final ClassLoaderHandle classLoaderHandle;
    /** The classpaths used by this task. */
    private final Collection<URL> requiredClasspaths;
    private final Job mantisJob;

    /**
     * This class loader should be set as the context class loader for threads that may dynamically
     * load user code.
     */
    private UserCodeClassLoader userCodeClassLoader;
    private Subscription subscription;

    public ExecuteStageRequestService(
        Observable<WrappedExecuteStageRequest> executeStageRequestObservable,
        Observer<Observable<Status>> tasksStatusObserver,
        WorkerExecutionOperations executionOperations,
        Optional<String> jobProviderClass,
        ClassLoaderHandle classLoaderHandle, Collection<URL> requiredClasspaths,
        Job mantisJob) {
        this.executeStageRequestObservable = executeStageRequestObservable;
        this.tasksStatusObserver = tasksStatusObserver;
        this.executionOperations = executionOperations;
        this.jobProviderClass = jobProviderClass;
        this.classLoaderHandle = classLoaderHandle;
        this.requiredClasspaths = requiredClasspaths;
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

//                        URL jobJarUrl = executeStageRequest.getJobJarUrl();
//                        // pull out file name from URL
//                        String jobJarFile = jobJarUrl.getFile();
//                        String jarName = jobJarFile.substring(jobJarFile.lastIndexOf('/') + 1);
//
//                        // path used to store job on local disk
//                        Path path = Paths.get("/tmp", "mantis-jobs", executeStageRequest.getJobId(),
//                                Integer.toString(executeStageRequest.getWorkerNumber()),
//                                "libs");
//                        URL pathLocation = null;
//                        try {
//                            pathLocation = Paths.get(path.toString(), "*").toUri().toURL();
//                        } catch (MalformedURLException e1) {
//                            logger.error("Failed to convert path location to URL", e1);
//                            executeRequest.getStatus().onError(e1);
//                            return Observable.empty();
//                        }
//                        logger.info("Creating job classpath with pathLocation " + pathLocation);
//                        ClassLoader cl = URLClassLoader.newInstance(new URL[] {pathLocation});
                        Job mantisJob = null;
                        ClassLoader cl = null;
                        try {
                            if (ExecuteStageRequestService.this.mantisJob == null) {
                                // first of all, get a user-code classloader
                                // this may involve downloading the job's JAR files and/or classes
                                log.info("Loading JAR files for task {}.", this);

                                userCodeClassLoader = createUserCodeClassloader(
                                    executeStageRequest);
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
                                mantisJob = ExecuteStageRequestService.this.mantisJob;
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
                            log.error("Failed to close stage cleanly", e);
                        }
//                        try {
//                            userCodeClassLoader.close();
//                        } catch (IOException ex) {
//                            log.error("Failed to close user class loader successfully", ex);
//                        }
                    }

                    @Override
                    public void onError(Throwable e) {
                        logger.error("Execute stage observable threw exception", e);
//                        try {
//                            userCodeClassLoader.close();
//                        } catch (IOException ex) {
//                            log.error("Failed to close user class loader successfully", ex);
//                        }
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
        try {
            executionOperations.shutdownStage();
        } catch (IOException e) {
            log.error("Failed to close cleanly", e);
        }
    }

    @Override
    public void enterActiveMode() {}

    private UserCodeClassLoader createUserCodeClassloader(ExecuteStageRequest executeStageRequest) throws Exception {
        long startDownloadTime = System.currentTimeMillis();

        // triggers the download of all missing jar files from the job manager
        final UserCodeClassLoader userCodeClassLoader =
             classLoaderHandle.getOrResolveClassLoader(ImmutableList.of(executeStageRequest.getJobJarUrl().toURI()), requiredClasspaths);

        log.info(
            "Getting user code class loader for task {} at library cache manager took {} milliseconds",
            executeStageRequest.getExecutionAttemptID(),
            System.currentTimeMillis() - startDownloadTime);

        return userCodeClassLoader;
    }

}
