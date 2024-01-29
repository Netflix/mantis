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

import static io.mantisrx.server.core.utils.StatusConstants.STATUS_MESSAGE_FORMAT;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.StageConfig;
import io.mantisrx.runtime.WorkerInfo;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.Status.TYPE;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.subjects.PublishSubject;


@SuppressWarnings("rawtypes")
public class RunningWorker {

    private static final Logger logger = LoggerFactory.getLogger(RunningWorker.class);
    private final int totalStagesNet;
    private Action0 onTerminateCallback;
    private Action0 onCompleteCallback;
    private Action1<Throwable> onErrorCallback;
    private CountDownLatch blockUntilTerminate = new CountDownLatch(1);
    private Job job;
    private SchedulingInfo schedulingInfo;
    private StageConfig stage;
    private Observer<Status> jobStatus;
    private String jobId;
    private final int stageNum;
    private final int workerNum;
    private final int workerIndex;
    private final String jobName;
    private final int totalStages;
    private final int metricsPort;
    private final Observable<Integer> stageTotalWorkersObservable;
    private final Observable<JobSchedulingInfo> jobSchedulingInfoObservable;
    private final Iterator<Integer> ports;
    private final PublishSubject<Boolean> requestSubject;
    private Context context;
    private final WorkerInfo workerInfo;

    public RunningWorker(Builder builder) {

        this.workerInfo = builder.workerInfo;
        this.requestSubject = builder.requestSubject;
        this.job = builder.job;
        this.ports = builder.ports;
        this.metricsPort = builder.metricsPort;
        this.schedulingInfo = builder.schedulingInfo;
        this.stage = builder.stage;
        this.jobId = builder.jobId;
        this.stageNum = builder.stageNum;
        this.workerNum = builder.workerNum;
        this.workerIndex = builder.workerIndex;
        this.jobName = builder.jobName;
        this.totalStages = builder.totalStages;
        this.totalStagesNet = this.totalStages - (builder.hasJobMaster ? 1 : 0);
        this.jobStatus = builder.jobStatus;
        this.stageTotalWorkersObservable = builder.stageTotalWorkersObservable;
        this.jobSchedulingInfoObservable = builder.jobSchedulingInfoObservable;

        this.onTerminateCallback = new Action0() {
            @Override
            public void call() {
                blockUntilTerminate.countDown();
            }
        };
        this.onCompleteCallback = new Action0() {
            @Override
            public void call() {
                logger.info("JobId: " + jobId + " stage: " + stageNum + ", completed");
                // setup a timeout to call forced exit as sure way to exit
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            sleep(3000);
                            System.exit(1);
                        } catch (Exception e) {
                            logger.error("Ignoring exception during exit: " + e.getMessage(), e);
                        }
                    }
                }.start();
                signalCompleted();
            }
        };
        this.onErrorCallback = new Action1<Throwable>() {
            @Override
            public void call(Throwable t) {
                signalFailed(t);
            }
        };
    }

    public void signalStartedInitiated() {
        logger.info("JobId: " + jobId + ", stage: " + stageNum + " workerIndex: " + workerIndex + " workerNumber: " + workerNum + ","
                + " signaling started initiated");
        // indicate start success
        requestSubject.onNext(true);
        requestSubject.onCompleted();
        jobStatus.onNext(new Status(jobId, stageNum, workerIndex, workerNum
                , TYPE.INFO, "Beginning job execution " +
                workerIndex, MantisJobState.StartInitiated));
    }

    public void signalStarted() {
        logger.info("JobId: " + jobId + ", " + String.format(STATUS_MESSAGE_FORMAT, stageNum, workerIndex, workerNum, "signaling started"));
        jobStatus.onNext(new Status(jobId, stageNum, workerIndex, workerNum,
                TYPE.INFO, String.format(STATUS_MESSAGE_FORMAT, stageNum, workerIndex, workerNum, "running"),
                MantisJobState.Started));
    }

    public void signalCompleted() {
        logger.info("JobId: " + jobId + ", stage: " + stageNum + " workerIndex: " + workerIndex + " workerNumber: " + workerNum + ","
                + " signaling completed");
        jobStatus.onNext(new Status(jobId, stageNum, workerIndex, workerNum,
                TYPE.INFO, String.format(STATUS_MESSAGE_FORMAT, stageNum, workerIndex, workerNum, "completed"),
                MantisJobState.Completed));
        // send complete status
        jobStatus.onCompleted();
    }

    public void signalFailed(Throwable t) {
        logger.info("JobId: " + jobId + ", stage: " + stageNum + " workerIndex: " + workerIndex + " workerNumber: " + workerNum + ","
                + " signaling failed");
        logger.error("Worker failure detected, shutting down job", t);
        jobStatus.onNext(new Status(jobId, stageNum, workerIndex, workerNum,
                TYPE.INFO, String.format(STATUS_MESSAGE_FORMAT, stageNum, workerIndex, workerNum, "failed. error: " + t.getMessage()),
                MantisJobState.Failed));
    }

    public void waitUntilTerminate() {
        try {
            blockUntilTerminate.await();
        } catch (InterruptedException e) {
            logger.error("Thread interrupted during await call", e);
        }
    }

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public WorkerInfo getWorkerInfo() {
        return workerInfo;
    }

    public StageSchedulingInfo stageSchedulingInfo(int stageNum) {
        return schedulingInfo.forStage(stageNum);
    }

    public StageSchedulingInfo stageSchedulingInfo() {
        return schedulingInfo.forStage(stageNum);
    }

    public Observable<Integer> getSourceStageTotalWorkersObservable() {
        return this.stageTotalWorkersObservable;
    }

    public Observable<JobSchedulingInfo> getJobSchedulingInfoObservable() { return this.jobSchedulingInfoObservable; }

    public Job getJob() {
        return job;
    }

    public Iterator<Integer> getPorts() {
        return ports;
    }

    public int getMetricsPort() {
        return metricsPort;
    }

    public StageConfig getStage() {
        return stage;
    }

    public SchedulingInfo getSchedulingInfo() {
        return schedulingInfo;
    }

    public Action0 getOnTerminateCallback() {
        return onTerminateCallback;
    }

    public Action0 getOnCompleteCallback() {
        return onCompleteCallback;
    }

    public Action1<Throwable> getOnErrorCallback() {
        return onErrorCallback;
    }

    public Observer<Status> getJobStatus() {
        return jobStatus;
    }

    public String getJobId() {
        return jobId;
    }

    public int getStageNum() {
        return stageNum;
    }

    public int getWorkerNum() {
        return workerNum;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public String getJobName() {
        return jobName;
    }

    public int getTotalStagesNet() {
        return totalStagesNet;
    }

    @Override
    public String toString() {
        return "RunningWorker ["
                + job + ", schedulingInfo=" + schedulingInfo + ", stage="
                + stage + ", jobStatus=" + jobStatus + ", jobId=" + jobId
                + ", stageNum=" + stageNum + ", workerNum=" + workerNum
                + ", workerIndex=" + workerIndex + ", jobName=" + jobName
                + ", totalStages=" + totalStages + ", metricsPort="
                + metricsPort + ", ports=" + ports
                + ", requestSubject=" + requestSubject + ", context=" + context
                + ", workerInfo=" + workerInfo + "]";
    }


    @SuppressWarnings("rawtypes")
    public static class Builder {

        private WorkerInfo workerInfo;
        private Job job;
        private Iterator<Integer> ports;
        private int metricsPort;
        private SchedulingInfo schedulingInfo;
        private StageConfig stage;
        private Observer<Status> jobStatus;
        private String jobId;
        private int stageNum;
        private int workerNum;
        private int workerIndex;
        private String jobName;
        private int totalStages;
        private Observable<Integer> stageTotalWorkersObservable;
        private Observable<JobSchedulingInfo> jobSchedulingInfoObservable;
        private PublishSubject<Boolean> requestSubject;
        private boolean hasJobMaster = false;

        public Builder workerInfo(WorkerInfo workerInfo) {
            this.workerInfo = workerInfo;
            return this;
        }

        public Builder ports(Iterator<Integer> ports) {
            this.ports = ports;
            return this;
        }

        public Builder job(Job job) {
            this.job = job;
            return this;
        }

        public Builder requestSubject(PublishSubject<Boolean> requestSubject) {
            this.requestSubject = requestSubject;
            return this;
        }

        public Builder stage(StageConfig stage) {
            this.stage = stage;
            return this;
        }

        public Builder schedulingInfo(SchedulingInfo schedulingInfo) {
            this.schedulingInfo = schedulingInfo;
            return this;
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder jobStatusObserver(Observer<Status> jobStatus) {
            this.jobStatus = jobStatus;
            return this;
        }

        public Builder stageNum(int stageNum) {
            this.stageNum = stageNum;
            return this;
        }

        public Builder metricsPort(int metricsPort) {
            this.metricsPort = metricsPort;
            return this;
        }

        public Builder workerNum(int workerNum) {
            this.workerNum = workerNum;
            return this;
        }

        public Builder workerIndex(int workerIndex) {
            this.workerIndex = workerIndex;
            return this;
        }

        public Builder jobName(String jobName) {
            this.jobName = jobName;
            return this;
        }

        public Builder totalStages(int totalStages) {
            this.totalStages = totalStages;
            return this;
        }

        public Builder hasJobMaster(boolean b) {
            this.hasJobMaster = b;
            return this;
        }

        public Builder stageTotalWorkersObservable(Observable<Integer> stageTotalWorkersObservable) {
            this.stageTotalWorkersObservable = stageTotalWorkersObservable;
            return this;
        }

        public Builder jobSchedulingInfoObservable(Observable<JobSchedulingInfo> jobSchedulingInfoObservable) {
            this.jobSchedulingInfoObservable = jobSchedulingInfoObservable;
            return this;
        }

        public RunningWorker build() {
            return new RunningWorker(this);
        }

    }
}
