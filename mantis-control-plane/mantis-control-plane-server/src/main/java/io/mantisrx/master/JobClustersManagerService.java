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

package io.mantisrx.master;

import static akka.pattern.PatternsCS.ask;

import akka.actor.ActorRef;
import akka.util.Timeout;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.Timer;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobClustersManagerService extends BaseService {
    private static final Logger logger = LoggerFactory.getLogger(JobClustersManagerService.class);
    private final ActorRef jobClustersManagerActor;
    private final MantisSchedulerFactory schedulerFactory;
    private final boolean loadJobsFromStore;
    private final Timer initializationTimeTracker;

    public JobClustersManagerService(final ActorRef jobClustersManagerActor,
                                     final MantisSchedulerFactory schedulerFactory,
                                     final boolean loadJobsFromStore) {
        super(true);
        this.jobClustersManagerActor = jobClustersManagerActor;
        this.schedulerFactory = schedulerFactory;
        this.loadJobsFromStore = loadJobsFromStore;
        Metrics metrics =
            new Metrics.Builder()
                .name("JobClustersManagerService")
                .addTimer("initializationTime")
                .build();

        this.initializationTimeTracker = metrics.getTimer("initializationTime");
    }

    @Override
    public void start() {
        super.awaitActiveModeAndStart(() -> {
            // initialize job clusters manager
            final CountDownLatch latch = new CountDownLatch(1);
            final long startTime = System.currentTimeMillis();
            try {
                long masterInitTimeoutSecs = ConfigurationProvider.getConfig().getMasterInitTimeoutSecs();
                CompletionStage<JobClusterManagerProto.JobClustersManagerInitializeResponse> initResponse =
                    ask(jobClustersManagerActor,
                        new JobClusterManagerProto.JobClustersManagerInitialize(schedulerFactory, loadJobsFromStore),
                        Timeout.apply(masterInitTimeoutSecs, TimeUnit.SECONDS))
                        .thenApply(JobClusterManagerProto.JobClustersManagerInitializeResponse.class::cast);
                initResponse.whenComplete((resp, t) -> {
                    logger.info("JobClustersManagerActor init response {}", resp);
                    if (t != null || !resp.responseCode.equals(BaseResponse.ResponseCode.SUCCESS)) {
                        logger.error("failed to initialize JobClustersManagerActor, committing suicide...", t);
                        System.exit(3);
                    }
                    latch.countDown();
                });
            } catch (Exception e) {
              logger.error("caught exception when initializing JobClustersManagerService, committing suicide...", e);
              System.exit(3);
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("interrupted waiting for latch countdown during JobClustersManagerInitialize, committing suicide..", e);
                System.exit(3);
            }

            logger.info("JobClustersManager initialize took {} sec",
                TimeUnit.SECONDS.convert(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS));
            initializationTimeTracker.record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
        });
    }
}
