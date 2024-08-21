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

package io.mantisrx.connector.job.core;


import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.client.MantisSSEJob;
import io.mantisrx.client.SinkConnectionsStatus;
import io.mantisrx.client.examples.SubmitEphemeralJob;
import io.mantisrx.runtime.parameter.SinkParameters;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.mantisrx.server.master.client.HighAvailabilityServicesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;
import rx.Subscription;


/**
 * Used to locate and connect to Mantis Source Jobs.
 */
public class MantisSourceJobConnector {

    @Argument(alias = "p", description = "Specify a configuration file")
    private static String propFile = "";
    private static final Logger LOGGER = LoggerFactory.getLogger(MantisSourceJobConnector.class);
    private final Properties props;
    public static final String MANTIS_SOURCEJOB_CLIENT_ID_PARAM = "clientId";
    public static final String MANTIS_SOURCEJOB_SUBSCRIPTION_ID = "subscriptionId";
    public static final String MANTIS_SOURCEJOB_CLIENT_ID = "clientId";
    public static final String MANTIS_SOURCEJOB_CRITERION = "criterion";
    public static final String MANTIS_SOURCEJOB_NAME_PARAM = "sourceJobName";
    public static final String MANTIS_SOURCEJOB_TARGET_KEY = "target";
    public static final String MANTIS_SOURCEJOB_IS_BROADCAST_MODE = "isBroadcastMode";
    public static final String MANTIS_SOURCEJOB_SAMPLE_PER_SEC_KEY = "sample";
    public static final String MANTIS_ENABLE_PINGS = "enablePings";
    public static final String MANTIS_ENABLE_META_MESSAGES = "enableMetaMessages";
    public static final String MANTIS_META_MESSAGE_INTERVAL_SEC = "metaMessagesSec";
    public static final String MANTIS_MQL_THREADING_PARAM = "mantis.mql.threading.enabled";

    private static final String ZK_CONNECT_STRING = "mantis.zookeeper.connectString";
    private static final String ZK_ROOT = "mantis.zookeeper.root";
    private static final String ZK_LEADER_PATH = "mantis.zookeeper.leader.announcement.path";

    public MantisSourceJobConnector(boolean configureDefaults) {
        if (configureDefaults) {
            props = defaultProperties();
        } else {
            props = null;
        }
    }

    // todo(kmg-stripe): Can we remove this?  It seems it is only used by main in this class for testing.
    private static Properties defaultProperties() {
        Properties props = new Properties();

        final String defaultZkConnect = "127.0.0.1:2181";
        final String defaultZkRoot = "/mantis/master";
        final String defaultZkLeaderPath = "/leader";

        String connectString;
        String zookeeperRoot;
        String zookeeperLeaderAnnouncementPath;

        Map<String, String> env = System.getenv();
        if (env == null || env.isEmpty()) {
            connectString = defaultZkConnect;
            zookeeperRoot = defaultZkRoot;
            zookeeperLeaderAnnouncementPath = defaultZkLeaderPath;
        } else {
            connectString = env.getOrDefault(ZK_CONNECT_STRING, defaultZkConnect);
            zookeeperRoot = env.getOrDefault(ZK_ROOT, defaultZkRoot);
            zookeeperLeaderAnnouncementPath = env.getOrDefault(ZK_LEADER_PATH, defaultZkLeaderPath);
            LOGGER.info("Mantis Zk settings read from ENV: connectString {} root {} path {}", env.get(ZK_CONNECT_STRING), env.get(ZK_ROOT), env.get(ZK_LEADER_PATH));
        }
        if (connectString != null && !connectString.isEmpty()
                && zookeeperRoot != null && !zookeeperRoot.isEmpty()
                && zookeeperLeaderAnnouncementPath != null && !zookeeperLeaderAnnouncementPath.isEmpty()) {
            props.put(ZK_CONNECT_STRING, connectString);
            props.put(ZK_ROOT, zookeeperRoot);
            props.put(ZK_LEADER_PATH, zookeeperLeaderAnnouncementPath);
            props.put("mantis.zookeeper.connectionTimeMs", "2000");
            props.put("mantis.zookeeper.connection.retrySleepMs", "500");
            props.put("mantis.zookeeper.connection.retryCount", "5");
        } else {
            throw new RuntimeException("Zookeeper properties not available!");
        }

        LOGGER.info("Mantis Zk settings used for Source Job connector: connectString {} root {} path {}", connectString, zookeeperRoot, zookeeperLeaderAnnouncementPath);
        return props;
    }

    @Deprecated
    public MantisSSEJob connecToJob(String jobName) {
        return connectToJob(jobName, new SinkParameters.Builder().build(), new NoOpSinkConnectionsStatusObserver());
    }

    public MantisSSEJob connectToJob(String jobName, SinkParameters params) {
        return connectToJob(jobName, params, new NoOpSinkConnectionsStatusObserver());
    }

    /**
     * @deprecated forPartition and totalPartitions is not used internally, this API will be removed in next release
     */
    @Deprecated
    public MantisSSEJob connectToJob(String jobName, SinkParameters params, int forPartition, int totalPartitions) {
        return connectToJob(jobName, params, new NoOpSinkConnectionsStatusObserver());
    }

    /**
     * @deprecated forPartition and totalPartitions is not used internally, this API will be removed in next release
     */
    @Deprecated
    public MantisSSEJob connectToJob(String jobName, SinkParameters params, int forPartition, int totalPartitions, Observer<SinkConnectionsStatus> sinkObserver) {
        return connectToJob(jobName, params, sinkObserver);
    }

    public MantisSSEJob connectToJob(
            String jobName,
            SinkParameters params,
            Observer<SinkConnectionsStatus> sinkObserver) {
        MantisSSEJob.Builder builder = props != null ? new MantisSSEJob.Builder(props) : new MantisSSEJob.Builder(HighAvailabilityServicesUtil.get());
        return builder
                .name(jobName)
                .sinkConnectionsStatusObserver(sinkObserver)
                .onConnectionReset(throwable -> LOGGER.error("Reconnecting due to error: " + throwable.getMessage()))
                .sinkParams(params)
                .buildJobConnector();
    }

    static class NoOpSinkConnectionsStatusObserver implements Observer<SinkConnectionsStatus> {

        @Override
        public void onCompleted() {
            LOGGER.warn("Got Completed on SinkConnectionStatus ");
        }

        @Override
        public void onError(Throwable e) {
            LOGGER.error("Got Error on SinkConnectionStatus ", e);
        }

        @Override
        public void onNext(SinkConnectionsStatus t) {
            LOGGER.info("Got Sink Connection Status update " + t);
        }
    }

    public static void main(String[] args) {
        try {
            SinkParameters params = new SinkParameters.Builder().withParameter("subscriptionId", "id1").
                    withParameter("criterion", "select * where true").build();
            Args.parse(MantisSourceJobConnector.class, args);

            final CountDownLatch latch = new CountDownLatch(20);
            MantisSourceJobConnector sourceJobConnector = new MantisSourceJobConnector(true);
            MantisSSEJob job = sourceJobConnector.connectToJob("TestSourceJob", params);
            Subscription subscription = job.connectAndGetObservable()
                    .doOnNext(o -> {
                        LOGGER.info("Got event:  data: " + o.getEventAsString());
                        latch.countDown();
                    })
                    .subscribe();
            Subscription s2 = job.connectAndGetObservable()
                    .doOnNext(event -> {
                        LOGGER.info("    2nd: Got event:  data: " + event.getEventAsString());
                        latch.countDown();
                    })
                    .subscribe();
            try {
                boolean await = latch.await(300, TimeUnit.SECONDS);
                if (await)
                    System.out.println("PASSED");
                else
                    System.err.println("FAILED!");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            subscription.unsubscribe();
            System.out.println("Unsubscribed");
        } catch (IllegalArgumentException e) {
            Args.usage(SubmitEphemeralJob.class);
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
