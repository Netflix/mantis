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

package io.mantisrx.server.worker.mesos;

import io.mantisrx.runtime.loader.config.MetricsCollector;
import io.mantisrx.runtime.loader.config.Usage;
import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import io.mantisrx.shaded.com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import io.reactivx.mantis.operators.OperatorOnErrorResumeNextViaFunction;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * Mesos implementation of MetricsCollector that collects metrics using the statistics endpoint on the mesos agent.
 * <a href="https://mesos.readthedocs.io/en/latest/endpoints/slave/monitor/statistics.json/">mesos statics endpoint link</a>
 */
public class MesosMetricsCollector implements MetricsCollector {
    private static final String MESOS_TASK_EXECUTOR_ID_KEY = "MESOS_EXECUTOR_ID";
    private static final Logger logger = LoggerFactory.getLogger(MesosMetricsCollector.class);
    private static final long GET_TIMEOUT_SECS = 5;
    private static final int MAX_REDIRECTS = 10;
    private final int slavePort;
    private final String taskId;
    private final Func1<Observable<? extends Throwable>, Observable<?>> retryLogic = attempts -> attempts
        .zipWith(Observable.range(1, 3), (Func2<Throwable, Integer, Integer>) (t1, integer) -> integer)
        .flatMap((Func1<Integer, Observable<?>>) integer -> {
            long delay = 2L;
            logger.info(": retrying conx after sleeping for {} secs", delay);
            return Observable.timer(delay, TimeUnit.SECONDS);
        });

    @SuppressWarnings("unused")
    public static MesosMetricsCollector valueOf(Properties properties) {
        int slavePort = Integer.parseInt(properties.getProperty("mantis.agent.mesos.slave.port", "5051"));
        String taskId = System.getenv(MESOS_TASK_EXECUTOR_ID_KEY);
        return new MesosMetricsCollector(slavePort, taskId);
    }

    public static MesosMetricsCollector valueOf(WorkerConfiguration workerConfiguration) {
        int slavePort = workerConfiguration.getMesosSlavePort();
        String taskId = System.getenv(MESOS_TASK_EXECUTOR_ID_KEY);
        return new MesosMetricsCollector(slavePort, taskId);
    }

    MesosMetricsCollector(int slavePort, String taskId) {
        logger.info("Creating MesosMetricsCollector to port {} of taskId: {}", slavePort, taskId);

        if (Strings.isNullOrEmpty(taskId)) {
            // only log error to avoid breaking tests.
            logger.error("Invalid task id for MesosMetricsCollector");
        }
        this.slavePort = slavePort;
        this.taskId = taskId;
    }

    private String getUsageJson() {
        String usageEndpoint = "monitor/statistics.json";
        final String url = "http://localhost:" + slavePort + "/" + usageEndpoint;
        return RxNetty
            .createHttpRequest(HttpClientRequest.createGet(url), new HttpClient.HttpClientConfig.Builder()
                .setFollowRedirect(true).followRedirect(MAX_REDIRECTS).build())
            .lift(new OperatorOnErrorResumeNextViaFunction<>(Observable::error))
            .timeout(GET_TIMEOUT_SECS, TimeUnit.SECONDS)
            .retryWhen(retryLogic)
            .flatMap((Func1<HttpClientResponse<ByteBuf>, Observable<ByteBuf>>) r -> r.getContent())
            .map(o -> o.toString(Charset.defaultCharset()))
            .doOnError(throwable -> logger.warn("Can't get resource usage from mesos slave endpoint ({}) - {}", url, throwable.getMessage(), throwable))
            .toBlocking()
            .firstOrDefault("");
    }

    @Override
    public Usage get() {
        return getCurentUsage(taskId, getUsageJson());
    }

    static Usage getCurentUsage(String taskId, String usageJson) {
        if (usageJson == null || usageJson.isEmpty()) {
            logger.warn("Empty usage on task {}", taskId);
            return null;
        }

        JSONArray array = new JSONArray(usageJson);
        if (array.length() == 0)
            return null;
        JSONObject obj = null;
        for (int i = 0; i < array.length(); i++) {
            JSONObject executor = array.getJSONObject(i);
            if (executor != null) {
                String id = executor.optString("executor_id");
                if (id != null && id.equals(taskId)) {
                    obj = executor.getJSONObject("statistics");
                    break;
                }
            }
        }
        if (obj == null)
            return null;
        double cpus_limit = obj.optDouble("cpus_limit");
        if (Double.isNaN(cpus_limit)) {
            cpus_limit = 0.0;
        }
        double cpus_system_time_secs = obj.optDouble("cpus_system_time_secs");
        if (Double.isNaN(cpus_system_time_secs)) {
            logger.warn("Didn't get cpus_system_time_secs from mesos stats");
            cpus_system_time_secs = 0.0;
        }
        double cpus_user_time_secs = obj.optDouble("cpus_user_time_secs");
        if (Double.isNaN(cpus_user_time_secs)) {
            logger.warn("Didn't get cpus_user_time_secs from mesos stats");
            cpus_user_time_secs = 0.0;
        }
        // Also, cpus_throttled_time_secs may be useful to notice when job is throttled, will look into it later
        double mem_rss_bytes = obj.optDouble("mem_rss_bytes");
        if (Double.isNaN(mem_rss_bytes)) {
            logger.warn("Couldn't get mem_rss_bytes from mesos stats");
            mem_rss_bytes = 0.0;
        }
        double mem_anon_bytes = obj.optDouble("mem_anon_bytes");
        if (Double.isNaN(mem_anon_bytes)) {
            mem_anon_bytes = mem_rss_bytes;
        }
        double mem_limit = obj.optDouble("mem_limit_bytes");
        if (Double.isNaN(mem_limit))
            mem_limit = 0.0;
        double network_read_bytes = obj.optDouble("net_rx_bytes");
        if (Double.isNaN(network_read_bytes))
            network_read_bytes = 0.0;
        double network_write_bytes = obj.optDouble("net_tx_bytes");
        if (Double.isNaN(network_write_bytes))
            network_write_bytes = 0.0;
        return new Usage(cpus_limit, cpus_system_time_secs, cpus_user_time_secs, mem_limit, mem_rss_bytes, mem_anon_bytes,
            network_read_bytes, network_write_bytes);
    }
}
