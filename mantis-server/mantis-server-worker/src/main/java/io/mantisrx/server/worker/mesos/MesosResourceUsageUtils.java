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

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.reactivx.mantis.operators.OperatorOnErrorResumeNextViaFunction;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;


public class MesosResourceUsageUtils {

    private static final Logger logger = LoggerFactory.getLogger(MesosResourceUsageUtils.class);
    private static final long GET_TIMEOUT_SECS = 5;
    private static final int MAX_REDIRECTS = 10;
    private static final double OneGbInBytes = 1024.0 * 1024.0 * 1024.0 / 8.0;
    private final int slavePort;
    private final String usageEndpoint = "monitor/statistics.json";
    private final Func1<Observable<? extends Throwable>, Observable<?>> retryLogic = new Func1<Observable<? extends Throwable>, Observable<?>>() {
        @Override
        public Observable<?> call(Observable<? extends Throwable> attempts) {
            return attempts
                    .zipWith(Observable.range(1, 3), new Func2<Throwable, Integer, Integer>() {
                        @Override
                        public Integer call(Throwable t1, Integer integer) {
                            return integer;
                        }
                    })
                    .flatMap(new Func1<Integer, Observable<?>>() {
                        @Override
                        public Observable<?> call(Integer integer) {
                            long delay = 2L;
                            logger.info(": retrying conx after sleeping for " + delay + " secs");
                            return Observable.timer(delay, TimeUnit.SECONDS);
                        }
                    });
        }
    };
    public MesosResourceUsageUtils(int slavePort) {
        this.slavePort = slavePort;
    }

    public static void main(String[] args) {
        String stats1 = "[\n" +
                "    {\n" +
                "        \"executor_id\": \"SpeedBump-66-worker-0-0\",\n" +
                "        \"executor_name\":  \"Mantis Worker Executor\",\n" +
                "        \"framework_id\": \"MantisFramework\",\n" +
                "        \"source\":\"SpeedBump-66\",\n" +
                "        \"statistics\":\n" +
                "        {\n" +
                "            \"cpus_limit\":8,\n" +
                "            \"cpus_nr_periods\":0,\n" +
                "            \"cpus_nr_throttled\":0,\n" +
                "            \"cpus_system_time_secs\":3820.39,\n" +
                "            \"cpus_throttled_time_secs\":0,\n" +
                "            \"cpus_user_time_secs\":61336.31,\n" +
                "            \"mem_anon_bytes\":10374922240,\n" +
                "            \"mem_file_bytes\":85868544,\n" +
                "            \"mem_limit_bytes\":25190989824,\n" +
                "            \"mem_mapped_file_bytes\":5910528,\n" +
                "            \"mem_rss_bytes\":10461376512,\n" +
                "            \"timestamp\":1420765834.07515\n" +
                "        }\n" +
                "    }\n" +
                "]";
        String stats2 = "[\n" +
                "    {\n" +
                "        \"executor_id\": \"SpeedBump-66-worker-0-0\",\n" +
                "        \"executor_name\":  \"Mantis Worker Executor\",\n" +
                "        \"framework_id\": \"MantisFramework\",\n" +
                "        \"source\":\"SpeedBump-66\",\n" +
                "        \"statistics\":\n" +
                "        {\n" +
                "            \"cpus_limit\":8,\n" +
                "            \"cpus_nr_periods\":0,\n" +
                "            \"cpus_nr_throttled\":0,\n" +
                "            \"cpus_system_time_secs\":3820.42,\n" +
                "            \"cpus_throttled_time_secs\":0,\n" +
                "            \"cpus_user_time_secs\":61337.31,\n" +
                "            \"mem_anon_bytes\":10374922240,\n" +
                "            \"mem_file_bytes\":85868544,\n" +
                "            \"mem_limit_bytes\":25190989824,\n" +
                "            \"mem_mapped_file_bytes\":5910528,\n" +
                "            \"mem_rss_bytes\":10461376512,\n" +
                "            \"timestamp\":1420765834.07515\n" +
                "        }\n" +
                "    }\n" +
                "]";
        String stats3 = "[\n" +
                "{\n" +
                "executor_id: \"SpeedBump-66-worker-0-0\",\n" +
                "executor_name: \"Mantis Worker Executor\",\n" +
                "framework_id: \"MantisFramework\",\n" +
                "source: \"Outliers-mock-84\",\n" +
                "statistics: \n" +
                "{\n" +
                "cpus_limit: 1,\n" +
                "cpus_system_time_secs: 0.11,\n" +
                "cpus_user_time_secs: 2.16,\n" +
                "mem_limit_bytes: 2147483648,\n" +
                "mem_rss_bytes: 97460224,\n" +
                "timestamp: 1420842205.86559\n" +
                "}\n" +
                "}\n" +
                "]";
        String stats4 = "[\n" +
                "{\n" +
                "executor_id: \"SpeedBump-66-worker-0-0\",\n" +
                "executor_name: \"Mantis Worker Executor\",\n" +
                "framework_id: \"MantisFramework\",\n" +
                "source: \"Outliers-mock-84\",\n" +
                "statistics: \n" +
                "{\n" +
                "cpus_limit: 1,\n" +
                "cpus_system_time_secs: 0.13,\n" +
                "cpus_user_time_secs: 3.16,\n" +
                "mem_limit_bytes: 2147483648,\n" +
                "mem_rss_bytes: 97460224,\n" +
                "timestamp: 1420842205.86559\n" +
                "}\n" +
                "}\n" +
                "]";
        String stats5 = "[\n" +
                "{\n" +
                "executor_id: \"APIHystrixMetricsSource-5-worker-0-10\",\n" +
                "executor_name: \"Mantis Worker Executor\",\n" +
                "framework_id: \"MantisFramework\",\n" +
                "source: \"APIHystrixMetricsSource-5\",\n" +
                "statistics: {\n" +
                "cpus_limit: 8,\n" +
                "cpus_system_time_secs: 5.4,\n" +
                "cpus_user_time_secs: 67.74,\n" +
                "mem_anon_bytes: 1265774592,\n" +
                "mem_file_bytes: 48386048,\n" +
                "mem_limit_bytes: 10510925824,\n" +
                "mem_mapped_file_bytes: 1232896,\n" +
                "mem_rss_bytes: 1314697216,\n" +
                "net_rx_bytes: 994208159,\n" +
                "net_rx_dropped: 0,\n" +
                "net_rx_errors: 0,\n" +
                "net_rx_packets: 723567,\n" +
                "net_tx_bytes: 195020860,\n" +
                "net_tx_dropped: 0,\n" +
                "net_tx_errors: 0,\n" +
                "net_tx_packets: 564689,\n" +
                "timestamp: 1421792142.02197\n" +
                "}\n" +
                "}\n" +
                "]";
        MesosResourceUsageUtils usageUtils = new MesosResourceUsageUtils(10240);
        final Usage usage1 = usageUtils.getCurentUsage("SpeedBump-66-worker-0-0", stats3);
        final Usage usage2 = usageUtils.getCurentUsage("SpeedBump-66-worker-0-0", stats4);
        System.out.println("cpuUsr=" + (usage2.cpus_user_time_secs - usage1.cpus_user_time_secs) + ", rss=" + (usage1.getMem_rss_bytes() / (1024 * 1024)));
        final Usage usage3 = usageUtils.getCurentUsage("APIHystrixMetricsSource-5-worker-0-10", stats5);
        System.out.println("network read MB: " + (usage3.getNetwork_read_bytes() / (1024.0 * 1024.0)) + ", write MB=" +
                (usage3.getNetwork_write_bytes() / (1024.0 * 1024.0)));
    }

    private String getUsageJson() {
        final String url = "http://localhost:" + slavePort + "/" + usageEndpoint;
        return RxNetty
                .createHttpRequest(HttpClientRequest.createGet(url), new HttpClient.HttpClientConfig.Builder()
                        .setFollowRedirect(true).followRedirect(MAX_REDIRECTS).build())
                .lift(new OperatorOnErrorResumeNextViaFunction<>(new Func1<Throwable, Observable<? extends HttpClientResponse<ByteBuf>>>() {
                    @Override
                    public Observable<? extends HttpClientResponse<ByteBuf>> call(Throwable t) {
                        return Observable.error(t);
                    }
                }))
                .timeout(GET_TIMEOUT_SECS, TimeUnit.SECONDS)
                .retryWhen(retryLogic)
                .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<ByteBuf>>() {
                    @Override
                    public Observable<ByteBuf> call(HttpClientResponse<ByteBuf> r) {
                        return r.getContent();
                    }
                })
                .map(new Func1<ByteBuf, String>() {
                    @Override
                    public String call(ByteBuf o) {
                        return o.toString(Charset.defaultCharset());
                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        logger.warn("Can't get resource usage from mesos slave endpoint (" + url + ") - " + throwable.getMessage(), throwable);
                    }
                })
                .toBlocking()
                .firstOrDefault("");
    }

    public Usage getCurrentUsage(String taskId) {
        return getCurentUsage(taskId, getUsageJson());
    }

    private Usage getCurentUsage(String taskId, String usageJson) {
        if (usageJson == null || usageJson.isEmpty())
            return null;
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
        double network_limit = OneGbInBytes;
        double network_read_bytes = obj.optDouble("net_rx_bytes");
        if (Double.isNaN(network_read_bytes))
            network_read_bytes = 0.0;
        double network_write_bytes = obj.optDouble("net_tx_bytes");
        if (Double.isNaN(network_write_bytes))
            network_write_bytes = 0.0;
        return new Usage(cpus_limit, cpus_system_time_secs, cpus_user_time_secs, mem_limit, mem_rss_bytes, mem_anon_bytes,
                network_limit, network_read_bytes, network_write_bytes);
    }

    public static class Usage {

        private final double cpus_limit;
        private final double cpus_system_time_secs;
        private final double cpus_user_time_secs;
        private final double mem_limit;
        private final double mem_rss_bytes;
        private final double mem_anon_bytes;
        private final double network_limit;
        private final double network_read_bytes;
        private final double network_write_bytes;

        public Usage(double cpus_limit, double cpus_system_time_secs, double cpus_user_time_secs,
                     double mem_limit, double mem_rss_bytes, double mem_anon_bytes, double network_limit,
                     double network_read_bytes, double network_write_bytes) {
            this.cpus_limit = cpus_limit;
            this.cpus_system_time_secs = cpus_system_time_secs;
            this.cpus_user_time_secs = cpus_user_time_secs;
            this.mem_limit = mem_limit;
            this.mem_rss_bytes = mem_rss_bytes;
            this.mem_anon_bytes = mem_anon_bytes;
            this.network_limit = network_limit;
            this.network_read_bytes = network_read_bytes;
            this.network_write_bytes = network_write_bytes;
        }

        public double getCpus_limit() {
            return cpus_limit;
        }

        public double getCpus_system_time_secs() {
            return cpus_system_time_secs;
        }

        public double getCpus_user_time_secs() {
            return cpus_user_time_secs;
        }

        public double getMem_limit() {
            return mem_limit;
        }

        public double getMem_rss_bytes() {
            return mem_rss_bytes;
        }

        public double getMem_anon_bytes() {
            return mem_anon_bytes;
        }

        public double getNetwork_limit() {
            return network_limit;
        }

        public double getNetwork_read_bytes() {
            return network_read_bytes;
        }

        public double getNetwork_write_bytes() {
            return network_write_bytes;
        }
    }
}
