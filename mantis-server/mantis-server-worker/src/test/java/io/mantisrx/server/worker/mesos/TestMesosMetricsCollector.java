/*
 * Copyright 2022 Netflix, Inc.
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

import io.mantisrx.runtime.loader.config.Usage;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class TestMesosMetricsCollector {
    private static final String stats3 = "[\n" +
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
    private static final String stats4 = "[\n" +
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
    private static final String stats5 = "[\n" +
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

    @Test
    public void test() {
        final Usage usage1 = MesosMetricsCollector.getCurentUsage("SpeedBump-66-worker-0-0", stats3);
        final Usage usage2 = MesosMetricsCollector.getCurentUsage("SpeedBump-66-worker-0-0", stats4);
        log.info("cpuUsr=" + (usage2.getCpusUserTimeSecs() - usage1.getCpusUserTimeSecs()) + ", rss=" + (usage1.getMemRssBytes() / (1024 * 1024)));
        final Usage usage3 = MesosMetricsCollector.getCurentUsage("APIHystrixMetricsSource-5-worker-0-10", stats5);
        log.info("network read MB: " + (usage3.getNetworkReadBytes() / (1024.0 * 1024.0)) + ", write MB=" +
            (usage3.getNetworkWriteBytes() / (1024.0 * 1024.0)));
    }

}
