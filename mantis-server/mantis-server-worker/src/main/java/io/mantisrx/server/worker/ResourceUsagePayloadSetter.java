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

import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.server.core.StatusPayloads;
import io.mantisrx.server.core.stats.MetricStringConstants;
import io.mantisrx.server.worker.config.WorkerConfiguration;
import io.mantisrx.server.worker.mesos.MesosResourceUsageUtils;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceUsagePayloadSetter implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ResourceUsagePayloadSetter.class);
    private static final String metricPrefix = "tcpServer";
    private static final long bigUsageChgReportingIntervalSecs = 10;
    private static final double bigIncreaseThreshold = 0.05;
    private final Heartbeat heartbeat;
    private final String workerName;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ScheduledThreadPoolExecutor executor;
    private final String defaultReportingSchedule = "5,5,10,10,20,30";
    private final long[] reportingIntervals;
    private final AtomicInteger counter = new AtomicInteger();
    private final double MB = 1024.0 * 1024;
    private final MesosResourceUsageUtils resourceUsageUtils;
    private final String cpuLimitGaugeName = MetricStringConstants.CPU_PCT_LIMIT;
    private final Gauge cpuLimitGauge;
    private final String cpuUsageCurrGaugeName = MetricStringConstants.CPU_PCT_USAGE_CURR;
    private final Gauge cpuUsageCurrGauge;
    private final String cpuUsagePeakGaugeName = MetricStringConstants.CPU_PCT_USAGE_PEAK;
    private final Gauge cpuUsagePeakGauge;
    private final String memLimitGaugeName = MetricStringConstants.MEM_LIMIT;
    private final Gauge memLimitGauge;
    private final String cachedMemUsageCurrGaugeName = MetricStringConstants.CACHED_MEM_USAGE_CURR;
    private final Gauge cachedMemUsageCurrGauge;
    private final String cachedMemUsagePeakGaugeName = MetricStringConstants.CACHED_MEM_USAGE_PEAK;
    private final Gauge cachedMemUsagePeakGauge;
    private final String totMemUsageCurrGaugeName = MetricStringConstants.TOT_MEM_USAGE_CURR;
    private final Gauge totMemUsageCurrGauge;
    private final String totMemUsagePeakGaugeName = MetricStringConstants.TOT_MEM_USAGE_PEAK;
    private final Gauge totMemUsagePeakGauge;
    private final Gauge nwBytesLimitGauge;
    private final String nwBytesLimitGaugeName = MetricStringConstants.NW_BYTES_LIMIT;
    private final String nwBytesUsageCurrGaugeName = MetricStringConstants.NW_BYTES_USAGE_CURR;
    private final Gauge nwBytesUsageCurrGauge;
    private final String nwBytesUsagePeakGaugeName = MetricStringConstants.NW_BYTES_USAGE_PEAK;
    private final Gauge nwBytesUsagePeakGauge;
    private final String jvmMemoryUsedGaugeName = "jvmMemoryUsedBytes";
    private final Gauge jvmMemoryUsedGauge;
    private final String jvmMemoryMaxGaugeName = "jvmMemoryMaxBytes";
    private final Gauge jvmMemoryMaxGauge;
    private final double nwBytesLimit;
    private double prev_cpus_system_time_secs = -1.0;
    private double prev_cpus_user_time_secs = -1.0;
    private double prev_bytes_read = -1.0;
    private double prev_bytes_written = -1.0;
    private long prevStatsGatheredAt = 0L;
    private double peakCpuUsage = 0.0;
    private double peakMemCache = 0.0;
    private double peakTotMem = 0.0;
    private double peakBytesRead = 0.0;
    private double peakBytesWritten = 0.0;
    private StatusPayloads.ResourceUsage oldUsage = null;

    public ResourceUsagePayloadSetter(Heartbeat heartbeat, WorkerConfiguration config, String workerName, double networkMbps) {
        this.heartbeat = heartbeat;
        this.workerName = workerName;
        this.nwBytesLimit = networkMbps * 1024.0 * 1024.0 / 8.0; // convert from bits to bytes
        executor = new ScheduledThreadPoolExecutor(1);
        StringTokenizer tokenizer = new StringTokenizer(defaultReportingSchedule, ",");
        reportingIntervals = new long[tokenizer.countTokens()];
        int t = 0;
        while (tokenizer.hasMoreTokens()) {
            reportingIntervals[t++] = Long.parseLong(tokenizer.nextToken());
        }
        resourceUsageUtils = new MesosResourceUsageUtils(config.getMesosSlavePort());
        Metrics m = new Metrics.Builder()
                .name("ResourceUsage")
                .addGauge(cpuLimitGaugeName)
                .addGauge(cpuUsageCurrGaugeName)
                .addGauge(cpuUsagePeakGaugeName)
                .addGauge(memLimitGaugeName)
                .addGauge(cachedMemUsageCurrGaugeName)
                .addGauge(cachedMemUsagePeakGaugeName)
                .addGauge(totMemUsageCurrGaugeName)
                .addGauge(totMemUsagePeakGaugeName)
                .addGauge(nwBytesLimitGaugeName)
                .addGauge(nwBytesUsageCurrGaugeName)
                .addGauge(nwBytesUsagePeakGaugeName)
                .addGauge(jvmMemoryUsedGaugeName)
                .addGauge(jvmMemoryMaxGaugeName)
                .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        cpuLimitGauge = m.getGauge(cpuLimitGaugeName);
        cpuUsageCurrGauge = m.getGauge(cpuUsageCurrGaugeName);
        cpuUsagePeakGauge = m.getGauge(cpuUsagePeakGaugeName);
        memLimitGauge = m.getGauge(memLimitGaugeName);
        cachedMemUsageCurrGauge = m.getGauge(cachedMemUsageCurrGaugeName);
        cachedMemUsagePeakGauge = m.getGauge(cachedMemUsagePeakGaugeName);
        totMemUsageCurrGauge = m.getGauge(totMemUsageCurrGaugeName);
        totMemUsagePeakGauge = m.getGauge(totMemUsagePeakGaugeName);
        nwBytesLimitGauge = m.getGauge(nwBytesLimitGaugeName);
        nwBytesUsageCurrGauge = m.getGauge(nwBytesUsageCurrGaugeName);
        nwBytesUsagePeakGauge = m.getGauge(nwBytesUsagePeakGaugeName);
        jvmMemoryUsedGauge = m.getGauge(jvmMemoryUsedGaugeName);
        jvmMemoryMaxGauge = m.getGauge(jvmMemoryMaxGaugeName);
    }

    private long getNextDelay() {
        if (counter.get() >= reportingIntervals.length)
            return reportingIntervals[reportingIntervals.length - 1];
        return reportingIntervals[counter.getAndIncrement()];
    }

    private void setPayloadAndMetrics() {
        // figure out resource usage
        StatusPayloads.ResourceUsage usage = evalResourceUsage();
        long delay = getNextDelay();
        if (usage != null) {
            try {
                heartbeat.addSingleUsePayload("" + StatusPayloads.Type.ResourceUsage, objectMapper.writeValueAsString(usage));
            } catch (JsonProcessingException e) {
                logger.warn("Error writing json for resourceUsage payload: " + e.getMessage());
            }
            cpuLimitGauge.set(Math.round(usage.getCpuLimit() * 100.0));
            cpuUsageCurrGauge.set(Math.round(usage.getCpuUsageCurrent() * 100.0));
            cpuUsagePeakGauge.set(Math.round(usage.getCpuUsagePeak() * 100.0));
            memLimitGauge.set(Math.round(usage.getMemLimit()));
            cachedMemUsageCurrGauge.set(Math.round(usage.getMemCacheCurrent()));
            cachedMemUsagePeakGauge.set(Math.round(usage.getMemCachePeak()));
            totMemUsageCurrGauge.set(Math.round(usage.getTotMemUsageCurrent()));
            totMemUsagePeakGauge.set(Math.round(usage.getTotMemUsagePeak()));
            nwBytesLimitGauge.set(Math.round(nwBytesLimit));
            nwBytesUsageCurrGauge.set(Math.round(usage.getNwBytesCurrent()));
            nwBytesUsagePeakGauge.set(Math.round(usage.getNwBytesPeak()));
            jvmMemoryUsedGauge.set(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
            jvmMemoryMaxGauge.set(Runtime.getRuntime().maxMemory());
            if (isBigIncrease(oldUsage, usage) || closeToLimit(usage))
                delay = Math.min(delay, bigUsageChgReportingIntervalSecs);
            oldUsage = usage;
        }
        logger.debug("scheduling next metrics report with delay=" + delay);
        executor.schedule(this::setPayloadAndMetrics, delay, TimeUnit.SECONDS);
    }

    private boolean closeToLimit(StatusPayloads.ResourceUsage usage) {
        if (usage == null)
            return false;
        if (usage.getCpuUsageCurrent() / usage.getCpuLimit() > 0.9)
            return true;
        if (usage.getTotMemUsageCurrent() / usage.getMemLimit() > 0.9)
            return true;
        if (usage.getNwBytesCurrent() / nwBytesLimit > 0.9)
            return true;
        return false;
    }

    private boolean isBigIncrease(StatusPayloads.ResourceUsage oldUsage, StatusPayloads.ResourceUsage newUsage) {
        if (oldUsage == null || newUsage == null)
            return true;
        if (isBigIncrease(oldUsage.getCpuUsageCurrent(), newUsage.getCpuUsageCurrent()))
            return true;
        if (isBigIncrease(oldUsage.getTotMemUsageCurrent(), newUsage.getTotMemUsageCurrent()))
            return true;
        if (isBigIncrease(oldUsage.getNwBytesCurrent(), newUsage.getNwBytesCurrent()))
            return true;
        return false;
    }

    private boolean isBigIncrease(double old, double curr) {
        if (old == 0.0)
            return curr != 0;
        return (curr - old) / old > bigIncreaseThreshold;
    }

    // todo(sundaram): why is this argument not used?
    void start(long intervalSecs) {
        executor.schedule(this::setPayloadAndMetrics, getNextDelay(), TimeUnit.SECONDS);
    }

    @Override
    public void close() throws IOException {
        executor.shutdownNow();
    }

    private StatusPayloads.ResourceUsage evalResourceUsage() {
        final MesosResourceUsageUtils.Usage usage = resourceUsageUtils.getCurrentUsage(workerName);
        if (prevStatsGatheredAt == 0L) {
            setPreviousStats(usage);
            return null;
        }
        double duration = ((double) System.currentTimeMillis() - (double) prevStatsGatheredAt) / 1000.0;
        double cpuSecs = (usage.getCpus_system_time_secs() - prev_cpus_system_time_secs) / duration +
                (usage.getCpus_user_time_secs() - prev_cpus_user_time_secs) / duration;
        if (cpuSecs > peakCpuUsage)
            peakCpuUsage = cpuSecs;
        if (usage.getMem_rss_bytes() > peakTotMem)
            peakTotMem = usage.getMem_rss_bytes();
        double memCache = Math.max(0.0, usage.getMem_rss_bytes() - usage.getMem_anon_bytes());
        if (memCache > peakMemCache)
            peakMemCache = memCache;
        double readBw = (usage.getNetwork_read_bytes() - prev_bytes_read) / duration; // TODO check if byteCounts are already rate counts
        double writeBw = (usage.getNetwork_write_bytes() - prev_bytes_written) / duration;
        if (readBw > peakBytesRead)
            peakBytesRead = readBw;
        if (writeBw > peakBytesWritten)
            peakBytesWritten = writeBw;
        // set previous values to new values
        setPreviousStats(usage);
        return new StatusPayloads.ResourceUsage(usage.getCpus_limit(), cpuSecs, peakCpuUsage, usage.getMem_limit() / MB,
                memCache / MB, peakMemCache / MB, usage.getMem_rss_bytes() / MB, peakTotMem / MB,
            Math.max(readBw, writeBw), Math.max(peakBytesRead, peakBytesWritten));
    }

    private void setPreviousStats(MesosResourceUsageUtils.Usage usage) {
        prev_cpus_system_time_secs = usage.getCpus_system_time_secs();
        prev_cpus_user_time_secs = usage.getCpus_user_time_secs();
        prev_bytes_read = usage.getNetwork_read_bytes();
        prev_bytes_written = usage.getNetwork_write_bytes();
        prevStatsGatheredAt = System.currentTimeMillis();
    }

    //    private long[] checkBytesCounts() {
    //        final Collection<Metrics> metrics = MetricsRegistry.getInstance().getMetrics(metricPrefix);
    //        long[] bytesCounts = {0L, 0L};
    //        if(metrics!=null && !metrics.isEmpty()) {
    //            //logger.info("Got " + metrics.size() + " metrics for bytesWritten");
    //            for(Metrics m: metrics) {
    //                Counter counter = m.getCounter("bytesWritten");
    //                if(counter!=null)
    //                    bytesCounts[1] += counter.rateValue(); // .value(); // TODO do we need .rateValue() instead?
    //                counter = m.getCounter("bytesRead");
    //                if(counter!=null)
    //                    bytesCounts[0] += counter.rateValue(); // .value(); // TODO do we need .rateValue() instead?
    //            }
    //        }
    //        return bytesCounts;
    //    }
}
