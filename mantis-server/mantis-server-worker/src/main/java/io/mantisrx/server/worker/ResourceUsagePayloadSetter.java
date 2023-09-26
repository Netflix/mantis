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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Gauge;
import io.mantisrx.common.storage.StorageUnit;
import io.mantisrx.runtime.loader.config.MetricsCollector;
import io.mantisrx.runtime.loader.config.Usage;
import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import io.mantisrx.server.core.StatusPayloads;
import io.mantisrx.server.core.stats.MetricStringConstants;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceUsagePayloadSetter implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ResourceUsagePayloadSetter.class);
    private static final long bigUsageChgReportingIntervalSecs = 10;
    private static final double bigIncreaseThreshold = 0.05;
    private final Heartbeat heartbeat;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ScheduledThreadPoolExecutor executor;
    private final long[] reportingIntervals;
    private final AtomicInteger counter = new AtomicInteger();
    private final MetricsCollector resourceUsageUtils;
    private final Gauge cpuLimitGauge;
    private final AtomicLong cpuLimitValue = new AtomicLong(0);
    private final Gauge cpuUsageCurrGauge;
    private final AtomicLong cpuUsageCurrValue = new AtomicLong(0);
    private final Gauge cpuUsagePeakGauge;
    private final AtomicLong cpuUsagePeakValue = new AtomicLong(0);
    private final Gauge memLimitGauge;
    private final AtomicLong memLimitValue = new AtomicLong(0);
    private final Gauge cachedMemUsageCurrGauge;
    private final AtomicLong cachedMemUsageCurrValue = new AtomicLong(0);
    private final Gauge cachedMemUsagePeakGauge;
    private final AtomicLong cachedMemUsagePeakValue = new AtomicLong(0);
    private final Gauge totMemUsageCurrGauge;
    private final AtomicLong totMemUsageCurrValue = new AtomicLong(0);
    private final Gauge totMemUsagePeakGauge;
    private final AtomicLong totMemUsagePeakValue = new AtomicLong(0);
    private final Gauge nwBytesLimitGauge;
    private final AtomicLong nwBytesLimitValue = new AtomicLong(0);
    private final Gauge nwBytesUsageCurrGauge;
    private final AtomicLong nwBytesUsageCurrValue = new AtomicLong(0);
    private final Gauge nwBytesUsagePeakGauge;
    private final AtomicLong nwBytesUsagePeakValue = new AtomicLong(0);
    private final Gauge jvmMemoryUsedGauge;
    private final AtomicLong jvmMemoryUsedValue = new AtomicLong(0);
    private final Gauge jvmMemoryMaxGauge;
    private final AtomicLong jvmMemoryMaxValue = new AtomicLong(0);
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
    private MeterRegistry meterRegistry;

    public ResourceUsagePayloadSetter(Heartbeat heartbeat, WorkerConfiguration config, double networkMbps, MeterRegistry meterRegistry) {
        this.heartbeat = heartbeat;
        this.nwBytesLimit = networkMbps * 1024.0 * 1024.0 / 8.0; // convert from bits to bytes
        this.meterRegistry = meterRegistry;
        executor = new ScheduledThreadPoolExecutor(1);
        String defaultReportingSchedule = "5,5,10,10,20,30";
        StringTokenizer tokenizer = new StringTokenizer(defaultReportingSchedule, ",");
        reportingIntervals = new long[tokenizer.countTokens()];
        int t = 0;
        while (tokenizer.hasMoreTokens()) {
            reportingIntervals[t++] = Long.parseLong(tokenizer.nextToken());
        }
        resourceUsageUtils = config.getUsageSupplier();
        String cpuLimitGaugeName = MetricStringConstants.CPU_PCT_LIMIT;
        String cpuUsageCurrGaugeName = MetricStringConstants.CPU_PCT_USAGE_CURR;
        String cpuUsagePeakGaugeName = MetricStringConstants.CPU_PCT_USAGE_PEAK;
        String memLimitGaugeName = MetricStringConstants.MEM_LIMIT;
        String cachedMemUsageCurrGaugeName = MetricStringConstants.CACHED_MEM_USAGE_CURR;
        String cachedMemUsagePeakGaugeName = MetricStringConstants.CACHED_MEM_USAGE_PEAK;
        String totMemUsageCurrGaugeName = MetricStringConstants.TOT_MEM_USAGE_CURR;
        String totMemUsagePeakGaugeName = MetricStringConstants.TOT_MEM_USAGE_PEAK;
        String nwBytesLimitGaugeName = MetricStringConstants.NW_BYTES_LIMIT;
        String nwBytesUsageCurrGaugeName = MetricStringConstants.NW_BYTES_USAGE_CURR;
        String nwBytesUsagePeakGaugeName = MetricStringConstants.NW_BYTES_USAGE_PEAK;
        String jvmMemoryUsedGaugeName = "jvmMemoryUsedBytes";
        String jvmMemoryMaxGaugeName = "jvmMemoryMaxBytes";
        cpuLimitGauge = addGauge(cpuLimitGaugeName, cpuLimitValue);
        cpuUsageCurrGauge = addGauge(cpuUsageCurrGaugeName, cpuUsageCurrValue);
        cpuUsagePeakGauge = addGauge(cpuUsagePeakGaugeName, cpuUsagePeakValue);
        memLimitGauge = addGauge(memLimitGaugeName, memLimitValue);
        cachedMemUsageCurrGauge = addGauge(cachedMemUsageCurrGaugeName, cachedMemUsageCurrValue);
        cachedMemUsagePeakGauge = addGauge(cachedMemUsagePeakGaugeName, cachedMemUsagePeakValue);
        totMemUsageCurrGauge = addGauge(totMemUsageCurrGaugeName, totMemUsageCurrValue);
        totMemUsagePeakGauge = addGauge(totMemUsagePeakGaugeName, totMemUsagePeakValue);
        nwBytesLimitGauge = addGauge(nwBytesLimitGaugeName, nwBytesLimitValue);
        nwBytesUsageCurrGauge = addGauge(nwBytesUsageCurrGaugeName, nwBytesUsageCurrValue);
        nwBytesUsagePeakGauge = addGauge(nwBytesUsagePeakGaugeName, nwBytesUsagePeakValue);
        jvmMemoryUsedGauge = addGauge(jvmMemoryUsedGaugeName, jvmMemoryUsedValue);
        jvmMemoryMaxGauge = addGauge(jvmMemoryMaxGaugeName, jvmMemoryMaxValue);
    }

    private Gauge addGauge(String name, AtomicLong value) {
        Gauge gauge = Gauge.builder("ResourceUsage_" + name, value::get)
            .register(meterRegistry);
        return gauge;
    }

    private long getNextDelay() {
        if (counter.get() >= reportingIntervals.length)
            return reportingIntervals[reportingIntervals.length - 1];
        return reportingIntervals[counter.getAndIncrement()];
    }

    private void setPayloadAndMetrics() {
        // figure out resource usage
        long delay = getNextDelay();
        try {
            StatusPayloads.ResourceUsage usage = evalResourceUsage();
            if (usage != null) {
                try {
                    heartbeat.addSingleUsePayload("" + StatusPayloads.Type.ResourceUsage, objectMapper.writeValueAsString(usage));
                } catch (JsonProcessingException e) {
                    logger.warn("Error writing json for resourceUsage payload: " + e.getMessage());
                }
                cpuLimitValue.set(Math.round(usage.getCpuLimit() * 100.0));
                cpuUsageCurrValue.set(Math.round(usage.getCpuUsageCurrent() * 100.0));
                cpuUsagePeakValue.set(Math.round(usage.getCpuUsagePeak() * 100.0));
                memLimitValue.set(Math.round(usage.getMemLimit()));
                cachedMemUsageCurrValue.set(Math.round(usage.getMemCacheCurrent()));
                cachedMemUsagePeakValue.set(Math.round(usage.getMemCachePeak()));
                totMemUsageCurrValue.set(Math.round(usage.getTotMemUsageCurrent()));
                totMemUsagePeakValue.set(Math.round(usage.getTotMemUsagePeak()));
                nwBytesLimitValue.set(Math.round(nwBytesLimit));
                nwBytesUsageCurrValue.set(Math.round(usage.getNwBytesCurrent()));
                nwBytesUsagePeakValue.set(Math.round(usage.getNwBytesPeak()));
                jvmMemoryUsedValue.set(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
                jvmMemoryMaxValue.set(Runtime.getRuntime().maxMemory());
                if (isBigIncrease(oldUsage, usage) || closeToLimit(usage)) {
                    delay = Math.min(delay, bigUsageChgReportingIntervalSecs);
                }
                oldUsage = usage;
            }
        } catch (Exception e) {
            logger.error("Failed to compute resource usage", e);
        } finally {
            logger.debug("scheduling next metrics report with delay=" + delay);
            executor.schedule(this::setPayloadAndMetrics, delay, TimeUnit.SECONDS);
        }

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

    private StatusPayloads.ResourceUsage evalResourceUsage() throws IOException {
        final Usage usage = resourceUsageUtils.get();
        if (prevStatsGatheredAt == 0L) {
            setPreviousStats(usage);
            return null;
        } else {
            double elapsedInSecs =
                ((double) System.currentTimeMillis() - (double) prevStatsGatheredAt) / 1000.0;
            double cpuUsage = ((usage.getCpusSystemTimeSecs() - prev_cpus_system_time_secs) / elapsedInSecs) +
                ((usage.getCpusUserTimeSecs() - prev_cpus_user_time_secs) / elapsedInSecs);
            if (cpuUsage > peakCpuUsage) {
                peakCpuUsage = cpuUsage;
            }
            if (cpuUsage > usage.getCpusLimit()) {
                logger.warn("CPU usage {} greater than limit {}, usage={}, elapsedInSecs={}", cpuUsage, usage.getCpusLimit(), usage, elapsedInSecs);
            }
            if (usage.getMemRssBytes() > peakTotMem)
                peakTotMem = usage.getMemRssBytes();
            double memCache = Math.max(0.0, usage.getMemRssBytes() - usage.getMemAnonBytes());
            if (memCache > peakMemCache)
                peakMemCache = memCache;
            double readBw = (usage.getNetworkReadBytes() - prev_bytes_read) / elapsedInSecs; // TODO check if byteCounts are already rate counts
            double writeBw = (usage.getNetworkWriteBytes() - prev_bytes_written) / elapsedInSecs;
            if (readBw > peakBytesRead)
                peakBytesRead = readBw;
            if (writeBw > peakBytesWritten)
                peakBytesWritten = writeBw;
            // set previous values to new values
            setPreviousStats(usage);
            return new StatusPayloads.ResourceUsage(
                usage.getCpusLimit(),
                cpuUsage,
                peakCpuUsage,
                StorageUnit.BYTES.toMBs(usage.getMemLimit()),
                StorageUnit.BYTES.toMBs(memCache),
                StorageUnit.BYTES.toMBs(peakMemCache),
                StorageUnit.BYTES.toMBs(usage.getMemRssBytes()),
                StorageUnit.BYTES.toMBs(peakTotMem),
                Math.max(readBw, writeBw),
                Math.max(peakBytesRead, peakBytesWritten));
        }
    }

    private void setPreviousStats(Usage usage) {
        prev_cpus_system_time_secs = usage.getCpusSystemTimeSecs();
        prev_cpus_user_time_secs = usage.getCpusUserTimeSecs();
        prev_bytes_read = usage.getNetworkReadBytes();
        prev_bytes_written = usage.getNetworkWriteBytes();
        prevStatsGatheredAt = System.currentTimeMillis();
    }
}
