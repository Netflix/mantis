/*
 * Copyright 2024 Netflix, Inc.
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

package io.mantisrx.control.clutch;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import io.mantisrx.control.clutch.metrics.IClutchMetricsRegistry;
import io.vavr.Function1;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import rx.Observable;

@Slf4j
public class ExperimentalClutchConfigurator implements Observable.Transformer<Event, ClutchConfiguration>  {
    private static int DEFAULT_K = 1024;

    private static int NUM_STATS_DATA_POINTS = (int) TimeUnit.DAYS.toMinutes(7) * 2;
    private IClutchMetricsRegistry metricsRegistry;
    private final Observable<Long> timer;
    private final long initialConfigMilis;
    private final Function1<Map<Clutch.Metric, UpdateDoublesSketch>, ClutchConfiguration> configurator;

    private static ConcurrentHashMap<Clutch.Metric, UpdateDoublesSketch> sketches = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<Clutch.Metric, DescriptiveStatistics> stats = new ConcurrentHashMap<>();
    static {
        sketches.put(Clutch.Metric.CPU, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.MEMORY, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.NETWORK, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.LAG, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.DROPS, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.UserDefined, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.RPS, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.SOURCEJOB_DROP, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());

        stats.put(Clutch.Metric.CPU, new DescriptiveStatistics(NUM_STATS_DATA_POINTS));
        stats.put(Clutch.Metric.MEMORY, new DescriptiveStatistics(NUM_STATS_DATA_POINTS));
        stats.put(Clutch.Metric.NETWORK, new DescriptiveStatistics(NUM_STATS_DATA_POINTS));
        stats.put(Clutch.Metric.LAG, new DescriptiveStatistics(NUM_STATS_DATA_POINTS));
        stats.put(Clutch.Metric.DROPS, new DescriptiveStatistics(NUM_STATS_DATA_POINTS));
        stats.put(Clutch.Metric.UserDefined, new DescriptiveStatistics(NUM_STATS_DATA_POINTS));
        stats.put(Clutch.Metric.RPS, new DescriptiveStatistics(NUM_STATS_DATA_POINTS));
        stats.put(Clutch.Metric.SOURCEJOB_DROP, new DescriptiveStatistics(NUM_STATS_DATA_POINTS));
    }

    public ExperimentalClutchConfigurator(IClutchMetricsRegistry metricsRegistry, Observable<Long> timer,
                                          long initialConfigMillis,
                                          Function1<Map<Clutch.Metric, UpdateDoublesSketch>, ClutchConfiguration> configurator) {
        this.metricsRegistry = metricsRegistry;
        this.timer = timer;
        this.initialConfigMilis = initialConfigMillis;
        this.configurator = configurator;
    }

    //
    // Configs
    //

    /**
     * Generates a configuration based on Clutch's best understanding of the job at this time.
     * @return A configuration suitable for autoscaling with Clutch.
     */
    private ClutchConfiguration getConfig() {
        return this.configurator.apply(sketches);
    }

    @Override
    public Observable<ClutchConfiguration> call(Observable<Event> eventObservable) {
        Observable<ClutchConfiguration> configs = timer
                .map(__ -> getConfig())
                .doOnNext(config -> log.info("New Config: {}", config.toString()));

        Observable<ClutchConfiguration> initialConfig = Observable
                .interval(this.initialConfigMilis, TimeUnit.MILLISECONDS)
                .take(1)
                .map(__ -> getConfig())
                .doOnNext(config -> log.info("Initial Config: {}", config.toString()));

        eventObservable
                .filter(event -> event != null && event.metric != null)
                .map(event -> {
                    UpdateDoublesSketch sketch = sketches.computeIfAbsent(event.metric, metric ->
                            UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
                    sketch.update(event.value);

                    DescriptiveStatistics stat = stats.computeIfAbsent(event.metric, metric ->
                            new DescriptiveStatistics(NUM_STATS_DATA_POINTS));
                    stat.addValue(event.value);
                    return null;
                }).subscribe();

        return initialConfig
                .concatWith(configs)
                .distinctUntilChanged()
                .doOnNext(__ -> log.info("RPS Sketch State: {}", sketches.get(Clutch.Metric.RPS)))
                .doOnNext(__ -> {
                    logSketchSummary(sketches.get(Clutch.Metric.RPS));
                    logStatsSummary(stats.get(Clutch.Metric.RPS), "Stats RPS metric: ");
                    logStatsSummary(stats.get(Clutch.Metric.CPU), "Stats CPU metric: ");
                    logStatsSummary(stats.get(Clutch.Metric.MEMORY), "Stats Memory metric: ");
                    logStatsSummary(stats.get(Clutch.Metric.NETWORK), "Stats Network metric: ");
                })
                .doOnNext(config -> log.info("Clutch switched to config: {}", config));
    }

    private static void logSketchSummary(DoublesSketch sketch) {
        double[] quantiles = sketch.getQuantiles(new double[]{0.0, 0.25, 0.5, 0.75, 0.99, 1.0});
        log.info("RPS Sketch Quantiles -- Min: {}, 25th: {}, 50th: {}, 75th: {}, 99th: {}, Max: {}",
                quantiles[0],
                quantiles[1],
                quantiles[2],
                quantiles[3],
                quantiles[4],
                quantiles[5]
        );
    }

    private static void logStatsSummary(DescriptiveStatistics stat, String prefix) {
        log.info("{} RPS Sketch Quantiles -- Min: {}, 25th: {}, 50th: {}, 75th: {}, 99th: {}, Max: {}",
                prefix,
                stat.getMin(),
                stat.getPercentile(25),
                stat.getPercentile(50),
                stat.getPercentile(75),
                stat.getPercentile(99),
                stat.getPercentile(100)
        );
    }
}
