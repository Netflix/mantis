/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.control.clutch;

import com.netflix.control.clutch.metrics.IClutchMetricsRegistry;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ExperimentalClutchConfigurator implements Observable.Transformer<Event, ClutchConfiguration>  {
    private static double DEFAULT_SETPOINT = 0.6;
    private static Tuple2<Double, Double> DEFAULT_ROPE = Tuple.of(10.0, 0.00);
    private static int DEFAULT_K = 1024;
    private static double DEFAULT_QUANTILE = 0.99;

    private IClutchMetricsRegistry metricsRegistry;
    private final Integer minSize;
    private final Integer maxSize;
    private final Observable<Long> timer;
    private final long initialConfigMilis;

    private static ConcurrentHashMap<Clutch.Metric, UpdateDoublesSketch> sketches = new ConcurrentHashMap<>();
    static {
        sketches.put(Clutch.Metric.CPU, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.MEMORY, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.NETWORK, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.LAG, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.DROPS, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.UserDefined, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.RPS, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
    }

    public ExperimentalClutchConfigurator(IClutchMetricsRegistry metricsRegistry, Integer minSize, Integer maxSize,
                                          Observable<Long> timer,
                                          long initialConfigMillis) {
        this.metricsRegistry = metricsRegistry;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.timer = timer;
        this.initialConfigMilis = initialConfigMillis;
    }

    //
    // Configs
    //

    /**
     * Generates a configuration based on Clutch's best understanding of the job at this time.
     * @return A configuration suitable for autoscaling with Clutch.
     */
    private ClutchConfiguration getConfig() {
        double setPoint = DEFAULT_SETPOINT * sketches.get(Clutch.Metric.RPS).getQuantile(DEFAULT_QUANTILE);
        Tuple2<Double, Double> rope = Tuple.of(setPoint * 0.1, 0.0);

        return new ClutchConfiguration.ClutchConfigurationBuilder()
                .metric(Clutch.Metric.RPS)
                .setPoint(setPoint)
                .kp(0.01)
                .ki(0.01)
                .kd(0.01)
                .minSize(this.minSize)
                .maxSize(this.maxSize)
                .rope(DEFAULT_ROPE)
                .cooldownInterval(5)
                .cooldownUnits(TimeUnit.MINUTES)
                .build();
    }

    @Override
    public Observable<ClutchConfiguration> call(Observable<Event> eventObservable) {
        Observable<ClutchConfiguration> configs = timer
                .map(__ -> getConfig())
                .doOnNext(config -> System.out.println("New Config: " + config.toString()));

        Observable<ClutchConfiguration> initialConfig = Observable
                .interval(this.initialConfigMilis, TimeUnit.MILLISECONDS)
                .take(1)
                .map(__ -> getConfig())
                .doOnNext(config -> System.out.println("Initial Config: " + config.toString()));

        eventObservable
                .filter(event -> event != null && event.metric != null)
                .map(event -> {
                    UpdateDoublesSketch sketch = sketches.computeIfAbsent(event.metric, metric ->
                            UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
                    sketch.update(event.value);
                    return null;
                }).subscribe();

        return initialConfig
                .concatWith(configs)
                .doOnNext(__ -> log.info("RPS Sketch State: {}", sketches.get(Clutch.Metric.RPS)))
                .doOnNext(__ -> logSketchSummary(sketches.get(Clutch.Metric.RPS)))
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
}
