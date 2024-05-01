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
import io.mantisrx.shaded.com.google.common.annotations.VisibleForTesting;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.schedulers.Schedulers;


/**
 * The ClutchConfigurator's responsibility is to Observe the metrics stream for the workers
 * in a single stage and recommend a configuration for the autoscaler.
 *
 * There are several responsibilities;
 *  - Determine the dominant metric and recommend scaling occur on this metric.
 *  - Determine the true maximum achievable value for a metric and instead scale on that.
 *
 *  WHAT ELSE?
 *  - Determine if a job is overprovisioned / underprovisioned.
 *  - What can we do with lag and drops?
 *  - What can we do with oscillation?
 *  - What can we do if maxSize is too small?
 *
 */
@Slf4j
public class ClutchConfigurator implements Observable.Transformer<Event, ClutchConfiguration> {

    private static double DEFAULT_SETPOINT = 60.0;
    private static Tuple2<Double, Double> DEFAULT_ROPE = Tuple.of(25.0, 0.00);
    private static int DEFAULT_K = 1024;
    private static double DEFAULT_QUANTILE = 0.99;

    private IClutchMetricsRegistry metricsRegistry;
    private final Integer minSize;
    private final Integer maxSize;
    private final Observable<Long> timer;
    private Integer loggingIntervalMins = 60;

    /** Metrics which represent a resources and are consequently usable for scaling. */
    private static Set<Clutch.Metric> resourceMetrics = HashSet
            .of(Clutch.Metric.CPU, Clutch.Metric.MEMORY, Clutch.Metric.NETWORK, Clutch.Metric.UserDefined);

    private static ConcurrentHashMap<Clutch.Metric, UpdateDoublesSketch> sketches = new ConcurrentHashMap<>();
    static {
        sketches.put(Clutch.Metric.CPU, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.MEMORY, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.NETWORK, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.LAG, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.DROPS, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
        sketches.put(Clutch.Metric.UserDefined, UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
    }

    public ClutchConfigurator(IClutchMetricsRegistry metricsRegistry, Integer minSize, Integer maxSize, Observable<Long> timer) {
        this.metricsRegistry = metricsRegistry;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.timer = timer;
    }

    public ClutchConfigurator(IClutchMetricsRegistry metricsRegistry, Integer minSize, Integer maxSize, Observable<Long> timer,
                              Integer loggingIntervalMins) {
        this(metricsRegistry, minSize, maxSize, timer);
        this.loggingIntervalMins = loggingIntervalMins;
    }

    //
    // Metrics
    //

    /**
     * Determines the dominant metric given a stream of Metric -> UpdateDoublesSketch.
     * If a User Defined metric is present we will always use it.
     * @param metrics
     * @return A Clutch.Metric on which the job should scale.
     */
    private static Clutch.Metric determineDominantMetric(List<Map.Entry<Clutch.Metric, UpdateDoublesSketch>> metrics) {
        if (metrics.stream().filter(metric -> metric.getKey() == Clutch.Metric.UserDefined).count() > 0) {
           return Clutch.Metric.UserDefined;
        }

        Clutch.Metric metric = metrics.stream()
                .max(Comparator.comparingDouble(a -> a.getValue().getQuantile(DEFAULT_QUANTILE)))
                .map(Map.Entry::getKey)
                .get();
        log.info("Determined dominant resource: {}", metric.toString());
        return metric;
    }


    /**
     * The objective is to determine a setpoint which takes into account the fact that
     * the worker may not be able to use all of the provisioned resources.
     *
     * @param metric A DoublesSketch representing the metric in question.
     * @return An appropriate setpoint for a controller to use for autoscaling.
     */
    private static double determineSetpoint(DoublesSketch metric) {
        double quantile = metric.getQuantile(DEFAULT_QUANTILE);
        double setPoint = quantile * (DEFAULT_SETPOINT / 100.0);
        setPoint = setPoint == Double.NaN ? DEFAULT_SETPOINT : setPoint;
        double bounded = bound(1.0, DEFAULT_SETPOINT, setPoint);
        log.info("Determined quantile {} and setPoint of {} bounding to {}.", quantile, setPoint, bounded);
        return bounded;
    }

    //
    // Configs
    //

    /**
     * Generates a configuration based on Clutch's best understanding of the job at this time.
     * @return A configuration suitable for autoscaling with Clutch.
     */
    protected ClutchConfiguration getConfig() {
        Clutch.Metric dominantResource = determineDominantMetric(sketches.entrySet().stream()
                .filter(x -> isResourceMetric(x.getKey()))
                .filter(x -> x.getValue().getN() > 0)
                .collect(Collectors.toList()));

        double setPoint = determineSetpoint(sketches.get(dominantResource));

        return new ClutchConfiguration.ClutchConfigurationBuilder()
                .metric(dominantResource)
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

    /**
     * Generates a configuration whose purpose is to pin high.
     * @return A config which simply pins the controller to the maximum value.
     */
    private ClutchConfiguration getPinHighConfig() {
        return new ClutchConfiguration.ClutchConfigurationBuilder()
                .metric(Clutch.Metric.CPU)
                .setPoint(DEFAULT_SETPOINT)
                .kp(0.01)
                .ki(0.01)
                .kd(0.01)
                .minSize(this.maxSize)
                .maxSize(this.maxSize)
                .rope(DEFAULT_ROPE)
                .cooldownInterval(5)
                .cooldownUnits(TimeUnit.MINUTES)
                .build();
    }

    protected UpdateDoublesSketch getSketch(Clutch.Metric metric) {
        return sketches.get(metric);
    }

    @Override
    public Observable<ClutchConfiguration> call(Observable<Event> eventObservable) {

        eventObservable = eventObservable.share();

        Observable<Object> logs = Observable.interval(this.loggingIntervalMins, TimeUnit.MINUTES)
                .observeOn(Schedulers.newThread())
                .map(__ -> {
                    logSketchSummary("CPU", sketches.get(Clutch.Metric.CPU));
                    logSketchSummary("MEMORY", sketches.get(Clutch.Metric.MEMORY));
                    logSketchSummary("NETWORK", sketches.get(Clutch.Metric.NETWORK));
                    logSketchSummary("UserDefined", sketches.get(Clutch.Metric.UserDefined));
                    return null;
                });

        Observable<ClutchConfiguration> configs = timer
                .map(__ -> getConfig());

        return eventObservable
                .filter(event -> event != null && event.metric != null)
                .map(event -> {
                    UpdateDoublesSketch sketch = sketches.computeIfAbsent(event.metric, metric ->
                            UpdateDoublesSketch.builder().setK(DEFAULT_K).build());
                    sketch.update(event.value);
                    return null;
                }) // Encourages RxJava to actually consume events.
                .mergeWith(logs) // Encourages RxJava to actually consume events.
                .filter(Objects::nonNull)
                .cast(ClutchConfiguration.class)
                .mergeWith(Observable.just(getPinHighConfig())) // Initial config
                .mergeWith(configs) // Stream of configs.
                .doOnNext(config -> log.info(config.toString()));
    }


    //
    // Utils
    //

    private void logSketchSummary(String name, UpdateDoublesSketch sketch) {
        log.info("{} sketch ({}) min: {}, max: {}, median: {}, 99th: {}", name, sketch.getN(), sketch.getMinValue(), sketch.getMaxValue(), sketch.getQuantile(0.5), sketch.getQuantile(0.99));
    }


    private static boolean isResourceMetric(Clutch.Metric metric) {
        return resourceMetrics.contains(metric);
    }

    @VisibleForTesting
    static double bound(double min, double max, double value) {
        return value < min
                ? min
                : value > max
                ? max
                : value;
    }
}
