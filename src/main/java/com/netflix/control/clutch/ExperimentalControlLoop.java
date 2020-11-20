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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.AtomicDouble;
import com.netflix.control.IActuator;
import com.netflix.control.controllers.ErrorComputer;
import com.netflix.control.controllers.Integrator;
import com.netflix.control.controllers.PIDController;
import io.vavr.Tuple;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;


@Slf4j
public class ExperimentalControlLoop implements Observable.Transformer<Event, Double> {

    private final ClutchConfiguration config;
    private final IActuator actuator;
    private final Double initialSize;
    private final AtomicDouble dampener;

    private final AtomicLong cooldownTimestamp;
    private final AtomicLong currentScale;
    private final AtomicDouble lastLag;
    private final Observable<Long> timer;
    private final Observable<Integer> size;
    private final IRpsMetricComputer rpsMetricComputer;
    private final IScaleComputer scaleComputer;
    private long cooldownMillis;

    public ExperimentalControlLoop(ClutchConfiguration config, IActuator actuator, Double initialSize,
                                   Observable<Long> timer, Observable<Integer> size) {
        this(config, actuator, initialSize, new AtomicDouble(1.0), timer, size,
                new DefaultRpsMetricComputer(), new DefaultScaleComputer());
    }

    public ExperimentalControlLoop(ClutchConfiguration config, IActuator actuator, Double initialSize,
                                   AtomicDouble dampener, Observable<Long> timer, Observable<Integer> size,
                                   IRpsMetricComputer rpsMetricComputer,
                                   IScaleComputer scaleComputer) {
        this.config = config;
        this.actuator = actuator;
        this.initialSize = initialSize;
        this.dampener = dampener;
        this.cooldownMillis = config.getCooldownUnits().toMillis(config.cooldownInterval);

        this.cooldownTimestamp = new AtomicLong(System.currentTimeMillis() + this.cooldownMillis);
        this.currentScale = new AtomicLong(Math.round(initialSize));
        this.lastLag = new AtomicDouble(0.0);
        this.timer = timer;
        this.size = size;
        this.rpsMetricComputer = rpsMetricComputer;
        this.scaleComputer = scaleComputer;
    }

    @Override
    public Observable<Double> call(Observable<Event> events) {
        events = events.share();

        Observable<Event> lag =
                Observable.just(new Event(Clutch.Metric.LAG, 0.0))
                        .mergeWith(events.filter(event -> event.getMetric() == Clutch.Metric.LAG));

        Observable<Event> drops =
                Observable.just(new Event(Clutch.Metric.DROPS, 0.0))
                        .mergeWith(events.filter(event -> event.getMetric() == Clutch.Metric.DROPS));

        Observable<Event> rps = events.filter(event -> event.getMetric() == Clutch.Metric.RPS);

        Integrator deltaIntegrator = new Integrator(0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, config.integralDecay);

        size
                .takeUntil(timer)
                .doOnNext(currentScale::set)
                .doOnNext(__ -> cooldownTimestamp.set(System.currentTimeMillis()))
                .doOnNext(n -> log.info("Clutch received new scheduling update with {} workers.", n))
                .subscribe();

        return rps.withLatestFrom(lag, drops, Tuple::of)
                .doOnNext(triple -> log.debug("Clutch received RPS: {}, Lag: {} (d {}), Drops: {}", triple._1.value, triple._2.value, triple._2.value - lastLag.get(), triple._3.value))
                .map(triple -> {
                    Map<Clutch.Metric, Double> metrics = new HashMap<>();
                    metrics.put(Clutch.Metric.RPS, triple._1.value);
                    metrics.put(Clutch.Metric.LAG, triple._2.value);
                    metrics.put(Clutch.Metric.DROPS, triple._3.value);
                    return this.rpsMetricComputer.apply(config, metrics);
                })
                .lift(new ErrorComputer(config.setPoint, true, config.rope._1, config.rope._2))
                .lift(new PIDController(config.kp, config.ki, config.kd, 1.0, new AtomicDouble(1.0), config.integralDecay))
                .lift(deltaIntegrator)
                .filter(__ -> this.cooldownMillis == 0 || cooldownTimestamp.get() <= System.currentTimeMillis() - this.cooldownMillis)
                .map(delta -> this.scaleComputer.apply(config, this.currentScale.get(), delta))
                .filter(scale -> this.currentScale.get() != Math.round(Math.ceil(scale)))
                .lift(actuator)
                .doOnNext(scale -> this.currentScale.set(Math.round(Math.ceil(scale))))
                .doOnNext(__ -> deltaIntegrator.setSum(0))
                .doOnNext(__ -> cooldownTimestamp.set(System.currentTimeMillis()));
    }

    /* For testing to trigger actuator on next event */
    protected void setCooldownMillis(long cooldownMillis) {
        this.cooldownMillis = cooldownMillis;
    }

    public static class DefaultRpsMetricComputer implements IRpsMetricComputer {
        private double lastLag = 0;

        public Double apply(ClutchConfiguration config, Map<Clutch.Metric, Double> metrics) {
            double lag = metrics.get(Clutch.Metric.LAG);
            double lagDerivative  = lag - lastLag;
            lastLag = lag;
            return metrics.get(Clutch.Metric.RPS) + lagDerivative + metrics.get(Clutch.Metric.DROPS);
        }
    }

    public static class DefaultScaleComputer implements IScaleComputer {
        public Double apply(ClutchConfiguration config, Long currentScale, Double delta) {
            return Math.min(config.maxSize, Math.max(config.minSize, currentScale + delta));
        }
    }
}
