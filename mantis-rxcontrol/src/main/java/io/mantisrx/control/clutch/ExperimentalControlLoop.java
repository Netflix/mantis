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

import io.mantisrx.control.IActuator;
import io.mantisrx.control.controllers.ErrorComputer;
import io.mantisrx.control.controllers.Integrator;
import io.mantisrx.control.controllers.PIDController;
import io.mantisrx.shaded.com.google.common.util.concurrent.AtomicDouble;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.Subscription;


@Slf4j
public class ExperimentalControlLoop implements Observable.Transformer<Event, Double> {
    private final ClutchConfiguration config;
    private final IActuator actuator;
    private final AtomicDouble dampener;

    private final AtomicLong cooldownTimestamp;
    private final AtomicLong currentSize;
    private final AtomicDouble lastLag;
    private final Observable<Integer> size;
    private final IRpsMetricComputer rpsMetricComputer;
    private final IScaleComputer scaleComputer;
    private long cooldownMillis;

    public ExperimentalControlLoop(ClutchConfiguration config, IActuator actuator, AtomicLong currentSize,
                                   Observable<Long> timer, Observable<Integer> size) {
        this(config, actuator, currentSize, new AtomicDouble(1.0), timer, size,
                new DefaultRpsMetricComputer(), new DefaultScaleComputer());
    }

    public ExperimentalControlLoop(ClutchConfiguration config, IActuator actuator, AtomicLong currentSize,
                                   AtomicDouble dampener, Observable<Long> timer, Observable<Integer> size,
                                   IRpsMetricComputer rpsMetricComputer,
                                   IScaleComputer scaleComputer) {
        this.config = config;
        this.actuator = actuator;
        this.dampener = dampener;
        this.cooldownMillis = config.getCooldownUnits().toMillis(config.cooldownInterval);

        this.cooldownTimestamp = new AtomicLong(System.currentTimeMillis());
        this.currentSize = currentSize;
        this.lastLag = new AtomicDouble(0.0);
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

        Observable<Event> sourceJobDrops =
                Observable.just(new Event(Clutch.Metric.SOURCEJOB_DROP, 0.0))
                        .mergeWith(events.filter(event -> event.getMetric() == Clutch.Metric.SOURCEJOB_DROP));

        Observable<Event> rps = events.filter(event -> event.getMetric() == Clutch.Metric.RPS);

        Integrator deltaIntegrator = new Integrator(0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, config.integralDecay);

        Subscription sizeSub = size
                .doOnNext(currentSize::set)
                .doOnNext(__ -> cooldownTimestamp.set(System.currentTimeMillis()))
                .doOnNext(n -> log.info("Clutch received new scheduling update with {} workers.", n))
                .subscribe();

        return rps
                .withLatestFrom(lag, drops, sourceJobDrops, (rpsEvent, lagEvent, dropEvent, sourceDropEvent) -> {
                    Map<Clutch.Metric, Double> metrics = new HashMap<>();
                    metrics.put(rpsEvent.getMetric(), rpsEvent.getValue());
                    metrics.put(lagEvent.getMetric(), lagEvent.getValue());
                    metrics.put(dropEvent.getMetric(), dropEvent.getValue());
                    metrics.put(sourceDropEvent.getMetric(), sourceDropEvent.getValue());
                    return metrics;
                })
                .doOnNext(metrics -> log.info("Latest metrics: {}", metrics))
                .map(metrics -> this.rpsMetricComputer.apply(config, metrics))
                .lift(new ErrorComputer(config.setPoint, true, config.rope._1, config.rope._2))
                .lift(new PIDController(config.kp, config.ki, config.kd, 1.0, new AtomicDouble(1.0), config.integralDecay))
                .doOnNext(d -> log.info("PID controller output: {}", d))
                .lift(deltaIntegrator)
                .doOnNext(d -> log.info("Integral: {}", d))
                .filter(__ -> this.cooldownMillis == 0 || cooldownTimestamp.get() <= System.currentTimeMillis() - this.cooldownMillis)
                .map(delta -> this.scaleComputer.apply(config, this.currentSize.get(), delta))
                .doOnNext(d -> log.info("New desired size: {}, existing size: {}", d, this.currentSize.get()))
                .filter(scale -> this.currentSize.get() != Math.round(Math.ceil(scale)))
                .lift(actuator)
                .doOnNext(scale -> this.currentSize.set(Math.round(Math.ceil(scale))))
                .doOnNext(__ -> deltaIntegrator.setSum(0))
                .doOnNext(__ -> cooldownTimestamp.set(System.currentTimeMillis()))
                .doOnUnsubscribe(() -> {
                    sizeSub.unsubscribe();
                });
    }

    /* For testing to trigger actuator on next event */
    protected void setCooldownMillis(long cooldownMillis) {
        this.cooldownMillis = cooldownMillis;
    }

    public static class DefaultRpsMetricComputer implements IRpsMetricComputer {
        private double lastLag = 0;

        public Double apply(ClutchConfiguration config, Map<Clutch.Metric, Double> metrics) {
            double rps = metrics.get(Clutch.Metric.RPS);
            double lag = metrics.get(Clutch.Metric.LAG);
            double sourceDrops = metrics.get(Clutch.Metric.SOURCEJOB_DROP);
            double drops = metrics.get(Clutch.Metric.DROPS);
            double lagDerivative  = lag - lastLag;
            lastLag = lag;
            return rps + lagDerivative + sourceDrops + drops;
        }
    }

    public static class DefaultScaleComputer implements IScaleComputer {
        public Double apply(ClutchConfiguration config, Long currentScale, Double delta) {
            return Math.min(config.maxSize, Math.max(config.minSize, currentScale + delta));
        }
    }
}
