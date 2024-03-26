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

package io.mantisrx.server.worker.jobmaster.clutch;

import static io.mantisrx.runtime.descriptor.StageScalingPolicy.ScalingReason.CPU;
import static io.mantisrx.runtime.descriptor.StageScalingPolicy.ScalingReason.JVMMemory;
import static io.mantisrx.runtime.descriptor.StageScalingPolicy.ScalingReason.Network;

import com.yahoo.labs.samoa.instances.Attribute;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.worker.jobmaster.JobAutoScaler;
import io.mantisrx.server.worker.jobmaster.control.actuators.ClutchMantisStageActuator;
import io.mantisrx.server.worker.jobmaster.control.controllers.PIDController;
import io.mantisrx.server.worker.jobmaster.control.utils.ErrorComputer;
import io.mantisrx.server.worker.jobmaster.control.utils.Integrator;
import io.mantisrx.shaded.com.google.common.cache.Cache;
import io.mantisrx.shaded.com.google.common.cache.CacheBuilder;
import io.mantisrx.shaded.com.google.common.util.concurrent.AtomicDouble;
import io.vavr.Tuple;
import io.vavr.Tuple3;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import moa.core.FastVector;
import org.slf4j.Logger;
import rx.Observable;


/**
 * ClutchAutoScaler is an Rx Transformer which can be composed with a stream
 * of Metrics in order to scale a specific stage.
 * Clutch is based on two key concepts;
 * - One of the three main resources (CPU, Memory, Network) will be the binding factor for a job.
 * - A machine learning model can correct for the error.
 */
public class ClutchAutoScaler implements Observable.Transformer<JobAutoScaler.Event, Object> {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(ClutchAutoScaler.class);
    private static final String autoscaleLogMessageFormat = "Autoscaling stage %d to %d instances on controller output: cpu/mem/network %f/%f/%f (dampening: %f) and predicted error: %f with dominant resource: %s";
    private static final FastVector attributes = new FastVector();

    static {
        attributes.add(new Attribute("cpu"));
        attributes.add(new Attribute("memory"));
        attributes.add(new Attribute("network"));
        attributes.add(new Attribute("minuteofday"));
        attributes.add(new Attribute("scale"));
        attributes.add(new Attribute("error"));
    }

    private final JobAutoScaler.StageScaler scaler;
    private final StageSchedulingInfo stageSchedulingInfo;
    private final long initialSize;
    private final ClutchConfiguration config;
    private final AtomicLong targetScale = new AtomicLong(0);
    private final AtomicDouble gainDampeningFactor = new AtomicDouble(1.0);
    private final AtomicLong cooldownTimestamp;
    private final AtomicLong rps = new AtomicLong(0);
    private final ClutchPIDConfig defaultConfig = new ClutchPIDConfig(60.0, Tuple.of(0.0, 25.0), 0.01, 0.01);
    Cache<Long, Long> actionCache = CacheBuilder.newBuilder()
            .maximumSize(12)
            .expireAfterWrite(60, TimeUnit.MINUTES)
            .build();
    AtomicDouble correction = new AtomicDouble(0.0);

    public ClutchAutoScaler(StageSchedulingInfo stageSchedulingInfo, JobAutoScaler.StageScaler scaler, ClutchConfiguration config, int initialSize) {
        this.stageSchedulingInfo = stageSchedulingInfo;
        this.scaler = scaler;
        this.initialSize = initialSize;
        this.targetScale.set(initialSize);
        this.config = config;
        this.rps.set(Math.round(config.rps));

        this.cooldownTimestamp = new AtomicLong(System.currentTimeMillis() + config.cooldownSeconds.getOrElse(0L) * 1000);

        Observable.interval(60, TimeUnit.SECONDS)
                .forEach(__ -> {
                    double factor = computeGainFactor(actionCache);
                    log.debug("Setting gain dampening factor to: {}.", factor);
                    this.gainDampeningFactor.set(factor);
                });
    }

    private static double enforceMinMax(double targetScale, double min, double max) {

        if (Double.isNaN(targetScale)) {
            targetScale = min;
        }

        if (targetScale < min) {
            return min;
        }
        if (targetScale > max) {
            return max;
        }
        return targetScale;
    }

    /**
     * Computes the dampening factor for gain.
     * The objective here is to reduce gain in a situation where actions are varying.
     * The dampening facor is defined as such:
     * x = the percentage of scaling actions in the same direction (up or down)
     * dampening = x ^ 3; 0.5 <= x <= 1.0
     *
     * @param actionCache A cache of timestamp -> scale
     *
     * @return The computed gain dampening factor.
     */
    private double computeGainFactor(Cache<Long, Long> actionCache) {
        long nUp = actionCache.asMap().values().stream().filter(x -> x > 0.0).count();
        long nDown = actionCache.asMap().values().stream().filter(x -> x < 0.0).count();
        long n = nUp + nDown;

        double x = n == 0
                ? 1.0
                : nUp > nDown
                ? (1.0 * nUp) / n
                : (1.0 * nDown) / n;

        return Math.pow(x, 3);
    }

    private ClutchControllerOutput findDominatingResource(Tuple3<ClutchControllerOutput, ClutchControllerOutput, ClutchControllerOutput> triple) {
        if (triple._1.scale >= triple._2.scale && triple._1.scale >= triple._3.scale) {
            return triple._1;
        } else if (triple._2.scale >= triple._1.scale && triple._2.scale >= triple._3.scale) {
            return triple._2;
        } else {
            return triple._3;
        }
    }

    @Override
    public Observable<Object> call(Observable<JobAutoScaler.Event> metrics) {
        metrics = metrics
                .share();

        ClutchController cpuController = new ClutchController(CPU, this.stageSchedulingInfo, this.config.cpu.getOrElse(defaultConfig), this.gainDampeningFactor, this.initialSize, this.config.minSize, this.config.maxSize);
        ClutchController memController = new ClutchController(JVMMemory, this.stageSchedulingInfo, this.config.memory.getOrElse(defaultConfig), this.gainDampeningFactor, this.initialSize, this.config.minSize, this.config.maxSize);
        ClutchController netController = new ClutchController(Network, this.stageSchedulingInfo, this.config.network.getOrElse(defaultConfig), this.gainDampeningFactor, this.initialSize, this.config.minSize, this.config.maxSize);

        Observable<ClutchControllerOutput> cpuSignal = metrics.filter(event -> event.getType().equals(CPU))
                .compose(cpuController);
        Observable<ClutchControllerOutput> memorySignal = metrics.filter(event -> event.getType().equals(JVMMemory))
                .compose(memController);
        Observable<ClutchControllerOutput> networkSignal = metrics.filter(event -> event.getType().equals(Network))
                .compose(netController);

        Observable<Tuple3<Double, Double, Double>> rawMetricsTuples = Observable.zip(
                metrics.filter(event -> event.getType().equals(CPU)).map(JobAutoScaler.Event::getValue),
                metrics.filter(event -> event.getType().equals(JVMMemory)).map(JobAutoScaler.Event::getValue),
                metrics.filter(event -> event.getType().equals(Network)).map(JobAutoScaler.Event::getValue),
                Tuple::of);

        Observable<Tuple3<ClutchControllerOutput, ClutchControllerOutput, ClutchControllerOutput>> controlSignals = Observable.zip(
                cpuSignal,
                memorySignal,
                networkSignal,
                Tuple::of);

        Observable<Double> kafkaLag = metrics.filter(event -> event.getType().equals(StageScalingPolicy.ScalingReason.KafkaLag))
                .map(JobAutoScaler.Event::getValue)
                .map(x -> x / this.config.rps);

        Observable<Double> dataDrop = metrics.filter(event -> event.getType().equals(StageScalingPolicy.ScalingReason.DataDrop))
                .map(x -> (x.getValue() / 100.0) * x.getNumWorkers());

        // Jobs either read from Kafka and produce lag, or they read from other jobs and produce drops.
        Observable<Double> error = Observable.merge(Observable.just(0.0), kafkaLag, dataDrop);

        Observable<Integer> currentScale = metrics.map(JobAutoScaler.Event::getNumWorkers);

        Observable<Tuple3<String, Double, Integer>> controllerSignal = Observable.zip(rawMetricsTuples, controlSignals, Tuple::of)
                .withLatestFrom(currentScale, (tup, scale) -> Tuple.of(tup._1, tup._2, scale))
                .withLatestFrom(error, (tup, err) -> Tuple.of(tup._1, tup._2, tup._3, err))
                .map(tup -> {
                    int currentWorkerCount = tup._3;
                    ClutchControllerOutput dominantResource = findDominatingResource(tup._2);
                    String resourceName = dominantResource.reason.name();

                    //
                    // Correction
                    //

                    double yhat = tup._4;
                    yhat = Math.min(yhat, config.maxAdjustment.getOrElse(config.maxSize * 1.0));
                    yhat = yhat < 1.0 ? 0.0 : yhat;

                    if (System.currentTimeMillis() > this.cooldownTimestamp.get()) {
                        double x = correction.addAndGet(yhat);
                        x = Math.min(x, config.maxAdjustment.getOrElse(config.maxSize * 1.0));
                        correction.set(x);
                    }
                    correction.set(correction.get() * 0.99); // Exponentially decay our correction.
                    correction.set(Double.isNaN(correction.get()) ? 0.0 : correction.get());

                    Double targetScale = enforceMinMax(Math.ceil(dominantResource.scale) + Math.ceil(correction.get()), this.config.minSize, this.config.maxSize);
                    String logMessage = String.format(autoscaleLogMessageFormat, scaler.getStage(), targetScale.intValue(), tup._2._1.scale, tup._2._2.scale, tup._2._3.scale, gainDampeningFactor.get(), correction.get(), resourceName);

                    return Tuple.of(logMessage, targetScale, currentWorkerCount);
                });

        return controllerSignal
                .filter(__ -> System.currentTimeMillis() > this.cooldownTimestamp.get())
                .filter(tup -> Math.abs(Math.round(tup._2) - tup._3) > 0.99) //
                .doOnNext(signal -> log.info(signal._1))
                .compose(new ClutchMantisStageActuator(this.scaler))
                .map(Math::round)
                .doOnNext(x -> actionCache.put(System.currentTimeMillis(), x - targetScale.get()))
                .doOnNext(targetScale::set)
                .doOnNext(__ -> cooldownTimestamp.set(System.currentTimeMillis() + config.cooldownSeconds.getOrElse(0L) * 1000))
                .map(x -> (Object) x);
    }

    private class ClutchController implements Observable.Transformer<JobAutoScaler.Event, ClutchControllerOutput> {

        private final ClutchPIDConfig config;
        private final StageScalingPolicy.ScalingReason metric;
        private final StageSchedulingInfo stageSchedulingInfo;
        private final AtomicDouble gainFactor;
        private final long initialSize;
        private final long min;
        private final long max;


        private final Integrator integrator;

        public ClutchController(StageScalingPolicy.ScalingReason metric, StageSchedulingInfo stageSchedulingInfo, ClutchPIDConfig config, AtomicDouble gainFactor, long initialSize, long min, long max) {
            this.metric = metric;
            this.config = config;
            this.gainFactor = gainFactor;
            this.initialSize = initialSize;
            this.stageSchedulingInfo = stageSchedulingInfo;
            this.min = min;
            this.max = max;

            this.integrator = new Integrator(this.initialSize, this.min - 1, this.max + 1);
        }

        @Override
        public Observable<ClutchControllerOutput> call(Observable<JobAutoScaler.Event> eventObservable) {
            return eventObservable.map(event -> {
                    log.debug("Received event: {}", event);
                    return event.getEffectiveValue();
                })
                .lift(new ErrorComputer(config.setPoint, true, config.rope._1, config.rope._2))
                .lift(PIDController.of(config.kp, 0.0, config.kd, 1.0, this.gainFactor))
                .lift(this.integrator)
                .map(x -> new ClutchControllerOutput(this.metric, x));
        }
    }
}
