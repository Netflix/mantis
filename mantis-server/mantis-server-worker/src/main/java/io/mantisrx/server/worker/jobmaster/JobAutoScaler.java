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

package io.mantisrx.server.worker.jobmaster;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.parameter.ParameterUtils;

import io.mantisrx.server.core.stats.UsageDataStats;

import com.netflix.control.clutch.Clutch;
import com.netflix.control.clutch.ClutchExperimental;

import io.mantisrx.server.worker.jobmaster.clutch.experimental.MantisClutchConfigurationSelector;
import io.vavr.jackson.datatype.VavrModule;

import io.vavr.control.Try;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.type.TypeReference;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;

import io.mantisrx.server.master.client.MantisMasterClientApi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.BackpressureOverflow;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.observers.SerializedObserver;
import rx.subjects.PublishSubject;

import io.mantisrx.server.worker.jobmaster.clutch.ClutchAutoScaler;
import io.mantisrx.server.worker.jobmaster.clutch.ClutchConfiguration;
import io.mantisrx.server.worker.jobmaster.control.actuators.MantisStageActuator;
import io.mantisrx.server.worker.jobmaster.control.utils.TransformerWrapper;


public class JobAutoScaler {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(JobAutoScaler.class);
    private static final String PercentNumberFormat = "%5.2f";
    private static final Map<StageScalingPolicy.ScalingReason, Clutch.Metric> metricMap = new HashMap<>();

    static {
        objectMapper.registerModule(new VavrModule());
    }

    static {
        metricMap.put(StageScalingPolicy.ScalingReason.CPU, Clutch.Metric.CPU);
        metricMap.put(StageScalingPolicy.ScalingReason.JVMMemory, Clutch.Metric.MEMORY);
        metricMap.put(StageScalingPolicy.ScalingReason.Network, Clutch.Metric.NETWORK);
        metricMap.put(StageScalingPolicy.ScalingReason.KafkaLag, Clutch.Metric.LAG);
        metricMap.put(StageScalingPolicy.ScalingReason.DataDrop, Clutch.Metric.DROPS);
        metricMap.put(StageScalingPolicy.ScalingReason.UserDefined, Clutch.Metric.UserDefined);
        metricMap.put(StageScalingPolicy.ScalingReason.RPS, Clutch.Metric.RPS);
    }

    private final String jobId;
    private final MantisMasterClientApi masterClientApi;
    private final SchedulingInfo schedulingInfo;
    private final PublishSubject<Event> subject;
    private final Context context;

    JobAutoScaler(String jobId, SchedulingInfo schedulingInfo, MantisMasterClientApi masterClientApi,
                  Context context) {
        this.jobId = jobId;
        this.masterClientApi = masterClientApi;
        this.schedulingInfo = schedulingInfo;
        subject = PublishSubject.create();
        this.context = context;
    }

    public static void main(String[] args) {
        Observable.interval(1, TimeUnit.DAYS)
                .doOnNext(x -> System.out.println(x))
                .take(1)
                .toBlocking()
                .last();
    }

    Observer<Event> getObserver() {
        return new SerializedObserver<>(subject);
    }

    private com.netflix.control.clutch.Event mantisEventToClutchEvent(StageSchedulingInfo stageSchedulingInfo, Event event) {
        return new com.netflix.control.clutch.Event(metricMap.get(event.type),
                Util.getEffectiveValue(stageSchedulingInfo, event.getType(), event.getValue()));
    }

    void start() {
        subject
                .onBackpressureBuffer(100, () -> {
                    logger.info("onOverflow triggered, dropping old events");
                }, BackpressureOverflow.ON_OVERFLOW_DROP_OLDEST)
                .doOnRequest(x -> logger.info("Scaler requested {} metrics.", x))
                .groupBy(event -> event.getStage())
                .flatMap(go -> {
                    Integer stage = Optional.ofNullable(go.getKey()).orElse(-1);

                    final StageSchedulingInfo stageSchedulingInfo = schedulingInfo.forStage(stage);
                    logger.info("System Environment:");
                    System.getenv().forEach((key, value) -> {
                        logger.info("{} = {}", key, value);
                    });

                    Optional<String> clutchCustomConfiguration =
                            Optional.ofNullable(System.getenv("JOB_PARAM_" +
                                    ParameterUtils.JOB_MASTER_CLUTCH_SYSTEM_PARAM));

                    if (stageSchedulingInfo != null && (stageSchedulingInfo.getScalingPolicy() != null ||
                            clutchCustomConfiguration.isPresent())) {

                        //
                        // Clutch Unofficial (invoked via job parameter)
                        //

                        if (clutchCustomConfiguration.isPresent()) {
                            try {

                                ClutchConfiguration config = getClutchConfiguration(clutchCustomConfiguration.get())
                                        .get(stage);

                                int initialSize = stageSchedulingInfo.getNumberOfInstances();
                                StageScaler scaler = new StageScaler(stage, stageSchedulingInfo);

                                logger.info("Initializing Clutch with config:");
                                logger.info(config.toString());

                                if (config != null && config.getUseExperimental().getOrElse(false)) {
                                    logger.info("Setting up Clutch Custom operator for job " + jobId + " stage " + stage);
                                    return go
                                            .map(event -> this.mantisEventToClutchEvent(stageSchedulingInfo, event))
                                            .filter(event -> event.metric != null)
                                            .compose(new Clutch(new MantisStageActuator(initialSize, scaler), stageSchedulingInfo.getNumberOfInstances(), config.minSize, config.maxSize));
                                } else if (config != null) {
                                    logger.info("Setting up Clutch Custom operator for job " + jobId + " stage " + stage);
                                    return go
                                            .compose(new ClutchAutoScaler(stageSchedulingInfo, scaler, config, initialSize));
                                }
                            } catch (Exception ex) {
                                logger.error("Error initializing Clutch: " + ex.getMessage());
                            }
                        }

                        //
                        // Clutch Official (invoked via scaling config)
                        //

                        if (stageSchedulingInfo != null &&
                                stageSchedulingInfo.getScalingPolicy() != null &&
                                stageSchedulingInfo
                                .getScalingPolicy()
                                .getStrategies() != null &&
                                stageSchedulingInfo
                                        .getScalingPolicy()
                                        .getStrategies()
                                        .values()
                                        .stream()
                                        .anyMatch(policy -> policy.getReason().equals(StageScalingPolicy.ScalingReason.Clutch))) {

                            int initialSize = stageSchedulingInfo.getNumberOfInstances();
                            StageScaler scaler = new StageScaler(stage, stageSchedulingInfo);

                            logger.info("Setting up Clutch Official scale operator for job " + jobId + " stage " + stage);

                            return go
                                    .map(event -> this.mantisEventToClutchEvent(stageSchedulingInfo, event))
                                    .filter(event -> event.metric != null)
                                    .compose(new Clutch(new MantisStageActuator(initialSize, scaler),
                                            stageSchedulingInfo.getNumberOfInstances(),
                                            stageSchedulingInfo.getScalingPolicy().getMin(),
                                            stageSchedulingInfo.getScalingPolicy().getMax()));

                        }

                        //
                        // Clutch experimental (invoked via scaling config)
                        //

                        if (stageSchedulingInfo != null &&
                                stageSchedulingInfo.getScalingPolicy() != null &&
                                stageSchedulingInfo
                                        .getScalingPolicy()
                                        .getStrategies() != null &&
                                stageSchedulingInfo
                                        .getScalingPolicy()
                                        .getStrategies()
                                        .values()
                                        .stream()
                                        .anyMatch(policy -> policy.getReason().equals(StageScalingPolicy.ScalingReason.ClutchExperimental))) {

                            int initialSize = stageSchedulingInfo.getNumberOfInstances();
                            StageScaler scaler = new StageScaler(stage, stageSchedulingInfo);

                            logger.info("Setting up Clutch Experimental scale operator for job " + jobId + " stage " + stage);

                            Observable<Integer> workerCounts = context.getWorkerMapObservable()
                                    .map(x -> x.getWorkersForStage(go.getKey()).size())
                                    .distinctUntilChanged()
                                    .throttleLast(5, TimeUnit.SECONDS);

                            com.netflix.control.clutch.ClutchConfiguration clutchConfig = com.netflix.control.clutch.ClutchConfiguration.builder().build();

                            return go
                              .map(event -> this.mantisEventToClutchEvent(stageSchedulingInfo, event))
                              .filter(event -> event.metric != null)
                              .compose(new ClutchExperimental(
                                    new MantisStageActuator(initialSize, scaler),
                                    stageSchedulingInfo.getNumberOfInstances(),
                                    stageSchedulingInfo.getScalingPolicy().getMin(),
                                    stageSchedulingInfo.getScalingPolicy().getMax(),
                                    workerCounts,
                                    Observable.interval(1, TimeUnit.HOURS),
                                    1000 * 60 * 10,
                                      new MantisClutchConfigurationSelector(stage, stageSchedulingInfo)
                                    ));
                        }

                        logger.info("Setting up stage scale operator for job " + jobId + " stage " + stage);

                        //
                        // Step Based Autoscaler
                        //

                        return go.compose(new TransformerWrapper<>(new StageScaleOperator<>(stage, stageSchedulingInfo)));

                    } else {
                      return go;
                    }
                })
                .doOnCompleted(() -> logger.info("onComplete on JobAutoScaler subject"))
                  .doOnError(t -> logger.error("got onError in JobAutoScaler", t))
                  .doOnSubscribe(() -> logger.info("onSubscribe JobAutoScaler"))
                  .doOnUnsubscribe(() -> {
                    logger.info("Unsubscribing for JobAutoScaler of job " + jobId);
                  })
                .retry()
                  .subscribe();
    }

    /**
     * Decodes the Clutch configuration parameter taking into account the parameter used to be a single
     * config for stage 1, we now accept a mapping of stage -> config and this method wraps
     * the logic for decoding either parameter.
     *
     * @param jsonConfig A JSON representation of a Clutch Configuration Map.
     *
     * @return A map of stage -> config for Clutch.
     */
    private Map<Integer, ClutchConfiguration> getClutchConfiguration(String jsonConfig) {
      return Try.<Map<Integer, ClutchConfiguration>>of(() -> objectMapper.readValue(jsonConfig, new TypeReference<Map<Integer, ClutchConfiguration>>() {}))
        .getOrElseGet(t -> Try.of(() -> {
          ClutchConfiguration config = objectMapper.readValue(jsonConfig, new TypeReference<ClutchConfiguration>() {});
          Map<Integer, ClutchConfiguration> configs = new HashMap<>();
          configs.put(1, config);
          return configs;
        }).get());
    }

    public static class Event {

      private final StageScalingPolicy.ScalingReason type;
      private final int stage;
      private final double value;
      private final int numWorkers;
      private final String message;

      public Event(StageScalingPolicy.ScalingReason type, int stage, double value, int numWorkers, String message) {
        this.type = type;
        this.stage = stage;
        this.value = value;
        this.numWorkers = numWorkers;
        this.message = message;
      }

      public StageScalingPolicy.ScalingReason getType() {
        return type;
      }

      public int getStage() {
        return stage;
      }

      public double getValue() {
        return value;
      }

      public int getNumWorkers() {
        return numWorkers;
      }

      public String getMessage() {
        return message;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Event event = (Event) o;

        if (stage != event.stage) return false;
        if (Double.compare(event.value, value) != 0) return false;
        if (numWorkers != event.numWorkers) return false;
        if (type != event.type) return false;
        return message != null ? message.equals(event.message) : event.message == null;

      }

      @Override
      public int hashCode() {
        int result;
        long temp;
        result = type != null ? type.hashCode() : 0;
        result = 31 * result + stage;
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + numWorkers;
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
      }

      @Override
      public String toString() {
        return "Event{" +
          "type=" + type +
          ", stage=" + stage +
          ", value=" + value +
          ", numWorkers=" + numWorkers +
          ", message='" + message + '\'' +
          '}';
      }
    }

    public class StageScaler {

      private final int stage;
      private final StageSchedulingInfo stageSchedulingInfo;
      private final AtomicReference<Subscription> inProgressScalingSubscription = new AtomicReference<>(null);


      private final Func1<Observable<? extends Throwable>, Observable<?>> retryLogic = attempts -> attempts
        .zipWith(Observable.range(1, Integer.MAX_VALUE), (Func2<Throwable, Integer, Integer>) (t1, integer) -> integer)
        .flatMap((Func1<Integer, Observable<?>>) integer -> {
          long delay = 2 * (integer > 5 ? 10 : integer);
          logger.info("retrying scaleJobStage request after sleeping for " + delay + " secs");
          return Observable.timer(delay, TimeUnit.SECONDS);
        });

      public StageScaler(int stage, StageSchedulingInfo stageSchedulingInfo) {
        this.stage = stage;
        this.stageSchedulingInfo = stageSchedulingInfo;
      }

      private void cancelOutstandingScalingRequest() {
        if (inProgressScalingSubscription.get() != null && !inProgressScalingSubscription.get().isUnsubscribed()) {
          inProgressScalingSubscription.get().unsubscribe();
          inProgressScalingSubscription.set(null);
        }
      }

      private void setOutstandingScalingRequest(final Subscription subscription) {
        inProgressScalingSubscription.compareAndSet(null, subscription);
      }

      public int getDesiredWorkersForScaleUp(final int increment, final int numCurrentWorkers) {
        final int desiredWorkers;
        if (!stageSchedulingInfo.getScalingPolicy().isEnabled()) {
          logger.warn("Job " + jobId + " stage " + stage + " is not scalable, can't increment #workers by " + increment);
          return numCurrentWorkers;
        }
        if (numCurrentWorkers < 0 || increment < 1) {
          logger.error("current number of workers({}) not known or increment({}) < 1, will not scale up", numCurrentWorkers, increment);
          return numCurrentWorkers;
        } else {
          final int maxWorkersForStage = stageSchedulingInfo.getScalingPolicy().getMax();
          desiredWorkers = Math.min(numCurrentWorkers + increment, maxWorkersForStage);
          return desiredWorkers;
        }
      }

      public void scaleUpStage(final int numCurrentWorkers, final int desiredWorkers, final String reason) {
        logger.info("scaleUpStage incrementing number of workers from {} to {}", numCurrentWorkers, desiredWorkers);
        cancelOutstandingScalingRequest();
        final Subscription subscription = masterClientApi.scaleJobStage(jobId, stage, desiredWorkers, reason)
          .retryWhen(retryLogic)
          .onErrorResumeNext(throwable -> {
            logger.error("caught error when scaling up stage {}", stage);
            return Observable.empty();
          })
        .subscribe();
        setOutstandingScalingRequest(subscription);
      }

      public int getDesiredWorkersForScaleDown(final int decrement, final int numCurrentWorkers) {
        final int desiredWorkers;
        if (!stageSchedulingInfo.getScalingPolicy().isEnabled()) {
          logger.warn("Job " + jobId + " stage " + stage + " is not scalable, can't decrement #workers by " + decrement);
          return numCurrentWorkers;
        }
        if (numCurrentWorkers < 0 || decrement < 1) {
          logger.error("current number of workers({}) not known or decrement({}) < 1, will not scale down", numCurrentWorkers, decrement);
          return numCurrentWorkers;
        } else if (decrement > numCurrentWorkers) {
          logger.error("trying to decrement by {} more than current number of workers({}), will set desired workers to 0", decrement, numCurrentWorkers);
          desiredWorkers = 0;
        } else {
          int min = stageSchedulingInfo.getScalingPolicy().getMin();
          desiredWorkers = Math.max(numCurrentWorkers - decrement, min);
        }
        return desiredWorkers;
      }

      public void scaleDownStage(final int numCurrentWorkers, final int desiredWorkers, final String reason) {
        logger.info("scaleDownStage decrementing number of workers from {} to {}", numCurrentWorkers, desiredWorkers);
        cancelOutstandingScalingRequest();
        final Subscription subscription = masterClientApi.scaleJobStage(jobId, stage, desiredWorkers, reason)
          .retryWhen(retryLogic)
          .onErrorResumeNext(throwable -> {
            logger.error("caught error when scaling down stage {}", stage);
            return Observable.empty();
          })
        .subscribe();
        setOutstandingScalingRequest(subscription);
      }

      public int getStage() {
        return stage;
      }
    }

    private class StageScaleOperator<T, R> implements Observable.Operator<Object, Event> {

      private final int stage;
      private final StageSchedulingInfo stageSchedulingInfo;
      private final StageScaler scaler;
      private volatile long lastScaledAt = 0L;

      private StageScaleOperator(int stage,
          StageSchedulingInfo stageSchedulingInfo) {
        this.stage = stage;
        this.stageSchedulingInfo = stageSchedulingInfo;
        this.scaler = new StageScaler(stage, this.stageSchedulingInfo);
        logger.info("cooldownSecs set to {}", stageSchedulingInfo.getScalingPolicy().getCoolDownSecs());
      }


      @Override
      public Subscriber<? super Event> call(final Subscriber<? super Object> child) {

        return new Subscriber<Event>() {
          private final Map<StageScalingPolicy.ScalingReason, UsageDataStats> dataStatsMap = new HashMap<>();

          @Override
          public void onCompleted() {
            child.unsubscribe();
          }

          @Override
          public void onError(Throwable e) {
            logger.error("Unexpected error: " + e.getMessage(), e);
          }

          @Override
          public void onNext(Event event) {
            final StageScalingPolicy scalingPolicy = stageSchedulingInfo.getScalingPolicy();
            long coolDownSecs = scalingPolicy == null ? Long.MAX_VALUE : scalingPolicy.getCoolDownSecs();
            boolean scalable = stageSchedulingInfo.getScalable() && scalingPolicy != null && scalingPolicy.isEnabled();
            logger.debug("Will check for autoscaling job " + jobId + " stage " + stage + " due to event: " + event);
            if (scalable && scalingPolicy != null) {
              final StageScalingPolicy.Strategy strategy = scalingPolicy.getStrategies().get(event.getType());
              if (strategy != null) {
                double effectiveValue = Util.getEffectiveValue(stageSchedulingInfo, event.getType(), event.getValue());
                UsageDataStats stats = dataStatsMap.get(event.getType());
                if (stats == null) {
                  stats = new UsageDataStats(
                      strategy.getScaleUpAbovePct(), strategy.getScaleDownBelowPct(), strategy.getRollingCount());
                  dataStatsMap.put(event.getType(), stats);
                }
                stats.add(effectiveValue);
                if (lastScaledAt < (System.currentTimeMillis() - coolDownSecs * 1000)) {
                  logger.info(jobId + ", stage " + stage + ": eff=" +
                      String.format(PercentNumberFormat, effectiveValue) + ", thresh=" + strategy.getScaleUpAbovePct());
                  if (stats.getHighThreshTriggered()) {
                    logger.info("Attempting to scale up stage " + stage + " of job " + jobId + " by " +
                        scalingPolicy.getIncrement() + " workers, because " +
                        event.type + " exceeded scaleUpThreshold of " +
                        String.format(PercentNumberFormat, strategy.getScaleUpAbovePct()) + " " +
                        stats.getCurrentHighCount() + "  times");
                    final int numCurrWorkers = event.getNumWorkers();
                    final int desiredWorkers = scaler.getDesiredWorkersForScaleUp(scalingPolicy.getIncrement(), numCurrWorkers);
                    if (desiredWorkers > numCurrWorkers) {
                      scaler.scaleUpStage(numCurrWorkers, desiredWorkers, event.getType() + " with value " +
                          String.format(PercentNumberFormat, effectiveValue) +
                          " exceeded scaleUp threshold of " + strategy.getScaleUpAbovePct());
                      lastScaledAt = System.currentTimeMillis();
                      logger.info("lastScaledAt set to {} after scale up request", lastScaledAt);
                    } else {
                      logger.debug("scale up NOOP: desiredWorkers same as current workers");
                    }
                  } else if (stats.getLowThreshTriggered()) {
                    logger.info("Attempting to scale down stage " + stage + " of job " + jobId + " by " +
                        scalingPolicy.getDecrement() + " workers because " + event.getType() +
                        " is below scaleDownThreshold of " + strategy.getScaleDownBelowPct() +
                        " " + stats.getCurrentLowCount() + " times");
                    final int numCurrentWorkers = event.getNumWorkers();
                    final int desiredWorkers = scaler.getDesiredWorkersForScaleDown(scalingPolicy.getDecrement(), numCurrentWorkers);
                    if (desiredWorkers < numCurrentWorkers) {
                      scaler.scaleDownStage(numCurrentWorkers, desiredWorkers, event.getType() + " with value " +
                          String.format(PercentNumberFormat, effectiveValue) +
                          " is below scaleDown threshold of " + strategy.getScaleDownBelowPct());
                      lastScaledAt = System.currentTimeMillis();
                      logger.info("lastScaledAt set to {} after scale down request", lastScaledAt);
                    } else {
                      logger.debug("scale down NOOP: desiredWorkers same as current workers");
                    }
                  }
                } else {
                  logger.debug("lastScaledAt {} within cooldown period", lastScaledAt);
                }
              }
            }
          }
        };
      }
    }
}
