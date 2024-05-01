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

import io.mantisrx.control.IController;
import io.mantisrx.shaded.com.google.common.cache.Cache;
import io.mantisrx.shaded.com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The OscillationDetctor collects scaling events from the actuator and
 * determines wether the event was a scale up / down. It then computes
 * a gain factor (see `OscillationDetector#computeOscillationFactor`) and sets
 * the dampener based on this factor.
 **/
public class OscillationDetector extends IController {

  private final Cache<Long, Double> history;
  private Double previous = -1.0;
  private final Consumer<Double> callback;

  public OscillationDetector(int historyMinutes, Consumer<Double> callback) {
    this.history = CacheBuilder.newBuilder()
      .maximumSize(12)
      .expireAfterWrite(historyMinutes, TimeUnit.MINUTES)
      .build();
    this.callback = callback;
  }

  @Override
  protected Double processStep(Double scale) {

    this.previous = this.previous == null ? scale : this.previous;
    double delta = scale > previous ? 1.0 : -1.0;
    this.previous = scale;

    history.put(System.currentTimeMillis(), delta);
    this.callback.accept(computeOscillationFactor(history));
    return scale;
  }

  /**
   * Computes the oscillation factor which is the percentage of scaling events
   * which were in different directions.
   * 0.5 <= oscillationFactor <= 1.0
   *
   * @param actionCache A cache of timestamp -> scale
   * @return The computed oscillation factor.
   */
  private double computeOscillationFactor(Cache<Long, Double> actionCache) {
    long nUp = actionCache.asMap().values().stream().filter(x -> x > 0.0).count();
    long nDown = actionCache.asMap().values().stream().filter(x -> x < 0.0).count();
    long n = nUp + nDown;

    return n == 0
      ? 1.0
      : nUp > nDown
      ? (1.0 * nUp) / n
      : (1.0 * nDown) / n;
  }
}
