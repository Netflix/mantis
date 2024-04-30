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

package io.mantisrx.control;

import rx.Observable;
import rx.Subscriber;

/**
 * The Feedback Principle: Constantly compare the actual output to the
 * setpoint; then apply a corrective action in the proper direction and
 * approximately of the correct size.
 *
 * Iteratively applying changes in the correct direction allows this
 * system to converge onto the correct value over time.
 *
 */
public abstract class IController implements Observable.Operator<Double, Double> {

  private final IController parent = this;

  /**
   * Implementation method for Controller components. Surrounding RxJava machinery will call this method.
   *
   * @param value Input from previous stage of control loop processing.
   * @return Output intended for next stage of control loop processing.
   */
  protected abstract Double processStep(Double value);


  @Override
  public Subscriber<? super Double> call(final Subscriber<? super Double> s) {

    return new Subscriber<Double>(s) {
      @Override
      public void onCompleted() {
        if (!s.isUnsubscribed()) {
          s.onCompleted();
        }
      }

      @Override
      public void onError(Throwable t) {
        if (!s.isUnsubscribed()) {
          s.onError(t);
        }
      }

      @Override
      public void onNext(Double error) {
        Double controlAction = parent.processStep(error);
        if (!s.isUnsubscribed()) {
          s.onNext(controlAction);
        }
      }
    };
  }
}
