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

package io.mantisrx.control.controllers;

import io.mantisrx.control.IController;

public class BoundToInterval extends IController {

  private final double min;
  private final double max;

  public BoundToInterval(double min, double max) {
    this.min = min;
    this.max = max;
  }

  @Override
  protected Double processStep(final Double input) {
    double x = input;
    return x > this.max ? this.max :
      x < this.min ? this.min :
      x;
  }

  public static BoundToInterval of(double min, double max) {
    return new BoundToInterval(min, max);
  }
}
