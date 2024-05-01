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
import java.util.concurrent.atomic.AtomicLong;

public class BoostComputer extends IController {

  private final AtomicLong size;

  public BoostComputer(final AtomicLong size) {
    this.size = size;
  }

  @Override
  protected Double processStep(final Double input) {
    return (input + this.size.get()) / this.size.get();
  }
}
