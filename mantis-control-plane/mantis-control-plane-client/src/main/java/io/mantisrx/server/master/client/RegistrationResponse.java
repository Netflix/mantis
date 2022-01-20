/*
 * Copyright 2022 Netflix, Inc.
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
package io.mantisrx.server.master.client;

import io.mantisrx.server.master.client.RegistrationResponse.Failure;
import io.mantisrx.server.master.client.RegistrationResponse.Success;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonSubTypes;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.mantisrx.shaded.com.google.common.base.Throwables;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
@JsonSubTypes({
    @Type(value = Success.class, name = "success"),
    @Type(value = Failure.class, name = "failure")
})
public interface RegistrationResponse {
  @Value
  class Success implements RegistrationResponse {
  }

  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  @Value
  class Failure implements RegistrationResponse {
    String error;

    public Throwable asThrowable() {
      return null;
    }
  }

  static Success success() {
    return new Success();
  }

  static Failure failure(Throwable throwable) {
    return new Failure(Throwables.getStackTraceAsString(throwable));
  }
}
