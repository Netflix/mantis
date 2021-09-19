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
package io.mantisrx.server.worker;

import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.ExecutionAttemptID;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import rx.Observer;
import rx.subjects.PublishSubject;

@RequiredArgsConstructor
public class TaskExecutor implements TaskExecutorGateway {
  // observer to which the ExecuteStageRequest needs to be published
  private final Observer<WrappedExecuteStageRequest> executeStageRequestObserver;

  @Override
  public CompletableFuture<Ack> submitTask(ExecuteStageRequest request) {
    executeStageRequestObserver.onNext(new WrappedExecuteStageRequest(PublishSubject.create(), request));
    return CompletableFuture.completedFuture(Ack.getInstance());
  }

  @Override
  public CompletableFuture<Ack> cancelTask(ExecutionAttemptID executionAttemptID) {
    return null;
  }

  @Override
  public CompletableFuture<String> requestThreadDump(Duration timeout) {
    return null;
  }
}
