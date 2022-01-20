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

import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.Status;
import lombok.RequiredArgsConstructor;
import rx.Observable;
import rx.Subscription;
import rx.functions.Func1;

@RequiredArgsConstructor
public abstract class BaseReportStatusService extends BaseService implements ReportStatusService {
  private final Observable<Observable<Status>> statusObservable;

  private Subscription subscription;

  @Override
  public final void start() {
    subscription = statusObservable
        .flatMap((Func1<Observable<Status>, Observable<Status>>) status -> status)
        .subscribe(this::updateTaskExecutionStatus);
  }

  @Override
  public final void shutdown() {
    subscription.unsubscribe();
  }

  @Override
  public final void enterActiveMode() { }
}
