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
package com.mantisrx.common.utils;

import io.mantisrx.shaded.com.google.common.util.concurrent.FutureCallback;
import io.mantisrx.shaded.com.google.common.util.concurrent.Futures;
import io.mantisrx.shaded.com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.CompletableFuture;

public class ListenableFutures {
  private static class ListenableFutureAdapter<T> {

    private final ListenableFuture<T> listenableFuture;
    private final CompletableFuture<T> completableFuture;

    private ListenableFutureAdapter(ListenableFuture<T> listenableFuture) {
      this.listenableFuture = listenableFuture;
      this.completableFuture = new CompletableFuture<T>() {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          boolean cancelled = listenableFuture.cancel(mayInterruptIfRunning);
          super.cancel(cancelled);
          return cancelled;
        }
      };

      Futures.addCallback(this.listenableFuture, new FutureCallback<T>() {
        @Override
        public void onSuccess(T result) {
          completableFuture.complete(result);
        }

        @Override
        public void onFailure(Throwable ex) {
          completableFuture.completeExceptionally(ex);
        }
      });
    }

    public CompletableFuture<T> getCompletableFuture() {
      return completableFuture;
    }
  }

  public static <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> listenableFuture) {
    ListenableFutureAdapter<T> listenableFutureAdapter = new ListenableFutureAdapter<>(listenableFuture);
    return listenableFutureAdapter.getCompletableFuture();
  }
}
