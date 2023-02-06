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

package io.mantisrx.runtime.core.sources;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import rx.Subscription;

public class ObservableSourceImpl<R> implements SourceFunction<R> {
    @Getter
    private final Source<R> source;
    private final AtomicReference<R> elemContainer = new AtomicReference<>();
    private Subscription subscription;

    public ObservableSourceImpl(Source<R> source) {
        this.source = source;
    }

    @Override
    public void init() {
        Context context = new Context();
        Index index = new Index(0, 0);
        this.source.init(context, index);
        subscription = this.source
            .call(context, index)
            .flatMap(x -> x)
            // single element buffer as a POC!
            .subscribe(elemContainer::set);
    }

    @Override
    public void close() throws IOException {
        if (subscription != null) {
            subscription.unsubscribe();
        }
    }

    @Override
    public R next() {
        return elemContainer.get();
    }
}
