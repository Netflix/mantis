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

package io.mantisrx.runtime;

import io.mantisrx.runtime.sink.SelfDocumentingSink;
import io.mantisrx.runtime.sink.Sink;


public class SinkHolder<T> {

    private Metadata metadata;
    private final Sink<T> sinkAction;

    public SinkHolder(final SelfDocumentingSink<T> sinkAction) {
        this.metadata = sinkAction.metadata();
        this.sinkAction = sinkAction;
    }

    public SinkHolder(final Sink<T> sinkAction) {
        this.sinkAction = sinkAction;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public Sink<T> getSinkAction() {
        return sinkAction;
    }

    public boolean isPortRequested() {
        return true;
    }
}
