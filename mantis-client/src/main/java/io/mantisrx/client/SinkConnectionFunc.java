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

package io.mantisrx.client;

import rx.functions.Action1;
import rx.functions.Func2;


public interface SinkConnectionFunc<T> extends Func2<String, Integer, SinkConnection<T>> {

    default SinkConnection<T> call(String t1, Integer t2, Action1<Boolean> updateConxStatus,
                                   Action1<Boolean> updateDataRecvngStatus, long dataRecvTimeoutSecs) {
        return call(t1, t2, updateConxStatus, updateDataRecvngStatus, dataRecvTimeoutSecs, false);
    }

    SinkConnection<T> call(String t1, Integer t2, Action1<Boolean> updateConxStatus,
                           Action1<Boolean> updateDataRecvngStatus, long dataRecvTimeoutSecs, boolean disablePingFiltering);
}
