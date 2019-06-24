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

package io.reactivex.mantis.remote.observable;

import com.netflix.numerus.NumerusRollingNumberEvent;


public enum RemoteRxNumerusEvent implements NumerusRollingNumberEvent {

    BOOTSTRAP(1),
    NEXT(1), NEXT_FAILURE(1),
    ERROR(1), ERROR_FAILURE(1),
    COMPLETED(1), COMPLETED_FAILURE(1),
    SUBSCRIBE(1),
    UNSUBSCRIBE(1),
    CONNECTION_COUNT(1);

    private final int type;

    RemoteRxNumerusEvent(int type) {
        this.type = type;
    }

    @Override
    public boolean isCounter() {
        return type == 1;
    }

    @Override
    public boolean isMaxUpdater() {
        return type == 2;
    }

    @Override
    public NumerusRollingNumberEvent[] getValues() {
        return values();
    }

}
