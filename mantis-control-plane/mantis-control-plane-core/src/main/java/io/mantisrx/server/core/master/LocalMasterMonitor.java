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

package io.mantisrx.server.core.master;

import rx.Observable;


/**
 * A {@code MasterMonitor} implementation that does not monitor anything. Use this
 * class for local testing.
 */
public class LocalMasterMonitor implements MasterMonitor {

    private final MasterDescription master;

    public LocalMasterMonitor(MasterDescription master) {
        this.master = master;
    }


    @Override
    public Observable<MasterDescription> getMasterObservable() {
        return Observable.just(master);
    }

    @Override
    public MasterDescription getLatestMaster() {
        return master;
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void enterActiveMode() {

    }
}
