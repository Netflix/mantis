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

import io.mantisrx.server.core.Service;
import javax.annotation.Nullable;
import rx.Observable;


public interface MasterMonitor extends Service {

    /**
     * An Observable to track the current Mantis Master node.
     * @return {@link Observable} of {@link MasterDescription} to track changes
     */
    Observable<MasterDescription> getMasterObservable();

    /**
     * Returns the latest master if there's one. If there has been no master in recently history,
     * then this return null.
     *
     * @return Latest description of the master
     */
    @Nullable
    MasterDescription getLatestMaster();
}
