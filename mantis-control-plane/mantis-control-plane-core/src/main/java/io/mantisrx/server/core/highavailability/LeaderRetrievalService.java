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

package io.mantisrx.server.core.highavailability;

import io.mantisrx.shaded.com.google.common.util.concurrent.Service;
import io.mantisrx.shaded.org.apache.curator.framework.listen.Listenable;
import javax.annotation.Nullable;

public interface LeaderRetrievalService extends Service {
    Listenable<Listener> getListenable();

    interface Listener {
        void onLeaderChanged(@Nullable byte[] leaderMetadata);
    }
}
