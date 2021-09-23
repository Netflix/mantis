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

package io.mantisrx.server.master.scheduler;

import com.netflix.fenzo.VirtualMachineCurrentState;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


public class SchedulingStateManager {

    private final AtomicReference<List<VirtualMachineCurrentState>> lastKnownVMState = new AtomicReference<>(null);

    public List<VirtualMachineCurrentState> getVMCurrentState() {
        return lastKnownVMState.get();
    }

    public void setVMCurrentState(final List<VirtualMachineCurrentState> latestState) {
        lastKnownVMState.set(latestState);
    }
}
