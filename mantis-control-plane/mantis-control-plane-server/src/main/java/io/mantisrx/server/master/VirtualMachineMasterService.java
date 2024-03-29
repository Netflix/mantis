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

package io.mantisrx.server.master;

import com.netflix.fenzo.VirtualMachineLease;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.scheduler.LaunchTaskRequest;
import io.mantisrx.server.master.scheduler.ScheduleRequest;
import java.util.List;
import java.util.Map;


public interface VirtualMachineMasterService {

    Map<ScheduleRequest, LaunchTaskException> launchTasks(List<LaunchTaskRequest> requests, List<VirtualMachineLease> leases);

    void rejectLease(VirtualMachineLease lease);

    void killTask(final WorkerId workerId);
}
