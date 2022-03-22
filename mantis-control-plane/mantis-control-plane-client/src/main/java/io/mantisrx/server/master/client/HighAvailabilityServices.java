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
package io.mantisrx.server.master.client;

import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.shaded.com.google.common.util.concurrent.Service;

/**
 * HighAvailabilityServices is a container for a group of services which are considered to be highly available because
 * of multiple standbys capable of handling the service in case the leader goes down for instance.
 *
 * In Mantis, the following services are considered highly-available:
 *   1. Mantis master which handles all the job-cluster/job/stage/worker interactions.
 *   2. Resource Manager which handles all the resource specific interactions such as resource status updates,
 *   registrations and heartbeats.
 *
 * These services can be obtained from the HighAvailabilityServices implementation.
 */
public interface HighAvailabilityServices extends Service {
  MantisMasterGateway getMasterClientApi();

  MasterMonitor getMasterMonitor();
}
