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

package io.mantisrx.master.scheduler;

import akka.actor.ActorRef;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.WorkerEvent;

public class JobMessageRouterImpl implements JobMessageRouter {

	final ActorRef jobClusterManagerRef;
	public JobMessageRouterImpl(final ActorRef jobClusterManagerActorRef) {
		this.jobClusterManagerRef = jobClusterManagerActorRef;
	}
	
	@Override
	public boolean routeWorkerEvent(final WorkerEvent workerEvent) {
		jobClusterManagerRef.tell(workerEvent, ActorRef.noSender());
		/* TODO - need a return value to indicate to scheduling service if the worker was marked Launched successfully
		 from the Job Management perspective, only then the Task is dispatched to Mesos. If this method returns false for the
		 WorkerLaunched event, no mesos task is launched. The return value would have to be async to work with the Actor model
		 unless we use the Ask pattern here. */
		return true;
	}

}
