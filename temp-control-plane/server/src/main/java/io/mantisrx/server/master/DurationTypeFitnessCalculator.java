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

import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.server.master.scheduler.ScheduleRequest;


public class DurationTypeFitnessCalculator implements VMTaskFitnessCalculator {

    @Override
    public String getName() {
        return "Mantis Job Duration Type Task Fitness Calculator";
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        MantisJobDurationType durationType = ((ScheduleRequest) taskRequest).getDurationType();
        int totalTasks = 0;
        int sameTypeTasks = 0;
        for (TaskRequest request : targetVM.getRunningTasks()) {
            totalTasks++;
            if (((ScheduleRequest) request).getDurationType() == durationType)
                sameTypeTasks++;
        }
        for (TaskAssignmentResult result : targetVM.getTasksCurrentlyAssigned()) {
            totalTasks++;
            if (((ScheduleRequest) result.getRequest()).getDurationType() == durationType)
                sameTypeTasks++;
        }
        if (totalTasks == 0)
            return 0.9; // an arbitrary preferential value to indicate that a fresh new host is not perfect
        // fit but a better fit than a host that has tasks of different type
        return (double) sameTypeTasks / (double) totalTasks;
    }
}
