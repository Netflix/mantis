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

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import java.util.Map;
import org.apache.mesos.Protos;


public class ClusterAffinityConstraint implements ConstraintEvaluator {

    private final String asgAttributeName;
    private final String clusterName;
    private final String name;

    public ClusterAffinityConstraint(String clusterAttributeName, String clusterName) {
        this.asgAttributeName = clusterAttributeName;
        this.clusterName = clusterName;
        this.name = ClusterAffinityConstraint.class.getName() + "-" + clusterAttributeName;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Determines whether a particular target host is appropriate for a particular task request by rejecting any
     * host that doesn't belong to the specified cluster.
     *
     * @param taskRequest      describes the task being considered for assignment to the host
     * @param targetVM         describes the host being considered as a target for the task
     * @param taskTrackerState describes the state of tasks previously assigned or already running throughout
     *                         the system
     *
     * @return a successful Result if the target does not have the same value for its unique constraint
     * attribute as another host that has already been assigned a co-task of {@code taskRequest}, or an
     * unsuccessful Result otherwise
     */
    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM,
                           TaskTrackerState taskTrackerState) {

        //String clusterName = AttributeUtilities.getAttrValue(targetVM.getCurrAvailableResources(), hostAttributeName);
        Map<String, Protos.Attribute> attributeMap = targetVM.getCurrAvailableResources().getAttributeMap();
        if (asgAttributeName != null && attributeMap != null && attributeMap.get(asgAttributeName) != null) {
            if (attributeMap.get(asgAttributeName).getText().isInitialized()) {
                String targetClusterName = attributeMap.get(asgAttributeName).getText().getValue();
                if (targetClusterName.startsWith(clusterName)) {
                    return new Result(true, "");
                } else {
                    return new Result(false, asgAttributeName + " does not begin with " + clusterName);
                }

            }

        }
        return new Result(false, asgAttributeName + " unavailable on host " + targetVM.getCurrAvailableResources().hostname());
    }

}
