/*
 * Copyright 2024 Netflix, Inc.
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

package io.mantisrx.server.core.scheduler;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * A class that represents scheduling constraints. These constraints include the resource constraints encapsulated within `SizeDefinition`, and a map of assignment attributes (e.g. jdkVersion:17 or springBootVersion:3).
 * The class provides functionality to calculate the fitness of a given set of scheduling constraints and assignment attributes by taking container size characteristics and assignment attributes into account.
 */
@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
@AllArgsConstructor(staticName = "of")
@ToString
public class SchedulingConstraints {
    // Defines the resource constraints for scheduling
    SizeDefinition sizeDefinition;

    // Additional attributes for assignment (ie. jdkVersion:17 or springBootVersion:3)
    Map<String, String> assignmentAttributes;

    /**
     * Measures the compatibility of provided scheduling constraints with those of this instance.
     * Evaluation is based on two factors: the `SizeDefinition` of the worker and the attributes for assignment.
     *
     * @param constraints - SchedulingConstraints object, carries resource constraints and assignment attributes.
     * @param assignmentAttributesAndDefaults - Map containing assignment attributes to be verified and their default values.
     *
     * @return - The fitness value ranging between 0 and 1. Returns 0 if the provided assignment attributes
     * do not match those of this instance or if the size definition doesn't fit into the current instance's
     * size definition. Conversely, it returns a value close to 1 indicating higher similarity between
     * the size definitions.
     */
    public double calculateFitness(SchedulingConstraints constraints, Map<String, String> assignmentAttributesAndDefaults) {
        if (!areAllocationConstraintsSatisfied(constraints, assignmentAttributesAndDefaults)) {
            return 0.0;
        }
        return sizeDefinition.calculateFitness(constraints.getSizeDefinition());
    }

    /**
     * Determines whether the instance assignment attributes satisfy the given
     * scheduling constraints.
     *
     * @param constraints - SchedulingConstraints object containing the machine definition and assignment attributes.
     * @param assignmentAttributesAndDefaults - Map of assignment attributes and their default values to be considered.
     * @return - boolean result indicating whether the given assignment attributes satisfy the scheduling constraints.
     */
    public boolean areAllocationConstraintsSatisfied(SchedulingConstraints constraints, Map<String, String> assignmentAttributesAndDefaults) {
        return assignmentAttributesAndDefaults.entrySet()
            .stream()
            .allMatch(entry -> this.getAssignmentAttributes()
                .getOrDefault(entry.getKey(), entry.getValue())
                .equalsIgnoreCase(constraints.getAssignmentAttributes()
                    .getOrDefault(entry.getKey(), entry.getValue())));
    }

    /**
     * Checks whether the current `SchedulingConstraints` can fit within another `SchedulingConstraints` object, based on the `sizeDefinition` and assignment attributes.
     *
     * @param constraints - The `SchedulingConstraints` instance to compare with this instance.
     * @param assignmentAttributesAndDefaults - A map containing assignment attributes and their default values.
     *
     * @return - true if the current instance can fit within the passed instance, else returns false.
     */
    public boolean canFit(SchedulingConstraints constraints, Map<String, String> assignmentAttributesAndDefaults) {
        return areAllocationConstraintsSatisfied(constraints, assignmentAttributesAndDefaults) &&
            sizeDefinition.canFit(constraints.sizeDefinition);
    }

    public Map<String, String> getTags() {
        return sizeDefinition.getTags();
    }
}
