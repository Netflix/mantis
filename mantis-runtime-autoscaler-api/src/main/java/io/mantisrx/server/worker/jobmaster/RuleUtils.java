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

package io.mantisrx.server.worker.jobmaster;

import java.util.Comparator;

/**
 * Utility class for rule-related operations.
 */
public class RuleUtils {

    /**
     * Returns a default comparator for rule IDs that sorts them based on their integer values.
     *
     * @return A comparator for rule IDs
     */
    public static Comparator<String> defaultIntValueRuleIdComparator() {
        return Comparator.comparingInt(Integer::parseInt);
    }
}
