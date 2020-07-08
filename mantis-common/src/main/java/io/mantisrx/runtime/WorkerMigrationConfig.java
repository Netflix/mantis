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

package io.mantisrx.runtime;

import java.util.Optional;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;


public class WorkerMigrationConfig {

    public static final WorkerMigrationConfig DEFAULT = new WorkerMigrationConfig(MigrationStrategyEnum.PERCENTAGE, "{\"percentToMove\":25,\"intervalMs\":60000}");
    private MigrationStrategyEnum strategy;
    private String configString;
    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public WorkerMigrationConfig(@JsonProperty("strategy") final MigrationStrategyEnum strategy,
                                 @JsonProperty("configString") final String configString) {
        this.strategy = Optional.ofNullable(strategy).orElse(MigrationStrategyEnum.ONE_WORKER);
        this.configString = configString;
    }

    public static void main(String[] args) {
        final WorkerMigrationConfig workerMigrationConfig = new WorkerMigrationConfig(MigrationStrategyEnum.ONE_WORKER, "{'name':'value'}");
        System.out.println(workerMigrationConfig);
        final WorkerMigrationConfig workerMigrationConfig2 = WorkerMigrationConfig.DEFAULT;
        System.out.println(workerMigrationConfig2);

    }

    public MigrationStrategyEnum getStrategy() {
        return strategy;
    }

    public String getConfigString() {
        return configString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WorkerMigrationConfig that = (WorkerMigrationConfig) o;

        if (strategy != that.strategy) return false;
        return configString != null ? configString.equals(that.configString) : that.configString == null;

    }

    @Override
    public int hashCode() {
        int result = strategy != null ? strategy.hashCode() : 0;
        result = 31 * result + (configString != null ? configString.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "WorkerMigrationConfig{" +
                "strategy=" + strategy +
                ", configString='" + configString + '\'' +
                '}';
    }

    public enum MigrationStrategyEnum {ONE_WORKER, PERCENTAGE}
}