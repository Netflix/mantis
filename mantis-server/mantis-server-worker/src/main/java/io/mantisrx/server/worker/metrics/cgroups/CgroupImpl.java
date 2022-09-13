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

package io.mantisrx.server.worker.metrics.cgroups;

import io.mantisrx.shaded.org.apache.curator.shaded.com.google.common.base.Preconditions;
import io.vavr.Tuple2;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CgroupImpl implements Cgroup {
    private final String path;

    @Override
    public Long getMetric(String subsystem, String metricName) throws IOException {
        Path metricPath = Paths.get(path, subsystem, metricName);
        try {
            return Files.readAllLines(metricPath).stream().findFirst().map(Long::valueOf).get();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Example usage:
     * user 43873627
     * system 4185541
     *
     * @param subsystem subsystem the stat file is part of.
     * @param stat name of the stat file
     * @return map of metrics to their corresponding values
     * @throws IOException
     */
    @Override
    public Map<String, Long> getStats(String subsystem, String stat) throws IOException {
        Path statPath = Paths.get(path, subsystem, stat);
        return
            Files.readAllLines(statPath)
                .stream()
                .map(l -> {
                    String[] parts = l.split("\\s+");
                    Preconditions.checkArgument(parts.length == 2, "Expected two parts only but was {} parts", parts.length);
                    return new Tuple2<>(parts[0], Long.parseLong(parts[1]));
                })
                .collect(Collectors.toMap(t -> t._1, t -> t._2));
    }
}
