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

package io.mantisrx.server.agent.metrics.cgroups;

import io.mantisrx.shaded.org.apache.curator.shaded.com.google.common.base.Preconditions;
import io.vavr.Tuple2;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CgroupImpl implements Cgroup {

    private final String path;

    private List<String> getSubsystems() {
        return
            Arrays.asList(Objects.requireNonNull(Paths.get(path).toFile().listFiles()))
                .stream()
                .filter(s -> s.isDirectory())
                .map(s -> s.getName())
                .collect(Collectors.toList());
    }

    /**
     * Maybe change this to the below command in the future.
     * <p>
     * ``` stat -fc %T /sys/fs/cgroup/ ``` This should return cgroup2fs for cgroupv2 and tmpfs for
     * cgroupv1.
     */
    @Getter(lazy = true, value = AccessLevel.PRIVATE)
    private final boolean old = getSubsystems().size() > 0;

    @Override
    public Boolean isV1() {
        return isOld();
    }

    @Override
    public Boolean isV2() {
        return !isV1();
    }

    @Override
    public List<Long> getMetrics(@Nullable String subsystem, String metricName) throws IOException {
        if (subsystem == null) {
            subsystem = "";
        }

        Path metricPath = Paths.get(path, subsystem, metricName);
        try {
            return
                Files.readAllLines(metricPath)
                    .stream()
                    .findFirst()
                    .map(s -> Arrays.asList(s.split(" ")))
                    .orElse(Collections.emptyList())
                    .stream()
                    .map(CgroupImpl::convertStringToLong)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Long getMetric(@Nullable String subsystem, String metricName) throws IOException {
        if (subsystem == null) {
            subsystem = "";
        }

        Path metricPath = Paths.get(path, subsystem, metricName);
        try {
            return Files.readAllLines(metricPath).stream().findFirst()
                .map(CgroupImpl::convertStringToLong).get();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Example usage: user 43873627 system 4185541
     *
     * @param subsystem subsystem the stat file is part of.
     * @param stat      name of the stat file
     * @return map of metrics to their corresponding values
     * @throws IOException
     */
    @Override
    public Map<String, Long> getStats(@Nullable String subsystem, String stat) throws IOException {
        if (subsystem == null) {
            subsystem = "";
        }

        Path statPath = Paths.get(path, subsystem, stat);
        return
            Files.readAllLines(statPath)
                .stream()
                .map(l -> {
                    String[] parts = l.split("\\s+");
                    Preconditions.checkArgument(parts.length == 2,
                        "Expected two parts only but was {} parts", parts.length);
                    return new Tuple2<>(parts[0], convertStringToLong(parts[1]));
                })
                .collect(Collectors.toMap(t -> t._1, t -> t._2));
    }

    /**
     * Convert a number from its string representation to a long.
     *
     * @param strval: value to convert
     * @return The converted long value. Long max value is returned if the string representation
     * exceeds the range of type long.
     */
    private static long convertStringToLong(String strval) {
        try {
            return Long.parseLong(strval);
        } catch (NumberFormatException e) {
            // For some properties (e.g. memory.limit_in_bytes, cgroups v1) we may overflow
            // the range of signed long. In this case, return Long max value.
            return Long.MAX_VALUE;
        }
    }
}
