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

package io.mantisrx.master.api.akka.route.utils;

import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class QueryParamUtils {
    private static final Logger logger = LoggerFactory.getLogger(QueryParamUtils.class);

    public static Optional<String> paramValue(final Map<String, List<String>> params, final String key) {
        List<String> values = params.get(key);
        return Optional.ofNullable(values)
            .filter(vs -> vs.size() > 0)
            .map(x -> x.get(0));
    }

    public static Optional<Integer> paramValueAsInt(final Map<String, List<String>> params, final String key) {
        List<String> values = params.get(key);
        return Optional.ofNullable(values)
            .filter(vs -> vs.size() > 0)
            .map(x -> {
                try {
                    return Integer.parseInt(x.get(0));
                } catch (NumberFormatException e) {
                    return null;
                }
            });
    }

    public static Optional<Boolean> paramValueAsBool(final Map<String, List<String>> params, final String key) {
        List<String> values = params.get(key);
        return Optional.ofNullable(values)
            .filter(vs -> vs.size() > 0)
            .map(x -> {
                try {
                    return Boolean.valueOf(x.get(0));
                } catch (NumberFormatException e) {
                    return null;
                }
            });
    }

    public static List<Integer> paramValuesAsInt(final Map<String, List<String>> params, final String key) {
        List<String> values = params.get(key);
        if (values == null) {
            return Collections.emptyList();
        } else {
            return values.stream().map(s -> {
                try {
                    return Integer.parseInt(s);
                } catch (NumberFormatException e) {
                    return null;
                }
            }).collect(Collectors.toList());
        }
    }

    public static List<WorkerState.MetaState> paramValuesAsMetaState(final Map<String, List<String>> params, final String key) {
        List<String> values = params.get(key);
        if (values != null) {
            return new ArrayList<>(values.stream()
                .map(s -> WorkerState.MetaState.valueOf(s))
                .collect(Collectors.toSet()));
        } else {
            return Collections.emptyList();
        }
    }
}
