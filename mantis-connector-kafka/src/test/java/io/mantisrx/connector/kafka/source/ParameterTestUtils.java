/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.kafka.source;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.mantisrx.runtime.parameter.Parameters;


public class ParameterTestUtils {
    static Parameters createParameters(Object... params) {
        Map<String, Object> paramsMap = new HashMap();
        Set<String> requiredParams = new HashSet<>();

        List<Object> paramsList = Arrays.asList(params);
        Iterator<Object> iterator = paramsList.iterator();
        while (iterator.hasNext()) {
            Object token = iterator.next();
            if (token instanceof String) {
                String paramkey = (String) token;
                if (iterator.hasNext()) {
                    Object pVal = iterator.next();
                    paramsMap.put(paramkey, pVal);
                    requiredParams.add(paramkey);
                }
            } else {
                throw new IllegalArgumentException("parameter key must be of type String, parameter key not supported with type " + token.getClass());
            }
        }
        return new Parameters(paramsMap, requiredParams, requiredParams);
    }
}
