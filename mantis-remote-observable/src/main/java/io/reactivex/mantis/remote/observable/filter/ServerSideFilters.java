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

package io.reactivex.mantis.remote.observable.filter;

import java.util.Map;
import rx.functions.Func1;


public class ServerSideFilters {

    private ServerSideFilters() {}

    public static <T> Func1<Map<String, String>, Func1<T, Boolean>> noFiltering() {
        return new Func1<Map<String, String>, Func1<T, Boolean>>() {
            @Override
            public Func1<T, Boolean> call(Map<String, String> t1) {
                return new Func1<T, Boolean>() {
                    @Override
                    public Boolean call(T t1) {
                        return true;
                    }
                };
            }
        };
    }

    public static Func1<Map<String, String>, Func1<Integer, Boolean>> oddsAndEvens() {
        return new Func1<Map<String, String>, Func1<Integer, Boolean>>() {
            @Override
            public Func1<Integer, Boolean> call(final Map<String, String> params) {
                return new Func1<Integer, Boolean>() {
                    @Override
                    public Boolean call(Integer t1) {
                        if (params == null || params.isEmpty() ||
                                !params.containsKey("type")) {
                            return true; // no filtering
                        } else {
                            String type = params.get("type");
                            if ("even".equals(type)) {
                                return (t1 % 2 == 0);
                            } else if ("odd".equals(type)) {
                                return (t1 % 2 != 0);
                            } else {
                                throw new RuntimeException("Unsupported filter param 'type' for oddsAndEvens: " + type);
                            }
                        }
                    }
                };
            }
        };
    }
}
