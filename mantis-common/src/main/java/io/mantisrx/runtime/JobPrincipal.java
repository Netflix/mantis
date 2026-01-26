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

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;


public class JobPrincipal {

    private final String email;
    private final PrincipalType type;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobPrincipal(@JsonProperty("email") String email,
                        @JsonProperty("type") PrincipalType type) {
        this.email = email;
        this.type = type != null ? type : PrincipalType.USER;
    }

    public String getEmail() {
        return email;
    }

    public PrincipalType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "JobPrincipal{" +
                "email='" + email + '\'' +
                ", type=" + type +
                '}';
    }

    public enum PrincipalType {
        USER,
        GROUP
    }
}
