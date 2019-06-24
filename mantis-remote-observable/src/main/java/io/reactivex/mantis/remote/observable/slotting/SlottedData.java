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

package io.reactivex.mantis.remote.observable.slotting;

import java.util.Set;

import io.mantisrx.common.network.Endpoint;


public class SlottedData<T> {

    private Set<Endpoint> endpoints;
    private T data;

    public SlottedData(Set<Endpoint> endpoints, T data) {
        this.endpoints = endpoints;
        this.data = data;
    }

    public boolean sendToEndpoint(Endpoint endpoint) {
        return endpoints.contains(endpoint);
    }

    public T getData() {
        return data;
    }
}
