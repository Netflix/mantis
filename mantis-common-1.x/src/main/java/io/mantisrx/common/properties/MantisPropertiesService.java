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

package io.mantisrx.common.properties;

public class MantisPropertiesService implements MantisPropertiesLoader {

    private MantisPropertiesLoader mantisPropertiesLoader;

    public MantisPropertiesService(MantisPropertiesLoader mantisProps) {
        this.mantisPropertiesLoader = mantisProps;
    }

    @Override
    public void initalize() {

        mantisPropertiesLoader.initalize();

    }

    @Override
    public String getStringValue(String name, String defaultVal) {

        return mantisPropertiesLoader.getStringValue(name, defaultVal);
    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub

    }

}
