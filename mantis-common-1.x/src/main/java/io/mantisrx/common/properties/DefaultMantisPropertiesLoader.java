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

import java.util.Map;
import java.util.Properties;


public class DefaultMantisPropertiesLoader implements MantisPropertiesLoader {

    protected Properties props;
    private Map<String, String> env;

    public DefaultMantisPropertiesLoader(Properties props) {
        this.props = props;
        env = System.getenv();
    }


    /* (non-Javadoc)
     * @see io.mantisrx.common.MantisProperties#getStringValue(java.lang.String)
     */
    @Override
    public String getStringValue(String name, String defaultVal) {
        if (name != null) {
            if (env.containsKey("JOB_PARAM_" + name)) {
                System.out.println(" Parameter overridden in job " + name + " value " + env.get("JOB_PARAM_" + name));
                return env.get("JOB_PARAM_" + name);
            } else if (env.containsKey(name)) {
                System.out.println(" Property set on env " + name + " value " + env.get(name));
                return env.get(name);
            }
        } //
        return defaultVal;


    }

    @Override
    public void initalize() {
        // TODO Auto-generated method stub

    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub

    }
}
