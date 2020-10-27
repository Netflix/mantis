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

package io.mantisrx.runtime.parameter;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;


/**
 * Sink parameter tuple
 *
 * @author njoshi
 */

public class SinkParameter {

    final private String name;
    final private String value;
    final private String encodedValue;

    /**
     * @param name
     * @param value
     *
     * @throws UnsupportedEncodingException
     */
    public SinkParameter(String name, String value) throws UnsupportedEncodingException {
        this.name = name;
        this.value = value;

        encodedValue = URLEncoder.encode(value, "UTF-8");

    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public String getEncodedValue() {
        return encodedValue;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((encodedValue == null) ? 0 : encodedValue.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SinkParameter other = (SinkParameter) obj;
        if (encodedValue == null) {
            if (other.encodedValue != null)
                return false;
        } else if (!encodedValue.equals(other.encodedValue))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }


}
