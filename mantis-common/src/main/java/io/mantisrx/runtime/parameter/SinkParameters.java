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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * @author njoshi
 * Parameters used to connect to an SSE sink
 */

public class SinkParameters {

    final List<SinkParameter> params;

    public SinkParameters(List<SinkParameter> params) {
        this.params = params;
    }

    public List<SinkParameter> getSinkParams() {
        return Collections.unmodifiableList(this.params);
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((params == null) ? 0 : params.hashCode());
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
        SinkParameters other = (SinkParameters) obj;
        if (params == null) {
            if (other.params != null)
                return false;
        } else if (!params.equals(other.params))
            return false;
        return true;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (params != null && params.size() > 0) {
            int size = params.size();
            int count = 0;
            sb.append("?");

            for (SinkParameter param : params) {
                sb.append(param.getName());
                sb.append("=");
                if (param.getValue() != null && param.getEncodedValue() != null) {

                    sb.append(param.getEncodedValue());

                }
                if (count < size - 1) {
                    sb.append("&");
                }
                count++;

            }

        }

        return sb.toString();
    }


    public static class Builder {

        List<SinkParameter> parameters = new ArrayList<SinkParameter>();

        public Builder parameters(SinkParameter... params) {
            for (SinkParameter p : params)
                this.parameters.add(p);
            return this;
        }

        public Builder withParameter(SinkParameter param) {
            this.parameters.add(param);
            return this;
        }

        public Builder withParameter(String name, String value) throws UnsupportedEncodingException {
            this.parameters(new SinkParameter(name, value));
            return this;
        }

        public SinkParameters build() {
            return new SinkParameters(parameters);
        }

    }


}
