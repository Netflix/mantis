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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;

import org.junit.Test;


public class SinkParameterTest {

    @Test
    public void testGenerateURI() throws Exception {

        SinkParameters sps;
        sps = new SinkParameters.Builder().withParameter("p1", "v1").withParameter("p2", "v2").withParameter("p3", "v3").build();
        assertEquals("?p1=v1&p2=v2&p3=v3", sps.toString());
    }

    @Test
    public void testGenerateURI2() throws Exception {

        SinkParameters sps;
        sps = new SinkParameters.Builder().withParameter("p1", "v1").withParameter("p2", null).withParameter("p3", "v3").build();
        assertEquals("?p1=v1&p2=&p3=v3", sps.toString());
    }

    @Test
    public void testGenerateURI3() throws Exception {

        SinkParameters sps;
        sps = new SinkParameters.Builder().withParameter("p1", "select esn, country where e[\"response.header.x-netflix.api-script-endpoint\"]==\"/account/geo\"").build();
        assertEquals("?p1=select+esn%2C+country+where+e%5B%22response.header.x-netflix.api-script-endpoint%22%5D%3D%3D%22%2Faccount%2Fgeo%22", sps.toString());
    }

    @Test
    public void testGenerateURI4() {
        SinkParameters sps = new SinkParameters.Builder().build();
        assertEquals("", sps.toString());
    }


}
