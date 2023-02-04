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

package com.netflix.mantis.samples.proto;

import io.mantisrx.common.codec.Codec;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.Serializable;
import lombok.Builder;
import lombok.Data;


/**
 * A simple POJO that holds the count of how many times a particular request path was invoked.
 */
@Data
@Builder
public class RequestAggregation implements Serializable {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectReader requestAggregationReader = mapper.readerFor(RequestAggregation.class);

    private String path;
    private int count;


    /**
     * Codec is used to customize how data is serialized before transporting across network boundaries.
     * @return
     */
    public static Codec<RequestAggregation> requestAggregationCodec() {

        return new Codec<RequestAggregation>() {
            @Override
            public RequestAggregation decode(byte[] bytes) {

                try {
                    return requestAggregationReader.readValue(bytes);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public byte[] encode(final RequestAggregation value) {

                try {
                    return mapper.writeValueAsBytes(value);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

}
