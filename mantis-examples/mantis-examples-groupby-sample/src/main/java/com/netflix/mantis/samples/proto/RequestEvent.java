/*
 * Copyright 2021 Netflix, Inc.
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

import io.mantisrx.common.codec.Codec;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.Serializable;
import lombok.Builder;
import lombok.Data;


/**
 * A simple POJO that holds data about a request event.
 */
@Data
@Builder
public class RequestEvent implements Serializable {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectReader requestEventReader = mapper.readerFor(RequestEvent.class);

    private final String requestPath;
    private final String ipAddress;
    private final int latency;

    /**
     * The codec defines how this class should be serialized before transporting across network.
     * @return
     */
    public static Codec<RequestEvent> requestEventCodec() {

        return new Codec<RequestEvent>() {
            @Override
            public RequestEvent decode(byte[] bytes) {

                try {
                    return requestEventReader.readValue(bytes);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public byte[] encode(final RequestEvent value) {

                try {
                    return mapper.writeValueAsBytes(value);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
