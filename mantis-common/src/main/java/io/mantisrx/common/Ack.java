/*
 * Copyright 2022 Netflix, Inc.
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
package io.mantisrx.common;

import java.io.Serializable;
import lombok.Value;

/**
 * Ack is sent whenever an effect has taken place on the side of the receiver.
 * Ack instance supports both JSON / Java serialization.
 */
@Value(staticConstructor = "getInstance")
public class Ack implements Serializable {
    private static final long serialVersionUID = 1L;
}
