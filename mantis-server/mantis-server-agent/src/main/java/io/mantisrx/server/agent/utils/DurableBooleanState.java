/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.server.agent.utils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DurableBooleanState {
    private final String fileName;

    public DurableBooleanState(String fileName) {
        this.fileName = fileName;
    }

    public void setState(boolean state) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(fileName)) {
            fos.write(state ? 1 : 0);
            fos.flush();
            log.info("The state has been stored successfully!");
        }
    }

    public boolean getState() throws IOException {
        try (FileInputStream fis = new FileInputStream(fileName)) {
            int state = fis.read();
            return state != 0;
        }
    }
}
