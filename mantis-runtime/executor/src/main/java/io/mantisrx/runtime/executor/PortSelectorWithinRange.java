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

package io.mantisrx.runtime.executor;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;


public class PortSelectorWithinRange {

    private int start;
    private int end;
    private int attempts = 5;

    public PortSelectorWithinRange(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public PortSelectorWithinRange(int start, int end, int attempts) {
        this.start = start;
        this.end = end;
        this.attempts = attempts;
    }

    public int acquirePort() {
        for (int i = 0; i < attempts; i++) {
            int randomPort = start + (int) (Math.random() * ((end - start) + 1));
            Socket socket = null;
            try {
                socket = new Socket("localhost", randomPort);
            } catch (ConnectException e) {
                return randomPort;
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    if (socket != null) {
                        socket.close();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException("Could not acquire a port within range, after " + attempts + " attempts");
    }
}
