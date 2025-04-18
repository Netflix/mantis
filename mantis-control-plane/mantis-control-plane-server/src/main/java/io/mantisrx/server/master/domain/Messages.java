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

package io.mantisrx.server.master.domain;

import org.apache.pekko.actor.ActorRef;


public class Messages {

    public static final Object Put = new PutMessage() {
        @Override
        public String toString() { return "Put"; }
    };
    public static final Object Take = new TakeMessage() {
        @Override
        public String toString() { return "Take"; }
    };

    public static final Object Think = new ThinkMessage() {};

    private interface PutMessage {}

    private interface TakeMessage {}

    private interface ThinkMessage {}

    public static final class Busy {

        public final ActorRef chopstick;

        public Busy(ActorRef chopstick) {
            this.chopstick = chopstick;
        }
    }

    public static final class Taken {

        public final ActorRef chopstick;

        public Taken(ActorRef chopstick) {
            this.chopstick = chopstick;
        }
    }
}
