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

package io.mantisrx.publish.netty.guice;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.spectator.nflx.SpectatorModule;
import io.mantisrx.publish.api.EventPublisher;
import org.junit.jupiter.api.Test;


public class LocalMantisPublishTester {

    @Test
    public void injectionTest() {
        try {
            Injector injector = Guice.createInjector(new ArchaiusModule(),
                    new MantisRealtimeEventsPublishModule(), new SpectatorModule());

            EventPublisher publisher = injector.getInstance(EventPublisher.class);
            /// FOR local testing uncomment
//            for(int i=0; i<100; i++) {
//                Map<String,Object> event = new HashMap<>();
//                event.put("id",i);
//                CompletionStage<PublishStatus> sendStatus = publisher.publish("requestEventStream", new Event(event));
//                sendStatus.whenCompleteAsync((status, throwable) -> {
//                    System.out.println("Send status => " + status);
//                });
//                Thread.sleep(1000);
//            }


            assertNotNull(publisher);
        } catch(Exception e) {
            fail();
        }
    }

}
