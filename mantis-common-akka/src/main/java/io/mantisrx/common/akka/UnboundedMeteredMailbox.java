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

package io.mantisrx.common.akka;

import org.apache.pekko.actor.ActorPath;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.dispatch.MailboxType;
import org.apache.pekko.dispatch.MessageQueue;
import org.apache.pekko.dispatch.ProducesMessageQueue;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;


/**
 * A simple unbounded metered mail box.
 */
public class UnboundedMeteredMailbox implements MailboxType, ProducesMessageQueue<MeteredMessageQueue> {
    private final ActorSystem.Settings settings;
    private final Config config;
    private static final Logger LOGGER = LoggerFactory.getLogger(UnboundedMeteredMailbox.class);

    /**
     * Creates an instance of this class.
     * @param settings
     * @param config
     */
    public UnboundedMeteredMailbox(final ActorSystem.Settings settings, final Config config) {
        this.settings = settings;
        this.config = config;
    }

    /**
     * Creates an instance of a {@link MessageQueue}.
     * @param owner
     * @param system
     * @return
     */
    public MessageQueue create(final Option<ActorRef> owner, final Option<ActorSystem> system) {
        String path = owner.fold(() -> "unknown", r -> tagValue(r.path()));
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("created message queue for {}", path);
        }
        return new MeteredMessageQueue(path);
    }

    /** Summarizes a path for use in a metric tag. */
    private String tagValue(ActorPath path) {
        return path.name();
    }
}
