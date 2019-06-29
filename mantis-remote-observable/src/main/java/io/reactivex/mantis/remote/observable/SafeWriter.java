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

package io.reactivex.mantis.remote.observable;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import io.mantisrx.common.network.WritableEndpoint;
import io.reactivex.mantis.remote.observable.slotting.SlottingStrategy;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;


public class SafeWriter {

    static final Logger logger = LoggerFactory.getLogger(SafeWriter.class);
    private static final AtomicLong checkIsOpenCounter = new AtomicLong();
    private static final int CHECK_IS_OPEN_INTERVAL = 1000;

    <T> boolean safeWrite(final ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection,
                          List<RemoteRxEvent> events,
                          final MutableReference<Subscription> subReference,
                          Action0 onSuccessfulWriteCallback,
                          final Action1<Throwable> onFailedWriteCallback,
                          final SlottingStrategy<T> slottingStrategyReference,
                          final WritableEndpoint<T> endpoint) {
        boolean writeSuccess = true;
        if (checkIsOpenCounter.getAndIncrement() % CHECK_IS_OPEN_INTERVAL == 0) {
            if (!connection.isCloseIssued() && connection.getChannel().isActive()) {
                writeSuccess = checkWriteableAndWrite(connection, events,
                        onSuccessfulWriteCallback, onFailedWriteCallback);
            } else {
                writeSuccess = false;
                logger.warn("Detected closed or inactive client connection, force unsubscribe.");
                subReference.getValue().unsubscribe();
                // release slot
                if (slottingStrategyReference != null) {
                    logger.info("Removing slot for endpoint: " + endpoint);
                    if (!slottingStrategyReference.removeConnection(endpoint)) {
                        logger.error("Failed to remove endpoint from slot,  endpoint: " + endpoint);
                    }
                }
            }

        } else {
            writeSuccess = checkWriteableAndWrite(connection, events,
                    onSuccessfulWriteCallback, onFailedWriteCallback);
        }
        return writeSuccess;
    }

    private boolean checkWriteableAndWrite(
            final ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection,
            List<RemoteRxEvent> events, Action0 onSuccessfulWriteCallback,
            final Action1<Throwable> onFailedWriteCallback) {
        boolean writeSuccess = true;
        if (connection.getChannel().isWritable()) {
            connection.writeAndFlush(events)
                    .doOnError(onFailedWriteCallback)
                    .doOnCompleted(onSuccessfulWriteCallback)
                    .subscribe();
        } else {
            writeSuccess = false;
        }
        return writeSuccess;
    }
}
