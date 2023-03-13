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

package com.mantisrx.common.utils;

import io.mantisrx.common.MantisProperties;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.channel.SingleNioLoopProvider;


public class NettyUtils {

    static final String KEY_MAX_THREADS = "mantis.worker.num.cpu";
    /**
     * The maximum number of netty threads.
     */
    static final int MAX_THREADS;

    static {
        int maxThreads = Integer.getInteger(KEY_MAX_THREADS, 0);
        int cpuCount = Runtime.getRuntime().availableProcessors();
        int max;
        if (maxThreads <= 0 || maxThreads > cpuCount) {
            max = cpuCount;
        } else {
            max = maxThreads;
        }
        MAX_THREADS = max;
    }

    public static void setNettyThreads() {
        // NJ

        String useSingleThreadKey = "JOB_PARAM_mantis.netty.useSingleThread";
        String useSingleThreadStr = MantisProperties.getProperty(useSingleThreadKey);

        if (useSingleThreadStr != null && !useSingleThreadStr.isEmpty() && useSingleThreadStr.equalsIgnoreCase("true")) {
            RxNetty.useEventLoopProvider(new SingleNioLoopProvider(1));
            System.out.println(">>>>>>Set Netty to use single thread ");
        } else {
            RxNetty.useEventLoopProvider(new SingleNioLoopProvider(MAX_THREADS));
            System.out.println(String.format("Set Netty to use %s threads", MAX_THREADS));
        }

    }

}
