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

package io.mantisrx.server.worker;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collection;

/**
 * Utilities for {@link java.lang.management.ManagementFactory}.
 */
public final class JvmUtils {

    /**
     * Creates a thread dump of the current JVM.
     *
     * @return the thread dump of current JVM
     */
    public static Collection<ThreadInfo> createThreadDump() {
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();

        return Arrays.asList(threadMxBean.dumpAllThreads(true, true));
    }

    public static String createThreadDumpAsString() {
        final StringBuilder dump = new StringBuilder();
        final Collection<ThreadInfo> threadInfos = createThreadDump();
        for (ThreadInfo threadInfo : threadInfos) {
            dump.append('"');
            dump.append(threadInfo.getThreadName());
            dump.append("\" ");
            final Thread.State state = threadInfo.getThreadState();
            dump.append("\n   java.lang.Thread.State: ");
            dump.append(state);
            final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
            for (final StackTraceElement stackTraceElement : stackTraceElements) {
                dump.append("\n        at ");
                dump.append(stackTraceElement);
            }
            dump.append("\n\n");
        }
        return dump.toString();
    }

    /**
     * Private default constructor to avoid instantiation.
     */
    private JvmUtils() {
    }

}
