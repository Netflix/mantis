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

package io.mantisrx.runtime;

import java.util.HashMap;
import java.util.Map;

public enum MantisJobState {

    Accepted,
    Launched, // scheduled and sent to slave
    StartInitiated, // initial message from slave worker, about to start
    Started, // actually started running
    Failed,  // OK to handle as a resubmit
    Completed, // terminal state, not necessarily successful
    Noop; // internal use only

    private static final Map<MantisJobState, MantisJobState[]> validChanges;
    private static final Map<MantisJobState, MetaState> metaStates;

    static {
        validChanges = new HashMap<>();
        validChanges.put(Accepted, new MantisJobState[] {Launched, Failed, Completed});
        validChanges.put(Launched, new MantisJobState[] {StartInitiated, Started, Failed, Completed});
        validChanges.put(StartInitiated, new MantisJobState[] {StartInitiated, Started, Completed, Failed});
        validChanges.put(Started, new MantisJobState[] {Started, Completed, Failed});
        validChanges.put(Failed, new MantisJobState[] {});
        validChanges.put(Completed, new MantisJobState[] {});
        metaStates = new HashMap<>();
        metaStates.put(Accepted, MetaState.Active);
        metaStates.put(Launched, MetaState.Active);
        metaStates.put(StartInitiated, MetaState.Active);
        metaStates.put(Started, MetaState.Active);
        metaStates.put(Failed, MetaState.Terminal);
        metaStates.put(Completed, MetaState.Terminal);
    }

    public enum MetaState {
        Active,
        Terminal
    }

    public static MetaState toMetaState(MantisJobState state) {
        return metaStates.get(state);
    }

    public boolean isValidStateChgTo(MantisJobState newState) {
        for(MantisJobState validState: validChanges.get(this))
            if(validState == newState)
                return true;
        return false;
    }

    public boolean isTerminalState() {
        return isTerminalState(this);
    }

    public static boolean isTerminalState(MantisJobState state) {
        switch (state) {
            case Failed:
            case Completed:
                return true;
            default:
                return false;
        }
    }

    public static boolean isErrorState(MantisJobState started) {
        switch (started) {
            case Failed:
                return true;
            default:
                return false;
        }
    }

    public static boolean isRunningState(MantisJobState state) {
        switch (state) {
            case Launched:
            case StartInitiated:
            case Started:
                return true;
            default:
                return false;
        }
    }

    public static boolean isOnSlaveState(MantisJobState state) {
        switch (state) {
            case StartInitiated:
            case Started:
                return true;
            default:
                return false;
        }
    }

    public static boolean isOnStartedState(MantisJobState state) {
        return state == MantisJobState.Started;
    }
}
