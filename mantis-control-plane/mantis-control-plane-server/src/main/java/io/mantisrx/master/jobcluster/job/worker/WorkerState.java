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

package io.mantisrx.master.jobcluster.job.worker;

import java.util.HashMap;
import java.util.Map;


/**
 * Enumeration of all the states a Worker can be in.
 * Worker State Machine:
 *            (Resource assignment)           (Worker startup)                 (Worker running)
 * [Accepted]           -->        [Launched]       -->       [StartInitiated]        -->      [Started]
 *      |                              |                             |                            /  |
 *
 *      [----------------------------------------Failed----------------------------------------] [Completed]
 */
public enum WorkerState {

    /**
     * Indicates a worker submission has been received by the Master.
     */
    Accepted,
    /**
     * Indicates the worker has been scheduled onto a Mesos slave.
     */
    Launched,
    /**
     * Indicates the worker is in the process of starting up.
     */
    StartInitiated,
    /**
     * Indicates the worker is running.
     */
    Started,
    /**
     * Indicates the worker has encountered a fatal error.
     */
    Failed,
    /**
     * Indicates the worker has completed execution.
     */
    Completed,
    /**
     * A place holder state.
     */
    Noop,
    /**
     * Indicates that the actual state of the worker is unknown.
     */
    Unknown;

    /**
     * A rollup worker state indicating whether worker is running or terminal.
     */
    public enum MetaState {
        /**
         * Indicates the worker is not in a final state.
         */
        Active,
        /**
         * Indicates worker is dead.
         */
        Terminal
    }

    private static final Map<WorkerState, WorkerState[]> STATE_TRANSITION_MAP;
    private static final Map<WorkerState, MetaState> META_STATES;

    static {
        STATE_TRANSITION_MAP = new HashMap<>();
        STATE_TRANSITION_MAP.put(WorkerState.Accepted, new WorkerState[]
                {WorkerState.Launched, WorkerState.Failed, WorkerState.Completed});
        STATE_TRANSITION_MAP.put(WorkerState.Launched, new WorkerState[] {
                WorkerState.StartInitiated, WorkerState.Started, WorkerState.Failed, WorkerState.Completed});
        STATE_TRANSITION_MAP.put(WorkerState.StartInitiated, new WorkerState[] {WorkerState.StartInitiated,
                WorkerState.Started, WorkerState.Failed, WorkerState.Completed});
        STATE_TRANSITION_MAP.put(WorkerState.Started, new WorkerState[] {WorkerState.Started,
                WorkerState.Failed, WorkerState.Completed});
        STATE_TRANSITION_MAP.put(WorkerState.Failed, new WorkerState[] {WorkerState.Failed});
        STATE_TRANSITION_MAP.put(WorkerState.Completed, new WorkerState[] {});
        META_STATES = new HashMap<>();
        META_STATES.put(WorkerState.Accepted, MetaState.Active);
        META_STATES.put(WorkerState.Launched, MetaState.Active);
        META_STATES.put(WorkerState.StartInitiated, MetaState.Active);
        META_STATES.put(WorkerState.Started, MetaState.Active);
        META_STATES.put(WorkerState.Failed, MetaState.Terminal);
        META_STATES.put(WorkerState.Completed, MetaState.Terminal);
    }

    /**
     * Returns true if the worker is in a state that indicates it is on Mesos slave.
     * @param state
     * @return
     */
    public static boolean isWorkerOnSlave(WorkerState state) {
        switch (state) {
        case StartInitiated:
        case Started:
            return true;
        default:
            return false;
        }
    }

    /**
     * Returns true if the worker is any valid non terminal state.
     * @param state
     * @return
     */
    public static boolean isRunningState(WorkerState state) {
        switch (state) {
        case Launched:
        case StartInitiated:
        case Started:
            return true;
        default:
            return false;
        }
    }

    /**
     * Returns true if the old state -> new state transition is valid.
     * @param currentState
     * @param newState
     * @return
     */
    public static boolean isValidStateChgTo(WorkerState currentState, WorkerState newState) {
        for (WorkerState validState : STATE_TRANSITION_MAP.get(currentState))
            if (validState == newState)
                return true;
        return false;
    }

    /**
     * Returns true if the worker is in a terminal state.
     * @param state
     * @return
     */

    public static boolean isTerminalState(WorkerState state) {
        switch (state) {
        case Completed:
        case Failed:
            return true;
        default:
            return false;
        }

    }

    /**
     * Returns true if the worker is in error state.
     * @param state
     * @return
     */
    public static boolean isErrorState(WorkerState state) {
        switch (state) {
        case Failed:
            return true;
        default:
            return false;
        }
    }


    /**
     * Translates the given {@link WorkerState} to a MetaState.
     * @param state
     * @return
     */
    public static MetaState toMetaState(WorkerState state) {
        return META_STATES.get(state);
    }

}
