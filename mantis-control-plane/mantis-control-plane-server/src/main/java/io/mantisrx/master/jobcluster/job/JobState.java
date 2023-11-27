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

package io.mantisrx.master.jobcluster.job;

import java.util.HashMap;
import java.util.Map;


/**
 * Declares the states a Job can be in.
 */
public enum JobState {

    /**
     * The Initial job state.
     */
    Accepted,
    /**
     * Indicates the job is running.
     */
    Launched, // scheduled and sent to slave
    /**
     * Indicates the job is in the process of terminating due to an error.
     */
    Terminating_abnormal,
    /**
     * Indicates the job is in the process of terminating due to normal reasons.
     */
    Terminating_normal,
    /**
     * Indicates job is in terminal state and that the termination was abnormal.
     */
    Failed,  // OK to handle as a resubmit
    /**
     * Indicates job is in terminal state and that the termination was normal.
     */
    Completed, // terminal state, not necessarily successful
    /**
     * Place holder state.
     */
    Noop; // internal use only

    private static final Map<JobState, JobState[]> VALID_CHANGES;
    private static final Map<JobState, MetaState> META_STATES;

    static {
        VALID_CHANGES = new HashMap<>();
        VALID_CHANGES.put(Accepted, new JobState[] {
                Accepted, Launched, Terminating_abnormal, Terminating_normal,
                Failed, Completed
        });
        VALID_CHANGES.put(Launched, new JobState[] {
                Launched, Terminating_abnormal, Terminating_normal,
                Failed, Completed
        });
        VALID_CHANGES.put(Terminating_abnormal, new JobState[] {
                Terminating_abnormal, Failed
        });
        VALID_CHANGES.put(Terminating_normal, new JobState[] {
                Terminating_normal, Completed
        });


        VALID_CHANGES.put(Failed, new JobState[] {});
        VALID_CHANGES.put(Completed, new JobState[] {});
        META_STATES = new HashMap<>();
        META_STATES.put(Accepted, MetaState.Active);
        META_STATES.put(Launched, MetaState.Active);
        META_STATES.put(Failed, MetaState.Terminal);
        META_STATES.put(Completed, MetaState.Terminal);
        META_STATES.put(Terminating_abnormal, MetaState.Terminal);
        META_STATES.put(Terminating_normal, MetaState.Terminal);
    }

    /**
     * A higher level roll up of states indicating active or terminal status of the job.
     */
    public enum MetaState {
        /**
         * Indicates the job is active.
         */
        Active,
        /**
         * Indicates the job is completed.
         */
        Terminal
    }

    /**
     * Rolls up given {@link JobState} to a {@link MetaState}.
     *
     * @param state
     *
     * @return
     */
    public static MetaState toMetaState(JobState state) {
        return META_STATES.get(state);
    }

    /**
     * Checks if the transition to the given state is valid from current state.
     *
     * @param newState
     *
     * @return
     */
    public boolean isValidStateChgTo(JobState newState) {
        for (JobState validState : VALID_CHANGES.get(this))
            if (validState == newState)
                return true;
        return false;
    }

    /**
     * Returns true if the current state is terminal.
     *
     * @param state
     *
     * @return
     */
    public static boolean isTerminalState(JobState state) {
        switch (state) {
        case Failed:
        case Completed:
        case Terminating_normal:
        case Terminating_abnormal:
            return true;
        default:
            return false;
        }
    }

    public boolean isTerminal() {
        return isTerminalState(this);
    }

    /**
     * Returns true if the current state is abnormal.
     *
     * @param started
     *
     * @return
     */
    public static boolean isErrorState(JobState started) {
        switch (started) {
        case Failed:
        case Terminating_abnormal:
            return true;
        default:
            return false;
        }
    }

    /**
     * Returns true if the job is active.
     *
     * @param state
     *
     * @return
     */
    public static boolean isRunningState(JobState state) {
        switch (state) {
        case Launched:

            return true;
        default:
            return false;
        }
    }

    /**
     * Returns true if the job is accepted.
     *
     * @param state
     *
     * @return
     */
    public static boolean isAcceptedState(JobState state) {
        switch (state) {
            case Accepted:

            return true;
        default:
            return false;
        }
    }
}
