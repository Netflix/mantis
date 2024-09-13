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

import io.mantisrx.common.Label;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.master.domain.Costs;
import io.mantisrx.server.master.domain.DataFormatAdapter;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobStateChangeException;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonFilter;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@JsonFilter("topLevelFilter")
public class MantisJobMetadataImpl implements IMantisJobMetadata {

    private static final Logger logger = LoggerFactory.getLogger(MantisJobMetadataImpl.class);
    private final JobId jobId;
    private final long submittedAt;
    @Getter
    private final long heartbeatIntervalSecs;
    @Getter
    private final long workerTimeoutSecs;
    private long startedAt = DEFAULT_STARTED_AT_EPOCH;
    private long endedAt = DEFAULT_STARTED_AT_EPOCH;

    private JobState state;
    private int nextWorkerNumberToUse;
    private final JobDefinition jobDefinition;
    private Costs jobCosts;

    @JsonIgnore
    private final Map<Integer, IMantisStageMetadata> stageMetadataMap = new HashMap<>();
    @JsonIgnore
    private final Map<Integer, Integer> workerNumberToStageMap = new HashMap<>();


    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown=true)
    public MantisJobMetadataImpl(@JsonProperty("jobId") JobId jobId,
                                 @JsonProperty("submittedAt") long submittedAt,
                                 @JsonProperty("startedAt") long startedAt,
                                 @JsonProperty("jobDefinition") JobDefinition jobDefinition,
                                 @JsonProperty("state") JobState state,
                                 @JsonProperty("nextWorkerNumberToUse") int nextWorkerNumberToUse,
                                 @JsonProperty("heartbeatIntervalSecs") long heartbeatIntervalSecs,
                                 @JsonProperty("workerTimeoutSecs") long workerTimeoutSecs) {
        this.jobId = jobId;

        this.submittedAt = submittedAt;
        this.startedAt = startedAt;
        this.state = state==null? JobState.Accepted : state;
        this.nextWorkerNumberToUse = nextWorkerNumberToUse;
        this.heartbeatIntervalSecs = heartbeatIntervalSecs;
        this.workerTimeoutSecs = workerTimeoutSecs;

        this.jobDefinition = jobDefinition;
    }


    @Override
    public JobId getJobId() {
        return jobId;
    }
    @Override
    public String getClusterName() {
        return this.jobDefinition.getName();
    }
    @JsonIgnore
    @Override
    public Instant getSubmittedAtInstant() {
        return Instant.ofEpochMilli(submittedAt);
    }

    public long getSubmittedAt() {
        return submittedAt;
    }

    @Override
    public long getSubscriptionTimeoutSecs() {
        return this.jobDefinition.getSubscriptionTimeoutSecs();
    }
    @Override
    public int getNextWorkerNumberToUse() {
        return nextWorkerNumberToUse;
    }

    public void setNextWorkerNumberToUse(int n, MantisJobStore store) throws Exception {
        this.nextWorkerNumberToUse = n;
        store.updateJob(this);
    }

    @Override
    public JobState getState() {
        return state;
    }
    @Override
    public JobDefinition getJobDefinition() {
    	return this.jobDefinition;
    }

    @Override
	public String getUser() {
		return this.jobDefinition.getUser();
	}

	@Override
	public Optional<JobSla> getSla() {
		return Optional.ofNullable(this.jobDefinition.getJobSla());
	}

	@Override
	public List<Parameter> getParameters() {
		return this.jobDefinition.getParameters();

	}

	@Override
	public List<Label> getLabels() {
		return this.jobDefinition.getLabels();
	}

	@JsonIgnore
	@Override
	public int getTotalStages() {
		return this.getJobDefinition().getNumberOfStages();
	}

    @JsonIgnore
	@Override
	public String getArtifactName() {
		return this.jobDefinition.getArtifactName();
	}

    void setJobState(JobState state, MantisJobStore store) throws Exception {
    	logger.info("Updating job State from {} to {} ", this.state, state);
        if (!this.state.isValidStateChgTo(state)) {
            throw new InvalidJobStateChangeException(jobId.getId(), this.state, state);
        }
        this.state = state;
        if (this.state.isTerminal()) {
            this.endedAt = System.currentTimeMillis();
        }
        store.updateJob(this);
    }


    void setStartedAt(long startedAt, MantisJobStore store) throws Exception {
        logger.info("Updating job start time  to {} ", startedAt);
		this.startedAt = startedAt;
		store.updateJob(this);
	}

    void setJobCosts(Costs jobCosts) {
        this.jobCosts = jobCosts;
    }

    /**
     * Add job stage if absent, returning true if it was actually added.
     * @param msmd The stage's metadata object.
     * @return true if actually added, false otherwise.
     */
    public boolean addJobStageIfAbsent(IMantisStageMetadata msmd) {
    	if(logger.isTraceEnabled()) { logger.trace("Adding stage {} ", msmd); }
    	boolean result = stageMetadataMap.put(msmd.getStageNum(), msmd) == null;
    	msmd.getAllWorkers().forEach((worker) -> {
            workerNumberToStageMap.put(worker.getMetadata().getWorkerNumber(), msmd.getStageNum());
        });
        return result;
    }

    @JsonIgnore
    public Map<Integer,? extends IMantisStageMetadata> getStageMetadata() {
        return Collections.unmodifiableMap(stageMetadataMap);
    }

    public final Map<Integer, Integer> getWorkerNumberToStageMap() {
    	return Collections.unmodifiableMap(this.workerNumberToStageMap);
    }

    @JsonIgnore
    public Optional<IMantisStageMetadata> getStageMetadata(int stageNum) {
        return Optional.ofNullable(stageMetadataMap.get(stageNum));
    }

    /**
     * Replace meta data for the given worker with a newly created worker that has not been dispatched yet.
     * Dispatch happens after this method returns.
     * Delegates the actual replacing to occur in the StageMetadata.
     * @param stageNum
     * @param newWorker
     * @param oldWorker
     * @param jobStore
     * @return
     * @throws Exception
     */
    boolean replaceWorkerMetaData(int stageNum, JobWorker newWorker, JobWorker oldWorker, MantisJobStore jobStore) throws Exception {
        boolean result = true;
        ((MantisStageMetadataImpl) stageMetadataMap.get(stageNum)).replaceWorkerIndex(newWorker, oldWorker, jobStore);
        // remove mapping for replaced worker

        removeWorkerMetadata(oldWorker.getMetadata().getWorkerNumber());

        Integer integer = workerNumberToStageMap.put(newWorker.getMetadata().getWorkerNumber(), stageNum);
        if (integer != null && integer != stageNum) {
            logger.error("Unexpected to put worker number mapping from {} to stage {} for job {}, prev mapping to stage {}",
                    newWorker.getMetadata().getWorkerNumber(), stageNum, newWorker.getMetadata().getJobId(), integer);
        }
        return result;
    }

    public boolean addWorkerMetadata(int stageNum, JobWorker newWorker) {

        if (logger.isTraceEnabled()) { logger.trace("Adding workerMetadata {} for stage {}", stageNum, newWorker); }

        if (!stageMetadataMap.containsKey(stageNum)) {
            logger.warn("No such stage {}", stageNum);
        }
        final boolean result = ((MantisStageMetadataImpl) stageMetadataMap.get(stageNum)).addWorkerIndex(newWorker);

        if (result) {
            Integer integer = workerNumberToStageMap.put(newWorker.getMetadata().getWorkerNumber(), stageNum);
            if (integer != null && integer != stageNum) {
                logger.error("Unexpected to put worker number mapping from {} to stage {} for job {}, prev mapping to stage {}",
                    newWorker.getMetadata().getWorkerNumber(), stageNum, newWorker.getMetadata().getJobId(), integer);
            }
            if (logger.isTraceEnabled()) { logger.trace("Exit addworkerMeta {}", workerNumberToStageMap); }
        }
        return result;
    }

    boolean removeWorkerMetadata(int workerNumber) {
        if(workerNumberToStageMap.containsKey(workerNumber)) {
            workerNumberToStageMap.remove(workerNumber);
            return true;
        }
        return false;
    }

    @JsonIgnore
    public Optional<JobWorker> getWorkerByIndex(int stageNumber, int workerIndex) throws InvalidJobException {
        Optional<IMantisStageMetadata> stage = getStageMetadata(stageNumber);
        if(stage.isPresent()) {
        	return Optional.ofNullable(stage.get().getWorkerByIndex(workerIndex));
        }
        return Optional.empty();
        //throw new InvalidJobException(jobId, stageNumber, workerIndex);
    }

    @JsonIgnore
    public Optional<JobWorker> getWorkerByNumber(int workerNumber) throws InvalidJobException {
        Integer stageNumber = workerNumberToStageMap.get(workerNumber);
        if(stageNumber == null) {
        	return Optional.empty();
        }
        IMantisStageMetadata stage = stageMetadataMap.get(stageNumber);
        if(stage == null) {
        	return Optional.empty();
        }
        return Optional.ofNullable(stage.getWorkerByWorkerNumber(workerNumber));
    }

    @JsonIgnore
    public int getMaxWorkerNumber() {
        // Expected to be called only during initialization, no need to synchronize/lock.
        // Resubmitted workers are expected to have a worker number greater than those they replace.
        int max=-1;
        for(int id: workerNumberToStageMap.keySet())
            if(max < id)  max = id;
        return max;
    }

    @JsonIgnore
   	@Override
   	public SchedulingInfo getSchedulingInfo() {
   		return this.jobDefinition.getSchedulingInfo();

   	}

   	@Override
   	public long getMinRuntimeSecs() {

   		return this.jobDefinition.getJobSla().getMinRuntimeSecs();
   	}
   	/**
   	 * Migrate to using the getArtifactName and getArtifactVersion
   	 */
   	@Deprecated @Override
   	public URL getJobJarUrl() {
   		try {
            return new URL(jobDefinition.getJobJarUrl());
   		} catch (MalformedURLException e) {
   			// should not happen
   			throw new RuntimeException(e);
   		}
   	}

   	@Override
	public Optional<Instant> getStartedAtInstant() {
		if(this.startedAt == DEFAULT_STARTED_AT_EPOCH) {
		    return Optional.empty();
		} else {
		    return Optional.of(Instant.ofEpochMilli(startedAt));
		}

	}

	public long getStartedAt() {
   	    return this.startedAt;
   	}

	@Override
	public Optional<Instant> getEndedAtInstant() {
	    if(this.endedAt == DEFAULT_STARTED_AT_EPOCH) {
            return Optional.empty();
        } else {
            return Optional.of(Instant.ofEpochMilli(endedAt));
        }
	}

	public long getEndedAt() {
        return this.endedAt;
    }

    public static class Builder {
    	JobId jobId;

        String user;
        JobDefinition jobDefinition;
        long submittedAt;
        long startedAt;
        JobState state;

        int nextWorkerNumberToUse = 1;

        long heartbeatIntervalSecs = 0;
        long workerTimeoutSecs = 0;

        Costs jobCosts;

        public Builder() {
    	}

        public Builder(MantisJobMetadataImpl mJob) {
            this.jobId = mJob.getJobId();

            this.jobDefinition = mJob.getJobDefinition();
            this.submittedAt = mJob.getSubmittedAt();
            this.state = mJob.getState();

            this.nextWorkerNumberToUse = mJob.getNextWorkerNumberToUse();
            this.heartbeatIntervalSecs = mJob.getHeartbeatIntervalSecs();
            this.workerTimeoutSecs = mJob.getWorkerTimeoutSecs();

            this.jobCosts = mJob.getJobCosts();
        }

    	public Builder withJobId(JobId jobId) {
    		this.jobId = jobId;
    		return this;
    	}

    	public Builder withJobDefinition(JobDefinition jD) {
    		this.jobDefinition = jD;
    		return this;
    	}

    	public Builder withSubmittedAt(long submittedAt) {
    		this.submittedAt = submittedAt;
    		return this;
    	}

    	public Builder withSubmittedAt(Instant submittedAt) {
            this.submittedAt = submittedAt.toEpochMilli();
            return this;
        }

        public Builder withStartedAt(Instant startedAt) {
    	    this.startedAt = startedAt.toEpochMilli();
    	    return this;
        }

    	public Builder withJobState(JobState state) {
    		this.state = state;
    		return this;
    	}

    	public Builder withNextWorkerNumToUse(int workerNum) {
    		this.nextWorkerNumberToUse = workerNum;
    		return this;
    	}

        public Builder withJobCost(Costs costs) {
            this.jobCosts = costs;
            return this;
        }

        public Builder withHeartbeatIntervalSecs(long secs) {
            this.heartbeatIntervalSecs = secs;
            return this;
        }

        public Builder withWorkerTimeoutSecs(long secs) {
            this.workerTimeoutSecs = secs;
            return this;
        }

    	public Builder from(MantisJobMetadataImpl mJob) {
    		this.jobId = mJob.getJobId();

    		this.jobDefinition = mJob.getJobDefinition();
    		this.submittedAt = mJob.getSubmittedAt();
    		this.state = mJob.getState();
            this.jobCosts = mJob.getJobCosts();
    		this.nextWorkerNumberToUse = mJob.getNextWorkerNumberToUse();
            this.heartbeatIntervalSecs = mJob.getHeartbeatIntervalSecs();
            this.workerTimeoutSecs = mJob.getWorkerTimeoutSecs();
    		return this;
    	}

    	public MantisJobMetadataImpl build() {
            return new MantisJobMetadataImpl(jobId, submittedAt, startedAt, jobDefinition, state, nextWorkerNumberToUse, heartbeatIntervalSecs, workerTimeoutSecs);
    	}
    }

    @Override
    public String toString() {
        return "MantisJobMetadataImpl{" +
                "jobId=" + jobId +
                ", submittedAt=" + submittedAt +
                ", startedAt=" + startedAt +
                ", endedAt=" + endedAt +
                ", state=" + state +
                ", nextWorkerNumberToUse=" + nextWorkerNumberToUse +
                ", jobDefinition=" + jobDefinition +
                ", stageMetadataMap=" + stageMetadataMap +
                ", heartbeatIntervalSecs=" + heartbeatIntervalSecs +
                ", workerTimeoutSecs=" + workerTimeoutSecs +
                ", workerNumberToStageMap=" + workerNumberToStageMap +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MantisJobMetadataImpl that = (MantisJobMetadataImpl) o;
        return submittedAt == that.submittedAt &&
                startedAt == that.startedAt &&
                endedAt == that.endedAt &&

                nextWorkerNumberToUse == that.nextWorkerNumberToUse &&
                heartbeatIntervalSecs == that.heartbeatIntervalSecs &&
                Objects.equals(jobId, that.jobId) &&
                state == that.state &&
                Objects.equals(jobDefinition, that.jobDefinition);
    }

    @Override
    public int hashCode() {

        return Objects.hash(jobId, submittedAt, startedAt, endedAt, state, nextWorkerNumberToUse, heartbeatIntervalSecs, jobDefinition);
    }

    @Override
    public Costs getJobCosts() {
        return jobCosts;
    }
}
