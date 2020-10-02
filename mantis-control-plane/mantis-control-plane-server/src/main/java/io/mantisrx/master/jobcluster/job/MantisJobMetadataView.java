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

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonFilter;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.server.master.domain.DataFormatAdapter;
import io.mantisrx.server.master.store.MantisJobMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

@JsonFilter("topLevelFilter")
public class MantisJobMetadataView {
    @JsonIgnore
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(MantisJobMetadataView.class);

    private FilterableMantisJobMetadataWritable jobMetadata;
    @JsonIgnore
    private long terminatedAt = -1;

    private List<FilterableMantisStageMetadataWritable> stageMetadataList = Lists.newArrayList();
    private List<FilterableMantisWorkerMetadataWritable> workerMetadataList = Lists.newArrayList();
    private String version = "";

    public MantisJobMetadataView() {}

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown=true)
    public MantisJobMetadataView(@JsonDeserialize(as=FilterableMantisJobMetadataWritable.class) @JsonProperty("jobMetadata") FilterableMantisJobMetadataWritable jobMeta,
                                  @JsonProperty("stageMetadataList") List<FilterableMantisStageMetadataWritable> stageMetadata,
                                  @JsonProperty("workerMetadataList") List<FilterableMantisWorkerMetadataWritable> workerMetadata) {
        this.jobMetadata = jobMeta;
        this.stageMetadataList = stageMetadata;
        this.workerMetadataList = workerMetadata;
    }





    public MantisJobMetadataView(IMantisJobMetadata jobMeta, long terminatedAt,
                                 List<Integer> stageNumberList, List<Integer> workerIndexList,
                                 List<Integer> workerNumberList, List<WorkerState.MetaState> workerStateList, boolean jobIdOnly) {
        if(logger.isTraceEnabled()) { logger.trace("Enter MantisJobMetadataView ctor jobMeta {} workerIndexList {} workerNumberList workerStateList {} jobIdOnly {}", workerIndexList, workerNumberList, workerStateList, jobIdOnly);}
        this.jobMetadata = DataFormatAdapter.convertMantisJobMetadataToFilterableMantisJobMetadataWriteable(jobMeta);
        this.terminatedAt = terminatedAt;

        version = jobMeta.getJobDefinition().getVersion();
        if(logger.isDebugEnabled()) { logger.debug("MantisJobMetadataView.terminatedAt set to {}, version set to {}", terminatedAt, version); }
        if(!jobIdOnly) {
            if(logger.isDebugEnabled()) { logger.debug("MantisJobMetadataView.jobIdOnly is {}", jobIdOnly); }
            this.stageMetadataList = jobMeta.getStageMetadata().values().stream()
                    .filter((IMantisStageMetadata mantisStageMetadata) -> stageFilter(mantisStageMetadata, stageNumberList))
                    .map(DataFormatAdapter::convertFilterableMantisStageMetadataToMantisStageMetadataWriteable)
                    .collect(Collectors.toList());

            this.workerMetadataList = jobMeta.getStageMetadata().values().stream()
                .map(IMantisStageMetadata::getAllWorkers)
                .flatMap(jobWorkers -> jobWorkers.stream()
                    .map(jw -> jw.getMetadata())
                    .filter((IMantisWorkerMetadata workerMetadata) -> workerFilter(workerMetadata, workerIndexList, workerNumberList, workerStateList))
                    .map(DataFormatAdapter::convertMantisWorkerMetadataToFilterableMantisWorkerMetadataWritable)
                )
                .collect(Collectors.toList());
        }

        if(logger.isTraceEnabled()) { logger.trace("Exit MantisJobMetadataView ctor");}
    }

    public MantisJobMetadataView(IMantisJobMetadata jobMeta,
                                 final List<Integer> stageNumberList,
                                 final List<Integer> workerIndexList,
                                 final List<Integer> workerNumberList,
                                 final List<WorkerState.MetaState> workerStateList,
                                 final boolean jobIdOnly) {
         this(jobMeta, -1, stageNumberList,workerIndexList,workerNumberList,workerStateList,jobIdOnly);


    }
    private boolean stageFilter(IMantisStageMetadata msmd, List<Integer> stageNumberList) {
        if(logger.isTraceEnabled()) { logger.trace("Enter MantisJobMetadataView:stageFilter Stage {} stageNumberList {}", msmd, stageNumberList);}
        // no filter specified
        if(stageNumberList.isEmpty()) {
            if(logger.isTraceEnabled()) { logger.trace("Exit stageFilter with true for stage {}", msmd); }
            return true;
        }
        for(int stageNumber : stageNumberList) {
            if(stageNumber == msmd.getStageNum()) {
                if(logger.isTraceEnabled()) { logger.trace("Exit stageFilter with true for stage {}", msmd); }
                return true;
            }
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit stageFilter with false for stage {}", msmd); }
        return false;
    }
    
    private boolean workerFilter(IMantisWorkerMetadata mwmd, final List<Integer> workerIndexList,
            final List<Integer> workerNumberList,
            final List<WorkerState.MetaState> workerStateList) {
        if(logger.isTraceEnabled()) { logger.trace("Enter MantisJobMetadataView:workerFilter worker {} indexList {} numberList {} stateList {}", mwmd, workerIndexList, workerNumberList, workerStateList);}
        boolean match=false;
        // no filter specified
        if(workerIndexList.isEmpty() && workerNumberList.isEmpty() && workerStateList.isEmpty()) {
            if(logger.isTraceEnabled()) { logger.trace("Exit workerFilter1 with true for worker {}", mwmd); }
            return true;
        }

       for(Integer workerIndex : workerIndexList) {
            if(workerIndex == mwmd.getWorkerIndex()) {
                if(logger.isTraceEnabled()) { logger.trace("Exit workerFilter2 with true for worker {}", mwmd); }
                match = true;
            } 
            if(!match) {
                if(logger.isTraceEnabled()) { logger.trace("Exit workerFilter3 with true for worker {}", mwmd); }
                return false;
            }
        } 
        
        for(Integer workerNumber : workerNumberList) {
            match = workerNumber == mwmd.getWorkerNumber();
            if(!match) {
                if(logger.isTraceEnabled()) { logger.trace("Exit workerFilter4 with false for worker {}", mwmd); }
                return false;
            }
        } 
        
        for(WorkerState.MetaState state : workerStateList) {
            match = false;
            try {
                match = WorkerState.toMetaState(mwmd.getState()).equals(state);
            } catch (IllegalArgumentException e) {
                
            }
        }
        if(!match) {
            if(logger.isTraceEnabled()) { logger.trace("Exit workerFilter5 with false for worker {}", mwmd); }
            return false;
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit workerFilter6 with true for worker {}", mwmd); }
        return true;
    }

    public MantisJobMetadata getJobMetadata() {
        return jobMetadata;
    }

    public List<FilterableMantisStageMetadataWritable> getStageMetadataList() {
        return stageMetadataList;
    }

    public List<FilterableMantisWorkerMetadataWritable> getWorkerMetadataList() {
        return workerMetadataList;
    }

    public String getTerminatedAt() {
        if(terminatedAt == -1) {
            return "";
        } else {
            return String.valueOf(this.terminatedAt);
        }
    }

    public String getVersion() {
        return this.version;
    }

    @Override
    public String toString() {
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {

            return "MantisJobMetadataView [jobMetadata=" + jobMetadata + ", stageMetadataList=" + stageMetadataList
                    + ", workerMetadataList=" + workerMetadataList + "]";
        }
    }
    
    
}