# JobArtifact Handling in Mantis

## Overview

This document provides a comprehensive analysis of how JobArtifacts are handled in the Mantis system, including scheduling attributes flow, storage implementation, versioning behavior, and API endpoints.

## Scheduling Attributes Flow

### How `schedulingAttributes` Gets Chosen in Schedule Requests

Starting from `JobActor.java`, here's the complete flow:

#### 1. Schedule Request Creation (`JobActor.java:1717`)
When a worker needs to be scheduled, `JobActor.WorkerManager.createSchedulingRequest()` creates a `ScheduleRequest` with `SchedulingConstraints`.

#### 2. SchedulingConstraints Creation (`JobActor.java:1765-1769`)
The scheduling constraints are created using:
```java
SchedulingConstraints.of(
    stageMetadata.getMachineDefinition(),
    stageMetadata.getSizeAttribute(),
    mergeJobDefAndArtifactAssigmentAttributes(jobMetadata.getJobJarUrl()))
```

#### 3. Attribute Merging (`JobActor.java:1787-1803`)
`mergeJobDefAndArtifactAssigmentAttributes()` merges attributes from two sources:

**a) JobArtifact Tags:**
- Extracts artifact name from job JAR URL
- Fetches `JobArtifact` from job store
- Uses `artifact.getTags()` as base attributes

**b) JobDefinition Scheduling Constraints:**
- Uses `mantisJobMetaData.getJobDefinition().getSchedulingConstraints()`
- **Job definition takes precedence** over artifact tags

#### 4. JobDefinition Scheduling Constraints Origin (`JobDefinition.java:119-128`)
`JobDefinition.getSchedulingConstraints()` extracts attributes from **labels** using this pattern:
```java
Pattern.compile("_mantis\\.schedulingAttribute\\.(.+)", Pattern.CASE_INSENSITIVE)
```

Labels matching `_mantis.schedulingAttribute.{key}` become scheduling constraints where:
- The captured group `{key}` becomes the constraint key
- The label's value becomes the constraint value

#### 5. Original Sources
The `schedulingAttributes` ultimately originate from:

1. **JobArtifact tags** - stored metadata about the artifact (e.g., JDK version, dependencies)
2. **Job labels** - specifically labels following the pattern `_mantis.schedulingAttribute.{attribute_name}`

**Key locations:**
- Schedule request creation: `JobActor.java:1765`
- Attribute merging: `JobActor.java:1787`
- Label pattern extraction: `JobDefinition.java:77-78, 119-128`
- JobArtifact tags: `JobArtifact.java:56`

## JobArtifact API Endpoints

### REST API Endpoints

All JobArtifact endpoints are under the `/api/v1/jobArtifacts` path prefix.

#### 1. GET /api/v1/jobArtifacts - Search Job Artifacts
- **Purpose**: Search job artifacts by name and optionally by version
- **Query Parameters**:
  - `name` (optional): Name of the job artifact to search for
  - `version` (optional): Specific version to filter by. If not provided, returns all versions matching the name
- **Response**: Returns a list of JobArtifact objects that match the search criteria
- **Implementation**: `JobArtifactsRoute.java:100-109`

#### 2. POST /api/v1/jobArtifacts - Register New Job Artifact
- **Purpose**: Register/upsert a new job artifact to the metadata store
- **Request Body**: JobArtifact JSON object
- **Response**: Returns the artifact ID of the created/updated artifact
- **Implementation**: `JobArtifactsRoute.java:111-132`

#### 3. GET /api/v1/jobArtifacts/names - List Job Artifact Names
- **Purpose**: Search job artifact names by prefix or contains filter for faster lookups
- **Query Parameters**:
  - `prefix` (optional): Filter names by prefix (default: "")
  - `contains` (optional): Filter names containing this string (default: "")
- **Response**: Returns a list of artifact names only (not full objects)
- **Implementation**: `JobArtifactsRoute.java:134-143`

### Example Usage

```bash
# Search all artifacts with a specific name
GET /api/v1/jobArtifacts?name=my-job-artifact

# Search for a specific version
GET /api/v1/jobArtifacts?name=my-job-artifact&version=1.0.0

# List all artifact names starting with "my-"
GET /api/v1/jobArtifacts/names?prefix=my-

# List all artifact names containing "streaming"
GET /api/v1/jobArtifacts/names?contains=streaming
```

### JobArtifact Data Structure

```json
{
  "artifactID": "artifact-123",
  "name": "my-app",
  "version": "1.0.0", 
  "createdAt": "2023-10-01T12:00:00Z",
  "runtimeType": "spring-boot",
  "dependencies": {
    "mantis-runtime": "1.0.0"
  },
  "entrypoint": "com.example.MyApp",
  "tags": {
    "jdkVersion": "17",
    "sbnVersion": "2.7.0",
    "environment": "production"
  }
}
```

## Storage Implementation

### Storage Backend

**Primary Storage Interface:** `IMantisPersistenceProvider`
- Uses pluggable storage backends via `IKeyValueStore` abstraction
- **Production Backend:** DynamoDB Store (`DynamoDBStore.java`)
- **Development/Testing:** In-Memory, File-based, or NoOp stores

**Storage Namespaces:**
```java
JOB_ARTIFACTS_NS = "mantis_global_job_artifacts"
JOB_ARTIFACTS_TO_CACHE_PER_CLUSTER_ID_NS = "mantis_global_cached_artifacts"
```

### Storage Key Structure (Triple Indexing)

JobArtifacts are stored using **three different indexing schemes**:

1. **By Name Collection**: `(JOB_ARTIFACTS_NS, "JobArtifactsByName", artifactName, artifact)`
2. **By Name+Version**: `(JOB_ARTIFACTS_NS, artifactName, version, artifact)`  
3. **By ArtifactID**: `(JOB_ARTIFACTS_NS, artifactID, artifactID, artifact)`

### Size Limits

**DynamoDB Limits (Production):**
- **Item Size Limit: 400KB per JobArtifact** (AWS DynamoDB constraint)
- Batch operations: 25 items max
- Query results: 100 items max

**HTTP API Limits:**
```properties
max-content-length = 8m        # 8MB HTTP request body
max-to-strict-bytes = 8m       # 8MB entity parsing
max-header-value-length = 8k   # 8KB headers
```

**No Application-Level Size Limits:**
- No explicit limits on `tags` map size or individual tag values
- No limits on artifact metadata fields
- Constrained only by underlying storage (DynamoDB's 400KB)

### Retention and Cleanup

**❌ No JobArtifact-Specific Retention:**
- JobArtifacts persist **indefinitely** 
- No automatic cleanup or TTL policies
- Must be manually deleted if needed

**General Cleanup Settings (for other data):**
```properties
# Archive data TTL (90 days) - for jobs/workers, not artifacts
DEFAULT_TTL_IN_MS = TimeUnit.DAYS.toMillis(90)

# Completed job purging (not artifacts)
mantis.master.purge.frequency.secs = 1200      # 20 min intervals
mantis.master.terminated.job.to.delete.delay.hours = 360  # 15 days
```

### Caching Configuration

```properties
mantis.job.worker.max.artifacts.to.cache = 5    # Max artifacts cached per cluster
mantis.artifactCaching.enabled = true           # Caching enabled by default
mantis.artifactCaching.jobClusters = ""         # All clusters by default
```

## JobArtifact Versioning and Override Behavior

### ⚠️ CRITICAL: Overwriting IS Possible

**The `addNewJobArtifact()` operation is an UPSERT:**
- Uses `kvStore.upsert()` method, not `create()`
- **Same name+version combination will completely replace** the existing artifact
- No existence checks or conflict detection
- API returns HTTP 201 regardless of new vs. updated

### Override Scenarios

| Scenario | Result |
|----------|---------|
| **Same name + same version** | ✅ **Complete overwrite** |
| **Same artifactID** | ✅ **Complete overwrite** |
| **Same name + different version** | ✅ **New version added** (no override) |
| **Different name + different version** | ✅ **New artifact added** (no override) |

### ❌ No Protection Mechanisms

**Missing safeguards:**
- No version conflict detection
- No existence checks before storing
- No optimistic locking
- No atomic compare-and-swap
- No immutability enforcement

### Potential Issues

**Accidental Overwrites:**
```bash
# First submission
POST /api/v1/jobArtifacts
{
  "name": "my-app",
  "version": "1.0.0",
  "tags": {"jdkVersion": "11"}
}

# Later submission - OVERWRITES the first one
POST /api/v1/jobArtifacts  
{
  "name": "my-app", 
  "version": "1.0.0",        # Same name+version!
  "tags": {"jdkVersion": "17"}  # Different tags - original lost
}
```

**Concurrent Updates:**
- No protection against concurrent updates to same name+version
- Eventual consistency may cause partial writes during failures

## Key Storage Operations

**JobArtifact CRUD Operations:**
```java
// Check existence
isArtifactExists(String resourceId)

// Retrieve
getArtifactById(String resourceId)
listJobArtifacts(String name, String version)

// Store/Update
addNewJobArtifact(JobArtifact jobArtifact)  // Upsert operation

// Search
listJobArtifactsByName(String prefix, String contains)
```

## Implementation Files

- **API Routes:** `JobArtifactsRoute.java:100-143`
- **Route Handler:** `JobArtifactRouteHandlerImpl.java`
- **Storage Provider:** `KeyValueBasedPersistenceProvider.java:789-812`
- **Domain Model:** `JobArtifact.java:56` (tags field)
- **Job Actor:** `JobActor.java:1787-1803` (attribute merging)
- **Job Definition:** `JobDefinition.java:119-128` (label extraction)
- **Scheduling Constraints:** `SchedulingConstraints.java`

## Best Practices

### To Avoid Accidental Overwrites
1. **Use unique versions** (timestamps, build numbers, git hashes)
2. **Check existence** before submitting if override is unintended
3. **Use semantic versioning** consistently
4. **Implement client-side checks** if immutability is required

### For Scheduling Attributes
1. **Use JobArtifact tags** for artifact-level metadata (JDK version, dependencies)
2. **Use job labels** with `_mantis.schedulingAttribute.{key}` pattern for job-specific constraints
3. **Remember precedence**: Job definition constraints override artifact tags

## Key Findings Summary

✅ **What's Limited:**
- DynamoDB's 400KB per item size limit
- HTTP request size (8MB)

❌ **What's NOT Limited:**
- Number of artifacts stored
- Tags map size (within 400KB item limit)
- Storage duration (no TTL/cleanup)

⚠️ **Considerations:**
- **Artifacts persist forever** - manual cleanup needed
- **Size constraint is per-artifact** (400KB total including tags)
- **No built-in archival** or retention policies
- **Production storage** relies on DynamoDB scalability
- **Overwriting is possible** - same name+version will replace existing artifacts
- **No version protection** - be careful with version management

The system is designed for **artifact replacement** rather than immutable versioning, making it easy to update artifacts but potentially dangerous for accidental overwrites.