package io.mantisrx.master.resourcecluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.mantisrx.master.resourcecluster.ResourceClusterActor.Reservation;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ReservationKey;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ReservationPriority;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.UpsertReservation;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;

public class ReservationTest {

    private static final MachineDefinition MACHINE =
        new MachineDefinition(2.0, 4096, 128.0, 10240, 1);
    private static final Instant TEST_INSTANT = Instant.parse("2024-01-01T00:00:00Z");
    private static final ClusterID TEST_CLUSTER_ID = ClusterID.of("test-cluster");

    @Test
    public void testFromUpsertReservation_withBasicFields() {
        // Arrange
        ReservationKey key = ReservationKey.builder()
            .jobId("test-job")
            .stageNumber(1)
            .build();

        SchedulingConstraints constraints = SchedulingConstraints.of(
            MACHINE,
            Optional.of("test-size"),
            Map.of("zone", "us-west")
        );

        ReservationPriority priority = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.NEW_JOB)
            .tier(2)
            .timestamp(TEST_INSTANT.toEpochMilli())
            .build();

        UpsertReservation upsert = UpsertReservation.builder()
            .reservationKey(key)
            .schedulingConstraints(constraints)
            .allocationRequests(Collections.emptySet())
            .stageTargetSize(10)
            .priority(priority)
            .build();

        // Act
        Reservation result = Reservation.fromUpsertReservation(
            upsert,
            "test-canonical-key"
        );

        // Assert
        assertNotNull(result);
        assertEquals(key, result.getKey());
        assertEquals(constraints, result.getSchedulingConstraints());
        assertEquals("test-canonical-key", result.getCanonicalConstraintKey());
        assertEquals(Collections.emptySet(), result.getRequestedWorkers());
        assertEquals(Collections.emptySet(), result.getAllocationRequests());
        assertEquals(10, result.getStageTargetSize());
        assertEquals(priority, result.getPriority());
        assertEquals(0, result.getRequestedWorkersCount()); // Should be 0 for empty set
    }

    @Test
    public void testFromUpsertReservation_withEmptyAllocationRequests() {
        // Arrange
        ReservationKey key = ReservationKey.builder()
            .jobId("job-with-empty-allocations")
            .stageNumber(2)
            .build();

        SchedulingConstraints constraints = SchedulingConstraints.of(
            MACHINE,
            Optional.empty(),
            Collections.emptyMap()
        );

        ReservationPriority priority = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.SCALE)
            .tier(1)
            .timestamp(TEST_INSTANT.plusSeconds(5).toEpochMilli())
            .build();

        Set<TaskExecutorAllocationRequest> allocationRequests = Collections.emptySet();

        UpsertReservation upsert = UpsertReservation.builder()
            .reservationKey(key)
            .schedulingConstraints(constraints)
            .allocationRequests(allocationRequests)
            .stageTargetSize(3)
            .priority(priority)
            .build();

        // Act
        Reservation result = Reservation.fromUpsertReservation(
            upsert,
            "canonical-key-2"
        );

        // Assert
        assertNotNull(result);
        assertEquals(key, result.getKey());
        assertEquals(constraints, result.getSchedulingConstraints());
        assertEquals("canonical-key-2", result.getCanonicalConstraintKey());
        assertEquals(Collections.emptySet(), result.getRequestedWorkers()); // Empty because no allocation requests
        assertEquals(allocationRequests, result.getAllocationRequests());
        assertEquals(3, result.getStageTargetSize());
        assertEquals(priority, result.getPriority());
        assertEquals(0, result.getRequestedWorkersCount()); // Should be 0 for empty set
    }

    @Test
    public void testFromUpsertReservation_withActualAllocationRequests() {
        // Arrange
        ReservationKey key = ReservationKey.builder()
            .jobId("job-with-real-allocations")
            .stageNumber(1)
            .build();

        SchedulingConstraints constraints = SchedulingConstraints.of(
            MACHINE,
            Optional.of("medium"),
            Map.of("zone", "us-east")
        );

        ReservationPriority priority = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.NEW_JOB)
            .tier(1)
            .timestamp(TEST_INSTANT.plusSeconds(10).toEpochMilli())
            .build();

        // Create WorkerIds
        WorkerId worker1 = WorkerId.fromId("job-123-worker-0-0").get();
        WorkerId worker2 = WorkerId.fromId("job-123-worker-0-1").get();
        WorkerId worker3 = WorkerId.fromId("job-123-worker-0-2").get();

        // Create mock TaskExecutorAllocationRequest objects
        // Note: In a real scenario, these would be properly constructed with all required fields
        TaskExecutorAllocationRequest req1 = TaskExecutorAllocationRequest.of(
            worker1, constraints, null, 1);

        TaskExecutorAllocationRequest req2 = TaskExecutorAllocationRequest.of(
            worker2, constraints, null, 1);

        TaskExecutorAllocationRequest req3 = TaskExecutorAllocationRequest.of(
            worker3, constraints, null, 1);

        Set<TaskExecutorAllocationRequest> allocationRequests = Set.of(req1, req2, req3);
        Set<WorkerId> expectedWorkerIds = Set.of(worker1, worker2, worker3);

        UpsertReservation upsert = UpsertReservation.builder()
            .reservationKey(key)
            .schedulingConstraints(constraints)
            .allocationRequests(allocationRequests)
            .stageTargetSize(3)
            .priority(priority)
            .build();

        // Act
        Reservation result = Reservation.fromUpsertReservation(
            upsert,
            "canonical-key-with-workers"
        );

        // Assert
        assertNotNull(result);
        assertEquals(key, result.getKey());
        assertEquals(constraints, result.getSchedulingConstraints());
        assertEquals("canonical-key-with-workers", result.getCanonicalConstraintKey());
        assertEquals(expectedWorkerIds, result.getRequestedWorkers()); // Should extract WorkerIds from allocation requests
        assertEquals(allocationRequests, result.getAllocationRequests());
        assertEquals(3, result.getStageTargetSize());
        assertEquals(priority, result.getPriority());
        assertEquals(3, result.getRequestedWorkersCount()); // Should be 3 for the set of 3 workers
    }

    @Test
    public void testFromUpsertReservation_withNullAllocationRequests() {
        // Arrange
        ReservationKey key = ReservationKey.builder()
            .jobId("job-null-allocations")
            .stageNumber(0)
            .build();

        SchedulingConstraints constraints = SchedulingConstraints.of(
            MACHINE,
            Optional.of("large"),
            Map.of("env", "prod")
        );

        ReservationPriority priority = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.REPLACE)
            .tier(0)
            .timestamp(TEST_INSTANT.toEpochMilli())
            .build();

        UpsertReservation upsert = UpsertReservation.builder()
            .reservationKey(key)
            .schedulingConstraints(constraints)
            .allocationRequests(null) // null allocation requests
            .stageTargetSize(1)
            .priority(priority)
            .build();

        // Act
        Reservation result = Reservation.fromUpsertReservation(
            upsert,
            "canonical-null-test"
        );

        // Assert
        assertNotNull(result);
        assertEquals(key, result.getKey());
        assertEquals(constraints, result.getSchedulingConstraints());
        assertEquals("canonical-null-test", result.getCanonicalConstraintKey());
        assertEquals(Collections.emptySet(), result.getRequestedWorkers());
        assertEquals(Collections.emptySet(), result.getAllocationRequests()); // Should default to empty set
        assertEquals(1, result.getStageTargetSize());
        assertEquals(priority, result.getPriority());
        assertEquals(0, result.getRequestedWorkersCount()); // Should be 0 for empty set
    }

    @Test
    public void testFromUpsertReservation_preservesAllPriorityFields() {
        // Arrange
        ReservationKey key = ReservationKey.builder()
            .jobId("priority-test-job")
            .stageNumber(5)
            .build();

        SchedulingConstraints constraints = SchedulingConstraints.of(
            MACHINE,
            Optional.empty(),
            Collections.emptyMap()
        );

        // Test all priority types
        for (ReservationPriority.PriorityType type : ReservationPriority.PriorityType.values()) {
            ReservationPriority priority = ReservationPriority.builder()
                .type(type)
                .tier(type.ordinal() + 1) // Different tier for each type
                .timestamp(TEST_INSTANT.plusSeconds(type.ordinal()).toEpochMilli())
                .build();

            UpsertReservation upsert = UpsertReservation.builder()
                .reservationKey(key)
                .schedulingConstraints(constraints)
                .allocationRequests(Collections.emptySet())
                .stageTargetSize(4)
                .priority(priority)
                .build();

            // Act
            Reservation result = Reservation.fromUpsertReservation(
                upsert,
                "priority-test-canonical"
            );

            // Assert
            assertNotNull("Result should not be null for type: " + type, result);
            assertEquals("Priority type should match for: " + type, priority, result.getPriority());
            assertEquals("Priority type should be preserved for: " + type, type, result.getPriority().getType());
            assertEquals("Priority tier should be preserved for: " + type, type.ordinal() + 1, result.getPriority().getTier());
            assertEquals("Priority timestamp should be preserved for: " + type,
                TEST_INSTANT.plusSeconds(type.ordinal()).toEpochMilli(), result.getPriority().getTimestamp());
        }
    }

    @Test
    public void testFromUpsertReservation_withDuplicateWorkerIds() {
        // Arrange
        ReservationKey key = ReservationKey.builder()
            .jobId("job-with-duplicate-workers")
            .stageNumber(3)
            .build();

        SchedulingConstraints constraints = SchedulingConstraints.of(
            MACHINE,
            Optional.of("small"),
            Collections.emptyMap()
        );

        ReservationPriority priority = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.SCALE)
            .tier(2)
            .timestamp(TEST_INSTANT.plusSeconds(15).toEpochMilli())
            .build();

        // Create WorkerIds with duplicates
        WorkerId worker1 = WorkerId.fromId("job-456-worker-1-0").get();
        WorkerId worker2 = WorkerId.fromId("job-456-worker-1-1").get();
        // worker1 again (duplicate)

        // Create allocation requests with duplicate WorkerIds
        TaskExecutorAllocationRequest req1 = TaskExecutorAllocationRequest.of(
            worker1, constraints, null, 2);

        TaskExecutorAllocationRequest req2 = TaskExecutorAllocationRequest.of(
            worker2, constraints, null, 2);

        TaskExecutorAllocationRequest req3 = TaskExecutorAllocationRequest.of(
            worker1, constraints, null, 2); // Duplicate worker1

        Set<TaskExecutorAllocationRequest> allocationRequests = new HashSet<>();
        allocationRequests.add(req1);
        allocationRequests.add(req2);
        allocationRequests.add(req3);
        Set<WorkerId> expectedWorkerIds = Set.of(worker1, worker2); // Should deduplicate to only 2 unique workers

        UpsertReservation upsert = UpsertReservation.builder()
            .reservationKey(key)
            .schedulingConstraints(constraints)
            .allocationRequests(allocationRequests)
            .stageTargetSize(2)
            .priority(priority)
            .build();

        // Act
        Reservation result = Reservation.fromUpsertReservation(
            upsert,
            "canonical-dedupe-test"
        );

        // Assert
        assertNotNull(result);
        assertEquals(key, result.getKey());
        assertEquals(constraints, result.getSchedulingConstraints());
        assertEquals("canonical-dedupe-test", result.getCanonicalConstraintKey());
        assertEquals(expectedWorkerIds, result.getRequestedWorkers()); // Should deduplicate to 2 unique workers
        assertEquals(allocationRequests, result.getAllocationRequests());
        assertEquals(2, result.getStageTargetSize());
        assertEquals(priority, result.getPriority());
        assertEquals(2, result.getRequestedWorkersCount()); // Should be 2 after deduplication
    }

    @Test
    public void testReservationPriorityComparison() {
        // 1. Compare by Type (REPLACE < SCALE < NEW_JOB)
        ReservationPriority pReplace = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.REPLACE)
            .tier(1)
            .timestamp(1000)
            .build();

        ReservationPriority pScale = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.SCALE)
            .tier(1)
            .timestamp(1000)
            .build();

        ReservationPriority pNewJob = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.NEW_JOB)
            .tier(1)
            .timestamp(1000)
            .build();

        // REPLACE < SCALE
        assertTrue("REPLACE should be less than SCALE", pReplace.compareTo(pScale) < 0);
        // SCALE < NEW_JOB
        assertTrue("SCALE should be less than NEW_JOB", pScale.compareTo(pNewJob) < 0);
        // REPLACE < NEW_JOB
        assertTrue("REPLACE should be less than NEW_JOB", pReplace.compareTo(pNewJob) < 0);

        // 2. Compare by Tier (Ascending)
        ReservationPriority pTier1 = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.SCALE)
            .tier(1)
            .timestamp(1000)
            .build();

        ReservationPriority pTier2 = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.SCALE)
            .tier(2)
            .timestamp(1000)
            .build();

        assertTrue("Lower tier should be less than higher tier", pTier1.compareTo(pTier2) < 0);

        // 3. Compare by Timestamp (Ascending)
        ReservationPriority pTime1 = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.SCALE)
            .tier(2)
            .timestamp(1000)
            .build();

        ReservationPriority pTime2 = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.SCALE)
            .tier(2)
            .timestamp(2000)
            .build();

        assertTrue("Earlier timestamp should be less than later timestamp", pTime1.compareTo(pTime2) < 0);

        // 4. Equality check for comparison
        ReservationPriority pTime1Copy = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.SCALE)
            .tier(2)
            .timestamp(1000)
            .build();

        assertEquals("Equal priorities should return 0", 0, pTime1.compareTo(pTime1Copy));
    }

    @Test
    public void testReservationPriorityEquality() {
        ReservationPriority p1 = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.SCALE)
            .tier(1)
            .timestamp(1000)
            .build();

        ReservationPriority p2 = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.SCALE)
            .tier(1)
            .timestamp(1000)
            .build();

        ReservationPriority p3 = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.NEW_JOB)
            .tier(1)
            .timestamp(1000)
            .build();

        assertEquals(p1, p2);
        assertEquals(p1.hashCode(), p2.hashCode());
        assertNotEquals(p1, p3);
        assertFalse(p1.equals(null));
        assertFalse(p1.equals(new Object()));
    }
}
