package io.mantisrx.master.resourcecluster;

public class TaskAssignmentManagerActor {
    /* todo: context
    - AssignmentTrackerActor: tracks and manages actual worker assignments
    - Host similar funcationality as onAssignedScheduleRequestEvent(AssignedScheduleRequestEvent event) in ResourceClusterAwareSchedulerActor; Since this is now a child actor in resourceClusterActor it doesn't need to go throgh resource cluster interface to get the connections etc. but can directly ask the parent actor for the needed objects.
    - track ongoing IO tasks to each TE and handle retry similar to logic in scheduler actor.
    - assignment tasks can be cancelled due to job kill and worker replacement.

    AssignmentTask needs to track:
        - Original Reservation
        - assigned workers
        - pending workers w/ retry cnt
        tracker timer on dispatcher;
        assignment IO to TE on IO;
        - route starting event to jobactor

     */

}
