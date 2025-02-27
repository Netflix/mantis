package io.mantisrx.server.worker.jobmaster;

import akka.actor.*;
import io.mantisrx.common.akka.DeadLetterActor;
import io.mantisrx.server.core.Service;
import io.mantisrx.server.worker.jobmaster.rules.CoordinatorActor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import scala.concurrent.Future;

@Slf4j
public class JobMasterServiceV2 implements Service {
    final JobScalerContext jobScalerContext;
    ActorSystem system;

    public JobMasterServiceV2(JobScalerContext jobScalerContext) {
        this.jobScalerContext = jobScalerContext;
    }

    @Override
    public void start() {
        log.info("Starting JobMasterServiceV2");
        system = ActorSystem.create("MantisJobMasterV2");
        // log the configuration of the actor system
        if (log.isDebugEnabled()) {
            system.logConfiguration();
        }

        // log dead letter messages
        final ActorRef actor = system.actorOf(Props.create(DeadLetterActor.class), "MantisDeadLetter");
        system.eventStream().subscribe(actor, DeadLetter.class);

        // no need to block and wait (done in root OperationExecution)
        system.actorOf(CoordinatorActor.Props(this.jobScalerContext), "JobScalerCoordinatorActor");
    }

    @Override
    public void shutdown() {
        system.terminate(); //todo: shall we wait for the termination future?
    }

    @Override
    public void enterActiveMode() {
    }
}
