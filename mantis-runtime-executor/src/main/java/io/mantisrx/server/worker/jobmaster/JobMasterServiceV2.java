package io.mantisrx.server.worker.jobmaster;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.DeadLetter;
import akka.actor.Props;
import io.mantisrx.common.akka.DeadLetterActor;
import io.mantisrx.server.core.Service;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobMasterServiceV2 implements Service {

    @Override
    public void start() {
        log.info("Starting JobMasterServiceV2");
        final ActorSystem system = ActorSystem.create("MantisJobMaster");
        // log the configuration of the actor system
        system.logConfiguration();

        // log dead letter messages
        final ActorRef actor = system.actorOf(Props.create(DeadLetterActor.class), "MantisDeadLetter");
        system.eventStream().subscribe(actor, DeadLetter.class);

        // todo: init coordinator, handle coordinator failure
        // no need to block and wait (done in root OperationExecution)
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void enterActiveMode() {

    }
}
