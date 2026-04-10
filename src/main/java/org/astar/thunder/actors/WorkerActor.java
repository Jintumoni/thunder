package org.astar.thunder.akka;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.util.Iterator;

public class WorkerActor extends AbstractBehavior<ThunderMessage> {
  private final String workerId;
  private final ActorRef<ThunderMessage> masterActor;

  public WorkerActor(ActorContext<ThunderMessage> context,
                     ActorRef<ThunderMessage> masterActor,
                     String workerId) {
    super(context);
    this.workerId = workerId;
    this.masterActor = masterActor;
  }

  public static Behavior<ThunderMessage> create(ActorRef<ThunderMessage> masterActor, String workerId) {
    return Behaviors.setup(context -> new WorkerActor(context, masterActor, workerId));
  }

  private Behavior<ThunderMessage> onRunTask(SubmitTask msg) {
    try {
      System.out.println("executing task on runner " + workerId);
      msg.task.run();
      System.out.println("done... output = " + msg.task.getTaskResult().result);
      msg.replyTo.tell(new TaskResult(msg.task.getTaskResult()));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Behaviors.same();
  }

  @Override
  public Receive<ThunderMessage> createReceive() {
    return newReceiveBuilder()
      .onMessage(SubmitTask.class, this::onRunTask)
      .build();
  }
}
