package org.astar.thunder.akka;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.util.Iterator;

public class WorkerActor extends AbstractBehavior<ThunderMessage> {
  private final String workerId;

  public WorkerActor(ActorContext<ThunderMessage> context, String workerId) {
    super(context);
    this.workerId = workerId;
  }

  public static Behavior<ThunderMessage> create(String workerId) {
    return Behaviors.setup(context -> new WorkerActor(context, workerId));
  }

  private Behavior<ThunderMessage> onRunTask(SubmitTask msg) {
    try {
      Iterator<?> result = msg.task.run();
      while(result.hasNext()) {
        System.out.println(this.workerId + " " + result.next());
      }
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
