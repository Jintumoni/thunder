package org.astar.thunder.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import org.astar.thunder.utils.ResultCollector;

public class WorkerActor<T> extends AbstractBehavior<ThunderMessage> {
  private final String workerId;
  private final ActorRef<ThunderMessage> masterActor;

  public WorkerActor(ActorContext<ThunderMessage> context, ActorRef<ThunderMessage> masterActor, String workerId) {
    super(context);
    this.workerId = workerId;
    this.masterActor = masterActor;
  }

  public static Behavior<ThunderMessage> create(ActorRef<ThunderMessage> masterActor, String workerId) {
    return Behaviors.setup(context -> new WorkerActor<>(context, masterActor, workerId));
  }

  private Behavior<ThunderMessage> onRunTask(SubmitTask msg) {
    try {
      ResultCollector<T> collector = new ResultCollector<>();
      msg.task.run(collector);
      this.masterActor.tell(new TaskResult<>(collector));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Behaviors.same();
  }

  @Override
  public Receive<ThunderMessage> createReceive() {
    return newReceiveBuilder().onMessage(SubmitTask.class, this::onRunTask).build();
  }
}
