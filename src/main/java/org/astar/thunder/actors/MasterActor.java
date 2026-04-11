package org.astar.thunder.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import org.astar.thunder.scheduler.Task;
import org.astar.thunder.utils.ResultCollector;
import org.astar.thunder.utils.ResultHandler;

import java.util.ArrayList;
import java.util.List;

public class MasterActor<T> extends AbstractBehavior<ThunderMessage> {
  private final List<ActorRef<ThunderMessage>> workers;
  private int nextWorker = 0;
  private ResultHandler<T> resultHandler;
  private int tasksCount = 0;
  private ActorRef<ThunderMessage> clientRef;

  private MasterActor(ActorContext<ThunderMessage> context, int workers) {
    super(context);
    this.workers = new ArrayList<>();
    for (int i = 0; i < workers; i++) {
      this.workers.add(context.spawn(
        WorkerActor.create(context.getSelf(), String.format("worker-%d", i)), "worker-" + i)
      );
    }
  }

  public static Behavior<ThunderMessage> create(int workers) {
    return Behaviors.setup(context -> new MasterActor<>(context, workers));
  }

  @Override
  public Receive<ThunderMessage> createReceive() {
    return newReceiveBuilder()
      .onMessage(SubmitJob.class, this::onSubmitJob)
      .onMessage(TaskResult.class, this::onResult)
      .build();
  }

  private Behavior<ThunderMessage> onResult(TaskResult<T> resultMsg) {
    tasksCount--;
    resultHandler.apply(resultMsg.collector.getResult());
    if (tasksCount == 0) {
      clientRef.tell(new TaskResult<>(new ResultCollector<>(resultHandler.get())));
    }
    return this;
  }

  private Behavior<ThunderMessage> onSubmitJob(SubmitJob submitJobMsg) {
    tasksCount = submitJobMsg.tasks.size();
    clientRef = submitJobMsg.replyToClient;

    for (Task<T> task : submitJobMsg.tasks) {
      if (resultHandler == null) {
        resultHandler = task.getResultHandler();
      }
      ActorRef<ThunderMessage> currentWorker = workers.get(this.nextWorker);
      this.nextWorker = (this.nextWorker + 1) % this.workers.size();
      currentWorker.tell(new SubmitTask(task, getContext().getSelf()));
    }
    return this;
  }
}
