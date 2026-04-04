package org.astar.thunder.akka;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.util.ArrayList;
import java.util.List;

public class MasterActor extends AbstractBehavior<ThunderMessage> {
  private final static int DEFAULT_PARALLELISM = 2;
  private final List<ActorRef<ThunderMessage>> workers;
  private int nextWorker = 0;

  private MasterActor(ActorContext<ThunderMessage> context, int workers) {
    super(context);
    this.workers = new ArrayList<>();
    for (int i = 0; i < workers; i++) {
      this.workers.add(context.spawn(WorkerActor.create(String.format("worker-%d", i)), "worker-" + i));
    }
  }

  private MasterActor(ActorContext<ThunderMessage> context) {
    super(context);
    this.workers = new ArrayList<>();
    for (int i = 0; i < this.DEFAULT_PARALLELISM; i++) {
      this.workers.add(context.spawn(WorkerActor.create(String.format("worker-%d", i)), "worker-" + i));
    }
  }

  public static Behavior<ThunderMessage> create(int workers) {
    return Behaviors.setup(context -> new MasterActor(context, workers));
  }

  public static Behavior<ThunderMessage> create() {
    return Behaviors.setup(context -> new MasterActor(context, DEFAULT_PARALLELISM));
  }

  @Override
  public Receive<ThunderMessage> createReceive() {
    return newReceiveBuilder()
      .onMessage(SubmitTask.class, this::onSubmitTask)
      .onMessage(Tombstone.class, this::onTombstone)
      .build();
  }

  private Behavior<ThunderMessage> onTombstone(Tombstone tombstoneMsg) {
    return Behaviors.stopped();
  }

  private Behavior<ThunderMessage> onSubmitTask(SubmitTask submitTaskMsg) {
    ActorRef<ThunderMessage> currentWorker = workers.get(this.nextWorker);
    this.nextWorker = (this.nextWorker + 1) % this.workers.size();
    currentWorker.tell(new SubmitTask(submitTaskMsg.task));
    return this;
  }
}
