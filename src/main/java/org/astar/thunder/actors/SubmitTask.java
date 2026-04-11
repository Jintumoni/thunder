package org.astar.thunder.actors;

import akka.actor.typed.ActorRef;
import org.astar.thunder.scheduler.Task;

public class SubmitTask implements ThunderMessage {
  public final Task task;
  public ActorRef<ThunderMessage> replyTo;

  public SubmitTask(Task task) {
    this.task = task;
  }

  public SubmitTask(Task task, ActorRef<ThunderMessage> replyTo) {
    this.task = task;
    this.replyTo = replyTo;
  }
}
