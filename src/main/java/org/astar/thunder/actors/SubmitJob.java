package org.astar.thunder.actors;

import akka.actor.typed.ActorRef;
import org.astar.thunder.scheduler.Task;

import java.util.List;

public class SubmitJob implements ThunderMessage {
  public final List<Task> tasks;
  public ActorRef<ThunderMessage> replyToClient;

  public SubmitJob(List<Task> tasks) {
    this.tasks = tasks;
  }

  public SubmitJob(List<Task> tasks, ActorRef<ThunderMessage> replyToClient) {
    this.tasks = tasks;
    this.replyToClient = replyToClient;
  }
}
