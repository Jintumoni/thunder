package org.astar.thunder.actors;

import akka.actor.typed.ActorRef;
import org.astar.thunder.scheduler.Task;

import java.util.ArrayList;

public class SubmitJob<T> implements ThunderMessage {
  public final ArrayList<Task<T>> tasks;
  public ActorRef<ThunderMessage> replyTo;

  public SubmitJob(ArrayList<Task<T>> tasks) {
    this.tasks = tasks;
  }

  public SubmitJob(ArrayList<Task<T>> tasks, ActorRef<ThunderMessage> replyTo) {
    this.tasks = tasks;
    this.replyTo = replyTo;
  }
}
