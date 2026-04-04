package org.astar.thunder.akka;

import org.astar.thunder.scheduler.Task;

public class SubmitTask implements ThunderMessage {
  public final Task task;

  public SubmitTask(Task task) {
    this.task = task;
  }
}
