package org.astar.thunder.akka;

public class TaskResult implements ThunderMessage {
  private final Object result;

  public TaskResult(Object result) {
    this.result = result;
  }
}
