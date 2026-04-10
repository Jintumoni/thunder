package org.astar.thunder.akka;

import org.astar.thunder.core.ResultHandler;

public class TaskResult implements ThunderMessage {
  public Object result;
  public ResultHandler resultHandler;

  public TaskResult(Object result) {
    this.result = result;
  }

  public TaskResult() {
    this.result = null;
  }

  public void setResultHandler(ResultHandler resultHandler) {
    this.resultHandler = resultHandler;
  }
}
