package org.astar.thunder.actors;

import org.astar.thunder.utils.ResultCollector;

public class TaskResult<T> implements ThunderMessage {
  public ResultCollector<T> collector;

  public TaskResult(ResultCollector<T> collector) {
    this.collector = collector;
  }
}
