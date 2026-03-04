package org.astar.thunder.cluster;

import org.astar.thunder.scheduler.Task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ExecutorInfo {
  public final String executorId;
  public long lastHeartBeat;
  public BlockingQueue<Task> taskQueue;

  public ExecutorInfo(String executorId) {
    this.executorId = executorId;
    this.lastHeartBeat = System.currentTimeMillis();
    this.taskQueue = new LinkedBlockingQueue<>();
  }
}
