package org.astar.thunder.cluster;

import org.astar.thunder.scheduler.Task;

import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorThread implements Runnable {
  private final String executorId;
  private final ExecutorInfo executorInfo;
  private final ResourceManager resourceManager;
  private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();

  public ExecutorThread(String executorId, ResourceManager resourceManager) {
    this.executorId = executorId;
    this.resourceManager = resourceManager;
    this.executorInfo = this.resourceManager.registerExecutor(this.executorId);
  }

  @Override
  public void run() {
    heartbeatScheduler.scheduleAtFixedRate(() -> this.resourceManager.receiveHeartbeat(this.executorId),
      0,
      2,
      TimeUnit.SECONDS
    );

    while (true) {
      try {
        Task task = executorInfo.taskQueue.take();

        if (task.isTaskTombstone()) {
          heartbeatScheduler.shutdown();
          break;
        }

        Iterator<?> result = task.run();

        System.out.println(executorId + " finished task: " + task);

        while(result.hasNext()) {
          System.out.println(executorId + " " + result.next());
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
