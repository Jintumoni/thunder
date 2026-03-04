package org.astar.thunder.cluster;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ResourceManager {
  private final int DEFAULT_PARALLELISM = 2;
  private final long HEARTBEAT_TIMEOUT_MS = 5000;
  private final ConcurrentHashMap<String, ExecutorInfo> executors = new ConcurrentHashMap<>();

  public void init() {
    for (int i = 0; i < this.DEFAULT_PARALLELISM; i++) {
      new Thread(new ExecutorThread("executorThread" + i, this)).start();
    }
  }

  public ExecutorInfo registerExecutor(String executorId) {
    ExecutorInfo info = new ExecutorInfo(executorId);
    this.executors.put(executorId, info);
    System.out.printf("Registered executor [%s] in the cluster%n", executorId);
    return info;
  }

  public void receiveHeartbeat(String executorId) {
    ExecutorInfo info = executors.get(executorId);
    if (info != null) {
      info.lastHeartBeat = System.currentTimeMillis();
    }
  }

  public ArrayList<ExecutorInfo> getActiveExecutors() {
    long now = System.currentTimeMillis();
    return (ArrayList<ExecutorInfo>) this.executors.values()
      .stream()
      .filter(e -> now - e.lastHeartBeat < this.HEARTBEAT_TIMEOUT_MS)
      .collect(Collectors.toList());
  }
}
