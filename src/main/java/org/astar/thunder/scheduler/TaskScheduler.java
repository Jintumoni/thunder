package org.astar.thunder.scheduler;

import org.astar.thunder.cluster.ExecutorInfo;
import org.astar.thunder.cluster.ResourceManager;
import org.astar.thunder.partition.Partition;
import org.astar.thunder.rdd.RDD;

import java.util.ArrayList;
import java.util.List;

public class TaskScheduler {
  public void scheduleTasks(List<Task> tasks, ResourceManager resourceManager) {
    ArrayList<ExecutorInfo> executors = resourceManager.getActiveExecutors();
    // assign the tasks to executors in Round Robin style
    for (int i = 0; i < tasks.size(); i++) {
      System.out.printf("Assigned task %d to executor [%s]%n", i, executors.get(i % executors.size()).executorId);
      executors.get(i % executors.size()).taskQueue.add(tasks.get(i));
    }
    for (ExecutorInfo executor : executors) {
      executor.taskQueue.add(Task.Tombstone());
    }
  }

  public ArrayList<Task> createTasks(List<Stage> stages) {
    ArrayList<Task> allTasks = new ArrayList<>();
    for (int i = 0; i < stages.size(); i++) {
      RDD<?> rdd = stages.get(i).getRdd();
      ArrayList<Partition> partitions = rdd.getPartitions();
      int stageId = stages.get(i).getStageId();
      for (int j = 0; j < partitions.size(); j++) {
        allTasks.add(new Task(j, stageId, rdd, partitions.get(j)));
      }
    }
    return allTasks;
  }
}
