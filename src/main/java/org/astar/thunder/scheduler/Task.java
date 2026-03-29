package org.astar.thunder.scheduler;

import org.astar.thunder.partition.Partition;
import org.astar.thunder.rdd.RDD;

import java.util.Iterator;

/**
 * Task is the fundamental building block of work. It is the
 * smallest unit or work that is carried out by one or many
 * threads/processes in parallel across one or many physical
 * machine(s)
 */
public class Task {
  public static final int TOMBSTONE_TASK_ID = Integer.MAX_VALUE;
  private final int taskId;
  private final Integer stageId;
  private final RDD<?> rdd;
  private final Partition partition;

  public Task(int taskId, Integer stageId, RDD<?> rdd, Partition partition) {
    this.taskId = taskId;
    this.stageId = stageId;
    this.rdd = rdd;
    this.partition = partition;
  }

  public static Task Tombstone() {
    return new Task(TOMBSTONE_TASK_ID, null, null, null);
  }

  public boolean isTaskTombstone() {
    return this.taskId == TOMBSTONE_TASK_ID;
  }

  public Iterator<?> run() throws Exception {
    return this.rdd.compute(this.partition);
  }

  @Override
  public String toString() {
    return String.format(
      "Task(TaskId: %d, StageId: %d, Partition: %s)",
      this.taskId,
      this.stageId,
      this.partition
    );
  }
}
