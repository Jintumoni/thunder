package org.astar.thunder.scheduler;

import org.astar.thunder.utils.ResultCollector;
import org.astar.thunder.utils.ResultHandler;
import org.astar.thunder.partition.Partition;
import org.astar.thunder.rdd.RDD;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

/**
 * Task is the fundamental building block of work. It is the
 * smallest unit or work that is carried out by one or many
 * threads/processes in parallel across one or many physical
 * machine(s)
 */
public class Task<T> {
  private int taskId;
  private Integer stageId;
  private RDD<T> rdd;
  private Partition partition;
  private Function<Iterator<T>, Optional<T>> processFunc;
  private ResultHandler<T> resultHandler;

  public Task(int taskId, Integer stageId, RDD<T> rdd, Partition partition) {
    new Task<>(taskId, stageId, rdd, partition, null, null);
  }

  public Task(int taskId, Integer stageId, RDD<T> rdd, Partition partition,
              Function<Iterator<T>, Optional<T>> processFunc) {
    this.taskId = taskId;
    this.stageId = stageId;
    this.rdd = rdd;
    this.partition = partition;
    this.processFunc = processFunc;
  }

  public Task(int taskId, Integer stageId, RDD<T> rdd, Partition partition,
              Function<Iterator<T>, Optional<T>> processFunc,
              ResultHandler<T> resultHandler) {
    this.taskId = taskId;
    this.stageId = stageId;
    this.rdd = rdd;
    this.partition = partition;
    this.processFunc = processFunc;
    this.resultHandler = resultHandler;
  }

  public void run(ResultCollector<T> collector) throws Exception {
    if (processFunc != null) {
      Optional<T> output = processFunc.apply(this.rdd.compute(this.partition));
      collector.collect(output);
    } else {
      this.rdd.compute(this.partition);
    }
  }

  public ResultHandler<T> getResultHandler() {
    return this.resultHandler;
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
