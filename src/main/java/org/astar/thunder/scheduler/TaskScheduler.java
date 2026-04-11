package org.astar.thunder.scheduler;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import org.astar.thunder.actors.*;
import org.astar.thunder.utils.ResultHandler;
import org.astar.thunder.partition.Partition;
import org.astar.thunder.rdd.RDD;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class TaskScheduler {
  public void scheduleTasks(List<Task> tasks, ActorSystem<ThunderMessage> actorSystem) {

    CompletionStage<ThunderMessage> future =
      AskPattern.ask(
        actorSystem,
        replyTo -> new SubmitJob(tasks, replyTo),
        Duration.ofSeconds(360),
        actorSystem.scheduler()
      );

    // wait for result
    future.toCompletableFuture().join();
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

  public <T> ArrayList<Task> createTasks(List<Stage> stages,
                                         Function<Iterator<T>, Optional<T>> processFunc,
                                         ResultHandler<T> resultHandler) {
    ArrayList<Task> allTasks = new ArrayList<>();
    for (int i = 0; i < stages.size(); i++) {
      RDD<?> rdd = stages.get(i).getRdd();
      ArrayList<Partition> partitions = rdd.getPartitions();
      int stageId = stages.get(i).getStageId();
      for (int j = 0; j < partitions.size(); j++) {
        Task<T> task = new Task(j, stageId, rdd, partitions.get(j), processFunc, resultHandler);
        allTasks.add(task);
      }
    }
    return allTasks;
  }
}
