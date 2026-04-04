package org.astar.thunder.scheduler;

import akka.actor.typed.ActorRef;
import org.astar.thunder.akka.SubmitTask;
import org.astar.thunder.akka.ThunderMessage;
import org.astar.thunder.akka.Tombstone;
import org.astar.thunder.partition.Partition;
import org.astar.thunder.rdd.RDD;

import java.util.ArrayList;
import java.util.List;

public class TaskScheduler {
  public void scheduleTasks(List<Task> tasks, ActorRef<ThunderMessage> masterActor) {

    // assign the tasks to executors in Round Robin style
    for (int i = 0; i < tasks.size(); i++) {
      masterActor.tell(new SubmitTask(tasks.get(i)));
    }
    // send tombstone message to shut down actor system
    masterActor.tell(new Tombstone());
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
