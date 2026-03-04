package org.astar.thunder.scheduler;

import org.astar.thunder.cluster.ResourceManager;
import org.astar.thunder.dependency.Dependency;
import org.astar.thunder.dependency.ShuffleDependency;
import org.astar.thunder.rdd.RDD;

import java.util.ArrayList;

public class JobScheduler {
  private int jobId = 0;
  private int stageId = 0;
  private final ResourceManager resourceManager;
  private final TaskScheduler taskScheduler;

  public JobScheduler(ResourceManager resourceManager) {
    this.resourceManager = resourceManager;
    this.taskScheduler = new TaskScheduler();
  }

  public int getJobId() {
    return this.jobId;
  }

  public int getStageId() {
    return this.stageId;
  }

  public void setJobId(int jobId) {
    this.jobId = jobId;
  }

  public void setStageId(int stageId) {
    this.stageId = stageId;
  }

  public void submitJob(RDD<?> rdd) {
    this.jobId++;
    this.stageId = 0;
    // create Stage by breaking the dependency graph at Shuffle boundary
    ArrayList<Stage> stages = createStages(rdd, true); // root is a barrier by convention
    // break down Stage into Task(s)
    ArrayList<Task> tasks = this.taskScheduler.createTasks(stages);
    this.taskScheduler.scheduleTasks(tasks, this.resourceManager);
  }

  /**
   * A dependency tree may look like the following diagram.
   * <pre>
   *              _______ RDD1 ______
   *             /                   \
   *          RDD2                  RDD3
   *            |                     |
   *          RDD4               ___RDD5___
   *                           /           \
   *                         RDD6         RDD7
   * </pre>
   *
   * @param rdd input RDD
   * @return a list of Stages broken down at Shuffle boundary
   */
  public ArrayList<Stage> createStages(RDD<?> rdd, boolean hitBarrier) {
    ArrayList<Stage> stages = new ArrayList<>();
    ArrayList<Integer> parentStageIds = new ArrayList<>();
    int ind = 0;
    for (Dependency<?> dependency : rdd.dependencies()) {
      if (dependency instanceof ShuffleDependency) {
        stages.addAll(createStages(dependency.rdd(), true));
        parentStageIds.add(this.stageId);
        this.stageId++;
      } else {
        stages.addAll(createStages(dependency.rdd(), false));
        for (; ind < stages.size(); ind++) {
          parentStageIds.add(stages.get(ind).getStageId());
        }
      }
    }

    if (hitBarrier) {
      stages.add(new Stage(this.stageId, rdd, parentStageIds));
    }

    return stages;
  }

  public ArrayList<Stage> createStages(RDD<?> rdd) {
    return createStages(rdd, true);
  }
}
