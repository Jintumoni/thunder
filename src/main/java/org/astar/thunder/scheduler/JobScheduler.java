package org.astar.thunder.scheduler;

import org.astar.thunder.dependency.Dependency;
import org.astar.thunder.dependency.ShuffleDependency;
import org.astar.thunder.rdd.RDD;

import java.util.ArrayList;

public class JobScheduler {
  private int jobId = 0;
  private int stageId = 0;

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

  public int submitJob(RDD<?> rdd) {
    this.jobId++;
    this.stageId = 0;
    // create Stage by breaking the dependency graph at Shuffle boundary
    ArrayList<Stage> stages = createStages(rdd, true); // root is a barrier by convention
    // pass on the Stages to Task Scheduler
    return this.jobId;
  }

  /**
   * A dependency tree may look like following. Suppose RDD1 and RDD4 are ShuffleDependency, then
   * this method shall create 3 stages: one at the edge between RDD1-RDD3, second at the edge between
   * RDD4-RDD5 and last one at the edge between RDD4-RDD6
   * <pre>
   *               ______ RDD ______
   *             /                  \
   *          RDD1                 RDD2
   *            |                    |
   *          RDD3              ___RDD4___
   *                          /           \
   *                       RDD5           RDD6
   * </pre>
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
      }
      else {
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
