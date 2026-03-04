package org.astar.thunder.scheduler;

import org.astar.thunder.rdd.RDD;

import java.util.ArrayList;

/**
 * Stage is an abstraction for Unit of Work that does not involve data shuffle.
 * A stage may contain one or many {@link Task}. Output of a stage is persisted
 * in memory or written to disk in case of spillage, this output becomes the
 * input for the next stage.
 */
public class Stage {
  private final int stageId;
  private final RDD<?> rdd;

  private final ArrayList<Integer> parentStageIds;

  public Stage(int stageId, RDD<?> rdd, ArrayList<Integer> parentStageIds) {
    this.stageId = stageId;
    this.rdd = rdd;
    this.parentStageIds = parentStageIds;
  }

  public int getStageId() {
    return this.stageId;
  }

  public ArrayList<Integer> getParentStageIds() {
    return this.parentStageIds;
  }

  public RDD<?> getRdd() {
    return this.rdd;
  }

  @Override
  public String toString() {
    return String.format(
      "Stage(Id: %d, Parent Stages: %s)",
      this.stageId,
      this.parentStageIds
    );
  }
}
