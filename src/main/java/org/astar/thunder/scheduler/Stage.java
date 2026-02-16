package org.astar.thunder.scheduler;

import org.astar.thunder.rdd.RDD;
import java.util.ArrayList;

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

  @Override
  public String toString() {
    return String.format(
      "Stage(Id: %d, Parent Stages: %s)",
      this.stageId,
      this.parentStageIds
    );
  }
}
