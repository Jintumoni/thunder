package org.astar.thunder.rdd;

import org.astar.thunder.dependency.Dependency;
import org.astar.thunder.dependency.ShuffleDependency;
import org.astar.thunder.partition.Partition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class ShuffleRDD<PARENT, SELF> extends RDD<SELF> {
  private final RDD<PARENT> parent;
  private final Function<PARENT, SELF> reducer;

  public ShuffleRDD(RDD<PARENT> parent, Function<PARENT, SELF> reducer) {
    super(parent.context);
    this.parent = parent;
    this.reducer = reducer;
  }

  @Override
  public Iterator<SELF> compute(Partition p) throws Exception {
    return null;
  }

  @Override
  public ArrayList<Dependency> dependencies() {
    return new ArrayList<>(List.of(new ShuffleDependency<>(this.parent)));
  }

  @Override
  public ArrayList<Partition> getPartitions() {
    return null;
  }

  @Override
  public ArrayList<SELF> collect() throws Exception {
    return null;
  }

  @Override
  public String getRDDName() {
    return "ShuffleRDD";
  }
}
