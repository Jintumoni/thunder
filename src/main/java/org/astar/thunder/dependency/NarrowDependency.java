package org.astar.thunder.dependency;

import org.astar.thunder.rdd.RDD;

public class NarrowDependency<T> implements Dependency {
  private final RDD<T> rdd;
  public NarrowDependency(RDD<T> rdd) {
    this.rdd = rdd;
  }

  @Override
  public RDD<T> rdd() {
    return this.rdd;
  }

  @Override
  public String toString(int level) {
    return this.rdd.toString(level);
  }
}
