package org.astar.thunder.dependency;

import org.astar.thunder.rdd.RDD;

public class ShuffleDependency<T> implements Dependency<T> {
  private final RDD<T> rdd;
  @Override
  public RDD<T> rdd() {
    return this.rdd;
  }

  public ShuffleDependency(RDD<T> rdd) {
    this.rdd = rdd;
  }

  @Override
  public String toString(int level) {
    return this.rdd.toString(level);
  }
}
