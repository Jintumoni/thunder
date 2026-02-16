package org.astar.thunder.dependency;

import org.astar.thunder.rdd.RDD;

public class ShuffleDependency<T> implements Dependency<T> {
  @Override
  public RDD<T> rdd() {
    return null;
  }

  @Override
  public String toString(int level) {
    return null;
  }
}
