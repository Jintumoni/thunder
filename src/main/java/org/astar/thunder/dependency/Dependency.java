package org.astar.thunder.dependency;

import org.astar.thunder.rdd.RDD;

public interface Dependency<T> {
  RDD<T> rdd();
  String toString(int level);
}
