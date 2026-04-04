package org.astar.thunder.rdd;

import org.astar.thunder.dependency.Dependency;
import org.astar.thunder.partition.Partition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Function;

public abstract class RDD<T> {
  abstract public Iterator<T> compute(Partition p) throws Exception;

  abstract public ArrayList<Dependency> dependencies();

  abstract public ArrayList<Partition> getPartitions();

  abstract public ArrayList<T> collect() throws Exception;

  public <K> RDD<K> map(Function<T, K> f) {
    return new MapRDD<>(this, f);
  }

  public RDD<T> filter(Function<T, Boolean> f) {
    return new FilterRDD<>(this, f);
  }

  public String getRDDName() {
    return "RDD<T>";
  }

  public String toString(int level) {
    StringBuilder rddString = new StringBuilder(getRDDName());
    ArrayList<Dependency> d = dependencies();
    for (int i = d.size() - 1; i >= 0; i--) {
      String dependency = "\n" + " " .repeat(level * 4) + "|____" + d.get(i).toString(level + 1);
      rddString.append(dependency);
    }
    return rddString.toString();
  }

  public String toString() {
    return toString(1);
  }
}