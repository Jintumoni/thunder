package org.astar.thunder.rdd;

import org.astar.thunder.dependency.Dependency;
import org.astar.thunder.dependency.NarrowDependency;
import org.astar.thunder.partition.Partition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class FilterRDD<T> extends RDD<T> {
  private final RDD<T> parent;
  private final Function<T, T> f;

  private final ArrayList<T> out;
  public FilterRDD(RDD<T> parent, Function<T, T> f) {
    this.parent = parent;
    this.f = f;
    this.out = new ArrayList<>(getPartitions().size());
  }
  @Override
  public Iterator<T> compute(Partition p) throws Exception {
    for (Iterator<T> it = this.parent.compute(p); it.hasNext(); ) {
      T result = this.f.apply(it.next());
      if (result != null) {
        out.add(result);
      }
    }
    return out.iterator();
  }

  @Override
  public ArrayList<Dependency> dependencies() {
    return new ArrayList<>(List.of(new NarrowDependency<>(this.parent)));
  }

  @Override
  public ArrayList<Partition> getPartitions() {
    return this.parent.getPartitions();
  }

  @Override
  public ArrayList<T> collect() throws Exception {
    for (Partition p : getPartitions()) {
      compute(p);
    }
    return this.out;
  }

  @Override
  public String getRDDName() {
    return "FilterRDD";
  }
}
