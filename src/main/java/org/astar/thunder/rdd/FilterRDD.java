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

  public FilterRDD(RDD<T> parent, Function<T, T> f) {
    this.parent = parent;
    this.f = f;
  }
  @Override
  public Iterator<T> compute(Partition p) throws Exception {
    Iterator<T> it = this.parent.compute(p);
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public T next() {
        return f.apply(it.next());
      }
    };
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
    ArrayList<T> out = new ArrayList<>();
    for (Partition p : getPartitions()) {
      Iterator<T> it = compute(p);
      while(it.hasNext()) {
        out.add(it.next());
      }
    }
    return out;
  }

  @Override
  public String getRDDName() {
    return "FilterRDD";
  }
}
