package org.astar.thunder.rdd;

import org.astar.thunder.dependency.Dependency;
import org.astar.thunder.dependency.NarrowDependency;
import org.astar.thunder.partition.Partition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class MapRDD<PARENT, SELF> extends RDD<SELF> {

  private final RDD<PARENT> parent;
  private final Function<PARENT, SELF> f;

  public MapRDD(RDD<PARENT> parent, Function<PARENT, SELF> f) {
    this.parent = parent;
    this.f = f;
  }

  @Override
  public Iterator<SELF> compute(Partition p) throws Exception {
    Iterator<PARENT> it = this.parent.compute(p);
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public SELF next() {
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
  public ArrayList<SELF> collect() throws Exception {
    ArrayList<SELF> out = new ArrayList<>();
    for (Partition p : getPartitions()) {
      Iterator<SELF> it = compute(p);
      while (it.hasNext()) {
        out.add(it.next());
      }
    }
    return out;
  }

  @Override
  public String getRDDName() {
    return "MapRDD";
  }
}
