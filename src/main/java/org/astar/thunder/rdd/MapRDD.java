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

  private final ArrayList<SELF> out;
  public MapRDD(RDD<PARENT> parent, Function<PARENT, SELF> f) {
    this.parent = parent;
    this.f = f;
    this.out = new ArrayList<>(getPartitions().size());
  }
  @Override
  public Iterator<SELF> compute(Partition p) throws Exception {
    this.out.clear();
    for (Iterator<PARENT> it = this.parent.compute(p); it.hasNext(); ) {
      PARENT result = it.next();
      out.add(this.f.apply(result));
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
  public ArrayList<SELF> collect() throws Exception {
    // for now just iterate through all the partitions and call compute()
    for (Partition p : getPartitions()) {
      compute(p);
    }
    return this.out;
  }

  @Override
  public String getRDDName() {
    return "MapRDD";
  }
}
