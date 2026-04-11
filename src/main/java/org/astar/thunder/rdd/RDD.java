package org.astar.thunder.rdd;

import org.astar.thunder.exceptions.ThunderRDDException;
import org.astar.thunder.utils.ResultHandler;
import org.astar.thunder.core.ThunderContext;
import org.astar.thunder.dependency.Dependency;
import org.astar.thunder.exceptions.ThunderSessionException;
import org.astar.thunder.partition.Partition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class RDD<T> {
  public ThunderContext context;

  public RDD(ThunderContext context) {
    this.context = context;
  }

  private ThunderContext context() {
    if (this.context == null) {
      throw new ThunderSessionException.RDDLacksThunderContext();
    }
    return this.context;
  }

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

  public T reduce(BiFunction<T, T, T> f) throws Exception {
    // executor side reduce
    Function<Iterator<T>, Optional<T>> reducer = itr -> {
      if (!itr.hasNext()) return Optional.empty();

      T acc = itr.next();

      while (itr.hasNext()) {
        acc = f.apply(acc, itr.next());
      }

      return Optional.of(acc);
    };

    // map side reduce
    ResultHandler<T> resultHandler = new ResultHandler<T>() {
      private T partialResult = null;
      private BiFunction<T, T, T> masterSideReducer = f;

      @Override
      public Optional<T> get() throws ThunderRDDException.EmptyResultException {
        return Optional.of(partialResult);
      }

      @Override
      public void apply(Optional<T> existing) {
        if (existing.isPresent()) {
          partialResult = (partialResult == null ? existing.get() : masterSideReducer.apply(existing.get(), partialResult));
        }
      }
    };

    context.submitJob(this, reducer, resultHandler);
    if (resultHandler.get().isEmpty()) {
      throw new ThunderRDDException.EmptyResultException();
    }
    return resultHandler.get().get();
  }

  public String getRDDName() {
    return "RDD<T>";
  }

  public String toString(int level) {
    StringBuilder rddString = new StringBuilder(getRDDName());
    ArrayList<Dependency> d = dependencies();
    for (int i = d.size() - 1; i >= 0; i--) {
      String dependency = "\n" + " ".repeat(level * 4) + "|____" + d.get(i).toString(level + 1);
      rddString.append(dependency);
    }
    return rddString.toString();
  }

  public String toString() {
    return toString(1);
  }
}