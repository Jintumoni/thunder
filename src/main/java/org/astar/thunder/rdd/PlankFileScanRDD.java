package org.astar.thunder.rdd;

import io.plank.PlankReader;
import io.plank.RecordBatch;
import org.astar.thunder.core.ThunderContext;
import org.astar.thunder.dependency.Dependency;
import org.astar.thunder.partition.Partition;
import org.astar.thunder.partition.PlankFilePartition;

import java.util.ArrayList;
import java.util.Iterator;

public class PlankFileScanRDD extends RDD<Object> {
  private final PlankReader plankReader;
  private final String fileLoc;

  public PlankFileScanRDD(ThunderContext context, PlankReader plankReader) {
    super(context);
    this.plankReader = plankReader;
    this.fileLoc = this.plankReader.getFilePath();
  }

  @Override
  public Iterator<Object> compute(Partition p) throws Exception {
    PlankFilePartition partition = (PlankFilePartition) p;
    RecordBatch batch = this.plankReader.readRowGroup(partition.getRowGroupId());
    final int ROW_COUNT = batch.columns[0].length;
    ArrayList<ArrayList<Object>> rows = new ArrayList<>(ROW_COUNT);
    for (int i = 0; i < ROW_COUNT; i++) {
      ArrayList<Object> row = new ArrayList<>();
      for (int j = 0; j < batch.columns.length; j++) {
        row.add(batch.columns[j][i]);
      }
      rows.add(row);
    }

    return new Iterator<Object>() {
      private int currentRow = 0;

      @Override
      public boolean hasNext() {
        return currentRow < ROW_COUNT;
      }

      @Override
      public Object next() {
        return rows.get(currentRow++);
      }
    };
  }

  @Override
  public ArrayList<Dependency> dependencies() {
    return new ArrayList<>();
  }

  @Override
  public ArrayList<Partition> getPartitions() {
    ArrayList<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < this.plankReader.getFooter().rowGroupCount; i++) {
      partitions.add(new PlankFilePartition(i, i, this.fileLoc));
    }
    return partitions;
  }

  @Override
  public ArrayList<Object> collect() throws Exception {
    ArrayList<Object> out = new ArrayList<>();
    for (Partition p : getPartitions()) {
      Iterator<Object> it = compute(p);
      while (it.hasNext()) {
        out.add(it.next());
      }
    }
    return out;
  }
}
