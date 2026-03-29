package org.astar.thunder.partition;

public class PlankFilePartition implements Partition {
  private final int index;
  private final int rowGroupId;
  private final String fileName;
  public PlankFilePartition(int index, int rowGroupId, String fileName) {
    this.index = index;
    this.rowGroupId = rowGroupId;
    this.fileName = fileName;
  }

  @Override
  public int getIndex() {
    return 0;
  }

  public int getRowGroupId() {
    return this.rowGroupId;
  }

  public String getFileName() {
    return this.fileName;
  }

  @Override
  public String toString() {
    return String.format("[Index: %s, RowGroupId: %s, FileName: %s]",
      this.index,
      this.rowGroupId,
      this.fileName
    );
  }
}
