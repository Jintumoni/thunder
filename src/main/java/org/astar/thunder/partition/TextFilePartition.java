package org.astar.thunder.partition;

public class TextFilePartition implements Partition {
  private final int index;
  private final long startOffset;
  private final long endOffset;
  private final String fileName;
  public TextFilePartition(int index, long startOffset, long endOffset, String fileName) {
    this.index = index;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.fileName = fileName;
  }
  @Override
  public int getIndex() {
    return this.index;
  }

  public long getStart() {
    return this.startOffset;
  }

  public long getEnd() {
    return this.endOffset;
  }

  public String getFileName() {
    return this.fileName;
  }

  @Override
  public String toString() {
    return String.format("[Index: %s, StartOffset: %s, EndOffset: %s, FileName: %s]",
      this.index,
      this.startOffset,
      this.endOffset,
      this.fileName
    );
  }
}
