package org.astar.thunder.rdd;

import org.astar.thunder.dependency.Dependency;
import org.astar.thunder.exceptions.PartitionProcessingException;
import org.astar.thunder.partition.Partition;
import org.astar.thunder.partition.TextFilePartition;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;

public class TextFileScanRDD extends RDD<String> {
  private int minPartitions = 1;
  private final Path path;
  private final long fileSizeBytes;
  private final long ALLOWED_MAX_BYTES_PER_PARTITION = 4 << 10; // 4kb

  private ArrayList<String> out;
  private final byte END_OF_LINE_MARKER = '\n';

  public TextFileScanRDD(int minPartitions, String fileLoc) throws IOException {
    this.minPartitions = minPartitions;
    this.path = Path.of(fileLoc);
    this.fileSizeBytes = Files.size(this.path);
    this.out = new ArrayList<>(this.minPartitions);
  }

  @Override
  public Iterator<String> compute(Partition p) throws PartitionProcessingException {
    this.out.clear();
    TextFilePartition filePartition = (TextFilePartition) p;
    System.out.println("Opening file partition " + filePartition);

    try(RandomAccessFile file = new RandomAccessFile(filePartition.getFileName(), "r")) {
      if (0 != filePartition.getStart()) {
        file.seek(filePartition.getStart() - 1);
      }

      if (END_OF_LINE_MARKER != file.readByte() && 0 != filePartition.getStart()) {
        // this offset starts in the middle of somewhere, SKIP
        file.readLine();
      }
      else {
        // good start
        file.seek(filePartition.getStart());
      }

      while (true) {
        if (file.getFilePointer() > filePartition.getEnd()) break;
        String line = file.readLine();
        if (line == null) break;
        out.add(line);
      }
    }
    catch (Exception e) {
      throw new PartitionProcessingException(
        String.format("error processing partition %s", filePartition.getIndex()) + e.getMessage(),
        e
      );
    }
    return out.iterator();
  }

  @Override
  public ArrayList<Dependency> dependencies() {
    return new ArrayList<>();
  }

  @Override
  public ArrayList<Partition> getPartitions() {
    ArrayList<Partition> partitions = new ArrayList<>();
    long totalPartitions = Math.max(this.minPartitions, this.fileSizeBytes / ALLOWED_MAX_BYTES_PER_PARTITION);
    long allowedBytesPerPartition = this.fileSizeBytes / totalPartitions;
    for (int i = 0; i < totalPartitions; i++) {
      long startOffset = i * allowedBytesPerPartition;
      long endOffset = (i == totalPartitions - 1 ? this.fileSizeBytes : startOffset + allowedBytesPerPartition) - 1;
      partitions.add(new TextFilePartition(
        i, startOffset, endOffset, this.path.toString())
      );
    }
    return partitions;
  }

  @Override
  public ArrayList<String> collect() throws PartitionProcessingException {
    // for now just iterate through all the partitions and call compute()
    for (Partition p : getPartitions()) {
      compute(p);
    }
    return this.out;
  }

  public long getFileSizeBytes() {
    return this.fileSizeBytes;
  }

  @Override
  public String getRDDName() {
    return String.format(
      "TextFileScanRDD of file %s",
      this.path
    );
  }
}
