package org.astar.thunder.rdd;

import org.astar.thunder.dependency.Dependency;
import org.astar.thunder.exceptions.PartitionProcessingException;
import org.astar.thunder.partition.Partition;
import org.astar.thunder.partition.TextFilePartition;

import java.io.FileNotFoundException;
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
  private final byte END_OF_LINE_MARKER = '\n';

  public TextFileScanRDD(int minPartitions, String fileLoc) throws IOException {
    this.minPartitions = minPartitions;
    this.path = Path.of(fileLoc);
    this.fileSizeBytes = Files.size(this.path);
  }

  @Override
  public Iterator<String> compute(Partition p) throws FileNotFoundException, IOException {

    TextFilePartition filePartition = (TextFilePartition) p;

    RandomAccessFile file = new RandomAccessFile(filePartition.getFileName(), "r");
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

    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        try {
          if (file.getFilePointer() > filePartition.getEnd()) {
            file.close();
            return false;
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return true;
      }

      @Override
      public String next() {
        try {
          return file.readLine();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
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
  public ArrayList<String> collect() throws PartitionProcessingException, IOException {
    ArrayList<String> out = new ArrayList<>();
    for (Partition p : getPartitions()) {
      Iterator<String> it = compute(p);
      while(it.hasNext()) {
        out.add(it.next());
      }
    }
    return out;
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
