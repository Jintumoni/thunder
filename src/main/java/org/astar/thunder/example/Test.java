package org.astar.thunder.example;

import io.plank.PlankReader;
import io.plank.RecordBatch;

public class Test {
  public static void main(String[] args) {
    PlankReader reader = new io.plank.PlankReader("/Users/upen/Desktop/Codes/thunder/data/addresses.plank");
    RecordBatch rb = reader.readRowGroupColumns(0, new String[]{"name", "metadata"});
    System.out.println((rb));
    System.out.println(reader.getFooter().columnCount);
  }
}
