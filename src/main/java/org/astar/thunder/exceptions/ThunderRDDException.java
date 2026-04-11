package org.astar.thunder.exceptions;

public class ThunderRDDException {
  public static class EmptyResultException extends RuntimeException {
    public EmptyResultException() {
      super("Empty records in RDD");
    }
  }
}
