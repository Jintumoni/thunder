package org.astar.thunder.exceptions;

public class ThunderSessionException {
  public static class NoActiveSessionException extends RuntimeException {
    public NoActiveSessionException() {
      super("No active Thunder session");
    }
  }

  public static class RDDLacksThunderContext extends RuntimeException {
    public RDDLacksThunderContext() {
      super("RDD doesn't have an active Thunder session");
    }
  }
}
