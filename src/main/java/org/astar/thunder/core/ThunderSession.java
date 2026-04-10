package org.astar.thunder.core;

import org.astar.thunder.exceptions.ThunderSessionException;

public class ThunderSession {
  private static ThunderSession INSTANCE;
  public ThunderContext context;
  ThunderSession() {
    this.context = new ThunderContext();
  }

  public static ThunderSessionBuilder builder() {
    return new ThunderSessionBuilder();
  }

  public ThunderSession getActiveSession()  {
    if (INSTANCE == null) {
      throw new ThunderSessionException.NoActiveSessionException();
    }
    return INSTANCE;
  }

  public static class ThunderSessionBuilder {
    public ThunderSession build() {
      INSTANCE = new ThunderSession();
      return INSTANCE;
    }
  }
}
