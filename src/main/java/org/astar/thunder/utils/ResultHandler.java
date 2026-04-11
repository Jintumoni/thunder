package org.astar.thunder.utils;

import java.util.Optional;

public interface ResultHandler<T> {
  // get the result back
  public Optional<T> get();

  // apply function(s) on the result
  public void apply(Optional<T> existing);
}
