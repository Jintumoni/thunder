package org.astar.thunder.utils;

import java.util.Optional;

public class ResultCollector<T> {
  private Optional<T> result;

  public ResultCollector() {
  }

  public ResultCollector(Optional<T> result) {
    this.result = result;
  }

  public void collect(Optional<T> result) {
    this.result = result;
  }

  public Optional<T> getResult() {
    return this.result;
  }
}
