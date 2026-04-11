package org.astar.thunder.core;

import akka.actor.typed.ActorSystem;
import io.plank.PlankReader;
import org.astar.thunder.actors.MasterActor;
import org.astar.thunder.actors.Shutdown;
import org.astar.thunder.actors.ThunderMessage;
import org.astar.thunder.rdd.PlankFileScanRDD;
import org.astar.thunder.rdd.RDD;
import org.astar.thunder.rdd.TextFileScanRDD;
import org.astar.thunder.scheduler.JobScheduler;
import org.astar.thunder.utils.ResultHandler;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

public class ThunderContext {
  private static final String THUNDER_PARALLELISM_KEY = "thunder.parallelism";
  private static final String NUM_WORKERS = "num.executors";
  private static HashMap<String, String> properties = new HashMap<>();

  static {
    properties.put(THUNDER_PARALLELISM_KEY, "2");
    properties.put(NUM_WORKERS, "2");
  }

  private static final ThunderContext INSTANCE = new ThunderContext();

  public ThunderContext() {
    int numOfWorkers = Integer.parseInt(properties.get(NUM_WORKERS));
    this.actorSystem = ActorSystem.create(MasterActor.create(numOfWorkers), "thunder");
    this.jobScheduler = new JobScheduler(this.actorSystem);
  }

  public void shutDownThunderContext() {
    this.actorSystem.terminate();
  }

  public static ThunderContext getInstance() {
    return INSTANCE;
  }

  public TextFileScanRDD textFile(String filePath) throws IOException {
    return textFile(filePath, Integer.parseInt(properties.get(THUNDER_PARALLELISM_KEY)));
  }

  public TextFileScanRDD textFile(String filePath, int minPartitions) throws IOException {
    return new TextFileScanRDD(this, minPartitions, filePath);
  }

  public PlankFileScanRDD plankFile(PlankReader reader) {
    return new PlankFileScanRDD(this, reader);
  }

  public <T> void submitJob(RDD<T> rdd) {
    this.jobScheduler.submitJob(rdd);
  }

  public <T> void submitJob(RDD<T> rdd, Function<Iterator<T>, Optional<T>> partitionFunc) {

  }

  public <T> void submitJob(RDD<T> rdd, Function<Iterator<T>, Optional<T>> partitionFunc, ResultHandler resultHandler) {
    this.jobScheduler.submitJob(rdd, partitionFunc, resultHandler);
  }

  // Data Structures
  private final JobScheduler jobScheduler;
  public final ActorSystem<ThunderMessage> actorSystem;
}
