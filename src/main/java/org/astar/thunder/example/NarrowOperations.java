package org.astar.thunder.example;

import io.plank.PlankReader;
import org.astar.thunder.rdd.PlankFileScanRDD;
import org.astar.thunder.rdd.RDD;
import org.astar.thunder.rdd.TextFileScanRDD;
import org.astar.thunder.scheduler.JobScheduler;

import java.util.ArrayList;
import java.util.Objects;
import java.util.function.Function;

public class NarrowOperations {
  public static void main(String[] args) throws Exception {
    RDD<String> rdd = new TextFileScanRDD(2, "./data/sample.csv");

    Function<String, Person> typeMapper = s -> {
      String[] splits = s.split(",");
      return new Person(Integer.parseInt(splits[0]), splits[1], Integer.parseInt(splits[2]));
    };

    Function<Person, Boolean> ageFilter = person -> person.getAge() <= 25;

    RDD<Person> mrdd =  rdd
      .map(typeMapper)
      .filter(ageFilter);

    // plank file format

    PlankReader reader = new PlankReader("/Users/upen/Desktop/Codes/thunder/data/addresses.plank");

    RDD<Object> plankRdd = new PlankFileScanRDD(reader);

    RDD<Person> mappedRdd = plankRdd.map(o -> {
      ArrayList<Object> row = (ArrayList<Object>) o;
      return new Person((int)row.get(0), (String)row.get(1), 20);
    })
      .filter(p -> !Objects.equals(p.getName(), "Bob"));

    JobScheduler sch = new JobScheduler();
    sch.submitJob(mrdd);
  }
}

class Person {
  private int id;
  private String name;
  private int age;

  public Person(int id, String name, int age) {
    this.id = id;
    this.name = name;
    this.age = age;
  }

  public String getName() {
    return name;
  }

  public int getAge() {
    return age;
  }

  public int getId() {
    return id;
  }

  @Override
  public String toString() {
    return String.format(
      "Person(%s, %d, %d)",
      this.name,
      this.id,
      this.age
    );
  }
}
