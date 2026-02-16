package org.astar.thunder.example;

import org.astar.thunder.rdd.RDD;
import org.astar.thunder.rdd.TextFileScanRDD;
import org.astar.thunder.scheduler.JobScheduler;
import org.astar.thunder.scheduler.Stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Function;

public class NarrowOperations {
  public static void main(String[] args) throws IOException {
    RDD<String> rdd = new TextFileScanRDD(2, "./data/sample.csv");

    Function<String, Person> typeMapper = s -> {
      String[] splits = s.split(",");
      return new Person(Integer.parseInt(splits[0]), splits[1], Integer.parseInt(splits[2]));
    };

    Function<Person, Person> ageFilter = person -> {
      if (person.getAge() > 25) {
        return new Person(person.getId(), person.getName(), person.getAge());
      }
      else {
        return null;
      }
    };

    RDD<Person> mrdd =  rdd
      .map(typeMapper)
      .filter(ageFilter);


    ArrayList<Stage> stages = new JobScheduler().createStages(mrdd);
    System.out.println(stages);
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
