package org.astar.thunder.example;

import io.plank.PlankReader;
import org.astar.thunder.core.ThunderContext;
import org.astar.thunder.core.ThunderSession;
import org.astar.thunder.rdd.RDD;

import java.util.function.Function;

public class NarrowOperations {

  static class Person {
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
  public static void main(String[] args) throws Exception {
    ThunderSession thunderSession = ThunderSession.builder().build();

    ThunderContext context = thunderSession.context;
    RDD<String> rdd =  context.textFile("./data/sample.csv", 2);

    Function<String, Person> typeMapper = s -> {
      String[] splits = s.split(",");
      return new Person(Integer.parseInt(splits[0]), splits[1], Integer.parseInt(splits[2]));
    };

    Function<Person, Boolean> ageFilter = person -> person.getAge() <= 25;

    RDD<Person> mrdd =  rdd
      .map(typeMapper);

    Person youngest = mrdd.reduce((person1, person2) -> new Person(Math.max(person1.getId(), person2.getId()), person1.getName() + "|" + person2.getName(), person1.getAge() + person2.getAge()));

    System.out.println(youngest);
    context.shutDownThunderContext();

    // plank file format

//    PlankReader reader = new PlankReader("/Users/upen/Desktop/Codes/thunder/data/addresses.plank");
//
//    RDD<Object> plankRdd = new PlankFileScanRDD(reader);

//    RDD<Person> mappedRdd = plankRdd.map(o -> {
//      ArrayList<Object> row = (ArrayList<Object>) o;
//      return new Person((int)row.get(0), (String)row.get(1), 20);
//    })
//      .filter(p -> !Objects.equals(p.getName(), "Bob"));

//    Person reducedPerson = plankRdd.map(o -> {
//        ArrayList<Object> row = (ArrayList<Object>) o;
//        return new Person((int)row.get(0), (String)row.get(1), 20);
//      })
//      .filter(p -> !Objects.equals(p.getName(), "Bob"))
//      .reduce((person1, person2) -> person1.getAge() <= person2.getAge() ? person1 : person2);
//
//    System.out.println("youngest person is " + reducedPerson);
//
//
//    ThunderSession thunder = ThunderSession.builder().build();
  }
}