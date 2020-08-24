package chap9.context_example;

import java.io.Serializable;

public class Person implements Serializable {

  public String id;

  public Person() {
  }

  public Person(String id, String name) {
    this.id = id;
    this.name = name;
  }

  public String name;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
