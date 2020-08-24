package chap5.json_read_write;

import java.io.Serializable;

public class Person implements Serializable {

  public String id;
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
