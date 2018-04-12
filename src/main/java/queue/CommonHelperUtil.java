package queue;

import java.util.UUID;

public class CommonHelperUtil {

  public static String getRandomHandle() {
    return UUID.randomUUID().toString();
  }

}
