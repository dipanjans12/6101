package jobs;

import java.text.SimpleDateFormat;
import common.*;

//Hello World
public class Hello_2_100 extends Job {
  static final SimpleDateFormat _sdf = new SimpleDateFormat("HH:mm:ss.SSS");

  @Override
  public void config() {
    setNumTasks(8); //set the number of tasks
  }

  @Override
  public void task(int tId) {
    //System.out.println(_sdf.format(System.currentTimeMillis()) +
    //    " task"+tId+": Hello_2_1000"); //this string will be printed out from worker instead of client
    try{
      Thread.sleep(10000);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
}
