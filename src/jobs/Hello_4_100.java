package jobs;

import java.text.SimpleDateFormat;
import common.*;

//Hello World
public class Hello_4_100 extends Job {
  static final SimpleDateFormat _sdf = new SimpleDateFormat("HH:mm:ss.SSS");

  @Override
  public void config() {
    setNumTasks(16); //set the number of tasks
  }

  @Override
  public void task(int tId) {
    //System.out.println(_sdf.format(System.currentTimeMillis()) +
    //    " task"+tId+": Hello_4_1000"); //this string will be printed out from worker instead of client
    try{
      Thread.sleep(5000);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
}
