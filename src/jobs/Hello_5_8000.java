package jobs;

import common.*;

//Hello World
public class Hello_5_8000 extends Job {

  @Override
  public void config() {
    setNumTasks(32); //set the number of tasks
  }

  @Override
  public void task(int tId) {
    System.out.println("task"+tId+": Hello World"); //this string will be printed out from worker instead of client
    try{
      Thread.sleep(2500);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
}
