package org.bonitasoft.truckmilk.schedule;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.tour.MilkCmdControl;

public class MilkScheduleThreadSleep implements MilkSchedulerInt {

  private static Logger logger = Logger.getLogger(MilkScheduleQuartz.class.getName());
  private static String logHeader = "MilkScheduleThreadSleep ~~ ";

  private static List<SchedulerThread> listSchedulerThread = new ArrayList<SchedulerThread>();

  private static BEvent eventSchedulerTreadSetup = new BEvent(MilkScheduleThreadSleep.class.getName(), 1, Level.ERROR,
      "Thread failed", "Check the error", "The different monitoring can't run", "See the error");

  private static BEvent eventSchedulerStoppedInProgress = new BEvent(MilkScheduleThreadSleep.class.getName(), 2,
      Level.INFO,
      "Stop in progress", "Scheduler is in progress to stop");

  private static BEvent eventSchedulerAlreadyStarted = new BEvent(MilkScheduleThreadSleep.class.getName(), 3,
      Level.INFO,
      "Already started", "Scheduler is already started");

  private static BEvent eventSchedulerStarted = new BEvent(MilkScheduleThreadSleep.class.getName(), 4, Level.SUCCESS,
      "Scheduler started", "Scheduler is started");

  private static BEvent eventSchedulerStopped = new BEvent(MilkScheduleThreadSleep.class.getName(), 5, Level.SUCCESS,
      "Scheduler stopped", "Scheduler is stopped");

  /* ******************************************************************************** */
  /*                                                                                  */
  /* Abstract for Milk Controler */
  /*                                                                                  */
  /*                                                                                  */
  /* ******************************************************************************** */
  public List<BEvent> check(long tenantId) {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    return listEvents;
  }

  /**
   * true if the scheduler is running (controled by start / stop
   * 
   * @return
   */
  public StatusScheduler getStatus(long tenantId) {
    StatusScheduler statusScheduler = new StatusScheduler();
    SchedulerThread sch = getSchedulerThread(tenantId);
    if (sch == null)
      statusScheduler.status = TypeStatus.STOPPED;
    else
      statusScheduler.status = sch.status;
    return statusScheduler;
  }

  /**
   * start the scheduler
   * 
   * @return
   */
  public List<BEvent> start(long tenantId) {
    ArrayList<BEvent> listEvents = new ArrayList<BEvent>();

    // already register ? 
    synchronized (listSchedulerThread) {
      SchedulerThread sch = getSchedulerThread(tenantId);
      if (sch != null) {
        if (sch.status == TypeStatus.STARTED)
          listEvents.add(eventSchedulerAlreadyStarted);
        else {
          sch.status = TypeStatus.STARTED;
          listEvents.add(eventSchedulerStarted);
        }
      } else {
        sch = new SchedulerThread(tenantId);
        listSchedulerThread.add(sch);
        sch.start();
        listEvents.add(eventSchedulerStarted);
      }
    }
    return listEvents;

  }

  /**
   * stop it
   * 
   * @return
   */
  public List<BEvent> stop(long tenantId) {
    ArrayList<BEvent> listEvents = new ArrayList<BEvent>();

    // already register ? 
    synchronized (listSchedulerThread) {
      SchedulerThread sch = getSchedulerThread(tenantId);
      if (sch != null) {
        if (sch.status == TypeStatus.STARTED) {
          sch.shutdown();
          listEvents.add(eventSchedulerStoppedInProgress);
        } else {
          listEvents.add(eventSchedulerStopped);
        }
      } else {
        listEvents.add(eventSchedulerStopped);
      }
    }
    return listEvents;

  }

  /**
   * return the scheduler type
   */

  public TypeScheduler getType() {
    return TypeScheduler.THREADSLEEP;
  }

  /**
   * info to give to the administrator
   * 
   * @return
   */
  public String getDescription() {
    return "";
  };

  /**
   * event to give to the admninistrator
   * 
   * @return
   */
  public List<BEvent> getEvents(long tenantId) {
    return new ArrayList<BEvent>();
  }

  /* ******************************************************************************** */
  /*                                                                                  */
  /* Abstract for Milk Controler */
  /*                                                                                  */
  /*                                                                                  */
  /* ******************************************************************************** */

  public List<BEvent> startup(long tenantId, boolean forceReset) {
    List<BEvent> listEvents = new ArrayList<BEvent>();

    try {

      // already register ? 
      synchronized (listSchedulerThread) {
        SchedulerThread sch = getSchedulerThread(tenantId);
        if (sch != null) {
          if (forceReset) {
            if (sch.status == TypeStatus.STARTED)
              sch.shutdown(); // shutdown to restart it
            listSchedulerThread.remove(sch);
          } else
            return listEvents; // already registered
        }

        // Create a new thread in any case : a thread can be start only one time
        sch = new SchedulerThread(tenantId);
        listSchedulerThread.add(sch);
        sch.start();
      } // end synchronized

    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      String exceptionDetails = sw.toString();
      logger.severe(logHeader + " ERROR startup scheduler " + e + " at " + exceptionDetails);
      listEvents.add(new BEvent(eventSchedulerTreadSetup, e, ""));
    }
    return listEvents;

  }

  public List<BEvent> shutdown(long tenantId) {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    SchedulerThread sch = getSchedulerThread(tenantId);
    if (sch != null)
      sch.shutdown();
    listSchedulerThread.remove(sch);
    return listEvents;

  }

  /* ******************************************************************************** */
  /*                                                                                  */
  /* Schedule Maintenance operation */
  /*                                                                                  */
  /*                                                                                  */
  /* ******************************************************************************** */
  /**
   * @Override
   */
  public List<BEvent> checkAndDeploy(boolean forceDeploy, File pageDirectory, long tenantId) {
    // nothing to do here
    return new ArrayList<BEvent>();
  }

  /**
   * @Override
   */
  public List<BEvent> reset(long tenantId) {
    return startup(tenantId, true);
  }

  public Date getDateNextHeartBeat(long tenantId) {
    SchedulerThread sch = getSchedulerThread(tenantId);
    return sch == null ? null : sch.nextHeartBeat;
  }

  /* ******************************************************************************** */
  /*                                                                                  */
  /* Initialize : create the job */
  /*                                                                                  */
  /*                                                                                  */
  /* ******************************************************************************** */

  private SchedulerThread getSchedulerThread(long tenantId) {
    for (SchedulerThread sch : listSchedulerThread) {
      if (sch.getTenantId() == tenantId) {
        return sch;
      }
    }
    return null;
  }

  public static class SchedulerThread extends Thread {

    public long tenantId;
    public TypeStatus status;
    public Date nextHeartBeat = null;
    public int countToDebug = 0;

    SchedulerThread(long tenantId) {
      this.tenantId = tenantId;
      status = TypeStatus.STARTED;
    }

    public long getTenantId() {
      return tenantId;
    };

    public void run() {
      logger.info(logHeader + " SchedulerThread [" + tenantId + "] started");
      status = TypeStatus.STARTED;

      countToDebug = 0;
      /* Stop after 1 month */
      while (status == TypeStatus.STARTED && countToDebug < 259200) {
        countToDebug++;
        try {
          logger.info(logHeader + " SchedulerThread [" + tenantId + "] weakup (" + countToDebug + "/100)");
          MilkCmdControl milkCmdControl = MilkCmdControl.getStaticInstance();
          milkCmdControl.executeOneTime(tenantId);
          nextHeartBeat = new Date(System.currentTimeMillis() + 60000);
          Thread.sleep(60000);
        } catch (InterruptedException e) {

        }
      }
      nextHeartBeat = null;
      status = TypeStatus.STOPPED;
      logger.info(logHeader + " SchedulerThread [\"+tenantId+\"] stopped");

    }

    public void shutdown() {
      if (status == TypeStatus.STARTED)
        status = TypeStatus.SHUTDOWN;
    }
  }

}
