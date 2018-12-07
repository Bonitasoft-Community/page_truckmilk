package org.bonitasoft.truckmilk.tour;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.command.BonitaCommand;
import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.command.SCommandExecutionException;
import org.bonitasoft.engine.command.SCommandParameterizationException;
import org.bonitasoft.engine.connector.ConnectorAPIAccessorImpl;
import org.bonitasoft.engine.service.TenantServiceAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.plugin.MilkDeleteCases;
import org.bonitasoft.truckmilk.plugin.MilkEmailUsersTasks;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.ExecutionStatus;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugTourInput;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugTourOutput;
import org.bonitasoft.truckmilk.plugin.MilkPurgeArchive;
import org.bonitasoft.truckmilk.plugin.MilkReplayFailedTask;
import org.bonitasoft.truckmilk.plugin.MilkSLA;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerFactory;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.StatusScheduler;

/* ******************************************************************************** */
/*                                                                                  */
/* Command Control */
/*                                                                                  */
/* this class is the main control for the Milk. This is a COMMAND side */
/*
 * The object register all the PLUG IN and call them.
 * Plug in management
 * There are 2 type of plug in : embeded or not.
 * - An embeded plug in means it's implementation are inside this library.
 * - Else, the plug in is in a different JAR file. CmdControl deploy this external
 * file as a command.
 * A "plugTour" is a job to be execute at a certain frequency, with certain parameters.
 * a PlugTour is an instance of a PlugIn.
 * Example: plugIn 'monitorEmail'
 * - plugTour "HumanRessourceEmail" / Every day / Monitor 'hr@bonitasoft.com'
 * - plugTour "SaleEmail" / Every hour / Monitor 'sale@bonitasoft.com'
 * Each plugTour manage their own parameters, and own schedule. CmdControl can update
 * the parameters and modify the schedule. Cmdmanagement retrieve the list of plug in and list of
 * plugTour
 * Companion : MilkCmdControlAPI
 * To ensure the object is live every time, it is deployed as a command.
 * External API MilkCmdControlAPI is used to communicate with the engine (this
 * object communicate via the CommandAPI to this object)
 * The companion call the method execute().
 * Companion: MilkQuartzJob
 * this object communicate with the Quartz scheduler to register a job to be call
 * every x minute. Then the companion call the method "timer()" at each intervalle
 */

public class MilkCmdControl extends BonitaCommand {

  static Logger logger = Logger.getLogger(MilkCmdControlAPI.class.getName());

  static String logHeader = "TruckMilkCommand ~~~";

  private static BEvent eventInternalError = new BEvent(MilkCmdControl.class.getName(), 1, Level.ERROR,
      "Internal error", "Internal error, check the log");
  private static BEvent eventTourRemoved = new BEvent(MilkCmdControl.class.getName(), 2, Level.SUCCESS,
      "Tour removed", "Tour is removed with success");

  private static BEvent eventTourStarted = new BEvent(MilkCmdControl.class.getName(), 4, Level.SUCCESS,
      "Tour started", "The Tour is now started");
  private static BEvent eventTourStopped = new BEvent(MilkCmdControl.class.getName(), 5, Level.SUCCESS,
      "Tour stopped", "The Tour is now stopped");

  private static BEvent EVENT_TOUR_UPDATED = new BEvent(MilkCmdControl.class.getName(), 6, Level.SUCCESS,
      "Tour updated", "The Tour is now updated");

  private static BEvent eventTourRegister = new BEvent(MilkCmdControl.class.getName(), 8, Level.SUCCESS,
      "Tour registered", "The Tour is now registered");

  private static BEvent eventPlugInViolation = new BEvent(MilkCmdControl.class.getName(), 9, Level.ERROR,
      "Plug in violation",
      "A plug in must return a status on each execution. The plug in does not respect the contract",
      "No report is saved", "Contact the plug in creator");

  private static BEvent eventPlugInError = new BEvent(MilkCmdControl.class.getName(), 10, Level.ERROR,
      "Plug in error", "A plug in throw an error", "No report is saved", "Contact the plug in creator");

  private static BEvent eventSchedulerResetSuccess = new BEvent(MilkCmdControl.class.getName(), 13, Level.SUCCESS,
      "Schedule reset", "The schedule is reset with success");

  private static BEvent EVENT_MISSING_ID = new BEvent(MilkCmdControl.class.getName(), 14, Level.ERROR,
      "ID is missing", "The Tour ID is missing", "Operation can't be realised", "Contact your administrator");

  private static BEvent EVENT_BUTTONARG_FAILED = new BEvent(MilkCmdControl.class.getName(), 15, Level.ERROR,
      "No Answer", "The Button Arg does not return an anwser", "Operation can't be realised", "Contact your administrator");
  
  /* ******************************************************************************** */
  /*                                                                                  */
  /* the companion MilkCmdControlAPI call this API */
  /*                                                                                  */
  /* ******************************************************************************** */

  /**
   * this constant is defined too in MilkQuartzJob to have an independent JAR
   */
  public static String cstCommandName = "truckmilk";
  public static String cstCommandDescription = "Execute TruckMilk plugin at frequency";

  /**
   * this enum is defined too in MilkQuartzJob to have an independent JAR
   */
  public enum VERBE {
    CHECKENVIRONMENT, INITALINFORMATION, REFRESH, DEPLOYPLUGIN, DELETEPLUGIN, ADDTOUR, REMOVETOUR, STOPTOUR, STARTTOUR, UPDATETOUR, IMMEDIATETOUR, SCHEDULERSTARTSTOP, SCHEDULERDEPLOY, SCHEDULERRESET, SCHEDULERSTATUS, SCHEDULERCHANGE, TESTBUTTONARGS, HEARTBEAT
  };

  public static String cstPageDirectory = "pagedirectory";
  // private static String cstResultTimeInMs = "TIMEINMS";

  private static String cstResultListPlugTour = "listplugtour";
  public static String cstResultListEvents = "listevents";

  public static String cstJsonSchedulerEvents = "schedulerlistevents";

  public static String cstJsonSchedulerStatus = "status";
  public static String cstJsonSchedulerStatus_V_RUNNING = "RUNNING";
  public static String cstJsonSchedulerStatus_V_SHUTDOWN = "SHUTDOWN";
  public static String cstJsonSchedulerStatus_V_STOPPED = "STOPPED";

  public static String cstSchedulerChangeType = "schedulerchangetype";

  
  public static String cstEnvironmentStatus = "ENVIRONMENTSTATUS";
  public static String cstEnvironmentStatus_V_CORRECT = "OK";
  public static String cstEnvironmentStatus_V_ERROR = "ERROR";
  
  public static String cstButtonName = "buttonName";

  List<MilkPlugIn> listPlugIns = new ArrayList<MilkPlugIn>();

  /**
   * keep the scheduler Factory
   */

  private static MilkSchedulerFactory milkSchedulerFactory = MilkSchedulerFactory.getInstance();

  public static MilkCmdControl milkCmdControl = new MilkCmdControl();

  // let's return a singleton
  public BonitaCommand getInstance() {
    return milkCmdControl;
  }

  public static MilkCmdControl getStaticInstance() {
    return milkCmdControl;
  }

  /**
   * Singleton object. All privates members are safed
   * 
   * @param parameters
   * @param serviceAccessor
   * @return
   * @throws SCommandParameterizationException
   * @throws SCommandExecutionException
   */
  public ExecuteAnswer executeCommandVerbe(String verbSt, Map<String, Serializable> parameters, long tenantId, TenantServiceAccessor serviceAccessor) {

    long currentTime = System.currentTimeMillis();
    ExecuteAnswer executeAnswer = new ExecuteAnswer();
    long startTime = System.currentTimeMillis();
    VERBE verbEnum = null;
    try {
      // ------------------- ping ?
      verbEnum = VERBE.valueOf(verbSt);
      logger.fine(logHeader + "MilkTourCommand Verb[" + verbEnum.toString() + "] Tenant[" + tenantId + "]");

      // initialise the factory ?
      MilkPlugInTourFactory milkPlugInTourFactory = MilkPlugInTourFactory.getInstance();

      if (VERBE.CHECKENVIRONMENT.equals(verbEnum)) {
        executeAnswer.listEvents.addAll(milkSchedulerFactory.checkEnvironment(tenantId));
        List<MilkPlugIn> list = detectListPlugIn(tenantId);
        for (MilkPlugIn plugIn : list) {
          executeAnswer.listEvents.addAll(plugIn.checkEnvironment(tenantId));
        }
        executeAnswer.listEvents.addAll(milkPlugInTourFactory.checkEnvironment(tenantId));
        
        executeAnswer.listEvents = BEventFactory.filterUnique(executeAnswer.listEvents );
        
        if (BEventFactory.isError( executeAnswer.listEvents))
          executeAnswer.result.put(cstEnvironmentStatus,  cstEnvironmentStatus_V_ERROR);
        else
          executeAnswer.result.put(cstEnvironmentStatus,  cstEnvironmentStatus_V_CORRECT);

        return executeAnswer;

      }

      if (milkSchedulerFactory.getScheduler() == null) {
        executeAnswer.listEvents.addAll(milkSchedulerFactory.startup(tenantId));
      }

      if (VERBE.INITALINFORMATION.equals(verbEnum) || VERBE.REFRESH.equals(verbEnum)) {

        if (VERBE.INITALINFORMATION.equals(verbEnum))
          executeAnswer.listEvents.addAll(initialization(true, false, tenantId));
        else
          // refresh ? Reload the tours
          executeAnswer.listEvents.addAll(initialization(true, false, tenantId));
        // get all lists 

        executeAnswer.result.put(cstResultListPlugTour, getListTourMap());

        List<Map<String, Object>> listPlugInMap = new ArrayList<Map<String, Object>>();
        for (MilkPlugIn plugin : getListPlugIn()) {
          listPlugInMap.add(plugin.getMap());
        }
        executeAnswer.result.put("listplugin", listPlugInMap);

        Map<String, Object> mapScheduler = new HashMap<String, Object>();
        if (milkSchedulerFactory.getScheduler() == null)
          mapScheduler.put(cstJsonSchedulerStatus, cstJsonSchedulerStatus_V_STOPPED);
        else {
          StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(tenantId);
          mapScheduler.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
          mapScheduler.put(cstJsonSchedulerEvents, BEventFactory.getHtml(statusScheduler.listEvents));
          mapScheduler.put("type", milkSchedulerFactory.getScheduler().getType().toString());
          mapScheduler.put("info", milkSchedulerFactory.getScheduler().getDescription());
        }

        mapScheduler.put("listtypeschedulers", milkSchedulerFactory.getListTypeScheduler());

        executeAnswer.result.put("scheduler", mapScheduler);

      } else if (VERBE.ADDTOUR.equals(verbEnum)) {

        MilkPlugIn plugIn = getPluginFromName((String) parameters.get("plugin"));
        if (plugIn == null)
          return null;
        MilkPlugInTour plugInTour = MilkPlugInTour.getInstanceFromPlugin((String) parameters.get("name"),
            plugIn);
        executeAnswer.listEvents.addAll(registerATour(plugInTour));
        if (!BEventFactory.isError(executeAnswer.listEvents)) {
          executeAnswer.listEvents.addAll(milkPlugInTourFactory.dbSavePlugInTour(plugInTour, tenantId));
          if (!BEventFactory.isError(executeAnswer.listEvents))
            executeAnswer.listEvents.add(new BEvent(eventTourRegister, "Tour registerd[" + plugInTour.getName() + "]"));
        }
        // get all lists            
        executeAnswer.result.put(cstResultListPlugTour, getListTourMap());

      } else if (VERBE.REMOVETOUR.equals(verbEnum)) {

        Long idTour = (Long) parameters.get("id");
        if (idTour == null) {
          executeAnswer.listEvents.add(EVENT_MISSING_ID);
        } else {
          MilkPlugInTour plugInTour = getTourById(idTour);
          executeAnswer.listEvents.addAll(removeTour(idTour, tenantId));

          if (!BEventFactory.isError(executeAnswer.listEvents)) {
            executeAnswer.listEvents.add(new BEvent(eventTourRemoved, "Tour removed[" + plugInTour.getName() + "]"));
          }
        }
        executeAnswer.result.put(cstResultListPlugTour, getListTourMap());
      } else if (VERBE.STARTTOUR.equals(verbEnum) || VERBE.STOPTOUR.equals(verbEnum)) {

        Long idTour = (Long) parameters.get("id");
        if (idTour == null) {
          executeAnswer.listEvents.add(EVENT_MISSING_ID);
        } else {
          MilkPlugInTour plugInTour = getTourById(idTour);

          if (plugInTour != null) {
            // save parameteres
            @SuppressWarnings("unchecked")
            Map<String, Object> parametersObject = (Map<String, Object>) parameters.get("parametersvalue");
            if (parametersObject != null)
              plugInTour.setTourParameters(parametersObject);
            String cronSt = (String) parameters.get("cron");
            if (cronSt != null)
              plugInTour.setCron(cronSt);

            plugInTour.setEnable(VERBE.STARTTOUR.equals(verbEnum));
            executeAnswer.listEvents.addAll(milkPlugInTourFactory.dbSavePlugInTour(plugInTour, tenantId));
            if (VERBE.STARTTOUR.equals(verbEnum))
              executeAnswer.listEvents.add(new BEvent(eventTourStarted, "Tour Activated[" + plugInTour.getName() + "]"));
            else
              executeAnswer.listEvents.add(new BEvent(eventTourStopped, "Tour Deactived[" + plugInTour.getName() + "]"));

            executeAnswer.result.put("enable", plugInTour.isEnable);
          } else
            executeAnswer.listEvents.add(new BEvent(MilkPlugInTourFactory.EVENT_TOUR_NOT_FOUND, "TourID[" + idTour + "]"));
        }
      }

      else if (VERBE.UPDATETOUR.equals(verbEnum)) {

        Long idTour = (Long) parameters.get("id");
        if (idTour == null) {
          executeAnswer.listEvents.add(EVENT_MISSING_ID);
        } else {
          MilkPlugInTour plugInTour = getTourById(idTour);

          if (plugInTour != null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> parametersObject = (Map<String, Object>) parameters.get("parametersvalue");
            String cronSt = (String) parameters.get("cron");
            plugInTour.setTourParameters(parametersObject);
            plugInTour.setCron(cronSt);
            plugInTour.setDescription((String) parameters.get("description"));
            String newName = (String) parameters.get("newname");
            plugInTour.setName(newName);
            executeAnswer.listEvents.addAll(milkPlugInTourFactory.dbSavePlugInTour(plugInTour, tenantId));
            executeAnswer.listEvents.add(new BEvent(EVENT_TOUR_UPDATED, "Tour updated[" + plugInTour.getName() + "]"));

          } else
            executeAnswer.listEvents.add(new BEvent(MilkPlugInTourFactory.EVENT_TOUR_NOT_FOUND, "Tour[" + idTour + "]"));
          executeAnswer.result.put(cstResultListPlugTour, getListTourMap());
        }
      } else if (VERBE.IMMEDIATETOUR.equals(verbEnum)) {
        Long idTour = (Long) parameters.get("id");
        if (idTour == null) {
          executeAnswer.listEvents.add(EVENT_MISSING_ID);
        } else {
          MilkPlugInTour plugInTour = getTourById(idTour);

          if (plugInTour != null) {
            plugInTour.setImmediateExecution(true);
            executeAnswer.listEvents.addAll(milkPlugInTourFactory.dbSavePlugInTour(plugInTour, tenantId));
            executeAnswer.listEvents.add(new BEvent(EVENT_TOUR_UPDATED, "Tour updated[" + plugInTour.getName() + "]"));
          } else
            executeAnswer.listEvents.add(new BEvent(MilkPlugInTourFactory.EVENT_TOUR_NOT_FOUND, "Tour[" + idTour + "]"));

          executeAnswer.result.put(cstResultListPlugTour, getListTourMap());
        }
      } else if (VERBE.TESTBUTTONARGS.equals(verbEnum)) {
        Long idTour = (Long) parameters.get("id");
        if (idTour == null) {
          executeAnswer.listEvents.add(EVENT_MISSING_ID);
        } else {
          MilkPlugInTour plugInTour = getTourById(idTour);

          if (plugInTour != null) {
            String buttonName = (String) parameters.get(cstButtonName);
            Map<String, Object> parametersObject = (Map<String, Object>) parameters.get("parametersvalue");
            plugInTour.setTourParameters(parametersObject);

            
            Map<String, Object> argsParameters= (Map<String, Object>) parameters.get("args");

            // execute it!
            PlugTourInput plugTourInput = new PlugTourInput(plugInTour);

            
            MySimpleTestThread buttonThread = new MySimpleTestThread(tenantId, buttonName, plugInTour, plugTourInput, argsParameters);
            
            buttonThread.start();
            int count=0;
            while( ! buttonThread.isFinish && count<1000)
            {
              count++;
              Thread.sleep(1000);
            }
            if (buttonThread.isFinish)
              executeAnswer.listEvents.addAll( buttonThread.listEvents );
            else
              executeAnswer.listEvents.add( new BEvent(EVENT_BUTTONARG_FAILED, "No answer"));
            
          }
      }

      } else if (VERBE.HEARTBEAT.equals(verbEnum)) {
        executeOneTimeNewThread(tenantId);
      }

      else if (VERBE.SCHEDULERSTARTSTOP.equals(verbEnum)) {
        boolean startScheduler = Boolean.valueOf(parameters.get("start").toString());
        if (milkSchedulerFactory.getScheduler() != null) {
          if (startScheduler)
            executeAnswer.listEvents.addAll(milkSchedulerFactory.getScheduler().start(tenantId));
          else
            executeAnswer.listEvents.addAll(milkSchedulerFactory.getScheduler().stop(tenantId));
        }
        StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(tenantId);
        executeAnswer.result.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
        executeAnswer.listEvents.addAll(statusScheduler.listEvents);
      } else if (VERBE.SCHEDULERDEPLOY.equals(verbEnum)) {
        String pageDirectory = (String) parameters.get(cstPageDirectory);
        File pageDirectoryFile = new File(pageDirectory);
        // now ask the deployment
        MilkSchedulerInt scheduler = milkSchedulerFactory.getScheduler();
        executeAnswer.listEvents.addAll(scheduler.checkAndDeploy(true, pageDirectoryFile, tenantId));
        // then reset it
        executeAnswer.listEvents.addAll(milkSchedulerFactory.getScheduler().reset(tenantId));
        if (!BEventFactory.isError(executeAnswer.listEvents)) {
          executeAnswer.listEvents.add(eventSchedulerResetSuccess);
        }
        // return the status
        StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(tenantId);
        executeAnswer.result.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
        executeAnswer.listEvents.addAll(statusScheduler.listEvents);
      } else if (VERBE.SCHEDULERRESET.equals(verbEnum)) {
        executeAnswer.listEvents.addAll(milkSchedulerFactory.getScheduler().reset(tenantId));
        if (!BEventFactory.isError(executeAnswer.listEvents)) {
          executeAnswer.listEvents.add(eventSchedulerResetSuccess);
        }
        StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(tenantId);
        executeAnswer.result.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
        executeAnswer.listEvents.addAll(statusScheduler.listEvents);
      } else if (VERBE.SCHEDULERSTATUS.equals(verbEnum)) {
        StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(tenantId);
        executeAnswer.result.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
        executeAnswer.listEvents.addAll(statusScheduler.listEvents);
      } else if (VERBE.SCHEDULERCHANGE.equals(verbEnum)) {
        MilkSchedulerFactory milkSchedulerFactory = MilkSchedulerFactory.getInstance();
        String newSchedulerChange = (String) parameters.get(cstSchedulerChangeType);
        executeAnswer.listEvents.addAll(milkSchedulerFactory.changeTypeScheduler(newSchedulerChange, tenantId));

        // get information on the new Scheduler then
        if ((!BEventFactory.isError(executeAnswer.listEvents))) {
          StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(tenantId);
          executeAnswer.result.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
          executeAnswer.listEvents.addAll(statusScheduler.listEvents);
        }
      }

    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      String exceptionDetails = sw.toString();
      logger.severe("MilkCommand: ~~~~~~~~~~  : ERROR " + e + " at " + exceptionDetails);

      executeAnswer.listEvents.add(new BEvent(eventInternalError, e.getMessage()));
    } catch (Error er) {
      StringWriter sw = new StringWriter();
      er.printStackTrace(new PrintWriter(sw));
      String exceptionDetails = sw.toString();
      logger.severe("MilkCommand: ~~~~~~~~~~  : ERROR " + er + " at " + exceptionDetails);

      executeAnswer.listEvents.add(new BEvent(eventInternalError, er.getMessage()));
    } finally {
      executeAnswer.result.put(cstResultTimeInMs, System.currentTimeMillis() - currentTime);
      executeAnswer.result.put(cstResultListEvents, BEventFactory.getHtml(executeAnswer.listEvents));
      if (verbEnum.HEARTBEAT.equals(verbEnum))
        logger.fine(logHeader + "MilkTourCommand Verb[" + (verbEnum == null ? "null" : verbEnum.toString()) + "] Tenant["
          + tenantId + "] Error?" + BEventFactory.isError(executeAnswer.listEvents) + " in "
          + (System.currentTimeMillis() - startTime) + " ms");
      else
        logger.info(logHeader + "MilkTourCommand Verb[" + (verbEnum == null ? "null" : verbEnum.toString()) + "] Tenant["
            + tenantId + "] Error?" + BEventFactory.isError(executeAnswer.listEvents) + " in "
            + (System.currentTimeMillis() - startTime) + " ms");
        

    }

    // ------------------- service
    //ProcessDefinitionService processDefinitionService = serviceAccessor.getProcessDefinitionService();
    //ProcessInstanceService processInstanceService = serviceAccessor.getProcessInstanceService();
    //SchedulerService schedulerService = serviceAccessor.getSchedulerService();
    //EventInstanceService eventInstanceService = serviceAccessor.getEventInstanceService();

    return executeAnswer;
  }

  private boolean isInitialized = false;

  /**
   * initialization : read all data, then check the Quart timer is correclty set.
   * 
   * @return
   */
  public synchronized List<BEvent> initialization(boolean forceReload, boolean forceSchedule, long tenantId) {
    List<BEvent> listEvents = new ArrayList<BEvent>();

    MilkPlugInTourFactory milkPlugInTourFactory = MilkPlugInTourFactory.getInstance();

    if (!isInitialized || forceReload) {
      //  load all PlugIn
      listEvents.addAll(initialiseListPlugIn(tenantId));
      // load all PlugTour
      listEvents.addAll(milkPlugInTourFactory.dbLoadAllPlugInTour(tenantId, this));
    }

    if (!isInitialized || forceSchedule) {
      // Start the Timer

      if (milkSchedulerFactory.getScheduler() != null) {
        milkSchedulerFactory.getScheduler().shutdown(tenantId);
        listEvents.addAll(milkSchedulerFactory.getScheduler().startup(tenantId, forceSchedule));
      }
    }
    isInitialized = true;
    return listEvents;
  }

  /* ******************************************************************************** */
  /*                                                                                  */
  /* timer() */
  /*
   * /*
   */
  /* ******************************************************************************** */
  private boolean isInProgress = false;

  // to avoid any transaction issue in the command (a transaction is maybe opennend by the engine, and then the processAPI usage is forbiden), let's create a thread
  public void executeOneTimeNewThread(long tenantId) {
    MyTruckMilkThread mythread = new MyTruckMilkThread(this, tenantId);
    mythread.start();
  }

  /**
   * thread to execute in a different thread to have a new connection
   * @author Firstname Lastname
   *
   */
  public class MyTruckMilkThread extends Thread {

    private MilkCmdControl cmdControl;
    private long tenantId;

    protected MyTruckMilkThread(MilkCmdControl cmdControl, long tenantId) {
      this.cmdControl = cmdControl;
      this.tenantId = tenantId;
    }

    public void run() {
      cmdControl.executeOneTime(tenantId);
    }
  }

  /**
   * execute the command
   * 
   * @param tenantId
   */
  public void executeOneTime(long tenantId) {
    // we want to work as a singleton: if we already manage a Timer, skip this one (the previous one isn't finish)
    if (isInProgress)
      return;
    isInProgress = true;

    // Advance the scheduler that we run now !
    milkSchedulerFactory.informRunInProgress(tenantId);

    // maybe this is the first call after a restart ? 
    if (!isInitialized) {
      initialization(false, false, tenantId);
    }

    try {
      MilkPlugInTourFactory milkPlugInTourFactory = MilkPlugInTourFactory.getInstance();

      ConnectorAPIAccessorImpl connectorAccessorAPI = new ConnectorAPIAccessorImpl(tenantId);
      Date currentDate = new Date();
      // check all the Tour now
      for (MilkPlugInTour plugInTour : getListTour()) {
        if (plugInTour.isEnable || plugInTour.isImmediateExecution()) {
          // protection : recalculate a date then
          if (plugInTour.nextExecutionDate == null)
            plugInTour.calculateNextExecution();

          if (plugInTour.isImmediateExecution() || plugInTour.nextExecutionDate != null
              && plugInTour.nextExecutionDate.getTime() < currentDate.getTime()) {

            List<BEvent> listEvents = new ArrayList<BEvent>();
            MilkPlugIn plugIn = plugInTour.getPlugIn();
            MilkPlugInTour plugTourReLoaded = null;
            PlugTourOutput output = null;
            try {
              // Reload it : parameters may have change by another cluster
              plugTourReLoaded = milkPlugInTourFactory.dbLoadPlugInTour(plugInTour.getId(), tenantId, this);

              // execute it!
              PlugTourInput plugTourInput = new PlugTourInput(plugTourReLoaded);
              plugTourInput.tourName = plugTourReLoaded.getName();

              // ----------------- Execution
              long timeBegin = System.currentTimeMillis();
              output = plugIn.execute(plugTourInput, connectorAccessorAPI);
              long timeEnd = System.currentTimeMillis();

              if (output == null) {
                output = new PlugTourOutput(plugTourReLoaded);
                output.addEvent(new BEvent(eventPlugInViolation, "PlugIn[" + plugIn.getName() + "]"));
                output.executionStatus = ExecutionStatus.CONTRACTVIOLATION;
              }
              output.executionTimeInMs = (timeEnd - timeBegin);

            } catch (Exception e) {
              output = new PlugTourOutput(plugTourReLoaded);
              output.addEvent(new BEvent(eventPlugInError, e, "PlugIn[" + plugIn.getName() + "]"));
              output.executionStatus = ExecutionStatus.ERROR;
            }
            if (output != null) {
              // maybe the plugin forgot to setup the execution ? So set it.
              if (output.executionStatus == ExecutionStatus.NOEXECUTION)
                output.executionStatus = ExecutionStatus.SUCCESS;

              plugInTour.setStatusLastExecution(currentDate, output);
              milkPlugInTourFactory.saveExecution(currentDate, output);
              listEvents.addAll(output.getListEvents());
            }
            // calculate the next time
            listEvents.addAll(plugInTour.calculateNextExecution());
            plugInTour.setImmediateExecution(false);
            listEvents.addAll(milkPlugInTourFactory.dbSavePlugInTour(plugInTour, tenantId));
          }
        } // end isEnable

      }
    } catch (Exception e) {
      logger.severe("MilkCmdControl.executeTimer: Exception " + e.getMessage());
    } catch (Error er) {
      logger.severe("MilkCmdControl.executeTimer: Error " + er.getMessage());
    }
    // we finished
    isInProgress = false;
  }

  /* ******************************************************************************** */
  /*                                                                                  */
  /* Local method() */
  /*
   * /*
   */
  /* ******************************************************************************** */

  public MilkPlugIn getPluginFromName(String name) {
    for (MilkPlugIn plugIn : listPlugIns)
      if (plugIn.getName().equals(name))
        return plugIn;
    return null;
  }

  public List<MilkPlugIn> getListPlugIn() {
    return listPlugIns;
  }

  /**
   * return the tour Index by the name
   * 
   * @param name
   * @return
   */

  public MilkPlugInTour getTourById(Long id) {
    MilkPlugInTourFactory milkPlugInTourFactory = MilkPlugInTourFactory.getInstance();

    return milkPlugInTourFactory.getById(id);
  }

  public Collection<MilkPlugInTour> getListTour() {
    MilkPlugInTourFactory milkPlugInTourFactory = MilkPlugInTourFactory.getInstance();

    return milkPlugInTourFactory.getMapTourId().values();
  }

  /**
   * return a list ordered by name
   * 
   * @return
   */
  public List<Map<String, Object>> getListTourMap() {
    List<Map<String, Object>> listTourMap = new ArrayList<Map<String, Object>>();

    for (MilkPlugInTour plugInTour : getListTour()) {

      plugInTour.checkByPlugIn();

      listTourMap.add(plugInTour.getMap(true));
    }

    // order now
    Collections.sort(listTourMap, new Comparator<Map<String, Object>>() {

      public int compare(Map<String, Object> s1,
          Map<String, Object> s2) {
        String s1name = (String) s1.get("name");
        String s2name = (String) s2.get("name");
        if (s1name == null)
          s1name = "";
        if (s2name == null)
          s2name = "";

        return s1name.compareTo(s2name);
      }
    });

    return listTourMap;
  }

  public List<BEvent> removeTour(long idTour, long tenantId) {
    MilkPlugInTourFactory milkPlugInTourFactory = MilkPlugInTourFactory.getInstance();
    return milkPlugInTourFactory.removeTour(idTour, tenantId);
  }

  public synchronized List<BEvent> registerATour(MilkPlugInTour plugInTour) {
    MilkPlugInTourFactory milkPlugInTourFactory = MilkPlugInTourFactory.getInstance();
    return milkPlugInTourFactory.registerATour(plugInTour);
  }

  /* ******************************************************************************** */
  /*                                                                                  */
  /* detection method */
  /*                                                                                  */
  /* ******************************************************************************** */
public List<BEvent> initialiseListPlugIn( long tenantId )
{
  List<BEvent> listEvents = new ArrayList<BEvent>();
  listPlugIns = detectListPlugIn( tenantId);
  try
  {
  // todo : get all the command deploy, and verify if this is not a PlugIn
  for (MilkPlugIn plugIn : listPlugIns) {
    // call the plug in, and init all now
    listEvents.addAll(plugIn.initialize(tenantId));
  }
  } catch(Exception e)
  {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    String exceptionDetails = sw.toString();
    logger.severe("MilkCommand: ~~~~~~~~~~  : ERROR " + e + " at " + exceptionDetails);

    listEvents.add(new BEvent(eventInternalError, e.getMessage()));
    
  } catch (Error er) {
  StringWriter sw = new StringWriter();
  er.printStackTrace(new PrintWriter(sw));
  String exceptionDetails = sw.toString();
  logger.severe("MilkCommand: ~~~~~~~~~~  : ERROR " + er + " at " + exceptionDetails);

  listEvents.add(new BEvent(eventInternalError, er.getMessage()));
  }
  return listEvents;
}
  
  public List<MilkPlugIn> detectListPlugIn(long tenantId) {
    List<MilkPlugIn> list = new ArrayList<MilkPlugIn>();
    // listPlugIns.add(new MilkDirectory());
    // listPlugIns.add(new MilkMail());
    list.add(new MilkReplayFailedTask());
    list.add(new MilkPurgeArchive());
    list.add(new MilkDeleteCases());
    list.add(new MilkSLA());
    list.add(new MilkEmailUsersTasks());
    
   
    return list;
  }

  /* ******************************************************************************** */
  /*                                                                                  */
  /* Execute Test Button */
  /*                                                                                  */
  /* ******************************************************************************** */

  public class MySimpleTestThread extends Thread {

    private MilkCmdControl cmdControl;
    private long tenantId;
    public boolean isFinish=false;
    public String buttonName; 
    public PlugTourInput input;
    List<BEvent> listEvents;
    MilkPlugInTour plugInTour;
    public Map<String, Object> argsParameters;
    
    protected MySimpleTestThread(long tenantId, String buttonName,  MilkPlugInTour plugInTour, PlugTourInput input, Map<String, Object> argsParameters) {
      this.tenantId = tenantId;
      this.buttonName = buttonName;
      this.input = input;
      this.argsParameters = argsParameters;
      this.plugInTour = plugInTour;
    }

    public void run() {
      try
      {
        ConnectorAPIAccessorImpl connectorAccessorAPI = new ConnectorAPIAccessorImpl(tenantId);
        listEvents= plugInTour.getPlugIn().buttonParameters( buttonName, input, argsParameters, connectorAccessorAPI);
      }
      catch(Exception e)
      {
        listEvents.add(new BEvent(eventInternalError, e.getMessage()));
      }
      isFinish=true;
    }
  }
  /**
   * load all plugtours
   * 
   * @return
   */

}
