package org.bonitasoft.truckmilk.tour;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.command.BonitaCommandDeployment;
import org.bonitasoft.command.BonitaCommandDeployment.CommandDescription;
import org.bonitasoft.command.BonitaCommandDeployment.DeployStatus;
import org.bonitasoft.engine.api.CommandAPI;
import org.bonitasoft.engine.api.PlatformAPI;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;

/**
 * this class is the API to the command. It's deploy the command on demand
 * 
 * @author Firstname Lastname
 */
public class MilkCmdControlAPI {

  static Logger logger = Logger.getLogger(MilkCmdControlAPI.class.getName());

  private static String logHeader = "TruckMilk.cmd";

  private static BEvent EVENT_ALREADY_DEPLOYED = new BEvent(MilkCmdControlAPI.class.getName(), 1, Level.INFO,
      "Command already deployed", "The command at the same version is already deployed");
  private static BEvent EventDeployedWithSuccess = new BEvent(MilkCmdControlAPI.class.getName(), 2, Level.INFO,
      "Command deployed with success", "The command are correctly deployed");
  private static BEvent EventErrorAtDeployment = new BEvent(MilkCmdControlAPI.class.getName(), 3,
      Level.APPLICATIONERROR, "Error during deployment of the command", "The command are not deployed",
      "The pâge can not work", "Check the exception");
  private static BEvent EventNotDeployed = new BEvent(MilkCmdControlAPI.class.getName(), 4, Level.ERROR,
      "Command not deployed", "The command is not deployed");
  private static BEvent EVENT_CALL_COMMAND = new BEvent(MilkCmdControlAPI.class.getName(), 5, Level.ERROR,
      "Error during calling a command", "Check the error", "Function can't be executed", "See the error");

  private static BEvent eventPingError = new BEvent(MilkCmdControlAPI.class.getName(), 6, Level.ERROR,
      "Ping error", "Command does not response", "A command is not responding", "See the error");

  private static BEvent eventUnkownSchedulerMaintenance = new BEvent(MilkCmdControlAPI.class.getName(), 7,
      Level.ERROR,
      "Unkown maintenance operation", "This operation is not known", "Operation not performed",
      "See the requested maintenance operation");

  public static String cstResultEventJobClassName = "JobClassName";

  public static String cstResultStatus_FAIL = "FAIL";
  public static String cstResultStatus_OK = "OK";
  public static String cstResultStatus_PING = "PING";

  public static String cstParamListMissingTimer = "listtimer";
  public static String cstParamMethodReset = "methodreset";
  public static String cstParamPing = "ping";

  public static MilkCmdControlAPI getInstance() {
    return new MilkCmdControlAPI();
  }

  /* ******************************************************************************** */
  /*                                                                                  */
  /* Communicate wit the command */
  /*                                                                                  */
  /* ******************************************************************************** */

  public DeployStatus checkAndDeployCommand(File pageDirectory, CommandAPI commandAPI, PlatformAPI platFormAPI,
      long tenantId) {
    CommandDescription commandDescription = new CommandDescription();
    commandDescription.pageDirectory = pageDirectory;

    commandDescription.forceDeploy = false;
    commandDescription.mainCommandClassName = MilkCmdControl.class.getName();
    commandDescription.mainJarFile = "TruckMilk-1.0-Page.jar";
    commandDescription.commandDescription = MilkCmdControl.cstCommandDescription;
    // "bonita-commanddeployment-1.2.jar" is deployed automaticaly with BonitaCommandDeployment
    commandDescription.dependencyJars = new String[] {  "bonita-event-1.4.0.jar", "bonita-properties-1.6.2.jar" }; // "mail-1.5.0-b01.jar", "activation-1.1.jar"};

    BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(MilkCmdControl.cstCommandName);
    DeployStatus deployStatus = bonitaCommand.checkAndDeployCommand(commandDescription, true, tenantId, commandAPI, platFormAPI);
    return deployStatus;
  }

  /**
   * maintenance on the scheduler
   * 
   * @param information
   * @param pageDirectory
   * @param commandAPI
   * @param platFormAPI
   * @param tenantId
   * @return
   */
  public Map<String, Object> schedulerMaintenance(Map<String, Object> information, File pageDirectory,
      CommandAPI commandAPI, PlatformAPI platFormAPI, long tenantId) {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    Map<String, Object> result = new HashMap<String, Object>();
    String operation = (String) information.get("operation");
    boolean operationManaged = false;
    HashMap<String, Serializable> parameters = new HashMap<String, Serializable>();
    BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(MilkCmdControl.cstCommandName);

    if ("deploy".equals(operation)) {
      operationManaged = true;
      parameters.put(MilkCmdControl.cstPageDirectory, pageDirectory.getAbsolutePath());

      result = bonitaCommand.callCommand(MilkCmdControl.VERBE.SCHEDULERDEPLOY.toString(), parameters, tenantId, commandAPI);

      // override the list event
      result.put(MilkCmdControl.cstResultListEvents, BEventFactory.getHtml(listEvents));
    }

    else if ("reset".equals(operation)) {
      operationManaged = true;
      // reset scheduler must be send to the command
      result = bonitaCommand.callCommand(MilkCmdControl.VERBE.SCHEDULERRESET.toString(), parameters, tenantId, commandAPI);
      result.put(MilkCmdControl.cstResultListEvents, BEventFactory.getHtml(listEvents));
    }

    if ("changescheduler".equals(operation)) {
      operationManaged = true;
      parameters.put(MilkCmdControl.cstSchedulerChangeType, (String) information.get("newscheduler"));
      result = bonitaCommand.callCommand(MilkCmdControl.VERBE.SCHEDULERCHANGE.toString(), parameters, tenantId, commandAPI);
    }

    // unknow command
    if (!operationManaged) {
      logger.severe(logHeader + " Unkown schedulerOperation[" + operation + "]");
      listEvents.add(new BEvent(eventUnkownSchedulerMaintenance, "Operation[" + operation + "]"));
      result.put(MilkCmdControl.cstResultListEvents, BEventFactory.getHtml(listEvents));
    }

    return result;
  }
  /* ******************************************************************************** */
  /*                                                                                  */
  /* Communication with the command */
  /*                                                                                  */
  /* ******************************************************************************** */

  public Map<String, Object> getStatus(CommandAPI commandAPI, long tenantId) {
    BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(MilkCmdControl.cstCommandName);

    return bonitaCommand.callCommand(MilkCmdControl.VERBE.GETSTATUS.toString(), null, tenantId, commandAPI);
  }


  public Map<String, Object> getRefreshInformation(CommandAPI commandAPI, long tenantId) {
    BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(MilkCmdControl.cstCommandName);
    return bonitaCommand.callCommand(MilkCmdControl.VERBE.REFRESH.toString(), null, tenantId, commandAPI);
  }

  public Map<String, Object> callTourOperation(MilkCmdControl.VERBE verbe, Map<String, Object> information,
      CommandAPI commandAPI, long tenantId) {
    HashMap<String, Serializable> parameters = new HashMap<String, Serializable>();
    if (information != null)
      for (String key : information.keySet())
        if (information.get(key) != null)
          parameters.put(key, (Serializable) information.get(key));

    BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(MilkCmdControl.cstCommandName);
    return bonitaCommand.callCommand(verbe.toString(), parameters, tenantId, commandAPI);
  }

}
