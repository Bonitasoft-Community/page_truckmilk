package org.bonitasoft.truckmilk;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.command.BonitaCommandDeployment.DeployStatus;
import org.bonitasoft.engine.api.CommandAPI;
import org.bonitasoft.engine.api.PlatformAPI;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.tour.MilkCmdControl;
import org.bonitasoft.truckmilk.tour.MilkCmdControlAPI;
import org.json.simple.JSONValue;

/**
 * this is the main access to the TruckMilk operation.
 * Truck Mil can be integrated in two different way:
 * - for the administration, to see all PlugIn available, create a Milk Tour, disable / enable,
 * change information on the Tour
 * - you develop a component (a page, a RestAPI...) and you want an execution of the component in
 * the TruckMilk word.
 * * develop your component as a MilkPlugIn
 * * register it via the MilkAccessAPI
 * How it's run ?
 * A TourJob is register in the timer. All Jobs contains a Detection (DetectionPlugIn) and an
 * operation (OperationPlugIn), with parameters
 * When the timer fire, TourJob call the TourCmd. This is a command deployed on the BonitaEngine.
 * The TourCmd will create the DetectionPlugIn, and runIt. THe DetectionPlugIn return a set of
 * DectectionItem
 * (example, the DetectinPlugIn is a MailPlug. It detect 3 mails, then return 3 DectectionItem)
 * The TourCmd call the OperationPlugIn for this 3 DetectinItem, and then register the execution.
 * On Return, TourJob register a new Timer.
 * Create your own DetectionPlugin or OperationPlugIn ? You then have a JAR file containing your own
 * detection / own operation.
 * Call the MilkAccessAPI.registerMyOperation and give a list of all detection / plug in class.
 * THe MilkAccessAPI create then a Command with all theses informations, plus the MilkAPi librairy.
 * It's all set ! You can then
 * register a Tour with your object.
 */
public class MilkAccessAPI {

  static Logger logger = Logger.getLogger(MilkAccessAPI.class.getName());

  public static String cstJsonListEvents = MilkCmdControl.cstResultListEvents;
  public static String cstJsonDeploimentsuc = "deploimentsuc";
  public static String cstJsonDeploimenterr = "deploimenterr";

  private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

  /**
   * Internal architecture
   * the TruckMilkAPI
   */
  public static MilkAccessAPI getInstance() {
    return new MilkAccessAPI();
  };

  public static class Parameter {

    CommandAPI commandAPI;
    PlatformAPI platFormAPI;
    File pageDirectory;
    APISession apiSession;

    public Map<String, Object> information;

    public static Parameter getInstanceFromJson(String jsonSt) {
      Parameter parameter = new Parameter();
      if (jsonSt == null)
        return parameter;

      parameter.setInformation(jsonSt);
      return parameter;
    }

    public void setInformation(String jsonSt) {
      try {
        information = (Map<String, Object>) JSONValue.parse(jsonSt);
      } catch (Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String exceptionDetails = sw.toString();
        logger.severe("Parameter: ~~~~~~~~~~  : ERROR " + e + " at " + exceptionDetails);
        information = null;
      }
    }

    public long getTenantId() {
      return apiSession.getTenantId();
    }

  }

  public Map<String, Object> startup(Parameter parameter) {
    // first, deploy the command if needed
    List<BEvent> listEvents = new ArrayList<BEvent>();
    Map<String, Object> result = new HashMap<String, Object>();

    MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
    DeployStatus deployStatus = milkCmdControlAPI.checkAndDeployCommand(parameter.pageDirectory, parameter.commandAPI,
        parameter.platFormAPI, parameter.getTenantId());
    listEvents.addAll(deployStatus.listEvents);

    if (BEventFactory.isError(listEvents)) {
      result.put(cstJsonListEvents, BEventFactory.getHtml(listEvents));
      result.put(cstJsonDeploimenterr, "Error during deploiment");
      return result;
    }

    // // startup: check the environment   
    String statusDeployment = "";
    if (deployStatus.newDeployment)
      statusDeployment = "Command deployed with success;";
    else if (!deployStatus.alreadyDeployed)
      statusDeployment = "Command not deployed;";

    if (MilkCmdControl.cstEnvironmentStatus_V_ERROR.equals(result.get(MilkCmdControl.cstEnvironmentStatus))) {
      statusDeployment = "Bad environment;";
    } else
      //  second call the command getStatus		    
      result.putAll(milkCmdControlAPI.getStatus(parameter.commandAPI, parameter.getTenantId()));

    result.put(cstJsonDeploimentsuc, statusDeployment);
    return result;
  }

  public Map<String, Object> getRefreshInformation(Parameter parameter) {
    // first, deploy the command if needed
    MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
    // second call the command          
    return milkCmdControlAPI.getRefreshInformation(parameter.commandAPI, parameter.getTenantId());
  }

  public Map<String, Object> getStatusInformation(Parameter parameter) {
    // first, deploy the command if needed
    MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
    // second call the command          
    return milkCmdControlAPI.getStatus(parameter.commandAPI, parameter.getTenantId());
  }
  
  
  public Map<String, Object> addTour(Parameter parameter) {
    MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
    return milkCmdControlAPI.callTourOperation(MilkCmdControl.VERBE.ADDTOUR, parameter.information, parameter.commandAPI, parameter.getTenantId());
  }

  public Map<String, Object> removeTour(Parameter parameter) {
    MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
    return milkCmdControlAPI.callTourOperation(MilkCmdControl.VERBE.REMOVETOUR, parameter.information, parameter.commandAPI, parameter.getTenantId());
  }

  public Map<String, Object> stopTour(Parameter parameter) {
    MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
    return milkCmdControlAPI.callTourOperation(MilkCmdControl.VERBE.STOPTOUR, parameter.information, parameter.commandAPI, parameter.getTenantId());
  }

  public Map<String, Object> updateTour(Parameter parameter) {
    MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
    return milkCmdControlAPI.callTourOperation(MilkCmdControl.VERBE.UPDATETOUR, parameter.information, parameter.commandAPI, parameter.getTenantId());
  }

  public Map<String, Object> testButton(Parameter parameter) {
    MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
    Map<String, Object> information = parameter.information;
    information.put(MilkCmdControl.cstButtonName, parameter.information.get("buttonName"));
    return milkCmdControlAPI.callTourOperation(MilkCmdControl.VERBE.TESTBUTTONARGS, parameter.information, parameter.commandAPI, parameter.getTenantId());
  }

  public Map<String, Object> immediateExecution(Parameter parameter) {
    MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
    return milkCmdControlAPI.callTourOperation(MilkCmdControl.VERBE.IMMEDIATETOUR, parameter.information, parameter.commandAPI, parameter.getTenantId());
  }

  public Map<String, Object> startTour(Parameter parameter) {
    MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
    return milkCmdControlAPI.callTourOperation(MilkCmdControl.VERBE.STARTTOUR, parameter.information, parameter.commandAPI, parameter.getTenantId());
  }

  public Map<String, Object> scheduler(Parameter parameter) {
    MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
    return milkCmdControlAPI.callTourOperation(MilkCmdControl.VERBE.SCHEDULERSTARTSTOP, parameter.information, parameter.commandAPI, parameter.getTenantId());
  }

  public Map<String, Object> schedulerMaintenance(Parameter parameter) {
    MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
    return milkCmdControlAPI.schedulerMaintenance(parameter.information, parameter.pageDirectory, parameter.commandAPI, parameter.platFormAPI, parameter.getTenantId());
  }

}
