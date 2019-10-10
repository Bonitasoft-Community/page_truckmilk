package org.bonitasoft.truckmilk;

import java.io.File;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
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
import org.bonitasoft.truckmilk.engine.MilkCmdControl;
import org.bonitasoft.truckmilk.engine.MilkCmdControlAPI;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugInFactory;
import org.bonitasoft.truckmilk.engine.MilkJobFactory;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.toolbox.TypesCast;
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

    /**
     * Internal architecture
     * the TruckMilkAPI
     */
    public static MilkAccessAPI getInstance() {
        return new MilkAccessAPI();
    };

    public static class Parameter {

        public CommandAPI commandAPI;
        public PlatformAPI platFormAPI;
        public File pageDirectory;
        public APISession apiSession;

        public Map<String, Object> information;

        public static Parameter getInstanceFromJson(String jsonSt) {
            Parameter parameter = new Parameter();
            if (jsonSt == null)
                return parameter;

            parameter.setInformation(jsonSt);
            return parameter;
        }

        @SuppressWarnings("unchecked")
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

        public void setApiSession(APISession apiSession) {
            this.apiSession = apiSession;
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

    public Map<String, Object> checkUpdateEnvironment(Parameter parameter) {
        // first, deploy the command if needed
        MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
        // second call the command          
        return milkCmdControlAPI.checkUpdateEnvironment(parameter.commandAPI, parameter.getTenantId());
    }

    public Map<String, Object> addJob(Parameter parameter) {
        MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
        return milkCmdControlAPI.callJobOperation(MilkCmdControl.VERBE.ADDJOB, parameter.information, parameter.pageDirectory, parameter.commandAPI, parameter.getTenantId());
    }

    public Map<String, Object> removeJob(Parameter parameter) {
        MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
        return milkCmdControlAPI.callJobOperation(MilkCmdControl.VERBE.REMOVEJOB, parameter.information, parameter.pageDirectory, parameter.commandAPI, parameter.getTenantId());
    }
    public Map<String, Object> stopJob(Parameter parameter) {
        MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
        return milkCmdControlAPI.callJobOperation(MilkCmdControl.VERBE.STOPTOUR, parameter.information, parameter.pageDirectory, parameter.commandAPI, parameter.getTenantId());
    }
    public Map<String, Object> abortJob(Parameter parameter) {
        MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
        return milkCmdControlAPI.callJobOperation(MilkCmdControl.VERBE.ABORTJOB, parameter.information, parameter.pageDirectory, parameter.commandAPI, parameter.getTenantId());
    }
    public Map<String, Object> resetJob(Parameter parameter) {
        MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
        return milkCmdControlAPI.callJobOperation(MilkCmdControl.VERBE.RESETJOB, parameter.information, parameter.pageDirectory, parameter.commandAPI, parameter.getTenantId());
    }
    public Map<String, Object> updateJob(Parameter parameter) {
        MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
        return milkCmdControlAPI.callJobOperation(MilkCmdControl.VERBE.UPDATEJOB, parameter.information, parameter.pageDirectory, parameter.commandAPI, parameter.getTenantId());
    }

    public Map<String, Object> testButton(Parameter parameter) {
        MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
        Map<String, Object> information = parameter.information;
        information.put(MilkCmdControl.cstButtonName, parameter.information.get("buttonName"));
        return milkCmdControlAPI.callJobOperation(MilkCmdControl.VERBE.TESTBUTTONARGS, parameter.information, parameter.pageDirectory, parameter.commandAPI, parameter.getTenantId());
    }

    public Map<String, Object> immediateExecution(Parameter parameter) {
        MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
        return milkCmdControlAPI.callJobOperation(MilkCmdControl.VERBE.IMMEDIATEJOB, parameter.information, parameter.pageDirectory, parameter.commandAPI, parameter.getTenantId());
    }

    public Map<String, Object> startJob(Parameter parameter) {
        MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
        return milkCmdControlAPI.callJobOperation(MilkCmdControl.VERBE.STARTJOB, parameter.information, parameter.pageDirectory, parameter.commandAPI, parameter.getTenantId());
    }

    /**
     * read a parameter file, and send the result in the outputStream (send to the browser)
     * 
     * @param parameter
     * @param output
     * @return
     */
    public Map<String, String> readParameterFile(Parameter parameter, OutputStream output) {
        Map<String, String> mapHeaders = new HashMap<String, String>();

        Long plugInTourId = TypesCast.getLong(parameter.information.get("plugintour"), 0L);
        long tenantId = parameter.apiSession.getTenantId();
        String paramname = TypesCast.getString(parameter.information.get("parametername"), null);

        MilkPlugInFactory milkPlugInFactory = MilkPlugInFactory.getInstance(tenantId);
        MilkJobFactory milkPlugInTourFactory = MilkJobFactory.getInstance(milkPlugInFactory);

        MilkJob milkPlugInTour = milkPlugInTourFactory.getById(plugInTourId);
        /*
         * MilkFactoryOp milkFactoryOp = milkPlugInTourFactory.dbLoadPlugInTour(plugInTourId);
         * if (milkFactoryOp.plugInTour==null)
         * logger.severe("## truckMilk: Can't access Tour by id=["+plugInTourId+"]" );
         * else
         * {
         */
        if (milkPlugInTour == null)
            logger.severe("Can't access plugInParameter[" + paramname + "]");
        else {
            // load the file then
            PlugInParameter plugInParameter = milkPlugInTour.getPlugIn().getDescription().getPlugInParameter(paramname);
            if (plugInParameter == null)
                logger.severe("Can't access plugInParameter[" + paramname + "] in Tour[" + milkPlugInTour.name + "]");
            else {
                milkPlugInTour.getParameterStream(plugInParameter, output);
                mapHeaders.put("content-disposition", "attachment; filename=" + plugInParameter.fileName);
                mapHeaders.put("content-type", "attachment; filename=" + plugInParameter.contentType);
            }
        }
        // response.addHeader("content-disposition", "attachment; filename=LogFiles.zip");
        // response.addHeader("content-type", "application/zip");
        return mapHeaders;
    }

    public Map<String, Object> scheduler(Parameter parameter) {
        MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
        return milkCmdControlAPI.callJobOperation(MilkCmdControl.VERBE.SCHEDULERSTARTSTOP, parameter.information, parameter.pageDirectory, parameter.commandAPI, parameter.getTenantId());
    }

    public Map<String, Object> schedulerMaintenance(Parameter parameter) {
        MilkCmdControlAPI milkCmdControlAPI = MilkCmdControlAPI.getInstance();
        return milkCmdControlAPI.schedulerMaintenance(parameter.information, parameter.pageDirectory, parameter.commandAPI, parameter.platFormAPI, parameter.getTenantId());
    }

}
