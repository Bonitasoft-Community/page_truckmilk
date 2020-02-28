package org.bonitasoft.truckmilk.engine;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.bonitasoft.command.BonitaCommandDeployment;
import org.bonitasoft.command.BonitaCommandDeployment.DeployStatus;
import org.bonitasoft.command.BonitaCommandDescription;
import org.bonitasoft.command.BonitaCommandDescription.CommandJarDependency;
import org.bonitasoft.engine.api.CommandAPI;
import org.bonitasoft.engine.api.PlatformAPI;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

/**
 * this class is the API to the command. It's deploy the command on demand
 * 
 * @author Firstname Lastname
 */
public class MilkCmdControlAPI {

    static MilkLog logger = MilkLog.getLogger(MilkCmdControlAPI.class.getName());

    /*
     * private static BEvent EVENT_UNKNOWN_SCHEDULEROPERATION = new BEvent(MilkCmdControlAPI.class.getName(), 1,
     * Level.ERROR,
     * "Unkown maintenance operation", "This operation is not known", "Operation not performed",
     * "See the requested maintenance operation");
     */
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

        BonitaCommandDescription commandDescription = getMilkCommandDescription(pageDirectory);
        BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(commandDescription);

        DeployStatus deployStatus = bonitaCommand.checkAndDeployCommand(commandDescription, true, tenantId, commandAPI, platFormAPI);
        return deployStatus;
    }

    /**
     * @param pageDirectory
     * @param commandAPI
     * @param platFormAPI
     * @param tenantId
     * @return
     */
    public DeployStatus forceDeployCommand(Map<String, Object> parameter, File pageDirectory, CommandAPI commandAPI, PlatformAPI platFormAPI,
            long tenantId) {

        boolean redeployDependency = false;
        try {
            redeployDependency = Boolean.valueOf(parameter.get("redeploydependencies").toString());
        } catch (Exception e) {
            redeployDependency = false;
        }
        BonitaCommandDescription commandDescription = getMilkCommandDescription(pageDirectory);
        BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(commandDescription);
        commandDescription.forceDeploy = false;

        if (redeployDependency) {
            for (CommandJarDependency jarDependency : commandDescription.getListDependencies())
                jarDependency.setForceDeploy( true );
        }
        DeployStatus deployStatus = bonitaCommand.deployCommand(commandDescription, true, tenantId, commandAPI, platFormAPI);
        return deployStatus;

    }

    /**
     * geth the command description
     * 
     * @param pageDirectory
     * @return
     */
    private BonitaCommandDescription getMilkCommandDescription(File pageDirectory) {

        BonitaCommandDescription commandDescription = new BonitaCommandDescription(MilkCmdControl.cstCommandName, pageDirectory);
        commandDescription.forceDeploy = false;
        commandDescription.mainCommandClassName = MilkCmdControl.class.getName();
        commandDescription.mainJarFile = "TruckMilk-2.3-Page.jar";
        commandDescription.commandDescription = MilkCmdControl.cstCommandDescription;
        // "bonita-commanddeployment-1.2.jar" is deployed automaticaly with BonitaCommandDeployment
        // commandDescription.dependencyJars = new String[] { "bonita-event-1.5.0.jar", "bonita-properties-2.0.0.jar" }; // "mail-1.5.0-b01.jar", "activation-1.1.jar"};

        commandDescription.addJarDependencyLastVersion("bonita-event", "1.7.0", "bonita-event-1.7.0.jar");
        commandDescription.addJarDependencyLastVersion("bonita-properties", "2.1.1", "bonita-properties-2.1.1.jar");
        CommandJarDependency cmdDependency= commandDescription.addJarDependencyLastVersion("custompage-worker", "1.4.0", "CustomPageWorker-1.4.0.jar");
        cmdDependency.setForceDeploy( true );
        
        // don't add the Meteor Dependency : with Bonita, all dependencies are GLOBAL. If we reference the MeteorAPI, we will have the same API for all pages
        // and that's impact the meteor page.
        return commandDescription;
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
        Map<String, Object> result = new HashMap<String, Object>();
        String operation = (String) information.get("operation");
        HashMap<String, Serializable> parameters = new HashMap<String, Serializable>();
        BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(MilkCmdControl.cstCommandName);

        if ("deploy".equals(operation)) {
            parameters.put(MilkCmdControl.CST_PAGE_DIRECTORY, pageDirectory.getAbsolutePath());
            result = bonitaCommand.callCommand(MilkCmdControl.VERBE.SCHEDULERDEPLOY.toString(), parameters, tenantId, commandAPI);
        }

        else if ("reset".equals(operation)) {
            // reset scheduler must be send to the command
            result = bonitaCommand.callCommand(MilkCmdControl.VERBE.SCHEDULERRESET.toString(), parameters, tenantId, commandAPI);
        }

        else if ("changescheduler".equals(operation)) {
            parameters.put(MilkCmdControl.cstSchedulerChangeType, (String) information.get("newscheduler"));
            result = bonitaCommand.callCommand(MilkCmdControl.VERBE.SCHEDULERCHANGE.toString(), parameters, tenantId, commandAPI);
        } else {
            parameters.put(MilkCmdControl.cstSchedulerOperation, operation);
            result = bonitaCommand.callCommand(MilkCmdControl.VERBE.SCHEDULEROPERATION.toString(), parameters, tenantId, commandAPI);
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

    public Map<String, Object> checkUpdateEnvironment(CommandAPI commandAPI, long tenantId) {
        BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(MilkCmdControl.cstCommandName);

        return bonitaCommand.callCommand(MilkCmdControl.VERBE.CHECKUPDATEENVIRONMENT.toString(), null, tenantId, commandAPI);
    }

    public Map<String, Object> getRefreshInformation(CommandAPI commandAPI, long tenantId) {
        BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(MilkCmdControl.cstCommandName);
        return bonitaCommand.callCommand(MilkCmdControl.VERBE.REFRESH.toString(), null, tenantId, commandAPI);
    }

    /**
     * callTourOperation : each operation on a tour (update, create, setparameters)
     * 
     * @param verbe
     * @param information
     * @param pageDirectory
     * @param commandAPI
     * @param tenantId
     * @return
     */
    public Map<String, Object> callJobOperation(MilkCmdControl.VERBE verbe, Map<String, Object> information, File pageDirectory,
            CommandAPI commandAPI, long tenantId) {
        HashMap<String, Serializable> parameters = new HashMap<>();
        if (information != null)
            for (Map.Entry<String,Object> entry : information.entrySet())
                if (entry.getValue() != null)
                    parameters.put(entry.getKey(), (Serializable) entry.getValue());

        parameters.put(MilkCmdControl.CST_PAGE_DIRECTORY, pageDirectory.getAbsolutePath());

        BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(MilkCmdControl.cstCommandName);
        return bonitaCommand.callCommand(verbe.toString(), parameters, tenantId, commandAPI);
    }

}
