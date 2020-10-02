package org.bonitasoft.truckmilk.engine;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
 *
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
     * 
     * @param parameter
     * @param pageDirectory
     * @param commandAPI
     * @param platFormAPI
     * @param tenantId
     * @return
     */
    public DeployStatus unDeployCommand(Map<String, Object> parameter, File pageDirectory, CommandAPI commandAPI, PlatformAPI platFormAPI,
            long tenantId) {
        BonitaCommandDescription commandDescription = getMilkCommandDescription(pageDirectory);
        BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(commandDescription);
        return bonitaCommand.undeployCommand(commandDescription, true, tenantId, commandAPI, platFormAPI);        
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
        commandDescription.mainJarFile = "bonita-truckmilk-2.7.0-Page.jar";
        commandDescription.commandDescription = MilkCmdControl.cstCommandDescription;



        CommandJarDependency cmdDependency;
        
        cmdDependency=commandDescription.addJarDependencyLastVersion(BonitaCommandDeployment.NAME, BonitaCommandDeployment.VERSION, BonitaCommandDeployment.JAR_NAME);
        cmdDependency.setForceDeploy( true );
        
        commandDescription.addJarDependencyLastVersion("mail", "1.5.0", "mail-1.5.0-b01.jar");
        commandDescription.addJarDependencyLastVersion("activation", "1.1.1", "activation-1.1.1.jar");
            
        cmdDependency=commandDescription.addJarDependencyLastVersion("bonita-event", "1.9.0", "bonita-event-1.9.0.jar");
        cmdDependency.setForceDeploy( true );

        cmdDependency=commandDescription.addJarDependencyLastVersion("bonita-properties", "2.8.0", "bonita-properties-2.8.0.jar");
        cmdDependency.setForceDeploy( true );

        cmdDependency=commandDescription.addJarDependencyLastVersion("bonita-casedetails", "1.1.0", "bonita-casedetails-1.1.0.jar");
        cmdDependency.setForceDeploy( true );
        
        cmdDependency=commandDescription.addJarDependencyLastVersion("custompage-meteor", "3.3.0", "bonita-meteor-3.3.0.jar");
        cmdDependency.setForceDeploy( true );

        cmdDependency=commandDescription.addJarDependencyLastVersion("custompage-sonar", "1.1.0", "bonita-sonar-1.1.0.jar");
        cmdDependency.setForceDeploy( true );

        cmdDependency=commandDescription.addJarDependencyLastVersion("CodeNarc", "1.6.1", "CodeNarc-1.6.1.jar");
        cmdDependency.setForceDeploy( true );

        cmdDependency=commandDescription.addJarDependencyLastVersion("custompage-worker", "1.9.0", "bonita-worker-1.9.0.jar");
        cmdDependency.setForceDeploy( true );
        
        cmdDependency= commandDescription.addJarDependencyLastVersion("custompage-grumman", "1.2.0", "bonita-grumman-1.2.0.jar");
        cmdDependency.setForceDeploy( true );

        cmdDependency= commandDescription.addJarDependencyLastVersion("custompage-explorer", "1.0.0", "bonita-explorer-1.0.0.jar");
        cmdDependency.setForceDeploy( true );

        cmdDependency=commandDescription.addJarDependencyLastVersion("custompage-logaccess", "2.6.0", "bonita-log-2.6.1.jar");
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
        Map<String, Object> result = new HashMap<>();
        String operation = (String) information.get("operation");
        HashMap<String, Serializable> parameters = new HashMap<>();
        BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(MilkCmdControl.cstCommandName);

        if ("status".equals(operation)) {
            // reset scheduler must be send to the command
            result = bonitaCommand.callCommand(MilkCmdControl.VERBE.SCHEDULERSTATUS.toString(), parameters, tenantId, commandAPI);
        }else if ("deploy".equals(operation)) {
            parameters.put(MilkCmdControl.CST_PAGE_DIRECTORY, pageDirectory.getAbsolutePath());
            result = bonitaCommand.callCommand(MilkCmdControl.VERBE.SCHEDULERDEPLOY.toString(), parameters, tenantId, commandAPI);
        }

        else if ("reset".equals(operation)) {
            // reset scheduler must be send to the command
            result = bonitaCommand.callCommand(MilkCmdControl.VERBE.SCHEDULERRESET.toString(), parameters, tenantId, commandAPI);
        }

        else if ("changescheduler".equals(operation)) {
            for (Entry<String, Object> entry : information.entrySet())
                parameters.put( entry.getKey(), (Serializable) entry.getValue());
            parameters.put(MilkConstantJson.cstSchedulerChangeType, (String) information.get("newscheduler"));
            result = bonitaCommand.callCommand(MilkCmdControl.VERBE.SCHEDULERCHANGE.toString(), parameters, tenantId, commandAPI);
        } else {
            parameters.put(MilkConstantJson.cstSchedulerOperation, operation);
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
