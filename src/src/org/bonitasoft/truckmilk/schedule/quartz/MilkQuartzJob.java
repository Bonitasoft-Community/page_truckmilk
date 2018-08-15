package org.bonitasoft.truckmilk.schedule.quartz;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.CommandAPI;
import org.bonitasoft.engine.command.CommandCriterion;
import org.bonitasoft.engine.command.CommandDescriptor;
import org.bonitasoft.engine.connector.ConnectorAPIAccessorImpl;
import org.bonitasoft.engine.events.model.SFireEventException;
import org.bonitasoft.engine.scheduler.StatelessJob;
import org.bonitasoft.engine.scheduler.exception.SJobConfigurationException;
import org.bonitasoft.engine.scheduler.exception.SJobExecutionException;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.TypeStatus;

/**
 * attention, the package / name is copy in a STRING in MilkScheduleQuartz
 * The class are generated in an independent jar file to avoid a limitation in Bonitasoft.
 * Bonitasoft quartz can't call a dynamic class, and the JAR has to be deployed in the web-inf/lib.
 * So the goal of this class to to have a simple job, which immediately call the command. All the control are in the command (to stop / restart Quartz for
 * example).
 * Hopefully, this class will exist one day in the BonitaEngine.
 */
public class MilkQuartzJob implements StatelessJob {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private static Logger logger = Logger.getLogger(MilkQuartzJob.class.getName());
    private static String logHeader = "MilkQuartzJob ~~ ";

    private final static String QuartzMilkJobName = "MilkQuartJob Trigger ~~~~";

    /**
     * this constant is defined too in MilkCmdControl to have an independent JAR
     */
    public static String cstVerb = "verb";
    /**
     * this constant is defined too in MilkCmdControl to have an independent JAR
     */
    public static String cstTenantId = "tenantId";

    /**
     * this enum is defined too in MilkCmdControl to have an independent JAR
     */
    public enum VERBE {
        PING, INITALINFORMATION, REFRESH, DEPLOYPLUGIN, DELETEPLUGIN, ADDTOUR, REMOVETOUR, STOPTOUR, STARTTOUR, UPDATETOUR, SCHEDULER, SCHEDULERRESET, SCHEDULERSTATUS, HEARTBEAT
    };

    /**
     * this constant is defined too in MilkCmdControl to have an independent JAR
     */
    public static String cstCommandName = "truckmilk";

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Abstract for Quartz */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public String getName() {
        return QuartzMilkJobName;
    }

    public String getDescription() {
        return "TruckMilk execution";
    }

    public final static String cstParamTenantId = "tenantId";
    private Long tenantId = 1L;

    /**
     * the scheduler give me back the parameters here
     */
    public void setAttributes(Map<String, Serializable> attributes) throws SJobConfigurationException {
        logger.info(logHeader + " Quartz.setAttributes");

        if (attributes == null)
            logger.info(logHeader + " quartz Attribut is NULL set tenant to 1");
        else
            tenantId = (Long) attributes.get(cstParamTenantId);
    }

    /**
     * call immediately the command on verb HEARTBEAT
     */
    public void execute() throws SJobExecutionException, SFireEventException {
        logger.info(logHeader + " Time to execute Jobs[" + QuartzMilkJobName + "]");

        //Quartz ask the job is very fast, else it broke it. So, play the job asynchrounly
        HeartBeatThread HeartBeatThread = new HeartBeatThread(tenantId);
        HeartBeatThread.start();

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Quartz ask the job is very fast, else it broke it. So, play the job asynchrounly */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static class HeartBeatThread extends Thread {

        private long tenantId;

        HeartBeatThread(long tenantId) {
            this.tenantId = tenantId;
        }

        public void run() {
            logger.info(logHeader + " HeartBeat Asynchone : call the command [" + cstCommandName + "]");

            // call the command
            ConnectorAPIAccessorImpl connectorAccessorAPI = new ConnectorAPIAccessorImpl(tenantId);
            CommandAPI commandAPI = connectorAccessorAPI.getCommandAPI();

            CommandDescriptor command = getCommandByName(cstCommandName, commandAPI);
            if (command == null) {
                logger.info(logHeader + " No Command[" + cstCommandName + "] deployed, stop");
                return;
            }

            // shot one
            Map<String, Serializable> parameters = new HashMap<String, Serializable>();

            try {
                parameters.put(cstTenantId, tenantId);
                parameters.put(cstVerb, VERBE.HEARTBEAT.toString());

                // see the command in CmdMeteor
                final Serializable resultCommand = commandAPI.execute(command.getId(), parameters);
                logger.info(logHeader + "Command[" + cstCommandName + "] call, result ["
                        + (resultCommand != null ? resultCommand.toString() : "null") + "]");

            } catch (final Exception e) {

                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String exceptionDetails = sw.toString();

                logger.severe(logHeader + "~~~~~~~~~~  : ERROR Command[" + command.getId() + "] Verb["
                        + parameters.get(cstVerb) + "] " + e + " at " + exceptionDetails);
            }
        } // end start

        protected static CommandDescriptor getCommandByName(String commandName, CommandAPI commandAPI) {
            List<CommandDescriptor> listCommands = commandAPI.getAllCommands(0, 1000, CommandCriterion.NAME_ASC);
            for (CommandDescriptor command : listCommands) {
                if (commandName.equals(command.getName()))
                    return command;
            }
            return null;

        }
    }
}
