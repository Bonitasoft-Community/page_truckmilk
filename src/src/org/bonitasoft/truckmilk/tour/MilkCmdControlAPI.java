package org.bonitasoft.truckmilk.tour;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.CommandAPI;
import org.bonitasoft.engine.api.PlatformAPI;
import org.bonitasoft.engine.builder.BuilderFactory;
import org.bonitasoft.engine.command.CommandCriterion;
import org.bonitasoft.engine.command.CommandDescriptor;
import org.bonitasoft.engine.command.CommandNotFoundException;
import org.bonitasoft.engine.core.process.definition.model.SFlowNodeType;
import org.bonitasoft.engine.core.process.definition.model.SProcessDefinition;
import org.bonitasoft.engine.core.process.definition.model.event.SEventDefinition;
import org.bonitasoft.engine.core.process.definition.model.event.SStartEventDefinition;
import org.bonitasoft.engine.core.process.instance.model.SFlowElementsContainerType;
import org.bonitasoft.engine.core.process.instance.model.event.SCatchEventInstance;
import org.bonitasoft.engine.exception.AlreadyExistsException;
import org.bonitasoft.engine.exception.CreationException;
import org.bonitasoft.engine.exception.DeletionException;
import org.bonitasoft.engine.execution.job.JobNameBuilder;
import org.bonitasoft.engine.jobs.TriggerTimerEventJob;
import org.bonitasoft.engine.platform.StartNodeException;
import org.bonitasoft.engine.platform.StopNodeException;
import org.bonitasoft.engine.scheduler.builder.SJobDescriptorBuilderFactory;
import org.bonitasoft.engine.scheduler.builder.SJobParameterBuilderFactory;
import org.bonitasoft.engine.scheduler.model.SJobDescriptor;
import org.bonitasoft.engine.scheduler.model.SJobParameter;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.schedule.MilkScheduleQuartz;
import org.bonitasoft.truckmilk.schedule.MilkScheduleThreadSleep;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerFactory;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.TypeScheduler;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.TypeStatus;

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
    /* API */
    /*                                                                                  */
    /* ******************************************************************************** */

    public List<BEvent> checkAndDeployCommand(File pageDirectory, CommandAPI commandAPI, PlatformAPI platFormAPI,
            long tenantId) {
        String message = "";
        boolean forceDeploy = true;
        File fileJar = new File(pageDirectory.getAbsolutePath() + "/lib/TruckMilk-1.0-Page.jar");;
        String signature = getSignature(fileJar);

        message += "CommandFile[" + fileJar.getAbsolutePath() + "],Signature[" + signature + "]";

        // so no need to have a force deploy here.
        DeployStatus deployStatus = deployCommand(forceDeploy, signature, fileJar, pageDirectory,
                commandAPI, platFormAPI);

        message += "Deployed ?[" + deployStatus.newDeployment + "], Success?["
                + BEventFactory.isError(deployStatus.listEvents) + "]";

        // ping the factory
        if (!BEventFactory.isError(deployStatus.listEvents)) {
            Map<String, Object> resultPing = ping(commandAPI, tenantId);
            if (!"OK".equals(resultPing.get("status"))) {
                message += "Ping : [Error]";
                deployStatus.listEvents.add(eventPingError);
            }

        }
        logger.info(logHeader + message);
        return deployStatus.listEvents;
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
        if ("deploy".equals(operation)) {
            operationManaged = true;
            Map<String, Serializable> parameters = new HashMap<String, Serializable>();
            parameters.put(MilkCmdControl.cstVerb, MilkCmdControl.VERBE.SCHEDULERDEPLOY.toString());
            parameters.put(MilkCmdControl.cstTenantId, tenantId);
            parameters.put(MilkCmdControl.cstPageDirectory, pageDirectory.getAbsolutePath());
            
            result = callCommand(parameters, commandAPI);

            // override the list event
            result.put(MilkCmdControl.cstResultListEvents, BEventFactory.getHtml(listEvents));
        }

        if ("reset".equals(operation)) {
            operationManaged = true;
            // reset scheduler must be send to the command
            Map<String, Serializable> parameters = new HashMap<String, Serializable>();
            parameters.put(MilkCmdControl.cstVerb, MilkCmdControl.VERBE.SCHEDULERRESET.toString());
            parameters.put(MilkCmdControl.cstTenantId, tenantId);
            return callCommand(parameters, commandAPI);
        }
        
        if ("status".equals(operation)) {
            operationManaged = true;
            Map<String, Serializable> parameters = new HashMap<String, Serializable>();
            parameters.put(MilkCmdControl.cstVerb, MilkCmdControl.VERBE.SCHEDULERSTATUS.toString());
            parameters.put(MilkCmdControl.cstTenantId, tenantId);
            result = callCommand(parameters, commandAPI);
        }
        
        if ("changescheduler".equals(operation)) {
            operationManaged = true;
            Map<String, Serializable> parameters = new HashMap<String, Serializable>();
            parameters.put(MilkCmdControl.cstVerb, MilkCmdControl.VERBE.SCHEDULERCHANGE.toString());
            parameters.put(MilkCmdControl.cstSchedulerChangeType, (String) information.get("newscheduler"));
            parameters.put(MilkCmdControl.cstTenantId, tenantId);
            result = callCommand(parameters, commandAPI);
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

    public Map<String, Object> ping(CommandAPI commandAPI, long tenantId) {
        Map<String, Serializable> parameters = new HashMap<String, Serializable>();
        parameters.put(MilkCmdControl.cstVerb, MilkCmdControl.VERBE.PING.toString());
        parameters.put(MilkCmdControl.cstTenantId, tenantId);
        return callCommand(parameters, commandAPI);
    }

    /**
     * get all initial information
     * 
     * @return
     */
    public Map<String, Object> getInitialInformation(CommandAPI commandAPI, long tenantId) {
        Map<String, Serializable> parameters = new HashMap<String, Serializable>();
        parameters.put(MilkCmdControl.cstVerb, MilkCmdControl.VERBE.INITALINFORMATION.toString());
        parameters.put(MilkCmdControl.cstTenantId, tenantId);
        return callCommand(parameters, commandAPI);
    }

    public Map<String, Object> getRefreshInformation(CommandAPI commandAPI, long tenantId) {
        Map<String, Serializable> parameters = new HashMap<String, Serializable>();
        parameters.put(MilkCmdControl.cstVerb, MilkCmdControl.VERBE.REFRESH.toString());
        parameters.put(MilkCmdControl.cstTenantId, tenantId);
        return callCommand(parameters, commandAPI);
    }

    public Map<String, Object> callTourOperation(MilkCmdControl.VERBE verbe, Map<String, Object> information,
            CommandAPI commandAPI, long tenantId) {
        Map<String, Serializable> parameters = new HashMap<String, Serializable>();
        for (String key : information.keySet())
            if (information.get(key) != null)
                parameters.put(key, (Serializable) information.get(key));

        parameters.put(MilkCmdControl.cstVerb, verbe.toString());
        parameters.put(MilkCmdControl.cstTenantId, tenantId);
        return callCommand(parameters, commandAPI);
    }

    /**
     * The internal command call
     * 
     * @param parameters
     * @param commandAPI
     * @return
     */
    private Map<String, Object> callCommand(Map<String, Serializable> parameters, CommandAPI commandAPI) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        Map<String, Object> resultCommandHashmap = new HashMap<String, Object>();

        final CommandDescriptor command = getCommandByName(MilkCmdControl.cstCommandName, commandAPI);
        if (command == null) {
            logger.info(logHeader + "~~~~~~~~~~ TruckMilk.start() No Command[" + MilkCmdControl.cstCommandName
                    + "] deployed, stop");
            listEvents.add(EventNotDeployed);
            resultCommandHashmap.put(MilkCmdControl.cstResultListEvents, BEventFactory.getHtml(listEvents));
            return resultCommandHashmap;
        }

        try {

            // see the command in CmdMeteor
            logger.info(logHeader + "~~~~~~~~~~ Call Command[" + command.getId() + "] Verb["
                    + parameters.get(MilkCmdControl.cstVerb) + "]");
            final Serializable resultCommand = commandAPI.execute(command.getId(), parameters);

            resultCommandHashmap = (Map<String, Object>) resultCommand;

        } catch (final Exception e) {

            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();

            logger.severe(logHeader + "~~~~~~~~~~  : ERROR Command[" + command.getId() + "] Verb["
                    + parameters.get(MilkCmdControl.cstVerb) + "] " + e + " at " + exceptionDetails);
            listEvents.add(new BEvent(EVENT_CALL_COMMAND, e, ""));
        }
        if (listEvents.size() != 0)
            resultCommandHashmap.put(MilkCmdControl.cstResultListEvents, BEventFactory.getHtml(listEvents));
        logger.info(logHeader + "~~~~~~~~~~ : END Command[" + command.getId() + "] Verb["
                + parameters.get(MilkCmdControl.cstVerb) + "]" + resultCommandHashmap);
        return resultCommandHashmap;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Command Management to deploy the MickCmdTour */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    protected static CommandDescriptor getCommandByName(String commandName, CommandAPI commandAPI) {
        List<CommandDescriptor> listCommands = commandAPI.getAllCommands(0, 1000, CommandCriterion.NAME_ASC);
        for (CommandDescriptor command : listCommands) {
            if (commandName.equals(command.getName()))
                return command;
        }
        return null;

    }

    /**
     * 
     *
     */
    public static class JarDependencyCommand {

        public String jarName;
        public File pageDirectory;

        public JarDependencyCommand(final String name, File pageDirectory) {
            this.jarName = name;
            this.pageDirectory = pageDirectory;
        }

        public String getCompleteFileName() {
            return pageDirectory.getAbsolutePath() + "/lib/" + jarName;
        }
    }

    /**
     * @param name
     * @param pageDirectory
     * @return
     */
    private static JarDependencyCommand getInstanceJarDependencyCommand(final String name, File pageDirectory) {
        return new JarDependencyCommand(name, pageDirectory);
    }

    /**
     * Deploy the command
     * 
     * @param forceDeploy
     * @param version
     * @param fileJar
     * @param pageDirectory
     * @param commandAPI
     * @param platFormAPI
     * @return
     */
    private static class DeployStatus {

        List<BEvent> listEvents = new ArrayList<BEvent>();;
        boolean newDeployment = false;
    }

    private static DeployStatus deployCommand(final boolean forceDeploy, final String version, File fileJar,
            File pageDirectory, final CommandAPI commandAPI, final PlatformAPI platFormAPI) {
        // String commandName, String commandDescription, String className,
        // InputStream inputStreamJarFile, String jarName, ) throws IOException,
        // AlreadyExistsException, CreationException, CommandNotFoundException,
        // DeletionException {
        DeployStatus deployStatus = new DeployStatus();

        List<JarDependencyCommand> jarDependencies = new ArrayList<JarDependencyCommand>();
        // execute the groovy scenario
        /*
         * jarDependencies.add( CmdMeteor.getInstanceJarDependencyCommand(
         * "bdm-jpql-query-executor-command-1.0.jar", pageDirectory));
         * jarDependencies.add( CmdMeteor.getInstanceJarDependencyCommand(
         * "process-starter-command-1.0.jar", pageDirectory));
         * jarDependencies.add( CmdMeteor.getInstanceJarDependencyCommand(
         * "scenario-utils-2.0.jar", pageDirectory));
         */
        // execute the meteor command
        jarDependencies.add(getInstanceJarDependencyCommand(fileJar.getName(), pageDirectory));
        jarDependencies.add(getInstanceJarDependencyCommand("bonita-event-1.1.0.jar", pageDirectory));
        jarDependencies.add(getInstanceJarDependencyCommand("bonita-properties-1.6.jar", pageDirectory));
        // build it process use the mail system : register it in the command then
        jarDependencies.add(getInstanceJarDependencyCommand("mail-1.5.0-b01.jar", pageDirectory));
        jarDependencies.add(getInstanceJarDependencyCommand("activation-1.1.jar", pageDirectory));

        String message = "";

        try {
            // pause the engine to deploy a command
            if (platFormAPI != null) {
                platFormAPI.stopNode();
            }

            final List<CommandDescriptor> listCommands = commandAPI.getAllCommands(0, 1000, CommandCriterion.NAME_ASC);
            for (final CommandDescriptor command : listCommands) {
                if (MilkCmdControl.cstCommandName.equals(command.getName())) {
                    final String description = command.getDescription();
                    if (!forceDeploy && description.startsWith("V " + version)) {
                        logger.info("TruckMilk.cmd >>>>>>>>>>>>>>>>>>>>>>>>> No deployment Command ["
                                + MilkCmdControl.cstCommandName
                                + "] Version[V " + version + "]");

                        deployStatus.listEvents.add(new BEvent(EVENT_ALREADY_DEPLOYED, "V " + version));
                        deployStatus.newDeployment = false;
                        return deployStatus;
                    }

                    commandAPI.unregister(command.getId());
                }
            }
            logger.info(logHeader + " >>>>>>>>>>>>>>>>>>>>>>>>> DEPLOIEMENT Command [" + MilkCmdControl.cstCommandName
                    + "] Version[V "
                    + version + "]");

            // register globaly
            // MilkScheduleQuartz.deployDependency(fileJar );

            /*
             * File commandFile = new File(jarFileServer); FileInputStream fis =
             * new FileInputStream(commandFile); byte[] fileContent = new
             * byte[(int) commandFile.length()]; fis.read(fileContent);
             * fis.close();
             */
            for (final JarDependencyCommand onejar : jarDependencies) {
                final ByteArrayOutputStream fileContent = new ByteArrayOutputStream();
                final byte[] buffer = new byte[10000];
                int nbRead = 0;
                InputStream inputFileJar = null;
                try {
                    inputFileJar = new FileInputStream(onejar.getCompleteFileName());

                    while ((nbRead = inputFileJar.read(buffer)) > 0) {
                        fileContent.write(buffer, 0, nbRead);
                    }

                    commandAPI.removeDependency(onejar.jarName);
                } catch (final Exception e) {
                    logger.info(logHeader + " Remove dependency[" + e.toString() + "]");
                    message += "Exception remove[" + onejar.jarName + "]:" + e.toString();
                } finally {
                    if (inputFileJar != null)
                        inputFileJar.close();
                }
                //	              message += "Adding jarName [" + onejar.jarName + "] size[" + fileContent.size() + "]...";
                commandAPI.addDependency(onejar.jarName, fileContent.toByteArray());
                message += "+";
            }

            message += "Registering...";
            final CommandDescriptor commandDescriptor = commandAPI.register(MilkCmdControl.cstCommandName,
                    "V " + version + " " + MilkCmdControl.cstCommandDescription, MilkCmdControl.class.getName());

            if (platFormAPI != null) {
                platFormAPI.startNode();
            }

            deployStatus.listEvents.add(new BEvent(EventDeployedWithSuccess, message));
            deployStatus.newDeployment = true;
            return deployStatus;

        } catch (final StopNodeException e) {
            logger.severe("Can't stop  [" + e.toString() + "]");
            message += e.toString();
            deployStatus.listEvents.add(new BEvent(EventErrorAtDeployment, e,
                    "Command[" + MilkCmdControl.cstCommandName + "V " + version + " "
                            + MilkCmdControl.cstCommandDescription + "]"));
            return null;
        } catch (final StartNodeException e) {
            logger.severe("Can't  start [" + e.toString() + "]");
            message += e.toString();
            deployStatus.listEvents.add(new BEvent(EventErrorAtDeployment, e,
                    "Command[" + MilkCmdControl.cstCommandName + "V " + version + " "
                            + MilkCmdControl.cstCommandDescription + "]"));
            return null;
        } catch (final CommandNotFoundException e) {
            logger.severe("Error during deploy command " + e);
        } catch (final DeletionException e) {
            logger.severe("Error during deploy command " + e);
            deployStatus.listEvents.add(new BEvent(EventErrorAtDeployment, e,
                    "Command[" + MilkCmdControl.cstCommandName + "V " + version + " "
                            + MilkCmdControl.cstCommandDescription + "]"));
        } catch (final IOException e) {
            logger.severe("Error during deploy command " + e);
            deployStatus.listEvents.add(new BEvent(EventErrorAtDeployment, e,
                    "Command[" + MilkCmdControl.cstCommandName + "V " + version + " "
                            + MilkCmdControl.cstCommandDescription + "]"));
        } catch (final AlreadyExistsException e) {
            logger.severe("Error during deploy command " + e);
            deployStatus.listEvents.add(new BEvent(EventErrorAtDeployment, e,
                    "Command[" + MilkCmdControl.cstCommandName + "V " + version + " "
                            + MilkCmdControl.cstCommandDescription + "]"));
        } catch (final CreationException e) {
            logger.severe("Error during deploy command " + e);
            deployStatus.listEvents.add(new BEvent(EventErrorAtDeployment, e,
                    "Command[" + MilkCmdControl.cstCommandName + "V " + version + " "
                            + MilkCmdControl.cstCommandDescription + "]"));
        }
        return deployStatus;
    }

    /**
     * copy from the TImerEventHandlerStrategy.java
     * 
     * @param processDefinition
     * @param eventDefinition
     * @param eventInstance
     * @return
     */
    private List<SJobParameter> getJobParameters(Long flowNodeInstanceId, Long processDefinitionId,
            String eventDefinitionName, Long eventDefinitionId) {
        final List<SJobParameter> jobParameters = new ArrayList<SJobParameter>();
        jobParameters.add(BuilderFactory.get(SJobParameterBuilderFactory.class)
                .createNewInstance("processDefinitionId", processDefinitionId).done());
        jobParameters.add(BuilderFactory.get(SJobParameterBuilderFactory.class)
                .createNewInstance("containerType", SFlowElementsContainerType.PROCESS.name()).done());
        jobParameters.add(BuilderFactory.get(SJobParameterBuilderFactory.class)
                .createNewInstance("eventType", eventDefinitionName).done());
        jobParameters.add(BuilderFactory.get(SJobParameterBuilderFactory.class)
                .createNewInstance("targetSFlowNodeDefinitionId", eventDefinitionId).done());
        jobParameters.add(BuilderFactory.get(SJobParameterBuilderFactory.class)
                .createNewInstance("flowNodeInstanceId", flowNodeInstanceId).done());

        return jobParameters;
    }

    /**
     * return the job timer name
     * 
     * @param processDefinitionId
     * @param sCatchEventInstance
     * @param sEventDefinition
     * @return
     */
    private String getTimerEventJobName(long processDefinitionId, SEventDefinition sEventDefinition,
            SCatchEventInstance sCatchEventInstance) {
        return JobNameBuilder.getTimerEventJobName(processDefinitionId, sEventDefinition, sCatchEventInstance);
    }

    /**
     * copy from the TImerEventHandlerStrategy.java
     * 
     * @param processDefinition
     * @param eventDefinition
     * @param eventInstance
     * @return
     */
    private List<SJobParameter> getJobParameters(final SProcessDefinition processDefinition,
            final SEventDefinition eventDefinition, final SCatchEventInstance eventInstance) {
        final List<SJobParameter> jobParameters = new ArrayList<SJobParameter>();
        jobParameters.add(BuilderFactory.get(SJobParameterBuilderFactory.class)
                .createNewInstance("processDefinitionId", processDefinition.getId()).done());
        jobParameters.add(BuilderFactory.get(SJobParameterBuilderFactory.class)
                .createNewInstance("containerType", SFlowElementsContainerType.PROCESS.name()).done());
        jobParameters.add(BuilderFactory.get(SJobParameterBuilderFactory.class)
                .createNewInstance("eventType", eventDefinition.getType().name()).done());
        jobParameters.add(BuilderFactory.get(SJobParameterBuilderFactory.class)
                .createNewInstance("targetSFlowNodeDefinitionId", eventDefinition.getId()).done());
        if (SFlowNodeType.START_EVENT.equals(eventDefinition.getType())) {
            final SStartEventDefinition startEvent = (SStartEventDefinition) eventDefinition;
            jobParameters.add(BuilderFactory.get(SJobParameterBuilderFactory.class)
                    .createNewInstance("isInterrupting", startEvent.isInterrupting()).done());
        }
        if (eventInstance != null) {
            jobParameters.add(BuilderFactory.get(SJobParameterBuilderFactory.class)
                    .createNewInstance("flowNodeInstanceId", eventInstance.getId()).done());
        }
        return jobParameters;
    }

    /**
     * copy from the TImerEventHandlerStrategy.java
     */
    private SJobDescriptor getJobDescriptor(final String jobName) {
        return BuilderFactory.get(SJobDescriptorBuilderFactory.class)
                .createNewInstance(TriggerTimerEventJob.class.getName(), jobName, false).done();
    }

    /**
     * in order to know if the file change on the disk, we need to get a signature.
     * the date of the file is not enough in case of a cluster: the file is read in the database then save on the local disk. On a cluster, on each node, the
     * date
     * will be different then. So, a signature is the reliable information.
     * 
     * @param fileToGetSignature
     * @return
     */
    private String getSignature(File fileToGetSignature) {
        long timeStart = System.currentTimeMillis();
        String checksum = "";
        try {
            //Use MD5 algorithm
            MessageDigest md5Digest = MessageDigest.getInstance("MD5");

            //Get the checksum
            checksum = getFileChecksum(md5Digest, fileToGetSignature);

        } catch (Exception e) {
            checksum = "Date_" + String.valueOf(fileToGetSignature.lastModified());
        } finally {
            logger.info(logHeader + " CheckSum [" + fileToGetSignature.getName() + "] is [" + checksum + "] is "
                    + (timeStart - System.currentTimeMillis()) + " ms");
        }
        //see checksum
        return checksum;

    }

    /**
     * calulate the checksum
     * 
     * @param digest
     * @param file
     * @return
     * @throws IOException
     */
    private static String getFileChecksum(MessageDigest digest, File file) throws IOException {
        //Get file input stream for reading the file content
        FileInputStream fis = new FileInputStream(file);

        //Create byte array to read data in chunks
        byte[] byteArray = new byte[1024];
        int bytesCount = 0;

        //Read file data and update in message digest
        while ((bytesCount = fis.read(byteArray)) != -1) {
            digest.update(byteArray, 0, bytesCount);
        } ;

        //close the stream; We don't need it now.
        fis.close();

        //Get the hash's bytes
        byte[] bytes = digest.digest();

        //This bytes[] has bytes in decimal format;
        //Convert it to hexadecimal format
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
        }

        //return complete hash
        return sb.toString();
    }
}
