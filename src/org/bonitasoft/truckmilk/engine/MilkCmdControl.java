package org.bonitasoft.truckmilk.engine;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
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
import org.bonitasoft.command.BonitaCommand.ExecuteAnswer;
import org.bonitasoft.command.BonitaCommand.ExecuteParameters;
import org.bonitasoft.command.BonitaCommandApiAccessor;
import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.command.SCommandExecutionException;
import org.bonitasoft.engine.command.SCommandParameterizationException;
import org.bonitasoft.engine.connector.ConnectorAPIAccessorImpl;
import org.bonitasoft.engine.service.TenantServiceAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.ExecutionStatus;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugTourOutput;
import org.bonitasoft.truckmilk.engine.MilkJobFactory.CreateJobStatus;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerFactory;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.StatusScheduler;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.TypeStatus;

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

public class MilkCmdControl extends BonitaCommandApiAccessor {

    static Logger logger = Logger.getLogger(MilkCmdControl.class.getName());

    static String logHeader = "TruckMilkCommand ~~~";

    private static BEvent EVENT_INTERNAL_ERROR = new BEvent(MilkCmdControl.class.getName(), 1, Level.ERROR,
            "Internal error", "Internal error, check the log");

    private static BEvent EVENT_TOUR_REMOVED = new BEvent(MilkCmdControl.class.getName(), 2, Level.SUCCESS,
            "Tour removed", "Tour is removed with success");

    private static BEvent EVENT_TOUR_STARTED = new BEvent(MilkCmdControl.class.getName(), 4, Level.SUCCESS,
            "Tour started", "The Tour is now started");

    private static BEvent EVENT_TOUR_STOPPED = new BEvent(MilkCmdControl.class.getName(), 5, Level.SUCCESS,
            "Tour stopped", "The Tour is now stopped");

    private static BEvent EVENT_TOUR_UPDATED = new BEvent(MilkCmdControl.class.getName(), 6, Level.SUCCESS,
            "Tour updated", "The Tour is now updated");

    private static BEvent EVENT_TOUR_REGISTER = new BEvent(MilkCmdControl.class.getName(), 8, Level.SUCCESS,
            "Tour registered", "The Tour is now registered");

    private static BEvent EVENT_PLUGIN_VIOLATION = new BEvent(MilkCmdControl.class.getName(), 9, Level.ERROR,
            "Plug in violation",
            "A plug in must return a status on each execution. The plug in does not respect the contract",
            "No report is saved", "Contact the plug in creator");

    private static BEvent EVENT_PLUGIN_ERROR = new BEvent(MilkCmdControl.class.getName(), 10, Level.ERROR,
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

    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

    /**
     * this enum is defined too in MilkQuartzJob to have an independent JAR
     * INIT : this order does not exist for the command point of view
     * REFRESH : refresh only the Tour information, no check environment
     * GETSTATUS : refresh + check environment
     */
    public enum VERBE {
        GETSTATUS, REFRESH, CHECKUPDATEENVIRONMENT, DEPLOYPLUGIN, DELETEPLUGIN, ADDJOB, REMOVEJOB, STOPTOUR, STARTJOB, UPDATEJOB, IMMEDIATEJOB, ABORTJOB, RESETJOB, SCHEDULERSTARTSTOP, SCHEDULERDEPLOY, SCHEDULERRESET, SCHEDULERCHANGE, TESTBUTTONARGS, HEARTBEAT
    };

    public static String cstPageDirectory = "pagedirectory";
    // private static String cstResultTimeInMs = "TIMEINMS";

    private static String cstResultListPlugTour = "listplugtour";
    public static String cstResultListEvents = "listevents";

    public static String cstJsonDashboardEvents = "dashboardlistevents";
    public static String cstJsonDashboardSyntheticEvents = "dashboardsyntheticlistevents";
    public static String cstJsonScheduler = "scheduler";
    public static String cstJsonSchedulerType = "type";
    public static String cstJsonSchedulerInfo = "info";

    public static String cstJsonSchedulerStatus = "status";
    public static String cstJsonSchedulerStatus_V_RUNNING = "RUNNING";
    public static String cstJsonSchedulerStatus_V_SHUTDOWN = "SHUTDOWN";
    public static String cstJsonSchedulerStatus_V_STOPPED = "STOPPED";

    public static String cstSchedulerChangeType = "schedulerchangetype";
    public static String cstJsonListTypesSchedulers = "listtypeschedulers";
    public static String cstJsonLastHeartBeat = "lastheartbeat";

    public static String cstEnvironmentStatus = "ENVIRONMENTSTATUS";
    public static String cstEnvironmentStatus_V_CORRECT = "OK";
    public static String cstEnvironmentStatus_V_ERROR = "ERROR";

    public static String cstButtonName = "buttonName";

    // keep a list of 10 last executions
    private List<String> lastHeartBeat = new ArrayList<String>();

    /**
     * keep the scheduler Factory
     */

    private static MilkSchedulerFactory milkSchedulerFactory = MilkSchedulerFactory.getInstance();

    public static MilkCmdControl milkCmdControl = new MilkCmdControl();

    // let's return a singleton
    public BonitaCommandApiAccessor getInstance() {
        return milkCmdControl;
    }

    public static MilkCmdControl getStaticInstance() {
        return milkCmdControl;
    }

    /** Not the correct Command library if the call come here */
    public ExecuteAnswer executeCommandVerbe(String verbSt, Map<String, Serializable> parameters, TenantServiceAccessor serviceAccessor) {
        logger.severe(logHeader + "ERROR: the Command Library is not the correct one");

        ExecuteAnswer executeAnswer = new ExecuteAnswer();
        return executeAnswer;
        // return executeCommandVerbe(verbSt, parameters, serviceAccessor);
    }

    /**
     * Do all action after the deployement
     * 
     * @param tenantId
     * @return
     */
    public ExecuteAnswer afterDeployment(ExecuteParameters executeParameters, TenantServiceAccessor serviceAccessor) {
        ExecuteAnswer executeAnswer = new ExecuteAnswer();
        executeAnswer.listEvents = checkAndUpdateEnvironment(executeParameters.tenantId);
        executeAnswer.result.put("status", BEventFactory.isError(executeAnswer.listEvents) ? "FAIL" : "OK");
        return executeAnswer;
    }

    /**
     * check the environment of the command
     * 
     * @param tenantId
     * @return
     */
    public List<BEvent> checkAndUpdateEnvironment(long tenantId) {
        // this is the time to check the BonitaProperties tables
        MilkPlugInFactory milkPlugInFactory = MilkPlugInFactory.getInstance(tenantId);
        MilkJobFactory milkJobFactory = MilkJobFactory.getInstance(milkPlugInFactory, false);

        return milkJobFactory.checkAndUpdateEnvironment(tenantId);
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
    public ExecuteAnswer executeCommandApiAccessor(ExecuteParameters executeParameters, APIAccessor apiAccessor) {

        long currentTime = System.currentTimeMillis();
        ExecuteAnswer executeAnswer = new ExecuteAnswer();
        long startTime = System.currentTimeMillis();

        VERBE verbEnum = null;
        try {
            // ------------------- ping ?
            verbEnum = VERBE.valueOf(executeParameters.verb);
            logger.fine(logHeader + "command Verb2[" + verbEnum.toString() + "] Tenant[" + executeParameters.tenantId + "]");

            boolean addSchedulerStatus = false;

            if (milkSchedulerFactory.getScheduler() == null) {
                executeAnswer.listEvents.addAll(milkSchedulerFactory.startup(executeParameters.tenantId));
            }
            // initialise the factory ?
            MilkPlugInFactory milkPlugInFactory = null;
            MilkJobFactory milkJobFactory = null;
            

            // manage immediately the HEARTBEAT
            if (VERBE.HEARTBEAT.equals(verbEnum)) {
                executeAnswer.logAnswer = false; // no log on the HeartBeat, to not pollute the log
                if ((milkSchedulerFactory.getScheduler() != null)) {
                    StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                    if (statusScheduler.status == TypeStatus.STARTED)
                        executeOneTimeNewThread(executeParameters.tenantId);
                }
            } else {
                // initialise the factory only if this is not a heartbeat
                milkPlugInFactory = MilkPlugInFactory.getInstance(executeParameters.tenantId);
                milkJobFactory = MilkJobFactory.getInstance(milkPlugInFactory);
            }

            /**
             * According the verb, do the job
             */
            if (VERBE.GETSTATUS.equals(verbEnum) || VERBE.REFRESH.equals(verbEnum)) {
                addSchedulerStatus = false; // still add it, why not?

                executeAnswer.listEvents.addAll(initialization(VERBE.GETSTATUS.equals(verbEnum), false, executeParameters.tenantId, milkJobFactory));

                if (VERBE.GETSTATUS.equals(verbEnum)) {
                    addSchedulerStatus = true;
                }

                executeAnswer.result.put(cstResultListPlugTour, getListTourMap(milkJobFactory));

                List<Map<String, Object>> listPlugInMap = new ArrayList<Map<String, Object>>();
                for (MilkPlugIn plugin : milkPlugInFactory.getListPlugIn()) {
                    listPlugInMap.add(plugin.getMap());
                }
                executeAnswer.result.put("listplugin", listPlugInMap);

            } else if (VERBE.CHECKUPDATEENVIRONMENT.equals(verbEnum)) {

                executeAnswer.listEvents.addAll(checkAndUpdateEnvironment(executeParameters.tenantId));

            } else if (VERBE.ADDJOB.equals(verbEnum)) {

                MilkPlugIn plugIn = milkPlugInFactory.getPluginFromName(executeParameters.getParametersString("plugin"));
                if (plugIn == null) {
                    logger.severe(logHeader + "No tour found with name[" + executeParameters.getParametersString("plugin") + "]");
                    executeAnswer.listEvents.add(new BEvent(EVENT_INTERNAL_ERROR, "No tour found with name[" + executeParameters.getParametersString("plugin") + "]"));

                    return null;
                }
                String tourName = executeParameters.getParametersString("name");
                logger.info(logHeader + "Add tourName[" + tourName + "] PlugIn[" + executeParameters.getParametersString("plugin") + "]");
                
                CreateJobStatus createTourStatus = milkJobFactory.createPlugTour(tourName, plugIn);
                executeAnswer.listEvents.addAll( createTourStatus.listEvents );
                if (!BEventFactory.isError(executeAnswer.listEvents)) {
                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(createTourStatus.job, false));
                    if (!BEventFactory.isError(executeAnswer.listEvents))
                        executeAnswer.listEvents.add(new BEvent(EVENT_TOUR_REGISTER, "Tour registered[" + createTourStatus.job.getName() + "]"));
                }
                // get all lists            
                executeAnswer.result.put(cstResultListPlugTour, getListTourMap(milkJobFactory));

            } else if (VERBE.REMOVEJOB.equals(verbEnum)) {

                Long idJob = executeParameters.getParametersLong("id");
                if (idJob == null) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else {
                    MilkJob milkJob = getJobById(idJob, milkJobFactory);
                    executeAnswer.listEvents.addAll(removeTour(idJob, executeParameters.tenantId, milkJobFactory));

                    if (!BEventFactory.isError(executeAnswer.listEvents)) {
                        executeAnswer.listEvents.add(new BEvent(EVENT_TOUR_REMOVED, "Tour removed[" + milkJob.getName() + "]"));
                    }
                }
                executeAnswer.result.put(cstResultListPlugTour, getListTourMap(milkJobFactory));

            } else if (VERBE.STARTJOB.equals(verbEnum) || VERBE.STOPTOUR.equals(verbEnum)) {

                Long idJob = executeParameters.getParametersLong("id");
                if (idJob == null) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else {
                    MilkJob milkJob = getJobById(idJob, milkJobFactory);

                    if (milkJob != null) {
                        // save parameters
                        Map<String, Object> parametersObject = executeParameters.getParametersMap("parametersvalue");
                        if (parametersObject != null)
                            milkJob.setTourParameters(parametersObject);
                        String cronSt = executeParameters.getParametersString("cron");
                        if (cronSt != null)
                            milkJob.setCron(cronSt);

                        milkJob.setEnable(VERBE.STARTJOB.equals(verbEnum));
                        executeAnswer.listEvents.addAll(milkJob.calculateNextExecution());
                        executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, false));
                        if (VERBE.STARTJOB.equals(verbEnum))
                            executeAnswer.listEvents.add(new BEvent(EVENT_TOUR_STARTED, "Tour Activated[" + milkJob.getName() + "]"));
                        else
                            executeAnswer.listEvents.add(new BEvent(EVENT_TOUR_STOPPED, "Tour Deactived[" + milkJob.getName() + "]"));

                        executeAnswer.result.put("enable", milkJob.isEnable);
                        executeAnswer.result.put("tour", milkJob.getMap(true));
                    } else
                        executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "TourID[" + idJob + "]"));
                }
            }

            else if (VERBE.UPDATEJOB.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                if (idJob == null) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else {
                    logger.info(logHeader + "Update tour [" + idJob + "]");
                    MilkJob milkJob = getJobById(idJob, milkJobFactory);
                    boolean saveFileRead = false;
                    if (milkJob != null) {

                        // this is maybe a call only to update a file parameters
                        if (executeParameters.parametersCommand.containsKey("file")) {
                            String pageDirectory = executeParameters.getParametersString(cstPageDirectory);
                            File pageDirectoryFile = new File(pageDirectory);
                            // Yes
                            String fileName = executeParameters.getParametersString("file");
                            String parameterName = executeParameters.getParametersString("parameter");
                            executeAnswer.listEvents.addAll(milkJob.setTourFileParameter(parameterName, fileName, pageDirectoryFile));
                            saveFileRead = true;
                        } else {

                            Map<String, Object> parametersObject = executeParameters.getParametersMap("parametersvalue");
                            String cronSt = executeParameters.getParametersString( MilkJob.cstJsonCron);
                            milkJob.setTourParameters(parametersObject);
                            milkJob.setCron(cronSt);
                            milkJob.setHostsRestriction( executeParameters.getParametersString( MilkJob.cstJsonHostsRestriction));
                            milkJob.setDescription(executeParameters.getParametersString("description"));
                            String newName = executeParameters.getParametersString("newname");
                            milkJob.setName(newName);
                        }
                        executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, saveFileRead));
                        executeAnswer.listEvents.add(new BEvent(EVENT_TOUR_UPDATED, "Tour updated[" + milkJob.getName() + "]"));

                    } else
                        executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "Tour[" + idJob + "]"));
                    executeAnswer.result.put(cstResultListPlugTour, getListTourMap(milkJobFactory));
                }

            } else if (VERBE.IMMEDIATEJOB.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                if (idJob == null) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else {
                    MilkJob milkJob = getJobById(idJob, milkJobFactory);

                    if (milkJob != null) {
                        milkJob.setImmediateExecution(true);
                        executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, false));
                        executeAnswer.listEvents.add(new BEvent(EVENT_TOUR_UPDATED, "Tour updated[" + milkJob.getName() + "]"));
                    } else {
                        executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "Tour[" + idJob + "]"));
                    }
                    executeAnswer.result.put(cstResultListPlugTour, getListTourMap(milkJobFactory));
                }
            } else if (VERBE.ABORTJOB.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                if (idJob == null) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else {
                    MilkJob milkJob = getJobById(idJob, milkJobFactory);

                    if (milkJob != null) {
                        milkJob.setAskForStop(true);
                        executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, false));
                        executeAnswer.listEvents.add(new BEvent(EVENT_TOUR_UPDATED, "Tour updated[" + milkJob.getName() + "]"));
                    } else {
                        executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "Tour[" + idJob + "]"));
                    }
                    executeAnswer.result.put(cstResultListPlugTour, getListTourMap(milkJobFactory));
                }
            } else if (VERBE.RESETJOB.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                if (idJob == null) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else {
                    MilkJob milkJob = getJobById(idJob, milkJobFactory);

                    if (milkJob != null) {
                        milkJob.lastExecutionDate = new Date();
                        milkJob.lastExecutionStatus = ExecutionStatus.KILL;
                        milkJob.trackExecution.inExecution = false;

                        executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, false));
                        executeAnswer.listEvents.add(new BEvent(EVENT_TOUR_UPDATED, "Tour updated[" + milkJob.getName() + "]"));
                    } else {
                        executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "Job[" + idJob + "]"));
                    }
                    executeAnswer.result.put(cstResultListPlugTour, getListTourMap(milkJobFactory));
                }

            
            } else if (VERBE.TESTBUTTONARGS.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                if (idJob == null) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else {
                    MilkJob milkJob = getJobById(idJob, milkJobFactory);

                    if (milkJob != null) {
                        String buttonName = executeParameters.getParametersString(cstButtonName);
                        Map<String, Object> parametersObject = executeParameters.getParametersMap("parametersvalue");
                        milkJob.setTourParameters(parametersObject);

                        Map<String, Object> argsParameters = executeParameters.getParametersMap("args");

                        // execute it!
                        MilkJobExecution plugTourInput = new MilkJobExecution(milkJob);

                        MySimpleTestThread buttonThread = new MySimpleTestThread(executeParameters.tenantId, buttonName, milkJob, plugTourInput, argsParameters);

                        buttonThread.start();
                        int count = 0;
                        while (!buttonThread.isFinish && count < 1000) {
                            count++;
                            Thread.sleep(1000);
                        }
                        if (buttonThread.isFinish)
                            executeAnswer.listEvents.addAll(buttonThread.listEvents);
                        else
                            executeAnswer.listEvents.add(new BEvent(EVENT_BUTTONARG_FAILED, "No answer"));

                    }
                }
            }

            else if (VERBE.SCHEDULERSTARTSTOP.equals(verbEnum)) {
                addSchedulerStatus = true; // still add it, why not?
                Boolean startScheduler = executeParameters.getParametersBoolean("start");
                logger.info(logHeader + "SchedulerStartStop requested[" + startScheduler + "] - ");
                if (startScheduler == null && "true".equals(executeParameters.parametersCommand.get("start")))
                    startScheduler = true;
                if (milkSchedulerFactory.getScheduler() != null && startScheduler != null) {
                    if (startScheduler) {
                        synchronizeHeart.heartBeatInProgress = false; // prevention, reset it to false
                        executeAnswer.listEvents.addAll(milkSchedulerFactory.getScheduler().start(executeParameters.tenantId));
                    } else
                        executeAnswer.listEvents.addAll(milkSchedulerFactory.getScheduler().stop(executeParameters.tenantId));
                }
                StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                executeAnswer.result.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
                // no need to add the event: it will be done by the getEvent after

            } else if (VERBE.SCHEDULERDEPLOY.equals(verbEnum)) {
                String pageDirectory = executeParameters.getParametersString(cstPageDirectory);
                File pageDirectoryFile = new File(pageDirectory);
                // now ask the deployment
                MilkSchedulerInt scheduler = milkSchedulerFactory.getScheduler();
                executeAnswer.listEvents.addAll(scheduler.checkAndDeploy(true, pageDirectoryFile, executeParameters.tenantId));
                // then reset it
                executeAnswer.listEvents.addAll(milkSchedulerFactory.getScheduler().reset(executeParameters.tenantId));
                if (!BEventFactory.isError(executeAnswer.listEvents)) {
                    executeAnswer.listEvents.add(eventSchedulerResetSuccess);
                }
                // return the status
                StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                executeAnswer.result.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
                executeAnswer.listEvents.addAll(statusScheduler.listEvents);

            } else if (VERBE.SCHEDULERRESET.equals(verbEnum)) {

                executeAnswer.listEvents.addAll(milkSchedulerFactory.getScheduler().reset(executeParameters.tenantId));

                // setup the isInProgress to false, to accept a new heartBeat
                synchronizeHeart.heartBeatInProgress = false;

                if (!BEventFactory.isError(executeAnswer.listEvents)) {
                    executeAnswer.listEvents.add(eventSchedulerResetSuccess);
                }
                StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                executeAnswer.result.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
                executeAnswer.listEvents.addAll(statusScheduler.listEvents);

            } else if (VERBE.SCHEDULERCHANGE.equals(verbEnum)) {
                MilkSchedulerFactory milkSchedulerFactory = MilkSchedulerFactory.getInstance();
                String newSchedulerChange = executeParameters.getParametersString(cstSchedulerChangeType);
                executeAnswer.listEvents.addAll(milkSchedulerFactory.changeTypeScheduler(newSchedulerChange, executeParameters.tenantId));

                // get information on the new Scheduler then
                if ((!BEventFactory.isError(executeAnswer.listEvents))) {
                    StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                    executeAnswer.result.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
                    executeAnswer.listEvents.addAll(statusScheduler.listEvents);
                }
            }

            //------------------------------ Check Environment
            if (addSchedulerStatus) {

                List<BEvent> listEvents = new ArrayList<BEvent>();

                // Schedule is part of any answer
                Map<String, Object> mapScheduler = new HashMap<String, Object>();
                if (milkSchedulerFactory.getScheduler() == null)
                    mapScheduler.put(cstJsonSchedulerStatus, cstJsonSchedulerStatus_V_STOPPED);
                else {
                    StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                    mapScheduler.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
                    listEvents.addAll(statusScheduler.listEvents);
                    mapScheduler.put(cstJsonSchedulerType, milkSchedulerFactory.getScheduler().getType().toString());
                    mapScheduler.put(cstJsonSchedulerInfo, milkSchedulerFactory.getScheduler().getDescription());
                }

                // Plug in
                List<MilkPlugIn> list = milkPlugInFactory.getListPlugIn();
                for (MilkPlugIn plugIn : list) {
                    listEvents.addAll(plugIn.checkEnvironment(executeParameters.tenantId, apiAccessor));
                }

                // filter then set status
                listEvents = BEventFactory.filterUnique(listEvents);

                mapScheduler.put(cstJsonDashboardEvents, BEventFactory.getHtml(listEvents));
                mapScheduler.put(cstJsonDashboardSyntheticEvents, BEventFactory.getSyntheticHtml(listEvents));

                mapScheduler.put(cstJsonListTypesSchedulers, milkSchedulerFactory.getListTypeScheduler());
                // return in scheduler the last heartBeat
                mapScheduler.put(cstJsonLastHeartBeat, lastHeartBeat);

                executeAnswer.result.put(cstJsonScheduler, mapScheduler);

                if (BEventFactory.isError(executeAnswer.listEvents))
                    executeAnswer.result.put(cstEnvironmentStatus, cstEnvironmentStatus_V_ERROR);
                else
                    executeAnswer.result.put(cstEnvironmentStatus, cstEnvironmentStatus_V_CORRECT);

            }

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(logHeader + "ERROR " + e + " at " + exceptionDetails);

            executeAnswer.listEvents.add(new BEvent(EVENT_INTERNAL_ERROR, e.getMessage()));
        } catch (Error er) {
            StringWriter sw = new StringWriter();
            er.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(logHeader + "ERROR " + er + " at " + exceptionDetails);

            executeAnswer.listEvents.add(new BEvent(EVENT_INTERNAL_ERROR, er.getMessage()));
        } finally {
            executeAnswer.result.put(cstResultTimeInMs, System.currentTimeMillis() - currentTime);
            executeAnswer.result.put(cstResultListEvents, BEventFactory.getHtml(executeAnswer.listEvents));
            if (VERBE.HEARTBEAT.equals(verbEnum))
                logger.fine(logHeader + "MilkTourCommand Verb[" + (verbEnum == null ? "null" : verbEnum.toString()) + "] Tenant["
                        + executeParameters.tenantId + "] Error?" + BEventFactory.isError(executeAnswer.listEvents) + " in "
                        + (System.currentTimeMillis() - startTime) + " ms");
            else
                logger.info(logHeader + "MilkTourCommand Verb[" + (verbEnum == null ? "null" : verbEnum.toString()) + "] Tenant["
                        + executeParameters.tenantId + "] Error?" + BEventFactory.isError(executeAnswer.listEvents) + " in "
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
    public synchronized List<BEvent> initialization(boolean forceReload, boolean forceSchedule, long tenantId, MilkJobFactory milkJobFactory) {
        List<BEvent> listEvents = new ArrayList<BEvent>();

        if (!isInitialized || forceReload) {
            //  load all PlugIn
            listEvents.addAll(milkJobFactory.getMilkPlugInFactory().getInitaliseStatus());
            // load all PlugTour
            listEvents.addAll(milkJobFactory.getInitialiseStatus());
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
    /** the object to synchronize must not change */
    public static class SynchronizeHeartBeat {

        Boolean heartBeatInProgress = false;
    }

    private static SynchronizeHeartBeat synchronizeHeart = new SynchronizeHeartBeat();

    private static long heartBeatLastExecution = System.currentTimeMillis();

    // to avoid any transaction issue in the command (a transaction is maybe opennend by the engine, and then the processAPI usage is forbiden), let's create a thread
    public void executeOneTimeNewThread(long tenantId) {
        synchronized (synchronizeHeart) {
            // protection : does not start a new Thread if the current one is not finish (no two Hearthread in the same time)
            if (synchronizeHeart.heartBeatInProgress) {
                logger.fine(logHeader + "heartBeat in progress, does not start a new one");
                return;
            }
            // second protection : Quartz can call the methode TOO MUCH !
            if (System.currentTimeMillis() < heartBeatLastExecution + 60 * 1000) {
                logger.fine(logHeader + "heartBeat in progress, does not start a new one");
                return;
            }
            synchronizeHeart.heartBeatInProgress = true;
            heartBeatLastExecution = System.currentTimeMillis();
        }
        // the end will be done by the tread

        MyTruckMilkHeartBeatThread mythread = new MyTruckMilkHeartBeatThread(this, tenantId);
        mythread.start();
    }

    public static class SynchronizeThreadId {

        public int countThreadId = 0;

    }

    public static SynchronizeThreadId synchronizeThreadId = new SynchronizeThreadId();

    /**
     * thread to execute in a different thread to have a new connection
     * 
     * @author Firstname Lastname
     */
    public class MyTruckMilkHeartBeatThread extends Thread {

        private MilkCmdControl cmdControl;
        private long tenantId;

        protected MyTruckMilkHeartBeatThread(MilkCmdControl cmdControl, long tenantId) {
            this.cmdControl = cmdControl;
            this.tenantId = tenantId;
        }

        public void run() {
            // New thread : create the new object
            MilkPlugInFactory milkPlugInFactory = MilkPlugInFactory.getInstance(tenantId);
            // the getInstance reload everything from the database
            MilkJobFactory milkJobFactory = MilkJobFactory.getInstance(milkPlugInFactory);

            cmdControl.doTheHeartBeat(milkJobFactory);

            synchronizeHeart.heartBeatInProgress = false;

        }
    }

    public int thisThreadId = 0;

    /**
     * execute the command
     * 
     * @param tenantId
     */
    public void doTheHeartBeat(MilkJobFactory milkJobFactory) {
        // we want to work as a singleton: if we already manage a Timer, skip this one (the previous one isn't finish)
        synchronized (synchronizeThreadId) {
            synchronizeThreadId.countThreadId++;
            thisThreadId = synchronizeThreadId.countThreadId;

        }
        long tenantId = milkJobFactory.getMilkPlugInFactory().getTenantId();
        // Advance the scheduler that we run now !
        milkSchedulerFactory.informRunInProgress(tenantId);

        // MilkPlugInFactory milkPlugInFactory = milkJobFactory.getMilkPlugInFactory();
        // maybe this is the first call after a restart ? 
        if (!isInitialized) {
            initialization(false, false, tenantId, milkJobFactory);
        }

        long timeBeginHearth = System.currentTimeMillis();
        Date currentDate = new Date();

        logger.fine("MickCmdControl.beathearth #" + thisThreadId + " : Start at " + sdf.format(currentDate));
        try {
            ConnectorAPIAccessorImpl connectorAccessorAPI = new ConnectorAPIAccessorImpl(tenantId);

            String executionDescription = "";

            // check all the Tour now
            for (MilkJob milkJob : getListJobs(milkJobFactory)) {
                // hostRestriction in place ? Do nothing then.
                if (! milkJob.isInsideHostsRestriction())
                    continue;
                
                if (milkJob.isEnable || milkJob.isImmediateExecution()) {
                    // protection : recalculate a date then
                    if (milkJob.nextExecutionDate == null)
                        milkJob.calculateNextExecution();

                    if (milkJob.isImmediateExecution() || milkJob.nextExecutionDate != null
                            && milkJob.nextExecutionDate.getTime() < currentDate.getTime()) {

                        if (milkJob.isImmediateExecution)
                            executionDescription += "(i)";
                        executionDescription += " " + milkJob.getName() + " ";

                        List<BEvent> listEvents = new ArrayList<BEvent>();
                        MilkPlugIn plugIn = milkJob.getPlugIn();
                        PlugTourOutput output = null;
                        try {

                            // execute it!
                            MilkJobExecution milkJobExecution = new MilkJobExecution(milkJob);
                            milkJob.setAskForStop(false);

                            // ----------------- Execution
                            long timeBegin = System.currentTimeMillis();
                            try {
                                milkJobExecution.start();
                                
                                // save the status in the database
                                listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, false));
                                
                                output = plugIn.execute(milkJobExecution, connectorAccessorAPI);
                                milkJobExecution.end();
                                // force the status ABORD status
                                if (milkJobExecution.pleaseStop() && ( output.executionStatus == ExecutionStatus.SUCCESS ||  output.executionStatus == ExecutionStatus.SUCCESSPARTIAL ))
                                    output.executionStatus = ExecutionStatus.SUCCESSABORT;
                            } catch (Exception e) {
                                if (output == null) {
                                    output = new PlugTourOutput(milkJob);
                                    output.addEvent(new BEvent(EVENT_PLUGIN_VIOLATION, "PlugIn[" + plugIn.getName() + "] Exception " + e.getMessage()));
                                    output.executionStatus = ExecutionStatus.CONTRACTVIOLATION;
                                }
                            }
                            long timeEnd = System.currentTimeMillis();

                            if (output == null) {
                                output = new PlugTourOutput(milkJob);
                                output.addEvent(new BEvent(EVENT_PLUGIN_VIOLATION, "PlugIn[" + plugIn.getName() + "]"));
                                output.executionStatus = ExecutionStatus.CONTRACTVIOLATION;
                            }
                            output.executionTimeInMs = (timeEnd - timeBegin);

                            executionDescription += "(" + output.executionStatus + ") " + output.nbItemsProcessed + " in " + output.executionTimeInMs + ";";

                        } catch (Exception e) {
                            output = new PlugTourOutput(milkJob);
                            output.addEvent(new BEvent(EVENT_PLUGIN_ERROR, e, "PlugIn[" + plugIn.getName() + "]"));
                            output.executionStatus = ExecutionStatus.ERROR;
                        }
                        if (output != null) {
                            // maybe the plugin forgot to setup the execution ? So set it.
                            if (output.executionStatus == ExecutionStatus.NOEXECUTION)
                                output.executionStatus = ExecutionStatus.SUCCESS;

                            milkJob.registerExecution(currentDate, output);
                            listEvents.addAll(output.getListEvents());
                        }
                        // calculate the next time
                        listEvents.addAll(milkJob.calculateNextExecution());
                        milkJob.setImmediateExecution(false);
                        listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, false));
                    }
                } // end isEnable

            }
            if (executionDescription.length() == 0)
                executionDescription = "No jobs executed;";
            executionDescription = sdf.format(currentDate) + ":" + executionDescription;

            lastHeartBeat.add(executionDescription);
            if (lastHeartBeat.size() > 10)
                lastHeartBeat.remove(0);

        } catch (Exception e) {
            logger.severe(logHeader + ".executeTimer: Exception " + e.getMessage());
        } catch (Error er) {
            logger.severe(logHeader + ".executeTimer: Error " + er.getMessage());
        }
        long timeEndHearth = System.currentTimeMillis();
        logger.info("MickCmdControl.beathearth #" + thisThreadId + " : Start at " + sdf.format(currentDate) + ", End in " + (timeEndHearth - timeBeginHearth) + " ms");

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Local method() */
    /*
     * /*
     */
    /* ******************************************************************************** */

    /**
     * return the tour Index by the name
     * 
     * @param name
     * @return
     */

    public MilkJob getJobById(Long id, MilkJobFactory milkJobFactory) {

        return milkJobFactory.getById(id);
    }

    public Collection<MilkJob> getListJobs(MilkJobFactory milkJobFactory) {
        return milkJobFactory.getMapJobsId().values();
    }

    /**
     * return a list ordered by name
     * 
     * @return
     */
    public List<Map<String, Object>> getListTourMap(MilkJobFactory milkJobFactory) {
        List<Map<String, Object>> listTourMap = new ArrayList<Map<String, Object>>();

        for (MilkJob milkJob : getListJobs(milkJobFactory)) {

            milkJob.checkByPlugIn();

            listTourMap.add(milkJob.getMap(true));
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

    public List<BEvent> removeTour(long idJob, long tenantId, MilkJobFactory milkJobFactory) {
        return milkJobFactory.removeJob(idJob, tenantId);
    }

    public synchronized List<BEvent> registerATour(MilkJob milkJob, MilkJobFactory milkJobFactory) {
        return milkJobFactory.registerAJob(milkJob);
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* detection method */
    /*                                                                                  */
    /* ******************************************************************************** */

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Execute Test Button */
    /*                                                                                  */
    /* ******************************************************************************** */

    public class MySimpleTestThread extends Thread {

        // private MilkCmdControl cmdControl;
        private long tenantId;
        public boolean isFinish = false;
        public String buttonName;
        public MilkJobExecution input;
        List<BEvent> listEvents;
        MilkJob milkJob;
        public Map<String, Object> argsParameters;

        protected MySimpleTestThread(long tenantId, String buttonName, MilkJob milkJob, MilkJobExecution input, Map<String, Object> argsParameters) {
            this.tenantId = tenantId;
            this.buttonName = buttonName;
            this.input = input;
            this.argsParameters = argsParameters;
            this.milkJob = milkJob;
        }

        public void run() {
            try {
                ConnectorAPIAccessorImpl connectorAccessorAPI = new ConnectorAPIAccessorImpl(tenantId);
                listEvents = milkJob.getPlugIn().buttonParameters(buttonName, input, argsParameters, connectorAccessorAPI);
            } catch (Exception e) {
                listEvents.add(new BEvent(EVENT_INTERNAL_ERROR, e.getMessage()));
            }
            isFinish = true;
        }
    }
    /**
     * load all plugtours
     * 
     * @return
     */

}
