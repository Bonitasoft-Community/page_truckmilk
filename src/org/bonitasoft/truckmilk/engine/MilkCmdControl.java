package org.bonitasoft.truckmilk.engine;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.command.BonitaCommandApiAccessor;
import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.command.SCommandExecutionException;
import org.bonitasoft.engine.command.SCommandParameterizationException;
import org.bonitasoft.engine.connector.ConnectorAPIAccessorImpl;
import org.bonitasoft.engine.service.TenantServiceAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkJobFactory.CreateJobStatus;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.ExecutionStatus;
import org.bonitasoft.truckmilk.engine.MilkSerializeProperties.SaveJobParameters;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJob.MapContentParameter;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerFactory;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.StatusScheduler;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.TypeStatus;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

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
 * A "job" is a job to be execute at a certain frequency, with certain parameters.
 * a MilkJob reference a PlugIn.
 * Example: plugIn 'monitorEmail'
 * - MilkJob "HumanRessourceEmail" / Every day / Monitor 'hr@bonitasoft.com'
 * - MilkJob "SaleEmail" / Every hour / Monitor 'sale@bonitasoft.com'
 * Each MilkJob manage their own parameters, and own schedule. CmdControl can update
 * the parameters and modify the schedule. Cmdmanagement retrieve the list of plug in and list of
 * MilkJob
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

    static MilkLog logger = MilkLog.getLogger(MilkCmdControl.class.getName());

  
    private static BEvent EVENT_INTERNAL_ERROR = new BEvent(MilkCmdControl.class.getName(), 1, Level.ERROR,
            "Internal error", "Internal error, check the log");

    private static BEvent EVENT_JOB_REMOVED = new BEvent(MilkCmdControl.class.getName(), 2, Level.SUCCESS,
            "Job removed", "Job is removed with success");

    private static BEvent EVENT_JOB_ACTIVATED = new BEvent(MilkCmdControl.class.getName(), 3, Level.SUCCESS,
            "Job started", "The Job is now activated");

    private static BEvent EVENT_JOB_DEACTIVATED = new BEvent(MilkCmdControl.class.getName(), 4, Level.SUCCESS,
            "Job stopped", "The Job is now deactivated");

    private static BEvent EVENT_JOB_UPDATED = new BEvent(MilkCmdControl.class.getName(), 5, Level.SUCCESS,
            "Job updated", "The Job is now updated");

    private static BEvent EVENT_JOB_REGISTER = new BEvent(MilkCmdControl.class.getName(), 6, Level.SUCCESS,
            "Job registered", "The Job is now registered");


    private static BEvent EVENT_SCHEDULER_RESET_SUCCESS = new BEvent(MilkCmdControl.class.getName(), 7, Level.SUCCESS,
            "Schedule reset", "The schedule is reset with success");

    private static BEvent EVENT_MISSING_ID = new BEvent(MilkCmdControl.class.getName(), 8, Level.ERROR,
            "ID is missing", "The Job ID is missing", "Operation can't be realised", "Contact your administrator");

    private static BEvent EVENT_BUTTONARG_FAILED = new BEvent(MilkCmdControl.class.getName(), 9, Level.ERROR,
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
     * REFRESH : refresh only the Job information, no check environment
     * GETSTATUS : refresh + check environment
     */
    public enum VERBE {
        GETSTATUS, REFRESH, CHECKUPDATEENVIRONMENT, DEPLOYPLUGIN, DELETEPLUGIN, ADDJOB, REMOVEJOB, ACTIVATEJOB, DEACTIVATEJOB, UPDATEJOB, IMMEDIATEJOB, ABORTJOB, RESETJOB, SCHEDULERSTARTSTOP, SCHEDULERDEPLOY, SCHEDULERRESET, SCHEDULERCHANGE, TESTBUTTONARGS, HEARTBEAT
    };

    public static String cstPageDirectory = "pagedirectory";
    // private static String cstResultTimeInMs = "TIMEINMS";

    private static String cstResultListJobs = "listplugtour";
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
        logger.severe( "ERROR: the Command Library is not the correct one");

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
    public ExecuteAnswer afterDeployment(ExecuteParameters executeParameters, APIAccessor apiAccessor) {
        ExecuteAnswer executeAnswer = new ExecuteAnswer();
        executeAnswer.listEvents = checkAndUpdateEnvironment(executeParameters.tenantId);
        executeAnswer.result.put("status", BEventFactory.isError(executeAnswer.listEvents) ? "FAIL" : "OK");
        return executeAnswer;
    }
    /**
     * Do all action after the deployement
     * 
     * @param tenantId
     * @return
     */
    public ExecuteAnswer afterRestart(ExecuteParameters executeParameters, APIAccessor apiAccessor) {
        ExecuteAnswer executeAnswer = new ExecuteAnswer();
        MilkPlugInFactory milkPlugInFactory = null;
        MilkJobFactory milkJobFactory = null;
        try
        {
            InetAddress ip = InetAddress.getLocalHost();
        
                
            milkPlugInFactory = MilkPlugInFactory.getInstance(executeParameters.tenantId);
            milkJobFactory = MilkJobFactory.getInstance(milkPlugInFactory);
            for (MilkJob milkJob :getListJobs(milkJobFactory)) {
                if (milkJob.inExecution() && milkJob.getHostRegistered().equals(ip.getHostAddress()))
                {
                    logger.info(" Server Restart reset jobId["+milkJob.getId()+"]");
                    // cancel the job: server restart
                    milkJob.trackExecution.lastExecutionDate = new Date();
                    milkJob.trackExecution.lastExecutionStatus = ExecutionStatus.KILL;
                    milkJob.trackExecution.inExecution = false;
                    milkJob.trackExecution.isImmediateExecution = false;
                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SaveJobParameters.getInstanceAllInformations()));
                }
            }

        }
        catch(Exception e)
        {
            executeAnswer.listEvents.add( new BEvent( EVENT_INTERNAL_ERROR, e, "During restart server"));
        }
        
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
        String detailsLogInfo="";
        try {
            // ------------------- ping ?
            verbEnum = VERBE.valueOf(executeParameters.verb);

            logger.fine( "command Verb2[" + verbEnum.toString() + "] Tenant[" + executeParameters.tenantId + "]");

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

                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory));

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
                    logger.severe( "No job found with name[" + executeParameters.getParametersString("plugin") + "]");
                    executeAnswer.listEvents.add(new BEvent(EVENT_INTERNAL_ERROR, "No job found with name[" + executeParameters.getParametersString("plugin") + "]"));

                    return null;
                }
                String jobName = executeParameters.getParametersString("name");
                logger.info( "Add jobName[" + jobName + "] PlugIn[" + executeParameters.getParametersString("plugin") + "]");
               
                CreateJobStatus createJobStatus = milkJobFactory.createMilkJob(jobName, plugIn);
                executeAnswer.listEvents.addAll( createJobStatus.listEvents );
                if (!BEventFactory.isError(executeAnswer.listEvents)) {
                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(createJobStatus.job, SaveJobParameters.getInstanceAllInformations()));
                    if (!BEventFactory.isError(executeAnswer.listEvents))
                        executeAnswer.listEvents.add(new BEvent(EVENT_JOB_REGISTER, "Job registered[" + createJobStatus.job.getName() + "]"));
                }
                // get all lists            
                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory));

            } else if (VERBE.REMOVEJOB.equals(verbEnum)) {

                Long idJob = executeParameters.getParametersLong("id");
                MilkJob milkJob = idJob==null ? null : getJobById(idJob, milkJobFactory);
                
                if (idJob == null ) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else if ( milkJob==null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo+="Job["+milkJob.getName()+"] ("+milkJob.getId()+")";
                    
                    executeAnswer.listEvents.addAll(removeJob(idJob, executeParameters.tenantId, milkJobFactory));

                    if (!BEventFactory.isError(executeAnswer.listEvents)) {
                        executeAnswer.listEvents.add(new BEvent(EVENT_JOB_REMOVED, "Job removed[" + milkJob.getName() + "]"));
                    }
                }
                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory));

            } else if (VERBE.ACTIVATEJOB.equals(verbEnum) || VERBE.DEACTIVATEJOB.equals(verbEnum)) {

                Long idJob = executeParameters.getParametersLong("id");
                MilkJob milkJob = idJob==null ? null : getJobById(idJob, milkJobFactory);
                
                if (idJob == null ) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else if ( milkJob==null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo+="Job["+milkJob.getName()+"] ("+milkJob.getId()+")";
                     
                    // save parameters
                    Map<String, Object> parametersObject = executeParameters.getParametersMap("parametersvalue");
                    if (parametersObject != null)
                        milkJob.setJobParameters(parametersObject);
                    String cronSt = executeParameters.getParametersString("cron");
                    if (cronSt != null)
                        milkJob.setCron(cronSt);

                    milkJob.setEnable(VERBE.ACTIVATEJOB.equals(verbEnum));
                    executeAnswer.listEvents.addAll(milkJob.calculateNextExecution());
                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob,  SaveJobParameters.getBaseInformations()));
                    if (VERBE.ACTIVATEJOB.equals(verbEnum))
                        executeAnswer.listEvents.add(new BEvent(EVENT_JOB_ACTIVATED, "Job Activated[" + milkJob.getName() + "]"));
                    else
                        executeAnswer.listEvents.add(new BEvent(EVENT_JOB_DEACTIVATED, "Job Deactived[" + milkJob.getName() + "]"));

                    executeAnswer.result.put("enable", milkJob.isEnable);
                    executeAnswer.result.put("tour", milkJob.getMap(MapContentParameter.getInstanceWeb()));
                }
            }

            else if (VERBE.UPDATEJOB.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                MilkJob milkJob = idJob==null ? null : getJobById(idJob, milkJobFactory);
                
                if (idJob == null ) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else if ( milkJob==null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo+="Job["+milkJob.getName()+"] ("+milkJob.getId()+")";
                     
                    SaveJobParameters saveJobParameters = SaveJobParameters.getBaseInformations();                    
                 
                    // this is maybe a call only to update a file parameters
                    if (executeParameters.parametersCommand.containsKey("file")) {
                        String pageDirectory = executeParameters.getParametersString(cstPageDirectory);
                        File pageDirectoryFile = new File(pageDirectory);
                        // Yes
                        String fileName = executeParameters.getParametersString("file");
                        String parameterName = executeParameters.getParametersString("parameter");
                        executeAnswer.listEvents.addAll(milkJob.setJobFileParameter(parameterName, fileName, pageDirectoryFile));
                        saveJobParameters.saveFileRead = true;
                    } else {

                        Map<String, Object> parametersObject = executeParameters.getParametersMap("parametersvalue");
                        String cronSt = executeParameters.getParametersString( MilkJob.cstJsonCron);
                        milkJob.setJobParameters(parametersObject);
                        milkJob.setCron(cronSt);
                        milkJob.setHostsRestriction( executeParameters.getParametersString( MilkJob.cstJsonHostsRestriction));
                        milkJob.setDescription(executeParameters.getParametersString("description"));
                        String newName = executeParameters.getParametersString("newname");
                        milkJob.setName(newName);
                    }
                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, saveJobParameters));
                    executeAnswer.listEvents.add(new BEvent(EVENT_JOB_UPDATED, "Job updated[" + milkJob.getName() + "]"));
                }
                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory));                

            } else if (VERBE.IMMEDIATEJOB.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                MilkJob milkJob = idJob==null ? null : getJobById(idJob, milkJobFactory);
                
                if (idJob == null ) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else if ( milkJob==null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo+="Job["+milkJob.getName()+"] ("+milkJob.getId()+")";
                     milkJob.setImmediateExecution(true);
                    milkJob.setAskForStop(false);
                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SaveJobParameters.getInstanceTrackExecution()));
                    executeAnswer.listEvents.add(new BEvent(EVENT_JOB_UPDATED, "Job updated[" + milkJob.getName() + "]"));
                }
                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory));
                
            } else if (VERBE.ABORTJOB.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                MilkJob milkJob = idJob==null ? null : getJobById(idJob, milkJobFactory);
                
                if (idJob == null ) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else if ( milkJob==null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo+="Job["+milkJob.getName()+"] ("+milkJob.getId()+")";
       
                    milkJob.setAskForStop(true);
                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SaveJobParameters.getAskStop()));
                    executeAnswer.listEvents.add(new BEvent(EVENT_JOB_UPDATED, "Job updated[" + milkJob.getName() + "]"));
                  }
                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory));
                
            } else if (VERBE.RESETJOB.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                MilkJob milkJob = idJob==null ? null : getJobById(idJob, milkJobFactory);
                
                if (idJob == null ) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else if ( milkJob==null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo+="Job["+milkJob.getName()+"] ("+milkJob.getId()+")";
       
                        milkJob.trackExecution.lastExecutionDate = new Date();
                        milkJob.trackExecution.lastExecutionStatus = ExecutionStatus.KILL;
                        milkJob.trackExecution.inExecution = false;
                        milkJob.trackExecution.isImmediateExecution = false;
                        executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SaveJobParameters.getInstanceAllInformations()));
                        executeAnswer.listEvents.add(new BEvent(EVENT_JOB_UPDATED, "Job updated[" + milkJob.getName() + "]"));
                }
                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory));
            } else if (VERBE.TESTBUTTONARGS.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                MilkJob milkJob = idJob==null ? null : getJobById(idJob, milkJobFactory);
                
                if (idJob == null ) {
                    executeAnswer.listEvents.add(EVENT_MISSING_ID);
                } else if ( milkJob==null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo+="Job["+milkJob.getName()+"] ("+milkJob.getId()+")";
                    String buttonName = executeParameters.getParametersString(cstButtonName);
                    Map<String, Object> parametersObject = executeParameters.getParametersMap("parametersvalue");
                    milkJob.setJobParameters(parametersObject);

                    Map<String, Object> argsParameters = executeParameters.getParametersMap("args");

                    // execute it!
                    MilkJobExecution milkJobExecution = new MilkJobExecution(milkJob);

                    MySimpleTestThread buttonThread = new MySimpleTestThread(executeParameters.tenantId, buttonName, milkJob, milkJobExecution, argsParameters);

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

            else if (VERBE.SCHEDULERSTARTSTOP.equals(verbEnum)) {
                addSchedulerStatus = true; // still add it, why not?
                Boolean startScheduler = executeParameters.getParametersBoolean("start");
                logger.info( "SchedulerStartStop requested[" + startScheduler + "] - ");
                ArrayList<BEvent> listEventsAction = new ArrayList<BEvent>();
                if (startScheduler == null && "true".equals(executeParameters.parametersCommand.get("start")))
                    startScheduler = true;
                if (milkSchedulerFactory.getScheduler() != null && startScheduler != null) {
                    if (startScheduler) {
                        Boolean reset = executeParameters.getParametersBoolean("reset");
                        synchronizeHeart.heartBeatInProgress = false; // prevention, reset it to false
                        listEventsAction.addAll(milkSchedulerFactory.getScheduler().startup(executeParameters.tenantId, reset==null ? false : reset));
                    } else
                        listEventsAction.addAll(milkSchedulerFactory.getScheduler().shutdown(executeParameters.tenantId));
                }
                StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                // so, if the status return an error, do not return the listEventActions, only errors
                if (BEventFactory.isError(statusScheduler.listEvents))
                {                    
                    for (BEvent event : listEventsAction)
                    {
                        if (event.isError())
                            executeAnswer.listEvents.add( event );
                    }
                    executeAnswer.listEvents.addAll( statusScheduler.listEvents);
                }
                else 
                {
                    executeAnswer.listEvents.addAll( listEventsAction );
                }
                executeAnswer.result.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
                executeAnswer.result.put(cstJsonListTypesSchedulers, milkSchedulerFactory.getListTypeScheduler());

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
                    executeAnswer.listEvents.add(EVENT_SCHEDULER_RESET_SUCCESS);
                }
                // return the status
                StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                executeAnswer.result.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
                executeAnswer.result.put(cstJsonListTypesSchedulers, milkSchedulerFactory.getListTypeScheduler());
                executeAnswer.listEvents.addAll(statusScheduler.listEvents);

            } else if (VERBE.SCHEDULERRESET.equals(verbEnum)) {

                executeAnswer.listEvents.addAll(milkSchedulerFactory.getScheduler().reset(executeParameters.tenantId));

                // setup the isInProgress to false, to accept a new heartBeat
                synchronizeHeart.heartBeatInProgress = false;

                if (!BEventFactory.isError(executeAnswer.listEvents)) {
                    executeAnswer.listEvents.add(EVENT_SCHEDULER_RESET_SUCCESS);
                }
                StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                executeAnswer.result.put(cstJsonSchedulerStatus, statusScheduler.status.toString());
                executeAnswer.result.put(cstJsonListTypesSchedulers, milkSchedulerFactory.getListTypeScheduler());
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
                executeAnswer.result.put(cstJsonListTypesSchedulers, milkSchedulerFactory.getListTypeScheduler());

            }

            //------------------------------ Check Environment
            if (addSchedulerStatus) {

                List<BEvent> listEvents =  executeAnswer.listEvents;

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
            logger.severe( "ERROR " + e + " at " + exceptionDetails);

            executeAnswer.listEvents.add(new BEvent(EVENT_INTERNAL_ERROR, e.getMessage()));
        } catch (Error er) {
            StringWriter sw = new StringWriter();
            er.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe( "ERROR " + er + " at " + exceptionDetails);

            executeAnswer.listEvents.add(new BEvent(EVENT_INTERNAL_ERROR, er.getMessage()));
        } finally {
            executeAnswer.result.put(cstResultTimeInMs, System.currentTimeMillis() - currentTime);
            executeAnswer.result.put(cstResultListEvents, BEventFactory.getHtml(executeAnswer.listEvents));
            if (VERBE.HEARTBEAT.equals(verbEnum))
                logger.fine( "MilkJobCommand Verb[" + (verbEnum == null ? "null" : verbEnum.toString()) + "] Tenant["
                        + executeParameters.tenantId + "] Error?" + BEventFactory.isError(executeAnswer.listEvents) + " in "
                        + (System.currentTimeMillis() - startTime) + " ms");
            else
            {
                if(BEventFactory.isError( executeAnswer.listEvents))
                {
                    detailsLogInfo += "Errors:";
                    for(BEvent event : executeAnswer.listEvents) {
                        detailsLogInfo+= event.toString()+" <~> ";
                    }
                }
                else
                    detailsLogInfo += "no errors";
                logger.info( "MilkJobCommand Verb[" + (verbEnum == null ? "null" : verbEnum.toString()) + "] Tenant["
                        + executeParameters.tenantId + "] "+detailsLogInfo + " in "
                        + (System.currentTimeMillis() - startTime) + " ms");
            }

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
            // load all jobs
            listEvents.addAll(milkJobFactory.getInitialiseStatus());
        }

        if (!isInitialized || forceSchedule) {
            // Start the Timer

            if (milkSchedulerFactory.getScheduler() != null) {
                milkSchedulerFactory.getScheduler().shutdown(tenantId);
                listEvents.addAll(milkSchedulerFactory.getScheduler().startup(tenantId, forceSchedule));
            }
        }
        isInitialized = ! BEventFactory.isError(listEvents);
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
                logger.info( "heartBeat in progress, does not start a new one");
                return;
            }
            // second protection : Quartz can call the methode TOO MUCH !
            if (System.currentTimeMillis() < heartBeatLastExecution + 60 * 1000) {
                logger.info( "heartBeat: last execution was too close (last was "+ (System.currentTimeMillis() - heartBeatLastExecution)+" ms ago)");
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
        InetAddress ip=null;
        try {
            ip = InetAddress.getLocalHost();
        } catch (UnknownHostException e1) {
            logger.severeException(e1,"can't get the ipAddress");
            
        }
        logger.info("MickCmdControl.beathearth #" + thisThreadId + " : Start at " + sdf.format(currentDate) +" on ["+(ip==null ? "":ip.getHostAddress())+"]");
        String executionDescription = "";
        try {
            // check all the Job now
            for (MilkJob milkJob : getListJobs(milkJobFactory)) {
                
                MilkExecuteJobThread milkExecuteJobThread = new MilkExecuteJobThread( milkJob);
                
                executionDescription+= milkExecuteJobThread.checkAndStart(currentDate);
            }
            if (executionDescription.length() == 0)
                executionDescription = "No jobs executed;";
            executionDescription = sdf.format(currentDate) + ":" + executionDescription;

            lastHeartBeat.add(executionDescription);
            if (lastHeartBeat.size() > 10)
                lastHeartBeat.remove(0);

        } catch (Exception e) {
            logger.severeException(e, ".executeTimer: Exception " + e.getMessage());
        } catch (Error er) {
            logger.severe( ".executeTimer: Error " + er.getMessage());
        }
        long timeEndHearth = System.currentTimeMillis();
        logger.info("MickCmdControl.beathearth #" + thisThreadId + " : Start at " + sdf.format(currentDate) + ", End in " + (timeEndHearth - timeBeginHearth) + " ms on ["+(ip==null ? "":ip.getHostAddress())+"] "+executionDescription);

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Local method() */
    /*
     * /*
     */
    /* ******************************************************************************** */

    /**
     * return the job Index by the Id
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
     * get the list, ordered
     * @param milkJobFactory
     * @return
     */
    public List<MilkJob> getListJobsOrdered(MilkJobFactory milkJobFactory) {
        List<MilkJob> listJobs = new ArrayList<MilkJob>();
        listJobs.addAll( getListJobs( milkJobFactory ) );
        // order now
        Collections.sort(listJobs, new Comparator<MilkJob>()
        {
          public int compare(MilkJob s1,
                  MilkJob s2)
          {
            return s1.getName().toUpperCase().compareTo(s2.getName().toUpperCase());
          }
        });
        return listJobs;


    }

    /**
     * return a list ordered by name
     * 
     * @return
     */
    public List<Map<String, Object>> getListMilkJobsMap(MilkJobFactory milkJobFactory) {
        List<Map<String, Object>> listJobMap = new ArrayList<Map<String, Object>>();

        
        for (MilkJob milkJob : getListJobsOrdered(milkJobFactory)) {

            milkJob.checkByPlugIn();

            listJobMap.add(milkJob.getMap(MapContentParameter.getInstanceWeb() ));
        }
        return listJobMap;
    }

    public List<BEvent> removeJob(long idJob, long tenantId, MilkJobFactory milkJobFactory) {
        return milkJobFactory.removeJob(idJob, tenantId);
    }

    public synchronized List<BEvent> registerAJob(MilkJob milkJob, MilkJobFactory milkJobFactory) {
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
     * load all jobs
     * 
     * @return
     */

}
