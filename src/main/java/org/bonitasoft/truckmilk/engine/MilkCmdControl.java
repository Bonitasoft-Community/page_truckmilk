package org.bonitasoft.truckmilk.engine;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.InetAddress;
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
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.service.TenantServiceAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkJobFactory.CreateJobStatus;
import org.bonitasoft.truckmilk.engine.MilkSerializeProperties.SerializationJobParameters;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJob.MapContentParameter;
import org.bonitasoft.truckmilk.job.MilkJob.SAVEDEXECUTIONPOLICY;
import org.bonitasoft.truckmilk.job.MilkJobContext;
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

    private final static String LOGGER_HEADER="MilkCmdControl";
    private static BEvent eventInternalError = new BEvent(MilkCmdControl.class.getName(), 1, Level.ERROR,
            "Internal error", "Internal error, check the log");

    private static BEvent eventJobRemoved = new BEvent(MilkCmdControl.class.getName(), 2, Level.SUCCESS,
            "Job removed", "Job is removed with success");

    private static BEvent eventJobActivated = new BEvent(MilkCmdControl.class.getName(), 3, Level.SUCCESS,
            "Job started", "The Job is now activated");

    private static BEvent eventJobDeactivated = new BEvent(MilkCmdControl.class.getName(), 4, Level.SUCCESS,
            "Job stopped", "The Job is now deactivated");

    private static BEvent eventJobUpdated = new BEvent(MilkCmdControl.class.getName(), 5, Level.SUCCESS,
            "Job updated", "The Job is now updated");

    private static BEvent eventJobRegister = new BEvent(MilkCmdControl.class.getName(), 6, Level.SUCCESS,
            "Job registered", "The Job is now registered");

    private static BEvent eventSchedulerResetSuccess = new BEvent(MilkCmdControl.class.getName(), 7, Level.SUCCESS,
            "Schedule reset", "The schedule is reset with success");

    private static BEvent eventMissingID = new BEvent(MilkCmdControl.class.getName(), 8, Level.ERROR,
            "ID is missing", "The Job ID is missing", "Operation can't be realised", "Contact your administrator");

    private static BEvent eventButtonArgFailed = new BEvent(MilkCmdControl.class.getName(), 9, Level.ERROR,
            "No Answer", "The Button Arg does not return an anwser", "Operation can't be realised", "Contact your administrator");

    private static BEvent eventMissingParametersToCreateAJob= new BEvent(MilkCmdControl.class.getName(), 10, Level.APPLICATIONERROR,
            "Missing parameters to create a job", "A job must be created with a name and a Plug In", "No job is created", "Specify a name and a plug in");

    /* ******************************************************************************** */
    /*                                                                                  */
    /* the companion MilkCmdControlAPI call this API */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * this constant is defined too in MilkQuartzJob to have an independent JAR
     */
    public final static String cstCommandName = "truckmilk";
    public final static String cstCommandDescription = "Execute TruckMilk plugin at frequency";

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

    /**
     * this enum is defined too in MilkQuartzJob to have an independent JAR
     * INIT : this order does not exist for the command point of view
     * REFRESH : refresh only the Job information, no check environment
     * GETSTATUS : refresh + check environment
     */
    public enum VERBE {
        GETSTATUS, REFRESH, CHECKUPDATEENVIRONMENT, DEPLOYPLUGIN, DELETEPLUGIN, ADDJOB, REMOVEJOB, ACTIVATEJOB, DEACTIVATEJOB, UPDATEJOB, IMMEDIATEJOB, ABORTJOB, RESETJOB, SCHEDULERSTARTSTOP, SCHEDULERDEPLOY, SCHEDULERRESET, SCHEDULERCHANGE, SCHEDULEROPERATION, TESTBUTTONARGS, HEARTBEAT
    };

    public final static String CST_PAGE_DIRECTORY = "pagedirectory";

    public final static String cstResultListJobs = "listplugtour";
    public final static String cstResultListEvents = "listevents";
    public final static String CST_RESULT_TIMEINMS = "timeinms";

  
    public final static String cstButtonName = "buttonName";

  
    /**
     * keep the scheduler Factory
     */

    private final static MilkSchedulerFactory milkSchedulerFactory = MilkSchedulerFactory.getInstance();

    public final static MilkCmdControl milkCmdControl = new MilkCmdControl();
    
    public final static MilkHeartBeat milkHeartBeat = new MilkHeartBeat();

    // let's return a singleton to simply the implementation
    @Override
    public BonitaCommandApiAccessor getInstance() {
        return milkCmdControl;
    }

    public static MilkCmdControl getStaticInstance() {
        return milkCmdControl;
    }

    /** Not the correct Command library if the call come here */
    @Override
    public ExecuteAnswer executeCommandVerbe(String verbSt, Map<String, Serializable> parameters, TenantServiceAccessor serviceAccessor) {
        logger.severe("ERROR: the Command Library is not the correct one");

        return  new ExecuteAnswer();
        
        
    }

    /**
     * Do all action after the deployement
     * 
     * @param tenantId
     * @return
     */
    @Override
    public ExecuteAnswer afterDeployment(ExecuteParameters executeParameters, APIAccessor apiAccessor, TenantServiceAccessor tenantServiceAccessor) {
        ExecuteAnswer executeAnswer = new ExecuteAnswer();
        MilkJobContext milkJobContext = new MilkJobContext( executeParameters.tenantId,apiAccessor, tenantServiceAccessor );

        executeAnswer.listEvents = checkAndUpdateEnvironment( milkJobContext );
        executeAnswer.result.put(MilkConstantJson.cstJsonSchedulerStatus, BEventFactory.isError(executeAnswer.listEvents) ? "FAIL" : "OK");
        return executeAnswer;
    }

    /**
     * Do all action after the deployement
     * 
     * @param tenantId
     * @return
     */
    @Override
    public ExecuteAnswer afterRestart(ExecuteParameters executeParameters, APIAccessor apiAccessor, TenantServiceAccessor tenantServiceAccessor) {
        ExecuteAnswer executeAnswer = new ExecuteAnswer();
        MilkPlugInFactory milkPlugInFactory = null;
        MilkJobFactory milkJobFactory = null;
        MilkJobContext milkJobContext = new MilkJobContext( executeParameters.tenantId,apiAccessor, tenantServiceAccessor );
        try {
            InetAddress ip = InetAddress.getLocalHost();

            milkPlugInFactory = MilkPlugInFactory.getInstance( milkJobContext );
            milkJobFactory = MilkJobFactory.getInstance(milkPlugInFactory);
            for (MilkJob milkJob : milkJobFactory.getListJobs()) {
                if (milkJob.inExecution() && milkJob.getHostRegistered().equals(ip.getHostAddress())) {
                    logger.info(" Server Restart reset jobId[" + milkJob.getId() + "]");
                    // cancel the job: server restart
                    milkJob.killJob();;
                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SerializationJobParameters.getInstanceAllInformations()));
                }
            }

        } catch (Exception e) {
            executeAnswer.listEvents.add(new BEvent(eventInternalError, e, "During restart server"));
        }

        return executeAnswer;
    }

    /**
     * check the environment of the command
     * 
     * @param tenantId
     * @return
     */
    public List<BEvent> checkAndUpdateEnvironment(MilkJobContext milkJobContext) {
        // this is the time to check the BonitaProperties tables
        MilkPlugInFactory milkPlugInFactory = MilkPlugInFactory.getInstance(milkJobContext );
        MilkJobFactory milkJobFactory = MilkJobFactory.getInstance(milkPlugInFactory, false);

        return milkJobFactory.checkAndUpdateEnvironment( milkJobContext );
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
    @Override
    public ExecuteAnswer executeCommandApiAccessor(ExecuteParameters executeParameters, APIAccessor apiAccessor, TenantServiceAccessor tenantServiceAccessor) {

        long currentTime = System.currentTimeMillis();
        ExecuteAnswer executeAnswer = new ExecuteAnswer();
        long startTime = System.currentTimeMillis();

        VERBE verbEnum = null;
        StringBuilder detailsLogInfo = new StringBuilder();
        try {
            // ------------------- ping ?
            verbEnum = VERBE.valueOf(executeParameters.verb);

            logger.fine("command Verb2[" + verbEnum.toString() + "] Tenant[" + executeParameters.tenantId + "]");

            Long userId = executeParameters.getParametersLong("userId");
            String userName = null;
            try
            {
                if (userId!=null) {
                    User user = apiAccessor.getIdentityAPI().getUser( userId);
                    userName = user.getFirstName()+" "+user.getLastName();
                }
            } catch(Exception e ) {
                logger.severe("Can't find user ["+userId+"] : "+e.getMessage());
            }
            
            
            boolean addSchedulerStatus = false;

            MilkReportEngine milkReportEngine = MilkReportEngine.getInstance();
            MilkJobContext milkJobContext = new MilkJobContext(executeParameters.tenantId, apiAccessor, tenantServiceAccessor);
                    MilkPlugInFactory milkPlugInFactory = null;
                    MilkJobFactory milkJobFactory = null;

            
            if (milkSchedulerFactory.getScheduler() == null) {
                executeAnswer.listEvents.addAll(milkSchedulerFactory.startup(executeParameters.tenantId));
            }
            // initialise the factory ?

            // manage immediately the HEARTBEAT
            if (VERBE.HEARTBEAT.equals(verbEnum)) {
                executeAnswer.logAnswer = false; // no log on the HeartBeat, to not pollute the log
                if ((milkSchedulerFactory.getScheduler() != null)) {
                    StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                    if (statusScheduler.status == TypeStatus.STARTED) {
                        milkHeartBeat.executeOneTimeNewThread(this, false, milkJobContext);
                    }
                    else
                    {
                        logger.info(LOGGER_HEADER+"HEARTBEAT: Scheduler is not started");
                    }
                }
            } else {
                // initialise the factory only if this is not a heartbeat
                milkPlugInFactory = MilkPlugInFactory.getInstance(milkJobContext);
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

                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory, milkJobContext));

                List<Map<String, Object>> listPlugInMap = new ArrayList<>();
                for (MilkPlugIn plugin : milkPlugInFactory.getListPlugIn()) {
                    listPlugInMap.add(plugin.getMap());
                }
                // sort the list
                Collections.sort(listPlugInMap, new Comparator<Map<String,Object>>()
                {
                  public int compare(Map<String,Object> s1,
                          Map<String,Object> s2)
                  {
                      if (s1.get(MilkConstantJson.CSTJSON_PLUGIN_DISPLAYNAME)==null || s2.get(MilkConstantJson.CSTJSON_PLUGIN_DISPLAYNAME)==null)
                          return 0;
                    return s1.get(MilkConstantJson.CSTJSON_PLUGIN_DISPLAYNAME).toString().compareTo(s2.get(MilkConstantJson.CSTJSON_PLUGIN_DISPLAYNAME).toString());
                  }
                });


                executeAnswer.result.put("listplugin", listPlugInMap);

            } else if (VERBE.CHECKUPDATEENVIRONMENT.equals(verbEnum)) {

                executeAnswer.listEvents.addAll(checkAndUpdateEnvironment( milkJobContext ));

            } else if (VERBE.ADDJOB.equals(verbEnum)) {

                MilkPlugIn plugIn = milkPlugInFactory.getPluginFromName(executeParameters.getParametersString("plugin"));
                String jobName = executeParameters.getParametersString("name");
                if (plugIn == null || jobName==null || jobName.trim().length()==0) {
                    StringBuilder message= new StringBuilder();
                    if (plugIn==null)
                        message.append("No job found with name[" + executeParameters.getParametersString("plugin") + "];");
                    if (jobName==null || jobName.trim().length()==0)
                        message.append("JobName is required, must not be empty");
                    logger.severe(message.toString());
                    executeAnswer.listEvents.add(new BEvent(eventMissingParametersToCreateAJob, message.toString()));

                    return executeAnswer;
                }
                logger.info("Add jobName[" + jobName + "] PlugIn[" + executeParameters.getParametersString("plugin") + "]");

                MilkJobExecution milkJobExecution = new MilkJobExecution( milkJobContext);

                CreateJobStatus createJobStatus = milkJobFactory.createMilkJob(jobName, plugIn, milkJobExecution);
                executeAnswer.listEvents.addAll(createJobStatus.listEvents);
                if (!BEventFactory.isError(executeAnswer.listEvents)) {
                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(createJobStatus.job, SerializationJobParameters.getInstanceAllInformations()));
                    if (!BEventFactory.isError(executeAnswer.listEvents))
                        executeAnswer.listEvents.add(new BEvent(eventJobRegister, "Job registered[" + createJobStatus.job.getName() + "]"));
                }
                // get all lists            
                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory, milkJobContext));

            } else if (VERBE.REMOVEJOB.equals(verbEnum)) {


                Long idJob = executeParameters.getParametersLong("id");
                MilkJob milkJob = idJob == null ? null : milkJobFactory.getJobById(idJob);
                MilkJobExecution milkJobExecution = new MilkJobExecution(milkJob, milkJobContext);

                if (idJob == null) {
                    executeAnswer.listEvents.add(eventMissingID);
                } else if (milkJob == null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo.append( "Job[" + milkJob.getName() + "] (" + milkJob.getId() + ")");

                    executeAnswer.listEvents.addAll(removeJob(milkJob, milkJobExecution));

                    if (!BEventFactory.isError(executeAnswer.listEvents)) {
                        executeAnswer.listEvents.add(new BEvent(eventJobRemoved, "Job removed[" + milkJob.getName() + "]"));
                    }
                }
                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory, milkJobContext));

            } else if (VERBE.ACTIVATEJOB.equals(verbEnum) || VERBE.DEACTIVATEJOB.equals(verbEnum)) {

                Long idJob = executeParameters.getParametersLong("id");
                MilkJob milkJob = idJob == null ? null : milkJobFactory.getJobById(idJob);

                if (idJob == null) {
                    executeAnswer.listEvents.add(eventMissingID);
                } else if (milkJob == null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo.append( "Job[" + milkJob.getName() + "] (" + milkJob.getId() + ")");

                    // save parameters
                    Map<String, Object> parametersObject = executeParameters.getParametersMap("parametersvalue");
                    if (parametersObject != null)
                        milkJob.setJobParameters(parametersObject);
                    String cronSt = executeParameters.getParametersString("cron");
                    if (cronSt != null)
                        milkJob.setCron(cronSt);

                    MilkJobExecution milkJobExecution = new MilkJobExecution(milkJob, milkJobContext);
                    // enable=true will recalculate the nextExecution date : collect error
                    executeAnswer.listEvents.addAll( milkJob.setEnable(VERBE.ACTIVATEJOB.equals(verbEnum), milkJobExecution) );
                    
                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SerializationJobParameters.getBaseInformations()));
                    if (VERBE.ACTIVATEJOB.equals(verbEnum))
                        executeAnswer.listEvents.add(new BEvent(eventJobActivated, "Job Activated[" + milkJob.getName() + "]"));
                    else
                        executeAnswer.listEvents.add(new BEvent(eventJobDeactivated, "Job Deactived[" + milkJob.getName() + "]"));

                    milkReportEngine.reportOperation(executeAnswer.listEvents);

                    executeAnswer.result.put("enable", milkJob.isEnable() );
                    executeAnswer.result.put("tour", milkJob.getMap(MapContentParameter.getInstanceWeb(), milkJobContext));
                }
            }

            else if (VERBE.UPDATEJOB.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                MilkJob milkJob = idJob == null ? null : milkJobFactory.getJobById(idJob);

                if (idJob == null) {
                    executeAnswer.listEvents.add(eventMissingID);
                } else if (milkJob == null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo.append( "Job[" + milkJob.getName() + "] (" + milkJob.getId() + ")");

                    SerializationJobParameters saveJobParameters = SerializationJobParameters.getBaseInformations();

                    // this is maybe a call only to update a file parameters
                    if (executeParameters.parametersCommand.containsKey("file")) {
                        String pageDirectory = executeParameters.getParametersString(CST_PAGE_DIRECTORY);
                        File pageDirectoryFile = new File(pageDirectory);
                        // Yes
                        String fileName = executeParameters.getParametersString("file");
                        String parameterName = executeParameters.getParametersString("parameter");
                        executeAnswer.listEvents.addAll(milkJob.setJobFileParameter(parameterName, fileName, pageDirectoryFile));
                        saveJobParameters.fileRead = true;
                    } else {

                        Map<String, Object> parametersObject = executeParameters.getParametersMap("parametersvalue");
                        // History
                        milkJob.setNbHistoryMesure( executeParameters.getParametersInt( MilkConstantJson.CSTJSON_NB_HISTORYMEASUREMENT, milkJob.getPlugIn().getDefaultNbHistoryMesures() ));
                        milkJob.setNbSavedExecution(executeParameters.getParametersInt( MilkConstantJson.CSTJSON_NB_SAVEDEXECUTION, milkJob.getPlugIn().getDefaultNbSavedExecution()));
                        // Limitation
                        milkJob.setUserStopAfterMaxItems( executeParameters.getParametersInt( MilkConstantJson.CSTJSON_STOPAFTER_NBITEMS, MilkJob.CSTDEFAULT_STOPAFTER_MAXITEMS));
                        milkJob.setUserStopAfterMaxMinutes( executeParameters.getParametersInt( MilkConstantJson.CSTJSON_STOPAFTER_NBMINUTES, MilkJob.CSTDEFAULT_STOPAFTER_MAXMINUTES ));

                        
                        milkJob.setJobParameters(parametersObject);
                        String cronSt = executeParameters.getParametersString(MilkConstantJson.CSTJSON_JOB_CRON);
                        milkJob.setCron(cronSt);
                        try {
                            String saveExecutionPolicy = executeParameters.getParametersString(MilkConstantJson.CSTJSON_SAVEDEXECUTIONPOLICY);
                            milkJob.setSavedExecutionPolicy( SAVEDEXECUTIONPOLICY.valueOf(saveExecutionPolicy));
                        } catch(Exception e) {                            
                        }
                        
                        milkJob.setHostsRestriction(executeParameters.getParametersString(MilkConstantJson.CSTJSON_HOSTSRESTRICTION));
                        milkJob.setDescription(executeParameters.getParametersString("description"));
                        String newName = executeParameters.getParametersString("newname");
                        milkJob.setName(newName);
                        saveJobParameters.trackExecution = true; // we may change the CronSt
                    }
                    MilkJobExecution milkJobExecution = new MilkJobExecution(milkJob, milkJobContext);
                    executeAnswer.listEvents.addAll( milkJob.notifyAChangeInJob( milkJobExecution ) );
                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, saveJobParameters));
                    
                    executeAnswer.listEvents.add(new BEvent(eventJobUpdated, "Job updated[" + milkJob.getName() + "]"));
                    milkReportEngine.reportOperation(executeAnswer.listEvents);
                }
                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory,  milkJobContext));

            } else if (VERBE.IMMEDIATEJOB.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                

                MilkJob milkJob = idJob == null ? null : milkJobFactory.getJobById(idJob);

                if (idJob == null) {
                    executeAnswer.listEvents.add(eventMissingID);
                } else if (milkJob == null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo.append( "Job[" + milkJob.getName() + "] (" + milkJob.getId() + ") by ["+userName+"]");
                    milkJob.setImmediateExecution(true);
                    milkJob.setAskForStop(false);
                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SerializationJobParameters.getInstanceTrackExecution()));
                    executeAnswer.listEvents.add(new BEvent(eventJobUpdated, "Job updated[" + milkJob.getName() + "]"));
                }
                milkReportEngine.reportOperation(executeAnswer.listEvents);

                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory, milkJobContext));

            } else if (VERBE.ABORTJOB.equals(verbEnum)) {
                Long idJob      = executeParameters.getParametersLong("id");
              
                        
                MilkJob milkJob = idJob == null ? null : milkJobFactory.getJobById(idJob);

                if (idJob == null) {
                    executeAnswer.listEvents.add(eventMissingID);
                } else if (milkJob == null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo.append( "Job[" + milkJob.getName() + "] (" + milkJob.getId() + ")");

                    milkJob.setAskForStop(true);
                    milkJob.registerExecution(new Date(), ExecutionStatus.KILL, "", "Aborted by "+(userName==null ? "system":userName) );

                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SerializationJobParameters.getAskStop()));
                    executeAnswer.listEvents.add(new BEvent(eventJobUpdated, "Job updated[" + milkJob.getName() + "]"));
                }
                milkReportEngine.reportOperation(executeAnswer.listEvents);

                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory, milkJobContext));

            } else if (VERBE.RESETJOB.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                MilkJob milkJob = idJob == null ? null : milkJobFactory.getJobById(idJob);

                if (idJob == null) {
                    executeAnswer.listEvents.add(eventMissingID);
                } else if (milkJob == null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo.append( "Job[" + milkJob.getName() + "] (" + milkJob.getId() + ")");

                    milkJob.killJob( );
                    executeAnswer.listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SerializationJobParameters.getInstanceAllInformations()));
                    executeAnswer.listEvents.add(new BEvent(eventJobUpdated, "Job updated[" + milkJob.getName() + "]"));
                }
                executeAnswer.result.put(cstResultListJobs, getListMilkJobsMap(milkJobFactory, milkJobContext));
            } else if (VERBE.TESTBUTTONARGS.equals(verbEnum)) {
                Long idJob = executeParameters.getParametersLong("id");
                MilkJob milkJob = idJob == null ? null : milkJobFactory.getJobById(idJob);

                if (idJob == null) {
                    executeAnswer.listEvents.add(eventMissingID);
                } else if (milkJob == null) {
                    executeAnswer.listEvents.add(new BEvent(MilkJobFactory.EVENT_JOB_NOT_FOUND, "JobID[" + idJob + "]"));
                } else {
                    detailsLogInfo.append( "Job[" + milkJob.getName() + "] (" + milkJob.getId() + ")");
                    String buttonName = executeParameters.getParametersString(cstButtonName);
                    Map<String, Object> parametersObject = executeParameters.getParametersMap("parametersvalue");
                    milkJob.setJobParameters(parametersObject);

                    Map<String, Object> argsParameters = executeParameters.getParametersMap("args");

                    // execute it!
                    MilkJobExecution milkJobExecution = new MilkJobExecution(milkJob, milkJobContext);

                    MySimpleTestThread buttonThread = new MySimpleTestThread(buttonName, milkJob, milkJobExecution, argsParameters);

                    buttonThread.start();
                    int count = 0;
                    while (!buttonThread.isFinish && count < 1000) {
                        count++;
                        Thread.sleep(1000);
                    }
                    if (buttonThread.isFinish)
                        executeAnswer.listEvents.addAll(buttonThread.listEvents);
                    else
                        executeAnswer.listEvents.add(new BEvent(eventButtonArgFailed, "No answer"));

                }

            }

            else if (VERBE.SCHEDULERSTARTSTOP.equals(verbEnum)) {
                addSchedulerStatus = true; // still add it, why not?
                Boolean startScheduler = executeParameters.getParametersBoolean("start");
                logger.info("SchedulerStartStop requested[" + startScheduler + "] - ");
                ArrayList<BEvent> listEventsAction = new ArrayList<>();
                if (startScheduler == null && "true".equals(executeParameters.parametersCommand.get("start")))
                    startScheduler = true;
                if (milkSchedulerFactory.getScheduler() != null && startScheduler != null) {
                    if (startScheduler.booleanValue()) {
                        Boolean reset = executeParameters.getParametersBoolean("reset");
                        milkHeartBeat.synchronizeHeart.heartBeatInProgress = false; // prevention, reset it to false
                        listEventsAction.addAll(milkSchedulerFactory.getScheduler().startup(executeParameters.tenantId, reset == null ? false : reset));
                    } else
                        listEventsAction.addAll(milkSchedulerFactory.getScheduler().shutdown(executeParameters.tenantId));
                }
                StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                // so, if the status return an error, do not return the listEventActions, only errors
                if (BEventFactory.isError(statusScheduler.listEvents)) {
                    for (BEvent event : listEventsAction) {
                        if (event.isError())
                            executeAnswer.listEvents.add(event);
                    }
                    executeAnswer.listEvents.addAll(statusScheduler.listEvents);
                } else {
                    executeAnswer.listEvents.addAll(listEventsAction);
                }
                executeAnswer.result.put(MilkConstantJson.cstJsonSchedulerStatus, statusScheduler.status.toString());
                executeAnswer.result.put(MilkConstantJson.cstJsonListTypesSchedulers, milkSchedulerFactory.getListTypeScheduler());

                // no need to add the event: it will be done by the getEvent after

            } else if (VERBE.SCHEDULERDEPLOY.equals(verbEnum)) {
                String pageDirectory = executeParameters.getParametersString(CST_PAGE_DIRECTORY);
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
                executeAnswer.result.put(MilkConstantJson.cstJsonSchedulerStatus, statusScheduler.status.toString());
                executeAnswer.result.put(MilkConstantJson.cstJsonListTypesSchedulers, milkSchedulerFactory.getListTypeScheduler());
                executeAnswer.listEvents.addAll(statusScheduler.listEvents);

            } else if (VERBE.SCHEDULERRESET.equals(verbEnum)) {
                addSchedulerStatus=true;
                // check the environment
                executeAnswer.listEvents.addAll( checkAndUpdateEnvironment( milkJobContext ) );
                
                executeAnswer.listEvents.addAll(milkSchedulerFactory.getScheduler().reset(executeParameters.tenantId));
                
                // setup the isInProgress to false, to accept a new heartBeat
                milkHeartBeat.synchronizeHeart.heartBeatInProgress = false;

                if (!BEventFactory.isError(executeAnswer.listEvents)) {
                    executeAnswer.listEvents.add(eventSchedulerResetSuccess);
                }
                StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                executeAnswer.result.put( MilkConstantJson.cstJsonSchedulerStatus, statusScheduler.status.toString());
                executeAnswer.result.put( MilkConstantJson.cstJsonListTypesSchedulers, milkSchedulerFactory.getListTypeScheduler());
                executeAnswer.listEvents.addAll(statusScheduler.listEvents);

                milkReportEngine.reportOperation(executeAnswer.listEvents );

                
            } else if (VERBE.SCHEDULERCHANGE.equals(verbEnum)) {
                addSchedulerStatus=true;
                String newSchedulerChange = executeParameters.getParametersString( MilkConstantJson.cstSchedulerChangeType);

                // update new parameters 
                Boolean logHeartbeat = executeParameters.getParametersBoolean( MilkConstantJson.CSTJSON_LOGHEARTBEAT);
                Long nbSavedHeartBeat = executeParameters.getParametersLong( MilkConstantJson.CSTJSON_NBSAVEDHEARTBEAT);
          

                executeAnswer.listEvents.addAll(milkSchedulerFactory.changeScheduler(newSchedulerChange, executeParameters.tenantId));
                milkReportEngine.setNbSavedHeartBeat(nbSavedHeartBeat==null ? 60 : nbSavedHeartBeat);
                milkReportEngine.setLogHeartBeat(logHeartbeat==null ? true : logHeartbeat);
                
                
                // get information on the new Scheduler then
                if ((!BEventFactory.isError(executeAnswer.listEvents))) {
                    StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                    executeAnswer.result.put( MilkConstantJson.cstJsonSchedulerStatus, statusScheduler.status.toString());
                    executeAnswer.listEvents.addAll(statusScheduler.listEvents);
                }
                executeAnswer.result.put( MilkConstantJson.cstJsonListTypesSchedulers, milkSchedulerFactory.getListTypeScheduler());

            } else if (VERBE.SCHEDULEROPERATION.equals(verbEnum)) {
                addSchedulerStatus=true;
                if ("heartbeat".equals( executeParameters.parametersCommand.get("scheduleroperation")))
                {
                    milkHeartBeat.executeOneTimeNewThread(this, true, milkJobContext);
                }
                else 
                {
                    executeAnswer.listEvents.addAll(milkSchedulerFactory.getScheduler().operation(executeParameters.parameters));
                }
                StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                executeAnswer.result.put( MilkConstantJson.cstJsonSchedulerStatus, statusScheduler.status.toString());
                executeAnswer.result.put( MilkConstantJson.cstJsonListTypesSchedulers, milkSchedulerFactory.getListTypeScheduler());
                executeAnswer.listEvents.addAll(statusScheduler.listEvents);

            }

            //------------------------------ Check Environment
            if (addSchedulerStatus) {

                List<BEvent> listEvents = executeAnswer.listEvents;

                // Schedule is part of any answer
                Map<String, Object> mapScheduler = new HashMap<>();
                if (milkSchedulerFactory.getScheduler() == null)
                    mapScheduler.put( MilkConstantJson.cstJsonSchedulerStatus, MilkConstantJson.cstJsonSchedulerStatus_V_STOPPED);
                else {
                    StatusScheduler statusScheduler = milkSchedulerFactory.getStatus(executeParameters.tenantId);
                    mapScheduler.put( MilkConstantJson.cstJsonSchedulerStatus, statusScheduler.status.toString());
                    listEvents.addAll(statusScheduler.listEvents);
                    mapScheduler.put( MilkConstantJson.cstJsonSchedulerType, milkSchedulerFactory.getScheduler().getType().toString());
                    mapScheduler.put( MilkConstantJson.CSTJSON_SCHEDULERINFO, milkSchedulerFactory.getScheduler().getDescription());
                    mapScheduler.put( MilkConstantJson.CSTJSON_LOGHEARTBEAT, milkReportEngine.isLogHeartBeat());
                    mapScheduler.put( MilkConstantJson.CSTJSON_NBSAVEDHEARTBEAT, milkReportEngine.getNbSavedHeartBeat());
                    mapScheduler.put( MilkConstantJson.CSTJSON_LASTHEARTBEAT, milkReportEngine.getListSaveHeartBeatInformation());

                    
                }

                // Plug in environment
                List<MilkPlugIn> list = milkPlugInFactory.getListPlugIn();

                for (MilkPlugIn plugIn : list) {
                    MilkJobExecution milkJobExecution = new MilkJobExecution( milkJobContext);
                    listEvents.addAll(plugIn.checkPluginEnvironment(milkJobExecution));
                }

                // Job environment
                Collection<MilkJob> listJobs = milkJobFactory.getListJobs();
                for (MilkJob milkJob : listJobs) {
                    MilkJobExecution milkJobExecution = new MilkJobExecution(milkJob, milkJobContext);
                    listEvents.addAll(milkJob.getPlugIn().checkJobEnvironment(milkJobExecution));
                }

                // filter then set status
                listEvents = BEventFactory.filterUnique(listEvents);

                mapScheduler.put(MilkConstantJson.cstJsonDashboardEvents, BEventFactory.getHtml(listEvents));
                mapScheduler.put(MilkConstantJson.cstJsonDashboardSyntheticEvents, BEventFactory.getSyntheticHtml(listEvents));

                mapScheduler.put(MilkConstantJson.cstJsonListTypesSchedulers, milkSchedulerFactory.getListTypeScheduler());
                // return in scheduler the last heartBeat

                executeAnswer.result.put(MilkConstantJson.cstJsonScheduler, mapScheduler);

                if (BEventFactory.isError(executeAnswer.listEvents))
                    executeAnswer.result.put(MilkConstantJson.cstEnvironmentStatus, MilkConstantJson.cstEnvironmentStatus_V_ERROR);
                else
                    executeAnswer.result.put(MilkConstantJson.cstEnvironmentStatus, MilkConstantJson.cstEnvironmentStatus_V_CORRECT);

            }
            /**
             *  remove double event 
             * 
             */
            if (executeAnswer.listEvents != null) {
                executeAnswer.listEvents= BEventFactory.filterUnique(executeAnswer.listEvents);
            }
            
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe("ERROR " + e + " at " + exceptionDetails);

            executeAnswer.listEvents.add(new BEvent(eventInternalError, e.getMessage()));
        } catch (Error er) {
            StringWriter sw = new StringWriter();
            er.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe("ERROR " + er + " at " + exceptionDetails);

            executeAnswer.listEvents.add(new BEvent(eventInternalError, er.getMessage()));
        } finally {
            executeAnswer.result.put(CST_RESULT_TIMEINMS, System.currentTimeMillis() - currentTime);
            executeAnswer.result.put(cstResultListEvents, BEventFactory.getHtml(executeAnswer.listEvents));
            if (VERBE.HEARTBEAT.equals(verbEnum))
                logger.fine("MilkJobCommand Verb[" + (verbEnum == null ? "null" : verbEnum.toString()) + "] Tenant["
                        + executeParameters.tenantId + "] Error?" + BEventFactory.isError(executeAnswer.listEvents) + " in "
                        + (System.currentTimeMillis() - startTime) + " ms");
            else {
                if (BEventFactory.isError(executeAnswer.listEvents)) {
                    detailsLogInfo.append( "Errors:");
                    for (BEvent event : executeAnswer.listEvents) {
                        detailsLogInfo.append( event.toString() + " <~> ");
                    }
                } else
                    detailsLogInfo.append( "no errors");
                logger.fine("MilkJobCommand Verb[" + (verbEnum == null ? "null" : verbEnum.toString()) + "] Tenant["
                        + executeParameters.tenantId + "] " + detailsLogInfo + " in "
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

 

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Initialisation */
    /*
     * /*
     */
    /* ******************************************************************************** */
    private boolean isInitialized = false;
    public boolean isInitialized() {
        return isInitialized;
    }
    /** the object to synchronize must not change */
    /**
     * initialization : read all data, then check the Quart timer is correclty set.
     * 
     * @return
     */
    public synchronized List<BEvent> initialization(boolean forceReload, boolean forceSchedule, long tenantId, MilkJobFactory milkJobFactory) {
        List<BEvent> listEvents = new ArrayList<>();

        if (!isInitialized || forceReload) {
            //  load all PlugIn
            listEvents.addAll(milkJobFactory.getMilkPlugInFactory().getInitaliseStatus());
            // load all jobs
            listEvents.addAll(milkJobFactory.getInitialiseStatus());
        }

        if (!isInitialized || forceSchedule) {
            // Start the Timer

            if (milkSchedulerFactory.getScheduler() != null) {
                if (milkSchedulerFactory.getScheduler().needRestartAtInitialization()) {
                    milkSchedulerFactory.getScheduler().shutdown(tenantId);
                    listEvents.addAll(milkSchedulerFactory.getScheduler().startup(tenantId, forceSchedule));
                }
            }
        }
        isInitialized = !BEventFactory.isError(listEvents);
        return listEvents;
    }

    /**
     * execute the command
     * 
     * @param tenantId
     */
   

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Local method() */
    /*
     * /*
     */
    /* ******************************************************************************** */

   
    /**
     * get the list, ordered
     * 
     * @param milkJobFactory
     * @return
     */
    public List<MilkJob> getListJobsOrdered(MilkJobFactory milkJobFactory) {
        List<MilkJob> listJobs = new ArrayList<>();
        listJobs.addAll( milkJobFactory.getListJobs());
        // order now
        Collections.sort(listJobs, new Comparator<MilkJob>() {

            public int compare(MilkJob s1,
                    MilkJob s2) {
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
    public List<Map<String, Object>> getListMilkJobsMap(MilkJobFactory milkJobFactory,  MilkJobContext milkJobContext) {
        List<Map<String, Object>> listJobMap = new ArrayList<>();

        for (MilkJob milkJob : getListJobsOrdered(milkJobFactory)) {

            milkJob.checkByPlugIn();

            listJobMap.add(milkJob.getMap(MapContentParameter.getInstanceWeb(), milkJobContext));
        }
        return listJobMap;
    }

    public List<BEvent> removeJob(MilkJob milkJob, MilkJobExecution jobExecution) {
        return milkJob.getMilkJobFactory().removeJob(milkJob,jobExecution);
    }

    public synchronized List<BEvent> registerAJob(MilkJob milkJob, MilkJobExecution milkJobExecution) {
        return milkJob.getMilkJobFactory().registerAJob(milkJob, milkJobExecution);
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

        public boolean isFinish = false;
        public String buttonName;
        public MilkJobExecution input;
        List<BEvent> listEvents;
        MilkJob milkJob;
        public Map<String, Object> argsParameters;

        protected MySimpleTestThread(String buttonName, MilkJob milkJob, MilkJobExecution jobExecution, Map<String, Object> argsParameters) {
            this.buttonName = buttonName;
            this.input = jobExecution;
            this.argsParameters = argsParameters;
            this.milkJob = milkJob;
        }

        @Override
        public void run() {
            try {
                ConnectorAPIAccessorImpl connectorAccessorAPI = new ConnectorAPIAccessorImpl(input.getTenantId());
                listEvents = milkJob.getPlugIn().buttonParameters(buttonName, input, argsParameters, connectorAccessorAPI);
            } catch (Exception e) {
                listEvents.add(new BEvent(eventInternalError, e.getMessage()));
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
