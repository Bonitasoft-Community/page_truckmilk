package org.bonitasoft.truckmilk.schedule;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.builder.BuilderFactory;
import org.bonitasoft.engine.dependency.model.ScopeType;
import org.bonitasoft.engine.scheduler.SchedulerService;
import org.bonitasoft.engine.scheduler.model.SJobDescriptor;
import org.bonitasoft.engine.scheduler.model.SJobParameter;
import org.bonitasoft.engine.scheduler.trigger.Trigger;
import org.bonitasoft.engine.scheduler.trigger.UnixCronTrigger;
import org.bonitasoft.engine.service.TenantServiceAccessor;
import org.bonitasoft.engine.service.TenantServiceSingleton;
import org.bonitasoft.engine.service.impl.ServiceAccessorFactory;
import org.bonitasoft.engine.sessionaccessor.SessionAccessor;
import org.bonitasoft.engine.transaction.UserTransactionService;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkReportEngine;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

/**
 * Quartz Job
 * The CmdControl has to be call by the Quartz job at a frequency. This is the goal of this class
 * This class Does not explicitaly call the DoHeartBreath, cmdControl does that for it.
 * The Tour describe :
 * - when it is executed (every hour, day...)
 * - what is the PlugInDetector
 * - what is the PlugInOperation
 * The Tour is registered in the Quartz Timer.
 * When the timer fire, the method
 * Logging properties:
 * org.quartz.handlers = 5bonita.org.apache.juli.AsyncFileHandler
 * org.quartz.level=INFO
 */
public class MilkScheduleQuartz extends MilkSchedulerInt {

    private static BEvent eventQuartzScheduleError = new BEvent(MilkScheduleQuartz.class.getName(), 1, Level.ERROR,
            "Quartz Job failed", "Check the error", "The different monitoring can't run", "RESET the Scheduler. Check installation.");

    private static BEvent eventDeployQuartzJob = new BEvent(MilkScheduleQuartz.class.getName(), 2, Level.ERROR,
            "Deploy Quartz Job failed", "To have an execution with the correct frequency, a job has to be deploy. Java class implementation of the job failed", "The different monitoring can't run", "See the error");

    private static BEvent EVENT_QUARTZ_NO_JOB = new BEvent(MilkScheduleQuartz.class.getName(), 3, Level.ERROR,
            "No Quartz Job found", "The Quartz Job does not exist", "No monitoring", "Reset the scheduler");

    private static BEvent EVENT_QUARTZ_JOB_CLASS_NOT_DEPLOYED = new BEvent(MilkScheduleQuartz.class.getName(), 4, Level.ERROR,
            "Quartz Job not deployed", "The JAR file containing the Quartz Job [TruckMilk-1.0-Quartzjob.jar] is not deployed. It must be deployed manually in the webapps/bonita/WEB-INF/lib", "No monitoring",
            "Deploy it: copy the JAR file [TruckMilk-1.0-Quartzjob.jar] delivered in the custom page to the Bonita Web Application (<tomcat>/webapps/bonita/WEB-INF/lib) ");

    private static BEvent EVENT_QUARTZ_JOB_UP_AND_RUNNING = new BEvent(MilkScheduleQuartz.class.getName(), 5, Level.SUCCESS,
            "Quartz Job Up and running", "The Quartz Job is up and running");

    private static BEvent EVENT_QUARTZ_SCHEDULER_STOPPED = new BEvent(MilkScheduleQuartz.class.getName(), 6, Level.SUCCESS,
            "Scheduler is stopped", "The Scheduler is stopped");

    private static BEvent EVENT_QUARTZ_SCHEDULER_STARTED = new BEvent(MilkScheduleQuartz.class.getName(), 7, Level.SUCCESS,
            "Scheduler is started", "The Scheduler is started");

    private static MilkLog logger = MilkLog.getLogger(MilkScheduleQuartz.class.getName());
    private static String logHeader = "MilkScheduleQuartz ~~ ";

    public String name;

    public enum Execution {
        FREQUENCE, ONCE
    };

    public Execution mExecution;

    public enum Frequency {
        MINUTES, HOUR, DAY, WEEK
    }

    public Frequency mFrequency;
    public int mTimeFrequency;

    public String mJSonMilkDetectionPlugIn;

    public String mJsonMilkOperation;

    public String mCommandName;

    /**
     * administrator ask to stop the scheduler
     */
    public boolean isStarted = true;

    public MilkScheduleQuartz(MilkSchedulerFactory factory) {
        super(factory);
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Abstract for Milk Controler */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    /** check if the class MilkQuartzJob is accessible */
    public List<BEvent> check(long tenantId) {
        List<BEvent> listEvents = new ArrayList<>();

        try {
            @SuppressWarnings("rawtypes")
            Class classMilk = Class.forName("org.bonitasoft.truckmilk.schedule.quartz.MilkQuartzJob");
            @SuppressWarnings("unused")
            Object ojb = classMilk.newInstance();
        } catch (Exception e) {
            logger.severe(logHeader + "Can't instanciate class - the class [org.bonitasoft.truckmilk.schedule.quartz.MilkQuartzJob] is not deployed");
            listEvents.add(new BEvent(EVENT_QUARTZ_JOB_CLASS_NOT_DEPLOYED, "org.bonitasoft.truckmilk.schedule.quartz.MilkQuartzJob"));

        }
        return listEvents;
    }

    /**
     * true if the scheduler is running (controled by start / stop
     * 
     * @return
     */

    public StatusScheduler getStatus(long tenantId) {
        StatusScheduler statusScheduler = new StatusScheduler();
        statusScheduler.status = TypeStatus.SHUTDOWN;
        // administrator ask to stop it ? Return immediately it's stopped.
        if (!isStarted) {
            statusScheduler.status = TypeStatus.STOPPED;
            return statusScheduler;
        }
        try {
            final TenantServiceAccessor tenantAccessor = TenantServiceSingleton.getInstance(tenantId);
            boolean isQuartzJob = tenantAccessor.getUserTransactionService().executeInTransaction(() -> {
                SchedulerService bonitaScheduler = ServiceAccessorFactory.getInstance().createPlatformServiceAccessor()
                        .getSchedulerService();
                List<String> allJobs = bonitaScheduler.getAllJobs();
                boolean isQuartzJobInternal = false;
                for (String jobName : allJobs) {
                    if (jobName.equals(getJobTriggerName(tenantId))) {
                        // no way to get the JobDescriptor !
                        isQuartzJobInternal = true;
                    }
                }
                return isQuartzJobInternal;
            }); // end transaction
            if (isQuartzJob)
                statusScheduler.status = TypeStatus.STARTED;
            if (!isQuartzJob)
                statusScheduler.listEvents.add(EVENT_QUARTZ_NO_JOB);
            // can access the Job class ?
            boolean isQuartzClass = false;
            statusScheduler.listEvents.addAll(check(tenantId));
            isQuartzClass = !BEventFactory.isError(statusScheduler.listEvents);
            if (isQuartzJob && isQuartzClass)
                statusScheduler.listEvents.add(EVENT_QUARTZ_JOB_UP_AND_RUNNING);
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(logHeader + "~~~~~~~~~~ : ERROR " + e + " at " + exceptionDetails);
        }
        return statusScheduler;
    }

    /**
     * return the scheduler type
     */

    public TypeScheduler getType() {
        return TypeScheduler.QUARTZ;
    }

    public Date getDateNextHeartBeat(long tenantId) {
        // with Quartz, no idea at this moment
        return null;
    }

    /**
     * info to give to the administrator
     * 
     * @return
     */
    public String getDescription() {
        return "The QUARTZ engine embeded in Bonita is used to schedule the monitoring. Copy TruckMilk-x.y.jar (in the Truckmilk.zip, under additionallib) to the Web Application server (webapp/bonita/WEB-INF/lib for a tomcat) and restart the server.";
    }

    public Execution getExecution() {
        return mExecution;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Initialize : create the job */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    private final static String QUARTZMILKJOBNAME = "MilktruckJob";

    /**
     * this definition is duplicated in the class MilkQuartzJob and can't be in the same class, 2 Jar
     * will be deployed and not at the same level
     */
    public final static String CSTPARAMTENANTID = "tenantId";
    /**
     * on which scope the Quartz Jar has to be deployed ?
     */
    public final static ScopeType CSTSCOPEDEPLOYMENT = ScopeType.GLOBAL;

    public boolean needRestartAtInitialization() {
        return false;
    }

    /**
     * artefactId is to keep a internal ID, like processId for a Scope.Process
     */

    public List<BEvent> startup(long tenantId, boolean forceReset) {
        List<BEvent> listEvents = new ArrayList<>();
        MilkReportEngine milkReportEngine = MilkReportEngine.getInstance();
        milkReportEngine.reportHeartBeatInformation("Startup QuartzJob reset[" + forceReset + "]", true);
        try {
            final TenantServiceAccessor tenantAccessor = TenantServiceSingleton.getInstance(tenantId);
            UserTransactionService userTransactionService = tenantAccessor.getUserTransactionService();

            userTransactionService.executeInTransaction(() -> {
                SchedulerService bonitaScheduler = ServiceAccessorFactory.getInstance().createPlatformServiceAccessor()
                        .getSchedulerService();
                SessionAccessor sessionAccessor = tenantAccessor.getSessionAccessor();
                sessionAccessor.setTenantId(tenantId);
                List<String> allJobs = bonitaScheduler.getAllJobs();
                for (String jobName : allJobs) {
                    if (jobName.equals(getJobTriggerName(tenantId))) {
                        logger.info(logHeader + " QuartzJob[" + getJobTriggerName(tenantId) + "] already exist," + (forceReset ? "Delete and schedule it" : ""));
                        if (forceReset)
                            bonitaScheduler.delete(jobName);
                        else
                            return null;
                    }
                }
                // Trigger is the WHEN job has to start
                // every minutes : see https://www.freeformatter.com/cron-expression-generator-quartz.html
                String cronString;
                cronString = "0 0/1 * 1/1 * ? *"; // every minutes
                // cronString = "0/20 0 0 ? * * *"; // every 20 s
                final Trigger jobTrigger = new UnixCronTrigger(
                        getJobTriggerName(tenantId),
                        new Date(),
                        cronString,
                        org.bonitasoft.engine.scheduler.trigger.Trigger.MisfireRestartPolicy.ALL);
                
                
                // Job descriptor is WHAT to run
                CallDeploimentStatus callDeployment = getJobBeforeV10(tenantId, bonitaScheduler, jobTrigger);
                if (callDeployment.jobDescriptor == null) {
                    callDeployment = getJobAfterV10(tenantId, bonitaScheduler, jobTrigger);
                }
                if (callDeployment.jobDescriptor == null) {
                    logger.severe(logHeader + "~~~~~~~~~~  : ERROR " + callDeployment.exception + " at " + callDeployment.stackTrace);
                    listEvents.add(new BEvent(eventQuartzScheduleError, callDeployment.exception, ""));
                    return listEvents;
                }
                logger.info(logHeader + " QuartzJob[" + getJobTriggerName(tenantId) + "] Started with Cron[" + cronString + "]");
                return null;
            });
            isStarted = true;
            listEvents.add(EVENT_QUARTZ_SCHEDULER_STARTED);
        } catch (final Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(logHeader + "~~~~~~~~~~  : ERROR " + e + " at " + exceptionDetails);
            listEvents.add(new BEvent(eventQuartzScheduleError, e, ""));
        }
        return listEvents;
    }

    private final static String CST_CLASSNAME = "org.bonitasoft.truckmilk.schedule.quartz.MilkQuartzJob";

    private class CallDeploimentStatus {
        SJobDescriptor jobDescriptor=null;
        String stackTrace;
        Exception exception;
    }
    

    /* ******************************************************************************** */
    /*                                                                                  */
    /* getAndSchedule method before and after V10 */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * 
     * @param tenantId
     * @param bonitaScheduler
     * @param syncJobTrigger
     * @param listEvents
     * @return
     */
    private CallDeploimentStatus getJobBeforeV10(long tenantId, SchedulerService bonitaScheduler, Trigger jobTrigger) {
        CallDeploimentStatus callDeploimentStatus = new CallDeploimentStatus();
        String jobName = getJobTriggerName(tenantId);

        /**
         * This is the code 7.8
         * SJobDescriptorBuilderFactory jobDescriptorBuilder = BuilderFactory.get(SJobDescriptorBuilderFactory.class);
         * jobDescriptorBuilder jobDescriptorBuilder = jobDescriptorBuilder.createNewInstance(CST_CLASSNAME, jobName, false);
         * JobDescriptor job= jobDescriptorBuilder.done();
         * SJobParameterBuilderFactory jobParameterBuilderFactory = BuilderFactory.get(SJobParameterBuilderFactory.class);
         * SJobParameterBuilder jobParameterBuilder = jobParameterBuilderFactory.createNewInstance(CSTPARAMTENANTID, tenantId);
         * SJobParameter jobParameter = jobParameterBuilder.done();
         * syncJobParameters.add( jobParameter );
         * bonitaScheduler.schedule(jobDescriptor, syncJobParameters, syncJobTrigger);
         */
        try {

            Class<?> sJobDescriptorBuilderFactoryClass = Class.forName("org.bonitasoft.engine.scheduler.builder.SJobDescriptorBuilderFactory");
            Object jobDescriptorBuilderFactory = callStaticMethod("org.bonitasoft.engine.builder.BuilderFactory", "get", new Object[] { sJobDescriptorBuilderFactoryClass },null);
            Object jobDescriptorBuilder = callObjectMethod( jobDescriptorBuilderFactory, "createNewInstance", new Object[] { CST_CLASSNAME, jobName, false },null);
            SJobDescriptor jobDescriptor = (SJobDescriptor) callObjectMethod( jobDescriptorBuilder, "done", new Object[] {},null);
            List<SJobParameter> listJobParameters = new ArrayList<>();
            // tenantId in the job param

            // SJobParameterBuilderFactory jobParameterBuilderFactory = BuilderFactory.get(SJobParameterBuilderFactory.class);
            Class<?> sJobParameterBuilderFactoryClass = Class.forName("org.bonitasoft.engine.scheduler.builder.SJobParameterBuilderFactory");
            Object jobParameterBuilderFactory = callStaticMethod("org.bonitasoft.engine.builder.BuilderFactory", "get", new Object[] { sJobParameterBuilderFactoryClass },null);

            // org.bonitasoft.engine.scheduler.builder.SJobParameterBuilder jobParameterBuilder = jobParameterBuilderFactory.createNewInstance(CSTPARAMTENANTID, tenantId);
            Object jobParameterBuilder = callObjectMethod( jobParameterBuilderFactory, "createNewInstance", new Object[] { CSTPARAMTENANTID, tenantId },new Class[] { String.class, Serializable.class });

            // SJobParameter jobParameter = jobParameterBuilder.done();
            SJobParameter jobParameter = (SJobParameter) callObjectMethod( jobParameterBuilder, "done", new Object[] {},null);

            listJobParameters.add(jobParameter);
            bonitaScheduler.schedule(jobDescriptor, listJobParameters, jobTrigger);
            callDeploimentStatus.jobDescriptor = jobDescriptor;
            return callDeploimentStatus;
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            callDeploimentStatus.stackTrace = exceptionDetails;
            callDeploimentStatus.exception = e;
            return callDeploimentStatus;
        }
    }

    /**
     * Deploy the quartz job after 7.10
     * @param tenantId
     * @param bonitaScheduler
     * @param syncJobTrigger
     * @param listEvents
     * @return
     */
    private CallDeploimentStatus getJobAfterV10(long tenantId, SchedulerService bonitaScheduler, Trigger jobTrigger) {
        CallDeploimentStatus callDeploimentStatus = new CallDeploimentStatus();
        String jobName = getJobTriggerName(tenantId);
        /*
         * This is the code 7.10
         * SJobDescriptorBuilder jobDescriptorBuilder = SJobDescriptor.builder();
         * jobDescriptorBuilder.jobClassName(CST_CLASSNAME);
         * jobDescriptorBuilder.jobName(jobName);
         * jobDescriptorBuilder.description("TruckMilkHeartBeat");
         * SJobParameter jobParameter = SJobParameter.builder()
         * .key("key")
         * .value("value").build();
         * return jobDescriptorBuilder.build();
         */
        try {
            Object jobDescriptorBuilder = callStaticMethod("org.bonitasoft.engine.scheduler.model.SJobDescriptor", "builder", new Object[] {},null);
            callObjectMethod( jobDescriptorBuilder, "jobClassName", new Object[] { CST_CLASSNAME },null);
            callObjectMethod( jobDescriptorBuilder, "jobName", new Object[] { jobName },null);
            callObjectMethod( jobDescriptorBuilder, "description", new Object[] { "TruckMilkHeartBeat" },null);
            SJobDescriptor jobDescriptor = (SJobDescriptor) callMethod(null, jobDescriptorBuilder, "build", new Object[] {},null);

            /* SJobParameterBuilder */
            Object jobParameterBuilder = callMethod("org.bonitasoft.engine.scheduler.model.SJobParameter", null, "builder", new Object[] {},null);
            callObjectMethod( jobParameterBuilder, "key", new Object[] { CSTPARAMTENANTID },null);
            callObjectMethod( jobParameterBuilder, "value", new Object[] {  Long.valueOf(tenantId) }, new Class[] { Serializable.class });

                    
            SJobParameter jobParameter = (SJobParameter) callObjectMethod( jobParameterBuilder, "build", new Object[] {},null);

            List<SJobParameter> listJobParameters = new ArrayList<>();
            listJobParameters.add(jobParameter);
            bonitaScheduler.schedule(jobDescriptor, listJobParameters, jobTrigger);
            callDeploimentStatus.jobDescriptor = jobDescriptor;
            return callDeploimentStatus;
            
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            callDeploimentStatus.stackTrace = exceptionDetails;
            callDeploimentStatus.exception = e;
            return callDeploimentStatus;
        }

    }


    /* ******************************************************************************** */
    /*                                                                                  */
    /* Call Method by reflection */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    @SuppressWarnings("rawtypes")
    private Object callStaticMethod(String className, String methodName, Object[] parameters,  Class[] parameterClass) throws Exception {
        return callMethod(className, null, methodName, parameters, parameterClass);
    }
        
    @SuppressWarnings("rawtypes")
    private Object callObjectMethod( Object object, String methodName, Object[] parameters,  Class[] parameterClass) throws Exception {
        return callMethod(null, object, methodName, parameters, parameterClass);
        
    }
    
    @SuppressWarnings("rawtypes")
    private Object callMethod(String className, Object object, String methodName, Object[] parameters,  Class[] parameterClass) throws Exception {
        Class<?> callClass = null;

        if (object == null)
            // this is a static method call
            callClass = Class.forName(className);
        else
            callClass = object.getClass();

        /**
         * if null, then calculate it
         */
        if (parameterClass==null)
        {
            parameterClass = new Class[parameters.length];
            for (int i = 0; i < parameters.length; i++) {
                if (parameters[i] instanceof Boolean)
                    parameterClass[i] = boolean.class;
                else
                    parameterClass[i] = parameters[i].getClass();
            }
        }
        Method correctMethod = callClass.getMethod(methodName, parameterClass);
        /*
         * Method[] methods = callClass.getMethods();
         * // search the correct method
         * Method correctMethod=null;
         * for (Method m : methods)
         * {
         * if (m.getName().equals(methodName)) {
         * Class[] methodParameters = m.getParameterTypes();
         * if ( methodParameters.length == parameters.length) {
         * boolean identical=true;
         * for (int i=0;i<methodParameters.length;i++) {
         * if (! methodParameters[ i ].equals(parameters[i].getClass()))
         * identical=false;
         * }
         * if (identical)
         * correctMethod=m;
         * }
         * }
         * }
         */
        if (correctMethod == null)
            throw new Exception("Can't find method");
        // then call
        return correctMethod.invoke(object, parameters);
    }
    
    public List<BEvent> shutdown(long tenantId) {
        List<BEvent> listEvents = new ArrayList<>();
        isStarted = false;
        MilkReportEngine milkReportEngine = MilkReportEngine.getInstance();
        milkReportEngine.reportHeartBeatInformation("SHUTDOWN Quartz Scheduler", true);
        // Kill the job
        try {
            final TenantServiceAccessor tenantAccessor = TenantServiceSingleton.getInstance(tenantId);
            tenantAccessor.getUserTransactionService().executeInTransaction(() -> {
                SchedulerService bonitaScheduler = ServiceAccessorFactory.getInstance().createPlatformServiceAccessor()
                        .getSchedulerService();
                SessionAccessor sessionAccessor = tenantAccessor.getSessionAccessor();
                sessionAccessor.setTenantId(tenantId);
                List<String> allJobs = bonitaScheduler.getAllJobs();
                for (String jobName : allJobs) {
                    if (jobName.equals(getJobTriggerName(tenantId))) {
                        logger.info(logHeader + " QuartzJob[" + getJobTriggerName(tenantId) + "] exist, Delete it");
                        bonitaScheduler.delete(jobName);
                    }
                }
                return null;
            });
            listEvents.add(EVENT_QUARTZ_SCHEDULER_STOPPED);
        } catch (final Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(logHeader + "~~~~~~~~~~  : ERROR " + e + " at " + exceptionDetails);
            listEvents.add(new BEvent(eventQuartzScheduleError, e, ""));
        }
        return listEvents;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Operations */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    @Override
    public List<BEvent> operation(Map<String, Serializable> parameters) {
        if ("heartbeat".equals(parameters.get("scheduleroperation"))) {

        }
        return new ArrayList<>();
    };

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Schedule Maintenance operation */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public List<BEvent> checkAndDeploy(boolean forceDeploy, File pageDirectory, long tenantId) {
        return new ArrayList<>();
    }

    public List<BEvent> reset(long tenantId) {
        return startup(tenantId, true);
    }

    /**
     * give back the name of the trigger
     * 
     * @param tenantId
     * @return
     */
    private String getJobTriggerName(long tenantId) {
        return "trg" + QUARTZMILKJOBNAME + "_" + tenantId;
    }

}
