package org.bonitasoft.truckmilk.schedule;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.bonitasoft.engine.builder.BuilderFactory;
import org.bonitasoft.engine.dependency.DependencyService;
import org.bonitasoft.engine.dependency.SDependencyException;
import org.bonitasoft.engine.dependency.SDependencyNotFoundException;
import org.bonitasoft.engine.dependency.model.SDependency;
import org.bonitasoft.engine.dependency.model.ScopeType;
import org.bonitasoft.engine.scheduler.SchedulerService;
import org.bonitasoft.engine.scheduler.builder.SJobDescriptorBuilderFactory;
import org.bonitasoft.engine.scheduler.builder.SJobParameterBuilderFactory;
import org.bonitasoft.engine.scheduler.model.SJobDescriptor;
import org.bonitasoft.engine.scheduler.model.SJobParameter;
import org.bonitasoft.engine.scheduler.trigger.Trigger;
import org.bonitasoft.engine.scheduler.trigger.UnixCronTrigger;
import org.bonitasoft.engine.service.PlatformServiceAccessor;
import org.bonitasoft.engine.service.impl.ServiceAccessorFactory;
import org.bonitasoft.engine.transaction.TransactionService;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;

/**
 * Quartz Job
 * The CmdControl has to be call by the Quartz job at a frequency. This is the goal of this class
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
public class MilkScheduleQuartz implements MilkSchedulerInt {

    private static BEvent EVENT_QUARTZ_SCHEDULE_ERROR = new BEvent(MilkScheduleQuartz.class.getName(), 1, Level.ERROR,
            "Quartz Job failed", "Check the error", "The different monitoring can't run", "RESET the Scheduler. Check installation.");

    private static BEvent EVENT_DEPLOY_QUARTZ_JOB = new BEvent(MilkScheduleQuartz.class.getName(), 2, Level.ERROR,
            "Deploy Quartz Job failed", "To have an execution with the correct frequency, a job has to be deploy. Java class implementation of the job failed", "The different monitoring can't run", "See the error");

    private static BEvent EVENT_QUARTZ_NO_JOB = new BEvent(MilkScheduleQuartz.class.getName(), 3, Level.ERROR,
            "No Quartz Job found", "The Quartz Job does not exist", "No monitoring", "Reset the scheduler");

    private static BEvent EVENT_QUARTZ_JOB_CLASS_NOT_DEPLOYED = new BEvent(MilkScheduleQuartz.class.getName(), 4, Level.ERROR,
            "Quartz Job not deployed", "The JAR file containing the Quartz Job [TruckMilk-1.0-Quartzjob.jar] is not deployed. It must be deployed manually in the webapps/bonita/WEB-INF/lib", "No monitoring",
            "Deploy it: copy the JAR file [TruckMilk-1.0-Quartzjob.jar] delivered in the custom page to the Bonita Web Application (<tomcat>/webapps/bonita/WEB-INF/lib) ");

    private static BEvent EVENT_QUARTZ_JOB_UP_AND_RUNNING = new BEvent(MilkScheduleQuartz.class.getName(), 4, Level.SUCCESS,
            "Quartz Job Up and running", "The Quartz Job is up and running");

    private static BEvent EVENT_QUARTZ_SCHEDULER_STOPPED = new BEvent(MilkScheduleQuartz.class.getName(), 5, Level.SUCCESS,
            "Scheduler is stopped", "The Scheduler is stopped");

    private static BEvent EVENT_QUARTZ_SCHEDULER_STARTED = new BEvent(MilkScheduleQuartz.class.getName(), 6, Level.SUCCESS,
            "Scheduler is started", "The Scheduler is started");

    private static Logger logger = Logger.getLogger(MilkScheduleQuartz.class.getName());
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

    public MilkScheduleQuartz() {
        super();
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Abstract for Milk Controler */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    /** check if the class MilkQuartzJob is accessible */
    public List<BEvent> check(long tenantId) {
        List<BEvent> listEvents = new ArrayList<BEvent>();

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
            SchedulerService bonitaScheduler = ServiceAccessorFactory.getInstance().createPlatformServiceAccessor()
                    .getSchedulerService();
            List<String> allJobs = bonitaScheduler.getAllJobs();
            boolean isQuartzJob = false;
            for (String jobName : allJobs) {
                if (jobName.equals(getJobTriggerName(tenantId))) {
                    {
                        statusScheduler.status = TypeStatus.STARTED;
                        // no way to get the JobDescriptor !
                        isQuartzJob = true;
                    }
                }
            }
            if (!isQuartzJob)
                statusScheduler.listEvents.add(EVENT_QUARTZ_NO_JOB);

            // can access the Job class ?
            boolean isQuartzClass = false;
            statusScheduler.listEvents.addAll(check(tenantId));
            isQuartzClass = !BEventFactory.isError(statusScheduler.listEvents);

            if (isQuartzJob && isQuartzClass)
                statusScheduler.listEvents.add(EVENT_QUARTZ_JOB_UP_AND_RUNNING);

        } catch (Exception e) {

        }

        return statusScheduler;

    }

    /**
     * start the scheduler
     * 
     * @return
     */
    public List<BEvent> start(long tenantId) {
        isStarted = true;
        ArrayList<BEvent> listEvents = new ArrayList<BEvent>();
        listEvents.add(EVENT_QUARTZ_SCHEDULER_STARTED);
        return listEvents;
    }

    /**
     * stop it
     * 
     * @return
     */
    public List<BEvent> stop(long tenantId) {
        isStarted = false;
        ArrayList<BEvent> listEvents = new ArrayList<BEvent>();
        listEvents.add(EVENT_QUARTZ_SCHEDULER_STOPPED);
        return listEvents;
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
        return "The QUARTZ engine embeded in Bonita is used to schedule the monitoring. Copy bonita-truckmilkquartzjob-client.jar to the Web Application server (webapp/bonita/WEB-INF/lib for a tomcat) and restart the server.";
    };

    public Execution getExecution() {
        return mExecution;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Initialize : create the job */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    private final static String QuartzMilkJobName = "MilktruckJob";

    /**
     * this definition is duplicated in the class MilkQuartzJob and can't be in the same class, 2 Jar
     * will be deployed and not at the same level
     */
    public final static String cstParamTenantId = "tenantId";
    /**
     * on which scope the Quartz Jar has to be deployed ?
     */
    public final static ScopeType cstScopeDeployment = ScopeType.GLOBAL;

    /**
     * artefactId is to keep a internal ID, like processId for a Scope.Process
     */
    // public final static long cstArtefactId=0;

    public List<BEvent> startup(long tenantId, boolean forceReset) {
        List<BEvent> listEvents = new ArrayList<BEvent>();

        logger.info(logHeader + "Startup QuartzJob");

        try {
            SchedulerService bonitaScheduler = ServiceAccessorFactory.getInstance().createPlatformServiceAccessor()
                    .getSchedulerService();

            List<String> allJobs = bonitaScheduler.getAllJobs();
            for (String jobName : allJobs) {
                if (jobName.equals(getJobTriggerName(tenantId))) {
                    logger.info(logHeader + " QuartzJob[" + getJobTriggerName(tenantId) + "] already exist," + (forceReset ? "Delete and schedule it" : ""));
                    if (forceReset)
                        bonitaScheduler.delete(jobName);
                    else
                        return listEvents;
                }
            }
            // Trigger is the WHEN job has to start
            // every minutes : see https://www.freeformatter.com/cron-expression-generator-quartz.html
            String cronString;

            cronString = "0 0/1 * 1/1 * ? *"; // every minutes
            // cronString = "0/20 0 0 ? * * *"; // every 20 s

            final Trigger syncJobTrigger = new UnixCronTrigger(
                    getJobTriggerName(tenantId),
                    new Date(),
                    cronString,
                    org.bonitasoft.engine.scheduler.trigger.Trigger.MisfireRestartPolicy.ALL);

            // Job descriptor is WHAT to run
            SJobDescriptorBuilderFactory jobDescriptorBuilder = BuilderFactory.get(SJobDescriptorBuilderFactory.class);
            SJobDescriptor jobDescriptor = jobDescriptorBuilder
                    .createNewInstance("org.bonitasoft.truckmilk.schedule.quartz.MilkQuartzJob", getJobTriggerName(tenantId), false).done();

            List<SJobParameter> syncJobParameters = new ArrayList<SJobParameter>();
            // tenantId in the job param
            SJobParameterBuilderFactory jobParameterBuilderFactory = BuilderFactory.get(SJobParameterBuilderFactory.class);
            syncJobParameters.add(jobParameterBuilderFactory.createNewInstance(cstParamTenantId, tenantId).done());

            bonitaScheduler.schedule(jobDescriptor, syncJobParameters, syncJobTrigger);
            logger.info(logHeader + " QuartzJob[" + getJobTriggerName(tenantId) + "] Started with Cron[" + cronString + "]");

        } catch (final Exception e) {

            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();

            logger.severe(logHeader + "~~~~~~~~~~  : ERROR " + e + " at " + exceptionDetails);
            listEvents.add(new BEvent(EVENT_QUARTZ_SCHEDULE_ERROR, e, ""));
        }

        return listEvents;
    }

    public List<BEvent> shutdown(long tenantId) {
        // do nothing, it's managed bvy quartz
        return new ArrayList<BEvent>();
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Schedule Maintenance operation */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public List<BEvent> checkAndDeploy(boolean forceDeploy, File pageDirectory, long tenantId) {
        File fileQuartzJar = new File(pageDirectory.getAbsolutePath() + "/lib/TruckMilkQuartzjob-1.0.0-client.jar");
        // signature=getSignature(fileQuartzJar);
        return MilkScheduleQuartz.deployQuartzJob(forceDeploy, "", fileQuartzJar, tenantId, pageDirectory);
    }

    public List<BEvent> reset(long tenantId) {
        return startup(tenantId, true);
    }

    /**
     * we need a callback for Quartz.
     * This JAR must be deployed at the GLOBAL level then
     * org.bonitasoft.engine.scheduler.impl.SchedulerServiceImpl can find it.
     * 
     * @param fileJar containing only the Quartz job
     * @return
     */
    private static List<BEvent> deployQuartzJob(final boolean forceDeploy, final String version, File fileJar, long tenantId,
            File pageDirectory) {
        List<BEvent> listEvents = new ArrayList<BEvent>();

        FileInputStream inputFileJar = null;
        final ByteArrayOutputStream fileContent = new ByteArrayOutputStream();
        final byte[] buffer = new byte[10000];
        int nbRead = 0;
        String message = "Deploy QuartzJob;";
        boolean messageError = false;
        PlatformServiceAccessor platformAccessor;
        TransactionService transactionService = null;
        /*
         * to detect a new JAR file, give as the dependency name the file name plus its modified date
         * This should not work on a Cluster environment, an another way have to be find.
         * The best will be to calculate a signature on the JAR.
         */
        String dependencyName = fileJar.getName(); // impossible to set the signature :name too big
        try {
            platformAccessor = ServiceAccessorFactory.getInstance().createPlatformServiceAccessor();
            transactionService = platformAccessor.getTransactionService();
            final DependencyService dependencyService = platformAccessor.getDependencyService();
            long classLoaderId = platformAccessor.getClassLoaderService().getGlobalClassLoaderId();
            // Ok, open the transaction (attention, seem not be thread safe, we will see that after).
            transactionService.begin();

            // if this dependency is already deployed ?
            if (existDependency(dependencyName, classLoaderId, tenantId, dependencyService) && (!forceDeploy)) {
                message += "Dependency[" + dependencyName + "] Already exist;";
            } else {
                // Let's remove some another old deployed jar file
                try {
                    String baseName = fileJar.getName();
                    int pos = baseName.indexOf("-");
                    if (pos != -1) {
                        baseName = baseName.substring(0, pos);
                    }
                    message += "baseName[" + baseName + "];";
                    if (baseName.length() > 10) {
                        // remove based on the baseName, to find all another data
                        int nbDepencency = removeAllDependencies(baseName, classLoaderId, tenantId, dependencyService);
                        message += "Remove[" + nbDepencency + "] dependencies;";
                    } else
                        message += "BaseName too small, don't remove dependencies;";
                } catch (SDependencyNotFoundException e) {
                    message += "Error " + e.getMessage();
                    messageError = true;
                } catch (SDependencyException e2) {
                    message += "Error " + e2.getMessage();
                    messageError = true;
                } ;

                // load the new one now
                message += "Load[" + fileJar.getAbsolutePath() + "];";
                inputFileJar = new FileInputStream(fileJar.getAbsolutePath());

                while ((nbRead = inputFileJar.read(buffer)) > 0) {
                    fileContent.write(buffer, 0, nbRead);
                }
                message += "Load dependency in the engine;";
                addDependency(dependencyName, fileContent.toByteArray(), classLoaderId, tenantId, dependencyService);
                // Yes, here we are
                message += "Deploy with success;";
            }
        } catch (final Exception e) {
            message += "Error " + e.getMessage();
            messageError = true;
            listEvents.add(new BEvent(EVENT_DEPLOY_QUARTZ_JOB, e, "Jar file[" + fileJar.getAbsolutePath() + "]"));
        } finally {
            try {
                if (transactionService != null)
                    transactionService.complete();
            } catch (Exception e) {
                message += "Error during commitTransaction:" + e.getMessage();
                messageError = true;
            } ;

            try {
                if (inputFileJar != null)
                    inputFileJar.close();
            } catch (Exception e) {
            } ;
        }
        if (messageError)
            logger.severe(logHeader + message);
        else
            logger.info(logHeader + message);
        return listEvents;
    }

    /**
     * @param name
     * @param dependencyService
     * @throws Exception
     *
    private static void removeDependency(final String name, long artefactId, long tenantId, DependencyService dependencyService) throws Exception {

        dependencyService.deleteDependency(name);
        dependencyService.refreshClassLoaderAfterUpdate(cstScopeDeployment, artefactId);
    }
    */

    /**
     * check if the dependency exist
     * 
     * @param name
     * @param dependencyService
     * @return
     * @throws Exception
     */
    public static boolean existDependency(final String name, long artefactId, long tenantId, DependencyService dependencyService) throws Exception {

        List<Long> listDepency = dependencyService.getDependencyIds(artefactId, cstScopeDeployment, 0, 1000);
        for (Long depencyId : listDepency) {
            SDependency dependency = dependencyService.getDependency(depencyId);
            if (dependency.getName().startsWith(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * remove all existing depencendy based on the name
     * 
     * @param baseName
     * @param dependencyService
     * @return
     * @throws Exception
     */
    private static int removeAllDependencies(final String baseName, long classLoaderId, long tenantId, DependencyService dependencyService) throws Exception {
        int count = 0;
        List<Long> listDepency = dependencyService.getDependencyIds(classLoaderId, cstScopeDeployment, 0, 1000);
        for (Long depencyId : listDepency) {
            SDependency dependency = dependencyService.getDependency(depencyId);
            if (dependency.getName().startsWith(baseName)) {
                count++;
                dependencyService.deleteDependency(dependency.getName());
            }

        }
        return count;
    }

    private static void addDependency(final String name, final byte[] jar, long artefactId, long tenantId, DependencyService dependencyService) throws Exception {

        dependencyService.createMappedDependency(name, jar, name, artefactId, cstScopeDeployment);
        dependencyService.refreshClassLoaderAfterUpdate(cstScopeDeployment, artefactId);
    }

    /**
     * give back the name of the trigger
     * 
     * @param tenantId
     * @return
     */
    private String getJobTriggerName(long tenantId) {
        return "trg" + QuartzMilkJobName + "_" + tenantId;
    }

}
