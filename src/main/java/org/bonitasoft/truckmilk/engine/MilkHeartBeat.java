package org.bonitasoft.truckmilk.engine;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.bonitasoft.engine.connector.ConnectorAPIAccessorImpl;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerFactory;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

/**
 * this class are in charge to execute the HeartBeat
 * @author Firstname Lastname
 *
 */
public class MilkHeartBeat {
    
    static MilkLog logger = MilkLog.getLogger(MilkHeartBeat.class.getName());

    private final static String LOGGER_HEADER="MilkHeartBeat";
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

    public static class SynchronizeHeartBeat {

        boolean heartBeatInProgress = false;
    }

    public final static SynchronizeHeartBeat synchronizeHeart = new SynchronizeHeartBeat();
    private static long heartBeatLastExecution = System.currentTimeMillis();

    
    // to avoid any transaction issue in the command (a transaction is maybe opennend by the engine, and then the processAPI usage is forbiden), let's create a thread
    public void executeOneTimeNewThread(MilkCmdControl milkCmdControl, boolean forceBeat, MilkJobContext milkJobContext) {
        // release the thread before entering the synchronized
        if (synchronizeHeart.heartBeatInProgress) {
            logger.fine(LOGGER_HEADER+"heartBeat in progress, does not start a new one");
            return;
        }
        synchronized (synchronizeHeart) {
            // protection : does not start a new Thread if the current one is not finish (no two Hearthread in the same time)
            if (synchronizeHeart.heartBeatInProgress) {
                logger.fine(LOGGER_HEADER+"heartBeat in progress, does not start a new one");
                return;
            }
            // second protection : Quartz can call the methode TOO MUCH !
            if ( (! forceBeat) && System.currentTimeMillis() < heartBeatLastExecution + 60 * 1000) {
                logger.fine(LOGGER_HEADER+"heartBeat: last execution was too close (last was " + (System.currentTimeMillis() - heartBeatLastExecution) + " ms ago)");
                return;
            }
            synchronizeHeart.heartBeatInProgress = true;
            heartBeatLastExecution = System.currentTimeMillis();
        }
        // the end will be done by the tread

        MyTruckMilkHeartBeatThread mythread = new MyTruckMilkHeartBeatThread(this, milkCmdControl, milkJobContext);
        mythread.start();
    }


    /* ******************************************************************************** */
    /*                                                                                  */
    /* Execution in a thread() */
    /*
     * /*
     */
    /* ******************************************************************************** */


    public static class SynchronizeThreadId {

        public int countThreadId = 0;

    }

    public final static SynchronizeThreadId synchronizeThreadId = new SynchronizeThreadId();

    private class MyTruckMilkHeartBeatThread extends Thread {
        private MilkCmdControl milkCmdControl;
        private MilkHeartBeat milkHeartBeat;
        private MilkJobContext milkJobContext;


        protected MyTruckMilkHeartBeatThread(MilkHeartBeat milkHeartBeat, MilkCmdControl milkCmdControl, MilkJobContext milkJobContext) {
            this.milkCmdControl = milkCmdControl;
            this.milkHeartBeat = milkHeartBeat;
            this.milkJobContext = milkJobContext;
        }

        @Override
        public void run() {
            // New thread : create the new object
            MilkPlugInFactory milkPlugInFactory = MilkPlugInFactory.getInstance(milkJobContext);
            // the getInstance reload everything from the database
            MilkJobFactory milkJobFactory = MilkJobFactory.getInstance(milkPlugInFactory);
            MilkSchedulerFactory milkSchedulerFactory = MilkSchedulerFactory.getInstance();

            milkHeartBeat.doTheHeartBeat(milkJobFactory,milkSchedulerFactory, milkCmdControl);

            synchronizeHeart.heartBeatInProgress = false;

        }
    }

    
    /**
     * thread to execute in a different thread to have a new connection
     * 
     * @author Firstname Lastname
     */
   

    private int thisThreadId = 0;

    public int getThreadId()
    { 
        return thisThreadId;
    }
    public void doTheHeartBeat(MilkJobFactory milkJobFactory,MilkSchedulerFactory milkSchedulerFactory, MilkCmdControl milkCmdControl ) {
        // we want to work as a singleton: if we already manage a Timer, skip this one (the previous one isn't finish)
        synchronized (synchronizeThreadId) {
            synchronizeThreadId.countThreadId++;
            thisThreadId = synchronizeThreadId.countThreadId;

        }
        // Recreate a new job context here, with the local apiAccessor and TenantServiceAccessor
        // to be safe, generate a new apiAccessor
        ConnectorAPIAccessorImpl apiAccessor = new ConnectorAPIAccessorImpl(milkJobFactory.getMilkPlugInFactory().getMilkJobContext().getTenantId());

        MilkJobContext milkJobContext= new MilkJobContext(milkJobFactory.getMilkPlugInFactory().getMilkJobContext().getTenantId(), apiAccessor, milkCmdControl.getTenantServiceAccessor());
        
        // Advance the scheduler that we run now !
        milkSchedulerFactory.informRunInProgress(milkJobContext.getTenantId());

        // MilkPlugInFactory milkPlugInFactory = milkJobFactory.getMilkPlugInFactory();
        // maybe this is the first call after a restart ? 
        if ( ! milkCmdControl.isInitialized()) {
            milkCmdControl.initialization(false, false, milkJobContext.getTenantId(), milkJobFactory);
        }

        long timeBeginHearth = System.currentTimeMillis();
        Date currentDate = new Date();
        InetAddress ip = null;
        try {
            ip = InetAddress.getLocalHost();
        } catch (UnknownHostException e1) {
            logger.severeException(e1, "can't get the ipAddress");

        }
        StringBuilder executionDescription = new StringBuilder();
        executionDescription.append(  " Heart #" + thisThreadId + ": Heat at "+sdf.format(currentDate)+", Host[" + (ip == null ? "" : ip.getHostAddress()) + "];<br>");
        logger.fine( executionDescription.toString());
        
        try {
            // check all the Job now
            for (MilkJob milkJob : milkJobFactory.getListJobs()) {
                
                MilkExecuteJobThread milkExecuteJobThread = new MilkExecuteJobThread(milkJob, milkJobContext);

                executionDescription.append( milkExecuteJobThread.checkAndStart(currentDate,milkSchedulerFactory.getCurrentScheduler())+"<br>");
            }
            if (executionDescription.length() == 0)
                executionDescription.append( "<b>No jobs executed;</b><br>");


        } catch (Exception e) {
            logger.severeException(e, ".executeTimer: Exception " + e.getMessage());
        } catch (Error er) {
            logger.severe(".executeTimer: Error " + er.getMessage());
        }
        long timeEndHearth = System.currentTimeMillis();
        executionDescription.append(" Heart executed in " + (timeEndHearth - timeBeginHearth) + " ms");
        // logger.info("MickCmdControl.beathearth #" + thisThreadId + " : Start at " + sdf.format(currentDate) + ", End in " + (timeEndHearth - timeBeginHearth) + " ms on [" + (ip == null ? "" : ip.getHostAddress()) + "] " + executionDescription);
        MilkReportEngine milkReportEngine = MilkReportEngine.getInstance();
        MilkSchedulerInt scheduler = milkSchedulerFactory.getCurrentScheduler();
        milkReportEngine.reportHeartBeatInformation(executionDescription.toString(),true, scheduler.isLogHeartBeat() );
        
    }
    

  
}
