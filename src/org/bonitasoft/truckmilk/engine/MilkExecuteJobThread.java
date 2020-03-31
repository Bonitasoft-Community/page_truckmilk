package org.bonitasoft.truckmilk.engine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.connector.ConnectorAPIAccessorImpl;
import org.bonitasoft.engine.service.TenantServiceAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkJobFactory.MilkFactoryOp;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.engine.MilkSerializeProperties.SerializationJobParameters;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

/**
 * Create a Thread to execute a job
 * 
 * @author Firstname Lastname
 */
public class MilkExecuteJobThread extends Thread {

    static MilkLog logger = MilkLog.getLogger(MilkExecuteJobThread.class.getName());

    
    private SimpleDateFormat sdfSynthetic = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static BEvent eventPlugInViolation = new BEvent(MilkExecuteJobThread.class.getName(), 1, Level.ERROR,
            "Plug in violation",
            "A plug in must return a status on each execution. The plug in does not respect the contract",
            "No report is saved", "Contact the plug in creator");

    private static BEvent eventPlugInError = new BEvent(MilkExecuteJobThread.class.getName(), 2, Level.ERROR,
            "Plug in error", "A plug in throw an error", "No report is saved", "Contact the plug in creator");

    private MilkJob milkJob;
    private Date tagTheDate;
    private APIAccessor apiAccessor;
    private TenantServiceAccessor tenantServiceAccessor;
    
    protected MilkExecuteJobThread(MilkJob milkJob, APIAccessor apiAccessor, TenantServiceAccessor tenantServiceAccessor) {
        this.milkJob = milkJob;
        this.apiAccessor = apiAccessor;
        this.tenantServiceAccessor = tenantServiceAccessor;
    }

    /**
     * check and start the job in a different thread
     * 
     * @param currentDate
     * @return
     */
    public String checkAndStart(Date currentDate) {
        StringBuilder executionDescription = new StringBuilder();
        executionDescription.append( "Job[" + milkJob.toString() + "]:" );
        boolean saveJob=false;
        tagTheDate = currentDate;

        if (!milkJob.isInsideHostsRestriction()) {
            executionDescription.append( "Host Restriction[" + milkJob.hostsRestriction + "] so can't start in this host;");
            return executionDescription.toString();
        }

        if (milkJob.inExecution()) {
            executionDescription.append( "In execution;");
            return executionDescription.toString();
        }
        // already logged in the toString() executionDescription.append( (milkJob.isEnable()? " ENABLE " : " Disable ") + (milkJob.isImmediateExecution()?" ImmediateExecution ":"") + ";");
        if (milkJob.isEnable() || milkJob.isImmediateExecution()) {
            
            boolean start=milkJob.isImmediateExecution();
            if (milkJob.isEnable()) {
                // protection : recalculate a date then for a enable
                if (milkJob.getNextExecutionDate() == null) {
                    milkJob.calculateNextExecution("HeartBeat-NextExecutionDate Is Set");
                    executionDescription.append("No NextExecutionDate, recalculate to["+sdfSynthetic.format(milkJob.getNextExecutionDate())+"]");
                    // we calculated a date : save it
                    saveJob=true;
                }
                // already in the toString() executionDescription.append( "Next[" + milkJob.getNextExecutionDateSt() + "];");
                // check if we need to start now
                if ( (milkJob.getNextExecutionDate() != null
                        && milkJob.getNextExecutionDate().getTime() <= currentDate.getTime())) {
                    start=true;
                }
            }
            // Attention, in a Cluster environment, we must not start the job on two different node.
            if (start) {
                String statusDoubleStart= synchronizeClusterDoubleStart();
                if (statusDoubleStart != null) {
                    executionDescription.append( statusDoubleStart );
                    return executionDescription.toString();
                }
             }
            
            // if we start, job will be saved
            if (start) {
                // now, it's locked on this node 
                executionDescription.append( "  *** STARTED *** ;");
                this.start();
            } else if (saveJob) {
                milkJob.milkJobFactory.dbSaveJob(milkJob, SerializationJobParameters.getInstanceStartExecutionJob());

            }
                
        } // end isEnable
        return executionDescription.toString();
    }

    /**
     * return null if everything is OK, else a status
     * @return
     */
    private String synchronizeClusterDoubleStart() {
        InetAddress ip;
        try {
            ip = InetAddress.getLocalHost();
        } catch (UnknownHostException e1) {
            return "synchronizeClusterDoubleStart: Can't get the ipAddress, don't start Jobs";
        }
        MilkJobFactory milkJobFactory = milkJob.milkJobFactory;
        // register this host to start
        milkJob.registerExecutionOnHost(ip.getHostAddress());

        // logger.info("synchronizeClusterDoubleStart StartJob - instantiationHost [" + ip.getHostAddress() + "]");

        milkJobFactory.dbSaveJob(milkJob, SerializationJobParameters.getInstanceTrackExecution());

        try {
            Thread.sleep(2L * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // ok, read it : is that still me ?
        MilkFactoryOp milkJobRead = milkJobFactory.dbLoadJob(milkJob.getId());
        if (milkJobRead.job != null && milkJobRead.job.getHostRegistered().equals(ip.getHostAddress()))
            return null; // that's me ! 

        // someone else steel my job, be fair, do nothing
        return "synchronizeClusterDoubleStart: Jobs already register on["+ (milkJobRead.job == null ? "Can't read job" : milkJobRead.job.getHostRegistered()) + "] myself=[" + ip.getHostAddress() + "]";
    }

    /** now do the real execution */
    public void run() {

        MilkJobFactory milkJobFactory = milkJob.milkJobFactory;
        MilkReportEngine milkReportEngine = MilkReportEngine.getInstance();
        // ConnectorAPIAccessorImpl connectorAccessorAPI = new ConnectorAPIAccessorImpl( milkJobFactory.getTenantId());
        
        List<BEvent> listEvents = new ArrayList<>();
        MilkPlugIn plugIn = milkJob.getPlugIn();
        MilkJobOutput output = null;
        try {
            String hostName = "";
            try {
                InetAddress ip = InetAddress.getLocalHost();
                hostName = ip.getHostName() + "(" + ip.getHostAddress() + ")";
            } catch (UnknownHostException e1) {
                logger.severe("MilkExecuteJobThread: can't get the ipAddress, synchronization on a cluster can't work");
            }
            // execute it!
            MilkJobExecution milkJobExecution = new MilkJobExecution(milkJob, milkJobFactory.getTenantId(), apiAccessor, tenantServiceAccessor );

            // ----------------- Execution
            long timeBegin = System.currentTimeMillis();
            milkJobExecution.start();
            try {

                milkReportEngine.reportHeartBeatInformation("Start Job[" + milkJob.getName() + "] (" + milkJob.getId() + ")" );

                // save the status in the database
                // save the start Status (so general) and the track Status, plus askStop to false
                listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SerializationJobParameters.getInstanceStartExecutionJob()));

                output = plugIn.execute(milkJobExecution);
                
                output.hostName = hostName;
                StringBuilder listEventsSt = new StringBuilder();
                for (final BEvent event : listEvents) {
                    listEventsSt.append( event.toString() + " <~> ");
                }
                logger.info("End Job[" + milkJob.getName() + "] (" + milkJob.getId() + ")" + listEventsSt.toString());

                // force the status ABORD status
                if (milkJobExecution.pleaseStop() && (output.executionStatus == ExecutionStatus.SUCCESS))
                    output.executionStatus = ExecutionStatus.SUCCESSABORT;
                // if the user ask to stop, then this is a successabort if this is correct (do not change a ERROR)
                if (milkJob.isAskForStop() && (output.executionStatus == ExecutionStatus.SUCCESS || output.executionStatus == ExecutionStatus.SUCCESSPARTIAL))
                    output.executionStatus = ExecutionStatus.SUCCESSABORT;
            } catch (Exception e) {

                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String exceptionDetails = sw.toString();
                logger.severe(" Job[" + milkJob.getName() + "] (" + milkJob.getId() + "] milkJobExecution.getPlugIn[" + plugIn.getName() + "]  Exception " + e.getMessage() + " at " + exceptionDetails);
                if (output == null) {
                    output = new MilkJobOutput(milkJob);
                    output.addEvent(new BEvent(eventPlugInViolation, "Job[" + milkJob.getName() + "] (" + milkJob.getId() + "] milkJobExecution.getPlugIn[" + plugIn.getName() + "]  Exception " + e.getMessage() + " at " + exceptionDetails));
                    output.executionStatus = ExecutionStatus.CONTRACTVIOLATION;
                }
            } catch (Error er) {
                StringWriter sw = new StringWriter();
                er.printStackTrace(new PrintWriter(sw));
                String exceptionDetails = sw.toString();
                logger.severe(" Job[" + milkJob.getName() + "] (" + milkJob.getId() + "] milkJobExecution.getPlugIn[" + plugIn.getName() + "]  Exception " + er.getMessage() + " at " + exceptionDetails);
                if (output == null) {
                    output = new MilkJobOutput(milkJob);
                    output.addEvent(new BEvent(eventPlugInViolation, "Job[" + milkJob.getName() + "] (" + milkJob.getId() + "] milkJobExecution.getPlugIn[" + plugIn.getName() + "]  Exception " + er.getMessage() + " at " + exceptionDetails));
                    output.executionStatus = ExecutionStatus.CONTRACTVIOLATION;
                }
            }
            milkJobExecution.end();

            long timeEnd = System.currentTimeMillis();

            output.executionTimeInMs = (timeEnd - timeBegin);

            output.setMesure( MilkPlugInDescription.cstMesureTimeExecution, output.executionTimeInMs);
            output.setMesure( MilkPlugInDescription.cstMesureNbItemProcessed, output.nbItemsProcessed);
            
            milkJob.addMesureValues( new Date(), output.getAllMesures());
            // executionDescription += "(" + output.executionStatus + ") " + output.nbItemsProcessed + " in " + output.executionTimeInMs + ";";

        } catch (Exception e) {
            output = new MilkJobOutput(milkJob);
            output.addEvent(new BEvent(eventPlugInError, e, "PlugIn[" + plugIn.getName() + "]"));
            output.executionStatus = ExecutionStatus.ERROR;
            logger.severe(" Execution error " + e.getMessage());
        }
        if (output != null) {
            // maybe the plugin forgot to setup the execution ? So set it.
            if (output.executionStatus == ExecutionStatus.NOEXECUTION)
                output.executionStatus = ExecutionStatus.SUCCESS;

            milkJob.registerExecution(tagTheDate, output);
            listEvents.addAll(output.getListEvents());
        }
        // calculate the next time

        listEvents.addAll(milkJob.calculateNextExecution("End-Of-Execution-recalculate"));
        milkReportEngine.reportHeartBeatInformation("End Job[" + milkJob.getName() + "] (" + milkJob.getId() + ") Status["+output.executionStatus.toString()+"] NewNextDate["+sdfSynthetic.format(milkJob.getNextExecutionDate())+"]");

        milkJob.setImmediateExecution(false);
        milkJob.setAskForStop(false);
        milkJob.setInExecution(false); // double check the end

        listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SerializationJobParameters.getInstanceEndExecutionJob()));

        // hum, nothing where to save the listEvent of the execution.
    }
}
