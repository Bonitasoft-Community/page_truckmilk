package org.bonitasoft.truckmilk.engine;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.bonitasoft.engine.connector.ConnectorAPIAccessorImpl;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkJobFactory.MilkFactoryOp;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.ExecutionStatus;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugTourOutput;
import org.bonitasoft.truckmilk.engine.MilkSerializeProperties.SaveJobParameters;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJobExecution;

/**
 * Create a Thread to execute a job
 * @author Firstname Lastname
 *
 */
public class MilkExecuteJobThread extends Thread {

    static Logger logger = Logger.getLogger(MilkExecuteJobThread.class.getName());

    private static BEvent EVENT_PLUGIN_VIOLATION = new BEvent(MilkExecuteJobThread.class.getName(), 1, Level.ERROR,
            "Plug in violation",
            "A plug in must return a status on each execution. The plug in does not respect the contract",
            "No report is saved", "Contact the plug in creator");

    private static BEvent EVENT_PLUGIN_ERROR = new BEvent(MilkExecuteJobThread.class.getName(), 2, Level.ERROR,
            "Plug in error", "A plug in throw an error", "No report is saved", "Contact the plug in creator");

    
    private MilkJob milkJob;
    private Date tagTheDate;

    protected MilkExecuteJobThread(MilkJob milkJob) {
        this.milkJob = milkJob;
    }

    /**
     * check and start the job in a different thread
     * @param currentDate
     * @return
     */
    public String checkAndStart(Date currentDate) {
        String executionDescription = "";
        
        tagTheDate = currentDate;
        
        if (! milkJob.isInsideHostsRestriction())
            return executionDescription;
        
        if (milkJob.inExecution())
            return executionDescription;
        if (milkJob.isEnable || milkJob.isImmediateExecution()) {
            // protection : recalculate a date then
            if (milkJob.nextExecutionDate == null)
                milkJob.calculateNextExecution();

            if (milkJob.isImmediateExecution() || milkJob.nextExecutionDate != null
                    && milkJob.nextExecutionDate.getTime() < currentDate.getTime()) {

                // Attention, in a Cluster environment, we must not start the job on two different node.
                if ( ! synchronizeClusterDoubleStart() )
                {
                    executionDescription += "Executed on differenot host";
                    return executionDescription;
                }
                // now, it's locked on this node 
                
                
                
                if (milkJob.isImmediateExecution)
                    executionDescription += "(i)";
                executionDescription += " " + milkJob.getName() + " ";

                this.start();
                
            }
        } // end isEnable
        return executionDescription;
    }
    
    
    private boolean synchronizeClusterDoubleStart() {
        InetAddress ip;
        try {
            ip = InetAddress.getLocalHost();
        } catch (UnknownHostException e1) {
            logger.severe("MilkExecuteJobThread: can't get the ipAddress, synchronization on a cluster can't work");
            return true;
        }
        MilkJobFactory milkJobFactory = milkJob.milkJobFactory;
        // register this host to start
        milkJob.registerExecutionOnHost(ip.getHostAddress());
        
        milkJobFactory.dbSaveJob(milkJob, SaveJobParameters.getInstanceTrackExecution());
        
        try {
            Thread.sleep(2*1000);
        } catch (InterruptedException e) {
        }
        
        // ok, read it : is that still me ?
        MilkFactoryOp milkJobRead = milkJobFactory.dbLoadJob(milkJob.getId());
        if (milkJobRead.job!=null && milkJobRead.job.getHostRegistered().equals( ip.getHostAddress() ))
            return true; // that's me ! 

        // someone else steel my job, be fair, do nothing
        return false;
    }
    
    
    /** now do the real execution */
    public void run() {

        MilkJobFactory milkJobFactory = milkJob.milkJobFactory;
        ConnectorAPIAccessorImpl connectorAccessorAPI = new ConnectorAPIAccessorImpl( milkJobFactory.getTenantId() );

        
        List<BEvent> listEvents = new ArrayList<BEvent>();
        MilkPlugIn plugIn = milkJob.getPlugIn();
        PlugTourOutput output = null;
        try {
            String hostName="";
            try {
                InetAddress ip = InetAddress.getLocalHost();
                hostName = ip.getHostName()+"("+ip.getHostAddress()+")";
            } catch (UnknownHostException e1) {
                logger.severe("MilkExecuteJobThread: can't get the ipAddress, synchronization on a cluster can't work");                
            }
            // execute it!
            MilkJobExecution milkJobExecution = new MilkJobExecution(milkJob);
            

            // ----------------- Execution
            long timeBegin = System.currentTimeMillis();
            try {
                milkJobExecution.start();
                
                // save the status in the database
                // save the start Status (so general) and the track Status, plus askStop to false
                listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SaveJobParameters.getInstanceAllInformations()));
                
                output = plugIn.execute(milkJobExecution, connectorAccessorAPI);
                output.hostName = hostName;
                milkJobExecution.end();
                // force the status ABORD status
                if (milkJobExecution.pleaseStop() && ( output.executionStatus == ExecutionStatus.SUCCESS) )
                    output.executionStatus = ExecutionStatus.SUCCESSABORT;
                // if the user ask to stop, then this is a successabort if this is correct (do not change a ERROR)
                if (milkJob.askForStop && ( output.executionStatus == ExecutionStatus.SUCCESS || output.executionStatus == ExecutionStatus.SUCCESSPARTIAL))
                    output.executionStatus = ExecutionStatus.SUCCESSABORT;
            } catch (Exception e) {
                if (output == null) {
                    output = new PlugTourOutput(milkJob);
                    output.addEvent(new BEvent(EVENT_PLUGIN_VIOLATION, "PlugIn[" + plugIn.getName() + "] Exception " + e.getMessage()));
                    output.executionStatus = ExecutionStatus.CONTRACTVIOLATION;
                }
            }
            long timeEnd = System.currentTimeMillis();

           
            output.executionTimeInMs = (timeEnd - timeBegin);

            // executionDescription += "(" + output.executionStatus + ") " + output.nbItemsProcessed + " in " + output.executionTimeInMs + ";";

        } catch (Exception e) {
            output = new PlugTourOutput(milkJob);
            output.addEvent(new BEvent(EVENT_PLUGIN_ERROR, e, "PlugIn[" + plugIn.getName() + "]"));
            output.executionStatus = ExecutionStatus.ERROR;
        }
        if (output != null) {
            // maybe the plugin forgot to setup the execution ? So set it.
            if (output.executionStatus == ExecutionStatus.NOEXECUTION)
                output.executionStatus = ExecutionStatus.SUCCESS;

            milkJob.registerExecution(tagTheDate, output);
            listEvents.addAll(output.getListEvents());
        }
        // calculate the next time
        listEvents.addAll(milkJob.calculateNextExecution());
        milkJob.setImmediateExecution(false);
        milkJob.setAskForStop(false);
        listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SaveJobParameters.getInstanceEndExecutionJob()));
        
        // hum, nothing where to save the listEvent of the execution.
    }
}
