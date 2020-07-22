package org.bonitasoft.truckmilk.engine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;




import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkJobFactory.MilkFactoryOp;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJob.SavedExecution;
import org.bonitasoft.truckmilk.engine.MilkSerializeProperties.SerializationJobParameters;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJobMonitor;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

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
    private MilkJobContext milkJobContext;
    
    
    protected MilkExecuteJobThread(MilkJob milkJob, MilkJobContext milkJobContext) {
        this.milkJob = milkJob;
        this.milkJobContext = milkJobContext;
    }

    /**
     * check and start the job in a different thread
     * 
     * @param currentDate
     * @return
     */
    public String checkAndStart(Date currentDate, MilkSchedulerInt scheduler) {
        StringBuilder executionDescription = new StringBuilder();
        executionDescription.append( milkJob.getHtmlInfo() );
        boolean saveJob=false;
        tagTheDate = currentDate;

        if (!milkJob.isInsideHostsRestriction()) {
            executionDescription.append( "Host Restriction[" + milkJob.hostsRestriction + "] so can't start in this host;");
            return executionDescription.toString();
        }

        if (milkJob.inExecution()) {
            executionDescription.append( "<b>In execution</b>;");
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
                    executionDescription.append("NextExecution:"+TypesCast.getHumanDate(milkJob.getNextExecutionDate())+" / CurrentDate:"+TypesCast.getHumanDate(currentDate)+" START");
                    start=true;
                }
            }
            // Attention, in a Cluster environment, we must not start the job on two different node.
            if (start) {
                if (!scheduler.isClusterProtected())
                {
                    String statusDoubleStart= synchronizeClusterDoubleStart();
                    if (statusDoubleStart != null) {
                        executionDescription.append( statusDoubleStart );
                        return executionDescription.toString();
                    }
                }
             }
            
            // if we start, job will be saved
            if (start) {
                // now, it's locked on this node 
                executionDescription.append( "<b>*** STARTED ***</b>;");
                this.start();
                this.setName(  getThreadName(milkJob) );
            } else if (saveJob) {
                milkJob.milkJobFactory.dbSaveJob(milkJob, SerializationJobParameters.getInstanceStartExecutionJob());

            }
                
        } // end isEnable
        return executionDescription.toString();
    }

    /**
     * Cluster is protected by the 
     */
   
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
        milkJob.registerExecutionOnThisHost();

        // logger.info("synchronizeClusterDoubleStart StartJob - instantiationHost [" + ip.getHostAddress() + "]");

        milkJobFactory.dbSaveJob(milkJob, SerializationJobParameters.getInstanceTrackExecution());

        try {
            Thread.sleep(2L * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // ok, read it : is that still me ?
        MilkFactoryOp milkJobRead = milkJobFactory.dbLoadJob(milkJob.getId());
        if (milkJobRead.job != null && milkJobRead.job.getHostNameRegistered().equals(ip.getHostName()))
            return null; // that's me ! 

        // someone else steel my job, be fair, do nothing
        return "synchronizeClusterDoubleStart: Jobs already register on["+ (milkJobRead.job == null ? "Can't read job" : milkJobRead.job.getHostNameRegistered()) + "] myself=[" + ip.getHostName() + "]";
    }

    /** now do the real execution */
    public void run() {
        
        MilkJobFactory milkJobFactory = milkJob.milkJobFactory;
        MilkReportEngine milkReportEngine = MilkReportEngine.getInstance();
        // ConnectorAPIAccessorImpl connectorAccessorAPI = new ConnectorAPIAccessorImpl( milkJobFactory.getTenantId());
        
        List<BEvent> listEvents = new ArrayList<>();
        MilkPlugIn plugIn = milkJob.getPlugIn();
        MilkJobOutput milkJobOutput = null;
        SavedExecution savedStartExecution=null;
        try {
            String hostName = "";
            String ipAddress="";
            try {
                InetAddress ip = InetAddress.getLocalHost();
                hostName = ip.getHostName() ;
                ipAddress = ip.getHostAddress();
            } catch (UnknownHostException e1) {
                logger.severe("MilkExecuteJobThread: can't get the ipAddress, synchronization on a cluster can't work");
            }
            // execute it!
            MilkJobExecution milkJobExecution = new MilkJobExecution(milkJob, milkJobContext );

            // ----------------- Execution
            long timeBegin = System.currentTimeMillis();
            milkJobExecution.start();

            try {

                milkReportEngine.reportHeartBeatInformation("Start Job[" + milkJob.getName() + "] (" + milkJob.getId() + ")",false, false /* not a heatbreath */ );
                savedStartExecution = milkJob.registerNewExecution(new Date(timeBegin), ExecutionStatus.START, "Start");
                // save the status in the database
                // save the start Status (so general) and the track Status, plus askStop to false
                listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SerializationJobParameters.getInstanceStartExecutionJob()));

                milkJobOutput = plugIn.execute(milkJobExecution);
                
                if (milkJobOutput.getReportInHtml().length()==0) {
                    // no report, then build one from the list of event
                    milkJobOutput.addReportTableBegin( new String[] {});
                    for (BEvent event : milkJobOutput.getListEvents()) {
                        String title;
                        
                        if (event.getLevel() == Level.CRITICAL || event.getLevel() == Level.ERROR) {
                            title="<div class=\"label label-danger\" style=\"color:white;\">"+event.getTitle()+"</div>";
                        } else if (event.getLevel() == Level.APPLICATIONERROR) {
                            title="<div class=\"label label-warning\" style=\"color:white;\" >"+event.getTitle()+"</div>";
                        } else if (event.getLevel() == Level.SUCCESS) {
                            title="<div class=\"label label-success\" style=\"color:white;\" >"+event.getTitle()+"</div>";
                        } else {
                            title="<div class=\"label label-info\" style=\"color:white;\" >"+event.getTitle()+"</div>";
                        }                        
                        milkJobOutput.addReportTableLine( new String[] { title,event.getParameters()});
                    }
                    milkJobOutput.addReportTableEnd();
                    
                } else {
                    // a Report HTML exist, so complete it by errors if there are
                    if (BEventFactory.isError(milkJobOutput.getListEvents())) {
                        milkJobOutput.addReportTableBegin( new String[] {""} );
                        for (BEvent event : milkJobOutput.getListEvents()) {
                            
                            if (! event.isError())
                                continue;
                            String title = null;
                            
                            if (event.getLevel() == Level.CRITICAL || event.getLevel() == Level.ERROR) {
                                title="<div class=\"label label-danger\" style=\"color:white;\">"+event.getTitle()+"</div>";
                            } else { 
                                title="<div class=\"label label-warning\" style=\"color:white;\" >"+event.getTitle()+"</div>";
                            }
                            milkJobOutput.addReportTableLine( new String[] { title,event.getParameters()});
                        }
                        milkJobOutput.addReportTableEnd();
                    }
                }
                milkJobOutput.hostName = hostName;
                StringBuilder listEventsSt = new StringBuilder();
                for (final BEvent event : listEvents) {
                    listEventsSt.append( event.toString() + " <~> ");
                }
                logger.info("End Job[" + milkJob.getName() + "] (" + milkJob.getId() + ")" + listEventsSt.toString());

                // force the status ABORD status
                if (milkJobExecution.pleaseStop() && (milkJobOutput.executionStatus == ExecutionStatus.SUCCESS))
                    milkJobOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;
                // if the user ask to stop, then this is a successabort if this is correct (do not change a ERROR)
                if (milkJob.isAskForStop() && (milkJobOutput.executionStatus == ExecutionStatus.SUCCESS || milkJobOutput.executionStatus == ExecutionStatus.SUCCESSPARTIAL))
                    milkJobOutput.executionStatus = ExecutionStatus.SUCCESSABORT;
            } catch (Exception e) {

                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String exceptionDetails = sw.toString();
                logger.severe(" Job[" + milkJob.getName() + "] (" + milkJob.getId() + "] milkJobExecution.getPlugIn[" + plugIn.getName() + "]  Exception " + e.getMessage() + " at " + exceptionDetails);
                if (milkJobOutput == null) {
                    milkJobOutput = new MilkJobOutput(milkJob);
                    milkJobOutput.addEvent(new BEvent(eventPlugInViolation, "Job[" + milkJob.getName() + "] (" + milkJob.getId() + "] milkJobExecution.getPlugIn[" + plugIn.getName() + "]  Exception " + e.getMessage() + " at " + exceptionDetails));
                    milkJobOutput.executionStatus = ExecutionStatus.CONTRACTVIOLATION;
                }
            } catch (Error er) {
                StringWriter sw = new StringWriter();
                er.printStackTrace(new PrintWriter(sw));
                String exceptionDetails = sw.toString();
                logger.severe(" Job[" + milkJob.getName() + "] (" + milkJob.getId() + "] milkJobExecution.getPlugIn[" + plugIn.getName() + "]  Exception " + er.getMessage() + " at " + exceptionDetails);
                if (milkJobOutput == null) {
                    milkJobOutput = new MilkJobOutput(milkJob);
                    milkJobOutput.addEvent(new BEvent(eventPlugInViolation, "Job[" + milkJob.getName() + "] (" + milkJob.getId() + "] milkJobExecution.getPlugIn[" + plugIn.getName() + "]  Exception " + er.getMessage() + " at " + exceptionDetails));
                    milkJobOutput.executionStatus = ExecutionStatus.CONTRACTVIOLATION;
                }
            }
            milkJobExecution.end();

            long timeEnd = System.currentTimeMillis();

            milkJobOutput.executionTimeInMs = (timeEnd - timeBegin);

            String reportTime = "<p><i>Started at "+sdfSynthetic.format( new Date( timeBegin))+" end at "+sdfSynthetic.format( new Date( timeEnd ))+"</i>";
            milkJobOutput.addReportInHtml(reportTime);;
            milkJobOutput.setMeasure( MilkPlugInDescription.cstMesureTimeExecution, milkJobOutput.executionTimeInMs);
            milkJobOutput.setMeasure( MilkPlugInDescription.cstMesureNbItemProcessed, milkJobOutput.nbItemsProcessed);
            
            milkJob.addMesureValues( new Date(), milkJobOutput.getAllMesures());
            // executionDescription += "(" + output.executionStatus + ") " + output.nbItemsProcessed + " in " + output.executionTimeInMs + ";";

        } catch (Exception e) {
            milkJobOutput = new MilkJobOutput(milkJob);
            milkJobOutput.addEvent(new BEvent(eventPlugInError, e, "PlugIn[" + plugIn.getName() + "]"));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            logger.severe(" Execution error " + e.getMessage());
        }
        if (milkJobOutput != null) {
            // maybe the plugin forgot to setup the execution ? So set it.
            if (milkJobOutput.executionStatus == ExecutionStatus.NOEXECUTION)
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;

            milkJob.replaceExecution(savedStartExecution, tagTheDate, milkJobOutput);
            listEvents.addAll(milkJobOutput.getListEvents());
        }
        // calculate the next time
        listEvents.addAll(milkJob.calculateNextExecution("End-Of-Execution-recalculate"));
        milkReportEngine.reportHeartBeatInformation("End Job[" + milkJob.getName() + "] (" + milkJob.getId() + ") Status["+milkJobOutput.executionStatus.toString()+"] NewNextDate["+sdfSynthetic.format(milkJob.getNextExecutionDate())+"]",false, false /* not a heatbreath */);

        milkJob.setImmediateExecution(false);
        milkJob.setAskForStop(false);
        milkJob.setInExecution(false); // double check the end

        listEvents.addAll(milkJobFactory.dbSaveJob(milkJob, SerializationJobParameters.getInstanceEndExecutionJob()));

        // hum, nothing where to save the listEvent of the execution.
    }
    
    public static String getThreadName(MilkJob milkJob) {
        return new MilkJobMonitor(milkJob).getThreadName();
    }
    public static void collectStackTrace(MilkJob milkJob) {
        String truckMilkThreadName = getThreadName(milkJob);
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        for (Thread th : threadSet) {
            // Bonita-Worker-1-10
            if (th.getName().equals(truckMilkThreadName)) {
                try {
                    th.getStackTrace();
                    th.getStackTrace();
                }
                catch(Exception e ) {}
            }
        }
    }
}
