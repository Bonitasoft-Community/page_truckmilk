package org.bonitasoft.truckmilk.plugin;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;


import java.util.logging.Logger;

import org.bonitasoft.grumman.GrummanAPI;
import org.bonitasoft.grumman.GrummanAPI.MessagesList;
import org.bonitasoft.grumman.message.MessagesFactory.SynthesisOnMessage;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage.ReconcialiationFilter;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage.ReconcialiationFilter.TYPEFILTER;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage.ResultExecution;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage.ResultMessageOperation;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;

public class MilkGrumman extends MilkPlugIn {

    Logger logger = Logger.getLogger( MilkGrumman.class.getName() );
    
    private static PlugInParameter cstParamOnlyDetection = PlugInParameter.createInstance("OnlyDetection", "Only Detection", TypeParameter.BOOLEAN, Boolean.TRUE, "Only detection are executed. No correction.");
    private static PlugInParameter cstParamReconciliationIncomplete = PlugInParameter.createInstance("ReconciliationIncomplete", "Reconciliation Incomplete", TypeParameter.BOOLEAN, Boolean.TRUE, "If true, then the Reconciliation Incomplete message are processed");
    private static PlugInParameter cstParamReconciliationComplete = PlugInParameter.createInstance("ReconciliationComplete", "Reconciliation Complete", TypeParameter.BOOLEAN, Boolean.TRUE, "If true, then the Reconciliation complete message are processed");
    private static PlugInParameter cstParamReconciliationNumberOfMessages = PlugInParameter.createInstance("ReconciliationNumberOfMessages", "Number of messages", TypeParameter.LONG, 500, "Number of reconciliation messages treated at each execution");

    private static BEvent eventSynthesis = new BEvent(MilkGrumman.class.getName(), 1, Level.INFO, "Synthesis", "Synthesis on message");
    
    private static BEvent eventReconcilationMessage = new BEvent(MilkGrumman.class.getName(), 2, Level.INFO, 
            "Reconciliation Message synthesis", "Synthesis on reconciliation message");
    private static BEvent eventExecutionReconcilation = new BEvent(MilkGrumman.class.getName(), 3, Level.INFO, 
            "Reconciliation Correction", "Result of correction on Reconciliation message");
    

    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    public MilkGrumman() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * plug in can check its environment, to detect if you missed something. An external component may
     * be required and are not installed.
     * 
     * @return a list of Events.
     */
    @Override
    public List<BEvent> checkPluginEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();
    }

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();

    }

    /**
     * return the description of ping job
     */
    @Override
    public MilkPlugInDescription getDefinitionDescription() {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();

        plugInDescription.setName( "Grumman");
        plugInDescription.setLabel( "Grumman Message correction");
        plugInDescription.setDescription( "Execute the Grumman correction on message. See the Grumman page on Community for details.");
        plugInDescription.setCategory( CATEGORY.MONITOR);

        plugInDescription.addParameter(cstParamOnlyDetection);
        plugInDescription.addParameter(cstParamReconciliationNumberOfMessages);
        plugInDescription.addParameter(cstParamReconciliationIncomplete);
        plugInDescription.addParameter(cstParamReconciliationComplete);
        return plugInDescription;
    }

    /**
     * execution of the job. Just calculated the result according the parameters, and return it.
     */
    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();
        
        // task name is required
        boolean onlyDetection = jobExecution.getInputBooleanParameter(cstParamOnlyDetection );
        boolean reconciliationIncomplete = jobExecution.getInputBooleanParameter(cstParamReconciliationIncomplete );
        boolean reconciliationComplete = jobExecution.getInputBooleanParameter(cstParamReconciliationComplete );
        long numberOfMessages = jobExecution.getInputLongParameter( cstParamReconciliationNumberOfMessages );
        
        logger.fine("MilkGrumman: onlyDetection:["+onlyDetection+"] reconciliationIncomplete:["+reconciliationIncomplete+"] reconciliationComplete:["+reconciliationComplete+"]");
        
        try {
            GrummanAPI grummanAPI = new GrummanAPI( jobExecution.getApiAccessor().getProcessAPI());

            SynthesisOnMessage synthesis = grummanAPI.getSynthesisMessage();
             StringBuilder synthesisSt = new StringBuilder();
             synthesisSt.append( "Total number of messages: "+synthesis.getNbMessageEvent()+", ");
             synthesisSt.append( "Total number of Waiting Event: "+synthesis.getNbWaitingEvent()+", ");
             plugTourOutput.addEvent( new BEvent(eventSynthesis, synthesisSt.toString()));

             
             // detection             
             ReconcialiationFilter reconciliationFilter = new ReconcialiationFilter( TYPEFILTER.MAXMESSAGES);
             reconciliationFilter.numberOfMessages = (int) numberOfMessages;
             ResultMessageOperation resultMessageOperation= grummanAPI.getIncompleteReconciliationMessage(reconciliationFilter, jobExecution.getApiAccessor().getProcessAPI());
             
             StringBuilder resultSynthesisSt = new StringBuilder();
             resultSynthesisSt.append( "Nb of messages detected: "+resultMessageOperation.getListMessages().size()+", ");
             resultSynthesisSt.append( "Nb Completes Message: "+ resultMessageOperation.getNbCompleteMessages()+", ");
             resultSynthesisSt.append( "Nb InCompletes Message: "+ resultMessageOperation.getNbIncompleteMessages()+", ");
             resultSynthesisSt.append( "Nb InCompletes Message: "+ resultMessageOperation.getNbIncompleteMessages()+", ");
             plugTourOutput.addEvent( new BEvent(eventReconcilationMessage, resultSynthesisSt.toString()));
             plugTourOutput.addEvents( resultMessageOperation.getListEvents());
             if (BEventFactory.isError(resultMessageOperation.getListEvents())) {
                 plugTourOutput.executionStatus = ExecutionStatus.ERROR;
             }

             if (onlyDetection)
             {
                 plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
                 return plugTourOutput;
             }
             // lauch the correction then
             MessagesList messagesList = MessagesList.getInstanceFromResultMessageOperation(resultMessageOperation);
             messagesList.setSendincomplete( reconciliationIncomplete );
             messagesList.setExecutecomplete( reconciliationComplete );

             ResultExecution resultExecution = grummanAPI.executeReconcialiationMessage( messagesList, jobExecution.getApiAccessor().getProcessAPI());
             StringBuilder resultExecutionSt = new StringBuilder();
             resultSynthesisSt.append( "Nb Messages corrected: "+resultExecution.getNbMessagesCorrects()+", ");
             if (resultExecution.getNbMessagesErrors()>0)
                 resultSynthesisSt.append( "Nb Errors during correction: "+resultExecution.getNbMessagesErrors()+", ");
             resultSynthesisSt.append( "Nb Messages deleted: "+resultExecution.getNbMessagesRowDeleted()+" (Datas:"+resultExecution.getNbDatasRowDeleted()+"), ");
             
             plugTourOutput.addEvent( new BEvent(eventExecutionReconcilation, resultExecutionSt.toString()));

             
             
             plugTourOutput.addEvents( resultExecution.getListEvents());
             
             if (BEventFactory.isError(resultExecution.getListEvents()) || resultExecution.getNbMessagesErrors()>0) {
                 plugTourOutput.executionStatus = ExecutionStatus.ERROR;
             }
             else if (resultExecution.getNbMessagesCorrects()>0 ){
                 plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
             } else {
                 plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
             }
            
        }
        catch(Exception e) {
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
            plugTourOutput.addEvent(new BEvent(eventExecutionError, e.getMessage()));
            return plugTourOutput;
        }
        
        
        logger.fine("Finished checking tasks to unassign");
        plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
        
        if (jobExecution.pleaseStop())
            plugTourOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;

        return plugTourOutput;
    }

}
