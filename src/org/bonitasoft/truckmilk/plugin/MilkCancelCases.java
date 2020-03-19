package org.bonitasoft.truckmilk.plugin;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.engine.bpm.process.ProcessInstanceSearchDescriptor;
import org.bonitasoft.engine.exception.DeletionException;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.ListProcessesResult;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

public class MilkCancelCases extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkCancelCases.class.getName());

    private final static String CSTACTION_CANCELLATION = "Cancellation";
    private final static String CSTACTION_DELETION = "Deletion";

    private final static String CSTOPERATION_GETLIST = "Get List (No operation)";
    private final static String CSTOPERATION_DIRECT = "Cancel or Delete";
    private final static String CSTOPERATION_FROMLIST = "Cancel or Delete from the CSV list";

    private final static String CSTTRIGGER_STARTDATE = "STARTDATE";
    private final static String CSTTRIGGER_LASTUPDATEDATE = "LASTUPDATEDATE";

    
    protected final static String CSTCOL_CASEID = "caseid";
    protected final static String CSTCOL_STATUS = "status";

    
    
    private static BEvent eventOperationSuccess = new BEvent(MilkCancelCases.class.getName(), 1, Level.SUCCESS,
            "Operation (cancellation, deletion) done with success", "Cases are treated with success");

    private static BEvent eventCancelFailed = new BEvent(MilkCancelCases.class.getName(), 2, Level.ERROR,
            "Error during cancelation", "An error arrived during the cancelation", "Cases are not cancelled", "Check the exception");

    private static BEvent eventDeletionFailed = new BEvent(MilkCancelCases.class.getName(), 3, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion. Cancellation was done", "Cases are cancelled, not deleted", "Check the exception");

    private static BEvent eventOperationUnknown = new BEvent(MilkCancelCases.class.getName(), 4, Level.APPLICATIONERROR,
            "Operation unknown", "Operation is unknown", "Nothing is done", "give a correction operation");

    
    private static PlugInParameter cstParamMaximumCancellationInCases = PlugInParameter.createInstance("maximumcancellationincases", "Maximum deletion in case", TypeParameter.LONG, 1000L, "Maximum case cancelled in one execution, to not overload the engine. Maximum of 5000 is hardcoded");
    private static PlugInParameter cstParamMaximumCancellationInMinutes = PlugInParameter.createInstance("maximumcancellationinminutes", "Maximum time in Mn", TypeParameter.LONG, 3L, "Maximum time in minutes for the job. After this time, it will stop.");

    private static PlugInParameter cstParamDelayInDay = PlugInParameter.createInstance("delayinday", "Delai in days", TypeParameter.DELAY, MilkPlugInToolbox.DELAYSCOPE.YEAR + ":1", "Only cases archived before this delay are in the perimeter");

    private static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Filter on process", TypeParameter.ARRAYPROCESSNAME, null, "Job manage only process which mach the filter. If no filter is given, all processes are inspected");

    private static PlugInParameter cstParamActionOnCases = PlugInParameter.createInstanceListValues("actiononcases", "Action on cases : cancellation or deletion",
            new String[] { CSTACTION_CANCELLATION, CSTACTION_DELETION }, CSTACTION_CANCELLATION, "Cases are cancelled or deleted");

    private static PlugInParameter cstParamOperation = PlugInParameter.createInstanceListValues("operation", "operation: Build a list of cases to operate, do directlly the operation, or do the operation from a list",
            new String[] { CSTOPERATION_GETLIST, CSTOPERATION_DIRECT, CSTOPERATION_FROMLIST }, CSTOPERATION_DIRECT, "Result may be the cancellation or deletion , or build a list, or used the uploaded list");

    private static PlugInParameter cstParamTriggerOnCases = PlugInParameter.createInstanceListValues("triggeroncases", "Trigger to detect case to work on",
            new String[] { CSTTRIGGER_STARTDATE, CSTTRIGGER_LASTUPDATEDATE }, CSTTRIGGER_LASTUPDATEDATE, CSTTRIGGER_STARTDATE + " : the cases started before the delay are in the perimeter, else " + CSTTRIGGER_LASTUPDATEDATE + " only cases without any operations after the delay ");

    private static PlugInParameter cstParamSeparatorCSV = PlugInParameter.createInstance("separatorCSV", "Separator CSV", TypeParameter.STRING, ",", "CSV use a separator. May be ; or ,");
    private static PlugInParameter cstParamReport = PlugInParameter.createInstanceFile("report", "List of cases reported (operation GETLIST or DIRECT)", TypeParameter.FILEWRITE, null, "List is calculated and saved in this parameter", "ListToOperate.csv", "application/CSV");

    private static PlugInParameter cstParamInputDocument = PlugInParameter.createInstanceFile("inputdocument", "Input List (CSV) (operation FROMLIST)", TypeParameter.FILEREAD, null, "List is a CSV containing caseid column and status column. When the status is 'DELETE' or 'CANCELLED' or 'OPERATE', then the case is operate according the status (if OPERATE, then the job operation is used)\nExample: caseId;status\n342;DELETE\n345;DELETE", "ListToPurge.csv", "text/csv");

    
    
    /**
     * it's faster to delete 100 per 100
     */

    public MilkCancelCases() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the deleteCase, nothing is required
     */
    public List<BEvent> checkPluginEnvironment(long tenantId, APIAccessor apiAccessor) {
        return new ArrayList<>();
    }

    @Override
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        // is the command Exist ? 
        return new ArrayList<>();
    }

    @Override
    public PlugTourOutput execute(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        PlugTourOutput plugTourOutput = jobExecution.getPlugTourOutput();

        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        // get Input 
        long maximumCancellationInCases = jobExecution.getInputLongParameter(cstParamMaximumCancellationInCases);
        if (maximumCancellationInCases>10000)
            maximumCancellationInCases = 10000;

        jobExecution.setPleaseStopAfterTime( maximumCancellationInCases, 24 * 60L);
        jobExecution.setPleaseStopAfterManagedItems( jobExecution.getInputLongParameter(cstParamMaximumCancellationInMinutes), 5000L);

        String separatorCSV = jobExecution.getInputStringParameter(cstParamSeparatorCSV);

        
        try {
            // +1 so we can detect the SUCCESSPARTIAL

            SearchOptionsBuilder searchBuilderCase = new SearchOptionsBuilder(0, (int) maximumCancellationInCases+1 );

            ListProcessesResult listProcessResult = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, true, searchBuilderCase, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, apiAccessor.getProcessAPI());

            if (listProcessResult.listProcessDeploymentInfo.isEmpty()) {
                // filter given, no process found : stop now
                plugTourOutput.addEvents(listProcessResult.listEvents);
                plugTourOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return plugTourOutput;
            }

            
            // Get the list of cases to process
            DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamDelayInDay, new Date(), false);
            if (BEventFactory.isError( delayResult.listEvents)) {
                plugTourOutput.addEvents( delayResult.listEvents );
                plugTourOutput.executionStatus = ExecutionStatus.ERROR;
                return plugTourOutput;
            }
                
            String operation = jobExecution.getInputStringParameter( cstParamOperation );
            String action = jobExecution.getInputStringParameter( cstParamActionOnCases );
            
            String trigger = jobExecution.getInputStringParameter( cstParamTriggerOnCases );

            // report
            ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
            Writer w = new OutputStreamWriter(arrayOutputStream);

            w.write(CSTCOL_CASEID + separatorCSV +CSTCOL_STATUS + separatorCSV + "explanation" + separatorCSV + "\n");

            
            
            SourceData sourceData;
            // get source of case
            if (CSTOPERATION_GETLIST.equals(operation) || CSTOPERATION_DIRECT.equals(operation))
            {
                long timeSearch = delayResult.delayDate.getTime();
                sourceData = new SourceDataProcess();
                plugTourOutput.addEvents( ((SourceDataProcess)sourceData).initialize( listProcessResult, trigger, timeSearch, processAPI) );
            }
            else if (CSTOPERATION_FROMLIST.equals(operation)) {
                sourceData = new SourceDataCSV();
                
            } else
            {
                plugTourOutput.addEvent( new BEvent(eventOperationUnknown, "Operation["+operation+"] unknonw") );
                return plugTourOutput;
                
            }
            
            // do the job            
            StringBuilder synthesis= new StringBuilder();
            long beginTime = System.currentTimeMillis();
            int count=0;
            int totalNumberCase=0;
            jobExecution.setAvancementTotalStep(sourceData.getCount());
            while (count < sourceData.getCount()) {
                if (jobExecution.pleaseStop())
                    break;
                jobExecution.setAvancementStep(count);
                Long processInstanceId = sourceData.getNextProcessId();
                synthesis.append( processInstanceId+", ");
                
                String status="";
                String explanation="";
                // according the action on the 
                if (CSTOPERATION_GETLIST.equals(operation)) {
                    // save in the report
                }
                else if (CSTACTION_DELETION.equals( action )){
                    try {
                        processAPI.deleteProcessInstance( processInstanceId );
                        status="DELETED";
                    } catch(DeletionException e) {
                        plugTourOutput.addEvent( new BEvent(eventDeletionFailed, e, ""));
                        status="DELETIONERROR";
                        explanation=e.getMessage();
                    }
                }
                else if (CSTACTION_CANCELLATION.equals( action )){ 
                    try
                    {
                        processAPI.cancelProcessInstance( processInstanceId );
                        status="CANCELLED";
                    } catch(Exception e) {
                    plugTourOutput.addEvent(new BEvent(eventCancelFailed, e, ""));
                    status="CANCELLATIONERROR";
                    explanation=e.getMessage();
                    }
                }
                totalNumberCase++;
                
                // report in CSV
                StringBuilder line = new StringBuilder();
                line.append( String.valueOf( processInstanceId ) + separatorCSV);
                /*
                long processId = archivedProcessInstance.getProcessDefinitionId();
                if (!cacheProcessDefinition.containsKey(processId)) {
                    try {
                        ProcessDefinition processDefinition = processAPI.getProcessDefinition(processId);
                        cacheProcessDefinition.put(processId, processDefinition.getName() + separatorCSV + processDefinition.getVersion() + separatorCSV);
                    } catch (Exception e) {
                        cacheProcessDefinition.put(processId, separatorCSV + " " + separatorCSV);
                    }
                }
                line.append( cacheProcessDefinition.get(processId);
                line.append( TypesCast.sdfCompleteDate.format(archivedProcessInstance.getArchiveDate());
                */
                line.append( status + separatorCSV);
                line.append( explanation + separatorCSV);
                line.append( "\n");
                w.write(line.toString());
            }
            long endTime = System.currentTimeMillis();

            plugTourOutput.addEvent(new BEvent(eventOperationSuccess, "Treated:" + totalNumberCase + " in " + (endTime-beginTime) + " ms "+synthesis.toString()));
            plugTourOutput.setParameterStream(cstParamReport, new ByteArrayInputStream(arrayOutputStream.toByteArray()));

            if (totalNumberCase==0)
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
            else if (totalNumberCase < sourceData.getCount())
                    plugTourOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;
            else
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;

        } catch (Exception e1) {
            plugTourOutput.addEvent(new BEvent(eventDeletionFailed, e1, ""));
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
        }

        return plugTourOutput;
    }

    @Override
    public PlugInDescription getDefinitionDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();
        plugInDescription.name = "CancelCase";
        plugInDescription.label = "Cancel/Delete Active Cases (active)";
        plugInDescription.description = "Cancel(or delete) all cases older than a delay, or inactive since a delay";
        plugInDescription.addParameter(cstParamMaximumCancellationInCases);
        plugInDescription.addParameter(cstParamMaximumCancellationInMinutes);
        plugInDescription.addParameter(cstParamProcessFilter);
        plugInDescription.addParameter(cstParamDelayInDay);

        plugInDescription.addParameter(cstParamOperation);
        plugInDescription.addParameter(cstParamTriggerOnCases);
        plugInDescription.addParameter(cstParamActionOnCases);

        plugInDescription.addParameter(cstParamSeparatorCSV);
        plugInDescription.addParameter(cstParamReport);

        plugInDescription.addParameter(cstParamInputDocument);

        return plugInDescription;
    }

    /********************************************************************************** */
    /*                                                                                  */
    /* SourceData interface                                                             */
    /*                                                                                  */
    /********************************************************************************** */

    private interface SourceData {

        public long getCount();

        public Long getNextProcessId();
    }

    /********************************************************************************** */
    /*                                                                                  */
    /* SourceData Process                                                             */
    /*                                                                                  */
    /********************************************************************************** */

    private class SourceDataProcess implements SourceData {

        private SearchResult<ProcessInstance> searchResult;
        private int currentPosition;
        
        public List<BEvent> initialize(ListProcessesResult listProcessResult, String trigger, long timeSearch, ProcessAPI processAPI) {
            List<BEvent> listEvents = new ArrayList<>();
            if (CSTTRIGGER_STARTDATE.equals(trigger)) {
                listProcessResult.sob.lessOrEquals(ProcessInstanceSearchDescriptor.START_DATE, timeSearch);
                listProcessResult.sob.sort(ProcessInstanceSearchDescriptor.START_DATE, Order.ASC);
            } else {
                listProcessResult.sob.lessOrEquals(ProcessInstanceSearchDescriptor.LAST_UPDATE, timeSearch);
                listProcessResult.sob.sort(ProcessInstanceSearchDescriptor.LAST_UPDATE, Order.ASC);
            }
            try {
                searchResult = processAPI.searchProcessInstances(listProcessResult.sob.done());
            } catch (Exception e1) {
                listEvents.add(new BEvent(eventDeletionFailed, e1, ""));
            }
            currentPosition=0;
            return listEvents;
        }

        @Override
        public long getCount() {
            return searchResult.getCount();
        }

        @Override
        public Long getNextProcessId() {
            if (currentPosition>= searchResult.getCount())
                return null;
            Long nextId = searchResult.getResult().get( currentPosition).getId();
            currentPosition++;
            return nextId;

        }

    }
    /********************************************************************************** */
    /*                                                                                  */
    /* SourceData CSV                                                             */
    /*                                                                                  */
    /********************************************************************************** */

    private class SourceDataCSV implements SourceData {

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public Long getNextProcessId() {
            // TODO Auto-generated method stub
            return null;
        }
        
    }

}
