package org.bonitasoft.truckmilk.plugin;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.flownode.FlowNodeInstance;
import org.bonitasoft.engine.bpm.flownode.FlowNodeInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.engine.bpm.process.ProcessInstanceNotFoundException;
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
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.ListProcessesResult;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkJobOutput.StartMarker;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.CSVOperation;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

public class MilkCancelCases extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkCancelCases.class.getName());

    private final static String CSTACTION_CANCELLATION = "Cancellation";
    private final static String CSTACTION_DELETION = "Deletion";

    private final static String CSTOPERATION_GETLIST = "Get List (No operation)";
    private final static String CSTOPERATION_DIRECT = "Cancel or Delete";
    private final static String CSTOPERATION_FROMLIST = "Cancel or Delete from the CSV list";

    private final static String CSTTRIGGER_STARTDATE = "Case Start Date";
    private final static String CSTTRIGGER_LASTUPDATEDATE = "Case Last Update Date";

    protected final static String CSTCOL_CASEID = "Caseid";
    protected final static String CSTCOL_STATUS = "Status";

    private static BEvent eventGetListSuccess = new BEvent(MilkCancelCases.class.getName(), 1, Level.SUCCESS,
            "Get List done with success", "Cases are treated with success");
    private static BEvent eventDeletionSuccess = new BEvent(MilkCancelCases.class.getName(), 2, Level.SUCCESS,
            "Deletion done with success", "Cases are treated with success");
    private static BEvent eventCancellationSuccess = new BEvent(MilkCancelCases.class.getName(), 3, Level.SUCCESS,
            "Cancellation done with success", "Cancellations are treated with success");

    private static BEvent eventCancelFailed = new BEvent(MilkCancelCases.class.getName(), 2, Level.ERROR,
            "Error during cancelation", "An error arrived during the cancelation", "Cases are not cancelled", "Check the exception");

    private static BEvent eventDeletionFailed = new BEvent(MilkCancelCases.class.getName(), 3, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion. Cancellation was done", "Cases are cancelled, not deleted", "Check the exception");

    private static BEvent eventOperationUnknown = new BEvent(MilkCancelCases.class.getName(), 4, Level.APPLICATIONERROR,
            "Operation unknown", "Operation is unknown", "Nothing is done", "give a correction operation");

    private static BEvent eventLoadCsvFailed = new BEvent(MilkCancelCases.class.getName(), 5, Level.ERROR,
            "Error during Load CSV", "An error arrived during loading CSV", "Cases can't be cancelled", "Check the exception");

    private static BEvent eventCantCalculatedTaskUpdated = new BEvent(MilkCancelCases.class.getName(), 6, Level.ERROR,
            "Can't calulated tasks updated", "Task Updated cannot be calculated, different cases are still in the list to study", "Last Update cases can't be calculated", "Check logs");

    private static BEvent eventSearchFailed = new BEvent(MilkCancelCases.class.getName(), 7, Level.ERROR,
            "Search failed", "Search failed, no case/task can be retrieved", "Case in the scope can't be calculated", "Check exception");

    private static PlugInParameter cstParamDelayInDay = PlugInParameter.createInstance("delayinday", "Delai", TypeParameter.DELAY, MilkPlugInToolbox.DELAYSCOPE.YEAR + ":1", "Only cases archived before this delay are in the perimeter");

    private static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Filter on process", TypeParameter.ARRAYPROCESSNAME, null, "Job manage only process which mach the filter. If no filter is given, all processes are inspected");

    private static PlugInParameter cstParamActionOnCases = PlugInParameter.createInstanceListValues("actiononcases", "Action on cases : cancellation or deletion",
            new String[] { CSTACTION_CANCELLATION, CSTACTION_DELETION }, CSTACTION_CANCELLATION, "Cases are cancelled or deleted");

    private static PlugInParameter cstParamOperation = PlugInParameter.createInstanceListValues("operation", "operation: Build a list of cases to operate, do directlly the operation, or do the operation from a list",
            new String[] { CSTOPERATION_GETLIST, CSTOPERATION_DIRECT, CSTOPERATION_FROMLIST }, CSTOPERATION_DIRECT, "Result may be the cancellation or deletion , or build a list, or used the uploaded list");

    private static PlugInParameter cstParamTriggerOnCases = PlugInParameter.createInstanceListValues("triggeroncases", "Trigger to detect case to work on",
            new String[] { CSTTRIGGER_STARTDATE, CSTTRIGGER_LASTUPDATEDATE }, CSTTRIGGER_LASTUPDATEDATE, CSTTRIGGER_STARTDATE + " : the cases started before the delay are in the perimeter, else " + CSTTRIGGER_LASTUPDATEDATE + " only cases without any operations after the delay ");

    private static PlugInParameter cstParamSeparatorCSV = PlugInParameter.createInstance("separatorCSV", "Separator CSV", TypeParameter.STRING, ",", "CSV use a separator. May be ; or ,");
    private static PlugInParameter cstParamReport = PlugInParameter.createInstanceFile("report", "Report Execution", TypeParameter.FILEWRITE, null, "List of cases managed is calculated and saved in this parameter", "ListToOperate.csv", "application/CSV");

    private static PlugInParameter cstParamInputDocument = PlugInParameter.createInstanceFile("inputdocument", "Cases to delete/cancel", TypeParameter.FILEREAD, null, "List is a CSV containing caseid column and status column. When the status is 'DELETE' or 'CANCELLED' or 'OPERATE', then the case is operate according the status (if OPERATE, then the job operation is used)\nExample: caseId;status\n342;DELETE\n345;DELETE", "ListToPurge.csv", "text/csv");

    /**
     * it's faster to delete 100 per 100
     */

    public MilkCancelCases() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the deleteCase, nothing is required
     */
    public List<BEvent> checkPluginEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();
    }

    @Override
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution) {
        // is the command Exist ? 
        return new ArrayList<>();
    }
    @Override
    public MilkPlugInDescription getDefinitionDescription() {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName( "CancelCase");
        plugInDescription.setLabel( "Cancel/Delete Active Cases (active)");
        plugInDescription.setExplanation( "Cancel(or delete) all cases older than a delay, or inactive since a delay");
        plugInDescription.setWarning("A case Cancelled can't be put alive again. A case Deleted can't be retrieved. Operation is final. Use with caution.");
        plugInDescription.setCategory( CATEGORY.CASES);
        
        plugInDescription.setStopJob( JOBSTOPPER.BOTH );
        
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
    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput milkJobOutput = jobExecution.getMilkJobOutput();

        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        // get Input 

        String separatorCSV = jobExecution.getInputStringParameter(cstParamSeparatorCSV);

        try {
            // +1 so we can detect the SUCCESSPARTIAL

            SearchOptionsBuilder searchBuilderCase = new SearchOptionsBuilder(0, jobExecution.getJobStopAfterMaxItems() + 1);

            ListProcessesResult listProcessResult = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, false, searchBuilderCase, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                milkJobOutput.addEvents(listProcessResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return milkJobOutput;
            }

            // Get the list of cases to process
            DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamDelayInDay, new Date(), false);
            if (BEventFactory.isError(delayResult.listEvents)) {
                milkJobOutput.addEvents(delayResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                return milkJobOutput;
            }

            String operation = jobExecution.getInputStringParameter(cstParamOperation);
            String action = jobExecution.getInputStringParameter(cstParamActionOnCases);

            String trigger = jobExecution.getInputStringParameter(cstParamTriggerOnCases);

            // report
            ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
            Writer w = new OutputStreamWriter(arrayOutputStream);

            w.write(CSTCOL_CASEID + separatorCSV + "Process" + separatorCSV + "Version" + separatorCSV + "StartDate" + separatorCSV + "LastUpdate" + separatorCSV + CSTCOL_STATUS + separatorCSV + "explanation" + separatorCSV + "\n");

            SourceData sourceData;
            //---------------------------------- get source of case
            if (CSTOPERATION_GETLIST.equals(operation) || CSTOPERATION_DIRECT.equals(operation)) {
                long timeSearch = delayResult.delayDate.getTime();
                sourceData = new SourceDataProcess();
                milkJobOutput.addEvents(((SourceDataProcess) sourceData).initialize(listProcessResult, trigger, timeSearch, milkJobOutput, processAPI));
            } else if (CSTOPERATION_FROMLIST.equals(operation)) {
                sourceData = new SourceDataCSV();
                try {
                    ((SourceDataCSV) sourceData).initialize(jobExecution, cstParamInputDocument, separatorCSV, processAPI);
                } catch (Exception e) {
                    milkJobOutput.addEvent(new BEvent(eventLoadCsvFailed, e, ""));
                    milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                    return milkJobOutput;
                }

            } else {
                milkJobOutput.addEvent(new BEvent(eventOperationUnknown, "Operation[" + operation + "] unknonw"));
                return milkJobOutput;

            }

            if (BEventFactory.isError(milkJobOutput.getListEvents())) {
                milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                return milkJobOutput;
            }

            if (sourceData.getCount() == 0) {
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
                return milkJobOutput;
            }
            //---------------------------------- operation

            Map<Long, String> cacheProcessDefinition = new HashMap<>();

            // do the job            
            long beginTime = System.currentTimeMillis();
            int count = 0;
            int totalNumberCase = 0;
            jobExecution.setAvancementTotalStep(sourceData.getCount());
            while (count < sourceData.getCount()) {
                if (jobExecution.pleaseStop())
                    break;
                jobExecution.setAvancementStep(count);
                count++;
                SourceProcessInstance sourceProcessInstance = sourceData.getNextProcessInstance();

                StringBuilder synthesis = new StringBuilder();
                if (sourceProcessInstance.processInstance != null)
                    synthesis.append(sourceProcessInstance.processInstance.getId() + separatorCSV);
                else if (sourceProcessInstance.originalCaseId != null)
                    synthesis.append(sourceProcessInstance.originalCaseId + separatorCSV);
                else
                    synthesis.append(" " + separatorCSV);

                if (sourceProcessInstance.processInstance != null) {
                    long processDefinitionId = sourceProcessInstance.processInstance.getProcessDefinitionId();
                    if (!cacheProcessDefinition.containsKey(processDefinitionId)) {
                        try {
                            ProcessDefinition processDefinition = processAPI.getProcessDefinition(processDefinitionId);
                            cacheProcessDefinition.put(processDefinitionId, processDefinition.getName() + separatorCSV + processDefinition.getVersion());
                        } catch (Exception e) {
                            cacheProcessDefinition.put(processDefinitionId, " " + separatorCSV + " ");
                        }
                    }
                    synthesis.append(cacheProcessDefinition.get(processDefinitionId) + separatorCSV);
                } else
                    synthesis.append("" + separatorCSV);
                synthesis.append((sourceProcessInstance.processInstance != null ? TypesCast.sdfCompleteDate.format(sourceProcessInstance.processInstance.getStartDate()) : "") + separatorCSV);
                synthesis.append((sourceProcessInstance.processInstance != null ? TypesCast.sdfCompleteDate.format(sourceProcessInstance.processInstance.getLastUpdate()) : "") + separatorCSV);

                String status = sourceProcessInstance.statusLoad;
                String explanation = "";
                if (sourceProcessInstance.processInstance != null) {
                    // according the action on the 
                    if (CSTOPERATION_GETLIST.equals(operation)) {
                        // save in the report
                    } else if (CSTACTION_DELETION.equals(action)) {
                        try {
                            StartMarker deleteCasesMarker = milkJobOutput.getStartMarker("DeleteCases");

                            processAPI.deleteProcessInstance(sourceProcessInstance.processInstance.getId());
                            milkJobOutput.endMarker(deleteCasesMarker);

                            status = "DELETED";
                        } catch (DeletionException e) {
                            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, ""));
                            status = "DELETIONERROR";
                            explanation = e.getMessage();
                        }
                    } else if (CSTACTION_CANCELLATION.equals(action)) {
                        try {
                            StartMarker cancelCasesMarker = milkJobOutput.getStartMarker("CancelCase");

                            processAPI.cancelProcessInstance(sourceProcessInstance.processInstance.getId());
                            milkJobOutput.endMarker(cancelCasesMarker);

                            status = "CANCELLED";
                        } catch (Exception e) {
                            milkJobOutput.addEvent(new BEvent(eventCancelFailed, e, ""));
                            status = "CANCELLATIONERROR";
                            explanation = e.getMessage();
                        }
                    }
                    totalNumberCase++;
                }
                // report in CSV

                synthesis.append(status + separatorCSV);
                synthesis.append(explanation + separatorCSV);
                w.write(synthesis.toString() + "\n");
            }
            long endTime = System.currentTimeMillis();
            w.flush();
            w.close();
            BEvent eventStatus = null;
            if (CSTOPERATION_GETLIST.equals(operation)) {
                eventStatus = eventGetListSuccess;
            } else if (CSTACTION_CANCELLATION.equals(action)) {
                eventStatus = eventCancellationSuccess;
            } else {
                eventStatus = eventDeletionSuccess;
            }

            milkJobOutput.addEvent(new BEvent(eventStatus, "Treated:" + totalNumberCase + " in " + (endTime - beginTime) + " ms " + milkJobOutput.collectPerformanceSynthesis(false)));
            milkJobOutput.setParameterStream(cstParamReport, new ByteArrayInputStream(arrayOutputStream.toByteArray()));
            milkJobOutput.nbItemsProcessed = totalNumberCase;
            if (totalNumberCase == 0)
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
            else if (totalNumberCase < sourceData.getCount())
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;
            else
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;

        } catch (Exception e1) {
            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e1, ""));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
        }

        return milkJobOutput;
    }

   

    /********************************************************************************** */
    /*                                                                                  */
    /* SourceData interface */
    /*                                                                                  */
    /********************************************************************************** */

    public class SourceProcessInstance {

        ProcessInstance processInstance;
        String statusLoad = "";
        Long originalCaseId;

        public SourceProcessInstance(ProcessInstance processInstance, String statusLoad, Long originalCaseId) {
            this.processInstance = processInstance;
            this.statusLoad = statusLoad;
            this.originalCaseId = originalCaseId;
        }
    }

    private interface SourceData {

        public long getCount();

        public SourceProcessInstance getNextProcessInstance();

    }

    /********************************************************************************** */
    /*                                                                                  */
    /* SourceData Process */
    /*                                                                                  */
    /********************************************************************************** */

    private class SourceDataProcess implements SourceData {

        private SearchResult<ProcessInstance> searchResult;
        private List<ProcessInstance> searchResultTaskUpdated = new ArrayList<>();
        private int currentPosition;
        private String trigger;

        public List<BEvent> initialize(ListProcessesResult listProcessResult, String trigger, long timeSearch, MilkJobOutput milkJobOutput, ProcessAPI processAPI) {
            List<BEvent> listEvents = new ArrayList<>();
            this.trigger = trigger;
            if (CSTTRIGGER_STARTDATE.equals(this.trigger)) {
                listProcessResult.sob.lessOrEquals(ProcessInstanceSearchDescriptor.START_DATE, timeSearch);
                listProcessResult.sob.sort(ProcessInstanceSearchDescriptor.START_DATE, Order.ASC);
            } else {
                // trigger LASTUPDATE : there are no direct request. The LASTUPDATEDATE is not updated in the database.
                // So, to collect it, for example a case with no update since Feb 10, method are
                // 1/ collect case created BEFORE Feb 10 (a case created after has mechanicaly an update after)
                // 2/ we have to collect MORE case than required, because we will eliminate some
                // 2/ then for each cases, search all tasks attached to the case, to detect if a task exist before

                listProcessResult.sob.lessOrEquals(ProcessInstanceSearchDescriptor.START_DATE, timeSearch);
                listProcessResult.sob.sort(ProcessInstanceSearchDescriptor.START_DATE, Order.ASC);
            }
            try {
                StartMarker searchProcessInstanceMarker = milkJobOutput.getStartMarker("SearchProcessInstance");
                searchResult = processAPI.searchOpenProcessInstances(listProcessResult.sob.done());

                milkJobOutput.endMarker(searchProcessInstanceMarker);

                if (CSTTRIGGER_LASTUPDATEDATE.equals(this.trigger)) {
                    listEvents.addAll(searchLastUpdateCases(searchResult, timeSearch, milkJobOutput, processAPI));
                }

            } catch (Exception e1) {
                listEvents.add(new BEvent(eventSearchFailed, e1, ""));
            }
            currentPosition = 0;
            return listEvents;
        }

        @Override
        public long getCount() {
            if (CSTTRIGGER_STARTDATE.equals(this.trigger)) {
                return searchResult.getResult().size();
            } else {
                return searchResultTaskUpdated.size();
            }
        }

        @Override
        public SourceProcessInstance getNextProcessInstance() {
            ProcessInstance processInstance = null;
            if (CSTTRIGGER_STARTDATE.equals(this.trigger)) {
                if (currentPosition >= searchResult.getResult().size())
                    return null;
                processInstance = searchResult.getResult().get(currentPosition);
            } else {
                if (currentPosition >= searchResultTaskUpdated.size())
                    return null;
                processInstance = searchResultTaskUpdated.get(currentPosition);
            }

            SourceProcessInstance sourceProcessInstance = new SourceProcessInstance(processInstance, "", processInstance.getRootProcessInstanceId());
            currentPosition++;
            return sourceProcessInstance;
        }

        private final static int CSTPROCESSPERPAGE = 1000;
        private final static int CSTMAXTASKS = 5000;

        private List<BEvent> searchLastUpdateCases(SearchResult<ProcessInstance> searchResultProcessInstance, long timeSearch, MilkJobOutput milkJobOutput, ProcessAPI processAPI) {
            List<BEvent> listEvents = new ArrayList<>();
            List<ProcessInstance> listToProcess = new ArrayList<>();

            for (ProcessInstance processInstance : searchResult.getResult())
                listToProcess.add(processInstance);
            int previousNumberOfItems = -1;
            while (!listToProcess.isEmpty() && listToProcess.size() != previousNumberOfItems) {
                // One round : we select a Page of 10 cases in the list, and we search tasks 1000 for that list order by recent.
                // for each received tasks, 
                // different situation: 
                //   - a task exist, date exist AFTER the limitation, so we eliminate the processID
                //   - a task exist, BEFORE the limitation. 
                //          processId does not exist? It was eliminated by step 1.
                //          processID still exist ? Because we order by Recent, if this is the first occurence, we keep it.
                //  At the end of the check task, two situation:
                //      - the result list of task is under 1000 ==> We processed all process in the page. If there are no news on processId, that's mean no tasks 
                //             at all for the case ==> Keep it, and remove it from the list
                //      - the result list is upper than 1000, the processId may not show up. Because we treated the first 1000 cases, so listToProcess should decrease by minimum one.

                // get a page of CSTPROCESSPERPAGE 
                int numberOfCasesInPage = Math.min(CSTPROCESSPERPAGE, listToProcess.size());
                Set<Long> processInactive = new HashSet<>();
                Set<Long> processStillActive = new HashSet<>();

                SearchOptionsBuilder sobTasks = new SearchOptionsBuilder(0, CSTMAXTASKS + 1);
                for (int i = 0; i < numberOfCasesInPage; i++) {
                    if (i > 0)
                        sobTasks.or();
                    sobTasks.filter(FlowNodeInstanceSearchDescriptor.ROOT_PROCESS_INSTANCE_ID, listToProcess.get(i).getRootProcessInstanceId());

                }
                // we want the last recent in first
                sobTasks.sort(FlowNodeInstanceSearchDescriptor.LAST_UPDATE_DATE, Order.DESC);
                try {
                    StartMarker searchFlowNodeMarker = milkJobOutput.getStartMarker("SearchFlowNode");
                    SearchResult<FlowNodeInstance> searchTasks = processAPI.searchFlowNodeInstances(sobTasks.done());
                    milkJobOutput.endMarker(searchFlowNodeMarker);

                    StartMarker detectionActiveCaseMarker = milkJobOutput.getStartMarker("DetectionActiveCase");

                    for (FlowNodeInstance flowNode : searchTasks.getResult()) {
                        if (flowNode.getLastUpdateDate() == null)
                            continue;
                        if (flowNode.getLastUpdateDate().getTime() >= timeSearch) {
                            processStillActive.add(flowNode.getRootContainerId());
                        } else {
                            if (!processStillActive.contains(flowNode.getRootContainerId()))
                                // eliminate this rootProcessInstanceId
                                processInactive.add(flowNode.getRootContainerId());
                        }
                    }
                    // if the searchTasks collect all tasks, all processID in the list should be keep
                    if (searchTasks.getCount() <= CSTMAXTASKS) {
                        for (int i = 0; i < numberOfCasesInPage; i++) {
                            if (!processStillActive.contains(listToProcess.get(i).getRootProcessInstanceId()))
                                processInactive.add(listToProcess.get(i).getRootProcessInstanceId());
                        }
                    }
                    milkJobOutput.endMarker(detectionActiveCaseMarker);

                } catch (Exception e) {
                    listEvents.add(new BEvent(eventSearchFailed, e, ""));
                }
                // now, reduce the list
                previousNumberOfItems = listToProcess.size();
                for (int i = numberOfCasesInPage - 1; i >= 0; i--) {
                    if (processStillActive.contains(listToProcess.get(i).getRootProcessInstanceId()))
                        listToProcess.remove(i);
                    else if (processInactive.contains(listToProcess.get(i).getRootProcessInstanceId())) {
                        searchResultTaskUpdated.add(listToProcess.get(i));
                        listToProcess.remove(i);
                    }
                }
            }
            if (!listToProcess.isEmpty()) {
                String listSt = listToProcess.toString();
                if (listSt.length() > 1000)
                    listSt = listSt.substring(0, 1000) + "...";
                listEvents.add(new BEvent(eventCantCalculatedTaskUpdated, "Style " + listToProcess.size() + " to check, " + listSt));
            }
            return listEvents;
        }
    }

    /********************************************************************************** */
    /*                                                                                  */
    /* SourceData CSV */
    /*                                                                                  */
    /********************************************************************************** */

    private class SourceDataCSV implements SourceData {

        ProcessAPI processAPI;
        CSVOperation csvOperation;

        public long initialize(MilkJobExecution jobExecution, PlugInParameter inputCsv, String separatorCSV, ProcessAPI processAPI) throws IOException {
            this.processAPI = processAPI;
            csvOperation = new CSVOperation();
            return csvOperation.loadCsvDocument(jobExecution, inputCsv, separatorCSV);
        }

        @Override
        public long getCount() {
            return csvOperation.getCount();
        }

        @Override
        public SourceProcessInstance getNextProcessInstance() {
            Long caseId = null;
            try {
                Map<String, String> record = csvOperation.getNextRecord();
                caseId = TypesCast.getLong(record.get(CSTCOL_CASEID), null);
                if (caseId == null)
                    return new SourceProcessInstance(null, "No column[" + CSTCOL_CASEID + "] in CSV", null);
                return new SourceProcessInstance(processAPI.getProcessInstance(caseId), "", caseId);

            } catch (ProcessInstanceNotFoundException e) {
                return new SourceProcessInstance(null, "ProcessInstance[" + caseId + "] not found", caseId);
            } catch (Exception e) {
                return new SourceProcessInstance(null, "Error " + e.getMessage(), caseId);
            }
        }

    }

}
