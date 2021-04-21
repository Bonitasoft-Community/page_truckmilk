/* ******************************************************************************** */
/*                                                                                  */
/*    PurgeDirect                                                                   */
/*   Direct purge                                                                   */
/*                                                                                  */
/*                                                                                  */
/* ******************************************************************************** */
package org.bonitasoft.truckmilk.plugin.cases.purge;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfoCriterion;
import org.bonitasoft.engine.exception.DeletionException;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.properties.BonitaEngineConnection;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkJobOutput.Chronometer;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobExecution.DelayResult;
import org.bonitasoft.truckmilk.job.MilkJobExecution.ListProcessesResult;
import org.bonitasoft.truckmilk.plugin.cases.MilkPurgeArchivedCases;
import org.bonitasoft.truckmilk.plugin.cases.purge.PurgeOperation.ManagePurgeResult;
import org.bonitasoft.truckmilk.plugin.cases.purge.PurgeOperation.SubProcessOperation;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

public class PurgeDirect {

    static Logger logger = Logger.getLogger(PurgeDirect.class.getName());
    private static final String LOGGER_LABEL = "MilkPurgeArchive.PurgeDirect ";

    
    private static BEvent eventDeletionSuccess = new BEvent(PurgeDirect.class.getName(), 1, Level.SUCCESS,
            "Deletion done with success", "Archived Cases are deleted with success");

    private static BEvent eventDeletionFailed = new BEvent(PurgeDirect.class.getName(), 2, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion of archived cases", "Cases are not deleted", "Check the exception");

    private static BEvent eventSearchFailed = new BEvent(PurgeDirect.class.getName(), 3, Level.ERROR,
            "Search failed", "Search failed task return an error", "No retry can be performed", "Check the error");

    
    
    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * DeletionMethod - use for REPORT per processID
     * /*
     */
    /* ******************************************************************************** */

    private static class DeletionItemProcess {

        ProcessDeploymentInfo processDeploymentInfo;
        ProcessDefinition processDefinition;
        long nbCaseDeleted = 0;;
    }

    protected static class DeletionPerProcess {

        private Map<Long, DeletionItemProcess> mapDeletionProcess = new HashMap<>();

        public DeletionItemProcess getDeletionProcessFromProcessId(long processDefinitionId, ProcessAPI processAPI) {
            DeletionItemProcess deletionPerProcess = mapDeletionProcess.get(processDefinitionId);
            if (deletionPerProcess != null)
                return deletionPerProcess;

            deletionPerProcess = new DeletionItemProcess();
            try {
                deletionPerProcess.processDefinition = processAPI.getProcessDefinition(processDefinitionId);
            } catch (Exception e) {

            }
            mapDeletionProcess.put(processDefinitionId, deletionPerProcess);
            return deletionPerProcess;

        }

        public DeletionItemProcess getDeletionProcessFromProcessDeployment(long processDefinitionId, ProcessDeploymentInfo processDeploymentInfo) {
            DeletionItemProcess deletionPerProcess = mapDeletionProcess.get(processDefinitionId);
            if (deletionPerProcess != null)
                return deletionPerProcess;

            deletionPerProcess = new DeletionItemProcess();
            deletionPerProcess.processDeploymentInfo = processDeploymentInfo;
            mapDeletionProcess.put(processDefinitionId, deletionPerProcess);
            return deletionPerProcess;
        }

        public void addInReport(MilkJobOutput milkJobOutput) {
            milkJobOutput.addReportTableBegin(new String[] { "Process Name", "Version", "Number of deletion" });
            boolean reportSomething = false;

            for (DeletionItemProcess deletionItemProcess : mapDeletionProcess.values()) {
                if (deletionItemProcess.nbCaseDeleted > 0) {
                    if (deletionItemProcess.processDeploymentInfo != null) {
                        milkJobOutput.addReportTableLine(new Object[] { deletionItemProcess.processDeploymentInfo.getName(), deletionItemProcess.processDeploymentInfo.getVersion(), deletionItemProcess.nbCaseDeleted });
                        reportSomething = true;
                    } else if (deletionItemProcess.processDefinition != null) {
                        milkJobOutput.addReportTableLine(new Object[] { deletionItemProcess.processDefinition.getName(), deletionItemProcess.processDefinition.getVersion(), deletionItemProcess.nbCaseDeleted });
                        reportSomething = true;
                    }
                }
            }
            if (!reportSomething)
                milkJobOutput.addReportTableLine(new Object[] { "No deletion performed", "", "" });
            milkJobOutput.addReportTableEnd();
        }

    }

   

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * Direct Purge
     * /*
     */
    /* ******************************************************************************** */

    public MilkJobOutput operationDirectPurge(MilkJobExecution milkJobExecution) {
        MilkJobOutput milkJobOutput = milkJobExecution.getMilkJobOutput();

        Integer maximumArchiveDeletionPerRound = milkJobExecution.getJobStopAfterMaxItems();
        // default value is 1 Million
        if (maximumArchiveDeletionPerRound == null || maximumArchiveDeletionPerRound.equals(MilkJob.CSTDEFAULT_STOPAFTER_MAXITEMS))
            maximumArchiveDeletionPerRound = 1000000;

        SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, (int) maximumArchiveDeletionPerRound + 1);

        try {

            // List of process ENABLE AND DISABLE
            ListProcessesResult listProcessResult = milkJobExecution.getInputArrayProcess(MilkPurgeArchivedCases.cstParamProcessFilter, false, searchActBuilder, ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, milkJobExecution.getApiAccessor().getProcessAPI());

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                milkJobOutput.addEvents(listProcessResult.listEvents);
                milkJobOutput.setExecutionStatus(ExecutionStatus.BADCONFIGURATION);
                return milkJobOutput;
            }

            // Delay
            DelayResult delayResult = milkJobExecution.getInputDelayParameter(MilkPurgeArchivedCases.cstParamDelay, new Date(), false);
            if (BEventFactory.isError(delayResult.listEvents)) {
                milkJobOutput.addEvents(delayResult.listEvents);
                milkJobOutput.setExecutionStatus(ExecutionStatus.ERROR);
                return milkJobOutput;
            }
            boolean withFilter=false;
            if (delayResult.delayInMs>0)
                withFilter = true;
            
            String typePurge = milkJobExecution.getInputStringParameter(MilkPurgeArchivedCases.cstParamTypePurge);
            if (MilkPurgeArchivedCases.CSTTYPEPURGE_PARTIALPURGE.equals(typePurge))
                withFilter = true;
            
            if (withFilter) {
                purgeArchivesWithFilter(listProcessResult, delayResult, milkJobExecution, searchActBuilder, milkJobOutput);
            } else {
                purgeArchiveNoDelay(listProcessResult, milkJobExecution, milkJobOutput);
            }
            if (milkJobOutput.nbItemsProcessed == 0 && milkJobOutput.getExecutionStatus() == ExecutionStatus.SUCCESS)
                milkJobOutput.setExecutionStatus(ExecutionStatus.SUCCESSNOTHING);
        } catch (SearchException e1) {
            milkJobOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
            milkJobOutput.setExecutionStatus(ExecutionStatus.ERROR);

        }
        milkJobOutput.addChronometersInReport(false, false);

        return milkJobOutput;
    }
    

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * DeletionMethod
     * /*
     */
    /* ******************************************************************************** */

    /**
     * @param listProcessResult
     */
    private static int casePerDeletion = 100;

    /**
     * DeleteArchiveNoDelay. Use deleteArchivedProcessInstances(). The, no search is needed, this API delete from archive case directly. 
     * 
     * @param listProcessResult
     * @param milkJobExecution
     * @param milkJobOutput
     */
    private void purgeArchiveNoDelay(ListProcessesResult listProcessResult, MilkJobExecution milkJobExecution, MilkJobOutput milkJobOutput) {
        DeletionPerProcess deletionPerProcess = new DeletionPerProcess();

        ProcessAPI processAPI = milkJobExecution.getApiAccessor().getProcessAPI();
        Integer maximumArchiveDeletionPerRound = milkJobExecution.getJobStopAfterMaxItems();
        // default value is 1 Million
        if (maximumArchiveDeletionPerRound == null || maximumArchiveDeletionPerRound.equals(MilkJob.CSTDEFAULT_STOPAFTER_MAXITEMS))
            maximumArchiveDeletionPerRound = 1000000;
        milkJobOutput.nbItemsProcessed = 0;
        String currentProcessToLog = "";
        List<ProcessDeploymentInfo> listProcesses = listProcessResult.listProcessDeploymentInfo;
        // base on the original filter : the filter can be set by no process found (process deleted)
        if (listProcessResult.listProcessNameVersion.isEmpty()) {
            // if listProcessResult.listProcessDeploymentInfo is empty, that's mean all process info
            listProcesses = processAPI.getProcessDeploymentInfos(0, 10000, ProcessDeploymentInfoCriterion.NAME_ASC);
        }
        StringBuilder buildTheSQLRequest = new StringBuilder();

        try { 
            String scopeDetection = milkJobExecution.getInputStringParameter( MilkPurgeArchivedCases.cstParamOperationScopeProcess);

            if ( MilkPurgeArchivedCases.CSTSCOPE_ROOTPROCESS.equals(scopeDetection) || MilkPurgeArchivedCases.CSTSCOPE_BOTH.equals(scopeDetection)) {
                int countNumberThisPass = 1;
                while (milkJobOutput.nbItemsProcessed < maximumArchiveDeletionPerRound && countNumberThisPass > 0) {
                    if (milkJobExecution.isStopRequired())
                        break;
                    countNumberThisPass = 0;

                    for (ProcessDeploymentInfo processDeploymentInfo : listProcesses) {
                        if (milkJobExecution.isStopRequired())
                            break;

                        currentProcessToLog = processDeploymentInfo.getName() + "(" + processDeploymentInfo.getVersion() + ")";
                        int realCasePerDeletion = (int) (maximumArchiveDeletionPerRound - milkJobOutput.nbItemsProcessed);
                        if (realCasePerDeletion > casePerDeletion)
                            realCasePerDeletion = casePerDeletion;

                        DeletionItemProcess deletionItemProcess = deletionPerProcess.getDeletionProcessFromProcessDeployment(processDeploymentInfo.getProcessId(), processDeploymentInfo);

                        // to estimate the number of case deleted, first get the list
                        buildTheSQLRequest.append("Process [" + processDeploymentInfo.getName() + "(" + processDeploymentInfo.getVersion() + ") id=[" + processDeploymentInfo.getProcessId() + "] ");
                        SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 1);
                        sob.filter(ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
                        SearchResult<ArchivedProcessInstance> searchResultArchived = processAPI.searchArchivedProcessInstances(sob.done());

                        long beforeNbCase = searchResultArchived.getCount();
                        buildTheSQLRequest.append("Nb Archives=" + beforeNbCase + "] ");

                        // we can't trust this information: by default, it's 3 per case 
                        // BUT if the case has sub process, it will be 3 per sub process 
                        // Example : a case call 10 times a subprocess? It will be 3+10*3 for one case
                        long numberArchivedDeleted = processAPI.deleteArchivedProcessInstances(processDeploymentInfo.getProcessId(), 0, realCasePerDeletion * 3) / 3;

                        // execute again
                        searchResultArchived = processAPI.searchArchivedProcessInstances(sob.done());
                        long afterNbCase = searchResultArchived.getCount();
                        if (numberArchivedDeleted > 0)
                            numberArchivedDeleted = beforeNbCase - afterNbCase;
                        buildTheSQLRequest.append("After=" + afterNbCase + "] = " + numberArchivedDeleted + ";<br>");

                        deletionItemProcess.nbCaseDeleted += numberArchivedDeleted;

                        milkJobOutput.nbItemsProcessed += numberArchivedDeleted;
                        countNumberThisPass += numberArchivedDeleted;
                    }
                }
            }

            // Manage now the sub process purge
            if (MilkPurgeArchivedCases.CSTSCOPE_TRANSIENTONLY.equals(scopeDetection) || MilkPurgeArchivedCases.CSTSCOPE_BOTH.equals(scopeDetection)) {
                SubProcessOperationCollect subProcessOperationCollect = new SubProcessOperationCollect();
                ManagePurgeResult managePurgeResult = PurgeOperation.detectPurgeSubProcessOnly(listProcessResult, 0L, milkJobExecution.getTenantId(), milkJobExecution.getJobStopAfterMaxItems(), subProcessOperationCollect);
                milkJobOutput.addEvents(managePurgeResult.listEvents);
                buildTheSQLRequest.append("Detect transient ProcessInstance:<br>" + managePurgeResult.sqlQuery);

                for (Long processId : subProcessOperationCollect.listArchProcessID) {
                    DeletionItemProcess deletionItemprocess = deletionPerProcess.getDeletionProcessFromProcessId(processId, processAPI);
                    deletionItemprocess.nbCaseDeleted++;
                }

                managePurgeResult = PurgeOperation.purgeSubProcess(subProcessOperationCollect.listArchSourceProcessId, milkJobExecution.getTenantId());
                milkJobOutput.nbItemsProcessed += managePurgeResult.nbRecords;
                milkJobOutput.addEvents(managePurgeResult.listEvents);

            }
            milkJobOutput.addReportInHtml(buildTheSQLRequest.toString());

            if (BEventFactory.isError(milkJobOutput.getListEvents())) {
                milkJobOutput.setExecutionStatus(ExecutionStatus.ERROR);
            } else if (milkJobOutput.nbItemsProcessed == 0)
                milkJobOutput.setExecutionStatus(ExecutionStatus.SUCCESSNOTHING);
            else
                milkJobOutput.setExecutionStatus(ExecutionStatus.SUCCESS);

        } catch (DeletionException | SearchException e) {
            logger.severe(LOGGER_LABEL + ".purgeNoDelay: Error Delete Archived ProcessDefinition=[" + currentProcessToLog + "] Error[" + e.getMessage() + "]");
            milkJobOutput.setExecutionStatus(ExecutionStatus.ERROR);
            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + currentProcessToLog));
        }
        // produce the report now
        milkJobOutput.setMeasure(MilkPurgeArchivedCases.cstMesureCasePurged, milkJobOutput.nbItemsProcessed);

        deletionPerProcess.addInReport(milkJobOutput);

    }

    /**
     * deleteArchivesWithDelay : Filter are in: delay to purge, what to purge. A search is needed use deleteArchivedProcessInstancesInAllStates
     * 
     * @param listProcessResult
     * @param delayResult
     * @param milkJobExecution
     * @param searchActBuilder
     */
    private void purgeArchivesWithFilter(ListProcessesResult listProcessResult, DelayResult delayResult, MilkJobExecution milkJobExecution, SearchOptionsBuilder searchActBuilder, MilkJobOutput milkJobOutput) {
        List<Long> sourceProcessInstanceIds = new ArrayList<>();
        DeletionPerProcess deletionPerProcess = new DeletionPerProcess();
        StringBuilder buildTheSQLRequest = new StringBuilder();

        try {
            PurgeOperation.buildSelectArchProcessInstance(buildTheSQLRequest, listProcessResult, delayResult.delayDate.getTime(), milkJobExecution.getTenantId());
            long timeSearch = delayResult.delayDate.getTime();
            ProcessAPI processAPI = milkJobExecution.getApiAccessor().getProcessAPI();

            // --------------------- 
            String scopeDetection = milkJobExecution.getInputStringParameter(MilkPurgeArchivedCases.cstParamOperationScopeProcess);

            if (MilkPurgeArchivedCases.CSTSCOPE_ROOTPROCESS.equals(scopeDetection) || MilkPurgeArchivedCases.CSTSCOPE_BOTH.equals(scopeDetection)) {

                SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance;

                Chronometer startSearch = milkJobOutput.beginChronometer("SearchArchivedCase");
                // Now, search items to delete
                searchActBuilder.lessOrEquals(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, timeSearch);
                searchActBuilder.sort(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, Order.ASC);

                buildTheSQLRequest.append(" and ROOTPROCESSINSTANCEID = SOURCEOBJECTID"); // only root case

                searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());
                milkJobOutput.endChronometer(startSearch);
                // do the purge now

                Chronometer startMarker = milkJobOutput.beginChronometer("Purge");
                buildTheSQLRequest.append(" nbResult=" + searchArchivedProcessInstance.getCount());

                for (ArchivedProcessInstance archivedProcessInstance : searchArchivedProcessInstance.getResult()) {
                    if (milkJobExecution.isStopRequired())
                        break;
                    // proceed page per page
                    sourceProcessInstanceIds.add(archivedProcessInstance.getSourceObjectId());

                    DeletionItemProcess deletionItemprocess = deletionPerProcess.getDeletionProcessFromProcessId(archivedProcessInstance.getProcessDefinitionId(), processAPI);
                    deletionItemprocess.nbCaseDeleted++;

                    if (sourceProcessInstanceIds.size() == 50) {
                        Chronometer startDeletion = milkJobOutput.beginChronometer("Deletion");
                        ManagePurgeResult managePurgeResult = PurgeOperation.deleteArchivedProcessInstance( sourceProcessInstanceIds, milkJobExecution, milkJobOutput, processAPI );
                        milkJobOutput.nbItemsProcessed += sourceProcessInstanceIds.size();
                        sourceProcessInstanceIds.clear();
                        milkJobOutput.addEvents( managePurgeResult.listEvents);
                        if (BEventFactory.isError(managePurgeResult.listEvents))
                            milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR);
                        
                        milkJobOutput.endChronometer(startDeletion);

                    }
                }
                // reliquat
                if (!sourceProcessInstanceIds.isEmpty()) {
                    Chronometer startDeletion = milkJobOutput.beginChronometer("Deletion");
                    
                    ManagePurgeResult managePurgeResult = PurgeOperation.deleteArchivedProcessInstance( sourceProcessInstanceIds, milkJobExecution, milkJobOutput, processAPI );
                    milkJobOutput.addEvents( managePurgeResult.listEvents);
                    if (BEventFactory.isError(managePurgeResult.listEvents))
                        milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR);

                    milkJobOutput.endChronometer(startDeletion);

                    milkJobOutput.nbItemsProcessed += sourceProcessInstanceIds.size();
                }
                milkJobOutput.endChronometer(startMarker);
                milkJobOutput.addEvent(new BEvent(eventDeletionSuccess, "Purge:" + milkJobOutput.nbItemsProcessed + " in " + TypesCast.getHumanDuration(startMarker.getTimeExecution(), true)));

            }

            // purge now subprocess operation
            if (MilkPurgeArchivedCases.CSTSCOPE_TRANSIENTONLY.equals(scopeDetection) || MilkPurgeArchivedCases.CSTSCOPE_BOTH.equals(scopeDetection)) {

                SubProcessOperationCollect subProcessOperationCollect = new SubProcessOperationCollect();
                ManagePurgeResult managePurgeResult = PurgeOperation.detectPurgeSubProcessOnly(listProcessResult, 0L, milkJobExecution.getTenantId(), milkJobExecution.getJobStopAfterMaxItems(), subProcessOperationCollect);
                milkJobOutput.addEvents(managePurgeResult.listEvents);
                buildTheSQLRequest.append("Detect transient ProcessInstance:<br>" + managePurgeResult.sqlQuery);

                for (Long processId : subProcessOperationCollect.listArchProcessID) {
                    DeletionItemProcess deletionItemprocess = deletionPerProcess.getDeletionProcessFromProcessId(processId, processAPI);
                    deletionItemprocess.nbCaseDeleted++;
                }

                managePurgeResult = PurgeOperation.purgeSubProcess(subProcessOperationCollect.listArchSourceProcessId, milkJobExecution.getTenantId());
                milkJobOutput.nbItemsProcessed += managePurgeResult.nbRecords;
                milkJobOutput.addEvents(managePurgeResult.listEvents);

            }
            milkJobOutput.addReportInHtml(buildTheSQLRequest.toString());

            if (milkJobOutput.nbItemsProcessed == 0)
                milkJobOutput.setExecutionStatus(ExecutionStatus.SUCCESSNOTHING);
            else
                milkJobOutput.setExecutionStatus(ExecutionStatus.SUCCESS);

        } catch (SearchException e1) {
            milkJobOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
            milkJobOutput.setExecutionStatus(ExecutionStatus.ERROR);
        }
        deletionPerProcess.addInReport(milkJobOutput);
        milkJobOutput.setMeasure(MilkPurgeArchivedCases.cstMesureCasePurged, milkJobOutput.nbItemsProcessed);

    }
    
    
    

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * Sub Process Options
     * /*
     */
    /* ******************************************************************************** */
   

    /**
     * Do a simple collect
     */
    public class SubProcessOperationCollect implements SubProcessOperation {

        public List<Long> listArchSourceProcessId = new ArrayList<>();
        public List<Long> listArchProcessID = new ArrayList<>();

        public List<BEvent> detectOneSubprocessInstance(Map<String, Object> recordSubProcess) {
            listArchSourceProcessId.add(TypesCast.getLong(recordSubProcess.get("SOURCEOBJECTID"), 0L));
            listArchProcessID.add(TypesCast.getLong(recordSubProcess.get("PROCESSDEFINITIONID"), 0L));
            return new ArrayList<>();
        }
    }

}
