package org.bonitasoft.truckmilk.plugin;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
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
import org.bonitasoft.engine.bpm.process.ProcessInstanceSearchDescriptor;
import org.bonitasoft.engine.exception.DeletionException;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.ListProcessesResult;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.CSVOperation;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

import lombok.Data;

public class MilkPurgeArchive extends MilkPlugIn {

    static Logger logger = Logger.getLogger(MilkPurgeArchive.class.getName());
    private final static String LOGGER_LABEL = "MilkPurgeArchive ";

    private final static String CSTOPERATION_GETLIST = "Get List (No operation)";
    private final static String CSTOPERATION_DIRECT = "Purge";
    private final static String CSTOPERATION_FROMLIST = "Purge from the CSV list";

    private static BEvent eventDeletionSuccess = new BEvent(MilkPurgeArchive.class.getName(), 1, Level.SUCCESS,
            "Deletion done with success", "Archived Cases are deleted with success");

    private static BEvent eventDeletionFailed = new BEvent(MilkPurgeArchive.class.getName(), 2, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion of archived cases", "Cases are not deleted", "Check the exception");

    private static BEvent eventSearchFailed = new BEvent(MilkPurgeArchive.class.getName(), 3, Level.ERROR,
            "Search failed", "Search failed task return an error", "No retry can be performed", "Check the error");

    private static BEvent eventWriteReportError = new BEvent(MilkPurgeArchive.class.getName(), 4, Level.ERROR,
            "Report generation error", "Error arrived during the generation of the report", "No report is available", "Check the error");

    private static BEvent eventSynthesisReport = new BEvent(MilkPurgeArchive.class.getName(), 5, Level.INFO,
            "Report Synthesis", "Result of search", "", "");
    private static BEvent eventUnknowOperation = new BEvent(MilkPurgeArchive.class.getName(), 6, Level.APPLICATIONERROR,
            "Operation unknown", "The operation is unknow, only [" + CSTOPERATION_GETLIST + "], [" + CSTOPERATION_DIRECT + "], [" + CSTOPERATION_FROMLIST + "] are known", "No operation is executed", "Check operation");

    private static BEvent eventReportError = new BEvent(MilkPurgeArchive.class.getName(), 7, Level.APPLICATIONERROR,
            "Error in source file", "The source file is not correct", "Check the source file, caseid is expected inside", "Check the error");

    private static PlugInParameter cstParamOperation = PlugInParameter.createInstanceListValues("operation", "operation: Build a list of cases to operate, do directly the operation, or do the operation from a list",
            new String[] { CSTOPERATION_GETLIST, CSTOPERATION_DIRECT, CSTOPERATION_FROMLIST }, CSTOPERATION_DIRECT, "Result is a purge, or build a list, or used the uploaded list");

    private static PlugInParameter cstParamDelay = PlugInParameter.createInstance("delayinday", "Delay", TypeParameter.DELAY, MilkPlugInToolbox.DELAYSCOPE.MONTH + ":3", "The case must be older than this number, in days. 0 means all archived case is immediately in the perimeter",
            true,
            "milkJob.parametersvalue[ 'operation' ] != '" + CSTOPERATION_FROMLIST + "'");
    private static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Process Filter", TypeParameter.ARRAYPROCESSNAME, null, "Give a list of process name. Name must be exact, no version is given (all versions will be purged)",
            false,
            "milkJob.parametersvalue[ 'operation' ] != '" + CSTOPERATION_FROMLIST + "'");
            
    private static PlugInParameter cstParamSeparatorCSV = PlugInParameter.createInstance("separatorCSV", "Separator CSV", TypeParameter.STRING, ",", "CSV use a separator. May be ; or ,",
            false,
            "milkJob.parametersvalue[ 'operation' ] != '" + CSTOPERATION_DIRECT + "'");
    
    private static PlugInParameter cstParamListOfCasesDocumment = PlugInParameter.createInstanceFile("report", "List of cases", TypeParameter.FILEREADWRITE, null, "List is calculated and saved in this parameter", "ListToPurge.csv", "application/CSV",
            false,
            "milkJob.parametersvalue[ 'operation' ] != '" + CSTOPERATION_DIRECT + "'");

    private final static PlugInMesure cstMesureCasePurged = PlugInMesure.createInstance("CasePurged", "cases purged", "Number of case purged in this execution");
    private final static PlugInMesure cstMesureCaseDetected = PlugInMesure.createInstance("CaseDetected", "cases detected", "Number of case detected in the scope");

    public MilkPurgeArchive() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the milkEmailUsersTasks, we require to be able to send an email
     */
    public List<BEvent> checkPluginEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();
    }

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription() {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName("PurgeArchivedCase");
        plugInDescription.setLabel("Purge Archived Case");
        plugInDescription.setCategory(CATEGORY.CASES);
        plugInDescription.setExplanation("3 operations: PURGE/ GET LIST / PURGE FROM LIST. Purge (or get the list of) archived case according the filter. Filter based on different process, and purge cases older than the delai. At each round with Purge / Purge From list, a maximum case are deleted. If the maximum is over than 100000, it's reduce to this limit.");
        plugInDescription.setWarning("A case purged can't be retrieved. Operation is final. Use with caution.");
        plugInDescription.addParameter(cstParamOperation);
        plugInDescription.addParameter(cstParamDelay);
        plugInDescription.addParameter(cstParamProcessFilter);
        plugInDescription.addParameter(cstParamSeparatorCSV);
        plugInDescription.addParameter(cstParamListOfCasesDocumment);

        plugInDescription.addMesure(cstMesureCasePurged);
        plugInDescription.addMesure(cstMesureCaseDetected);

        plugInDescription.setStopJob(JOBSTOPPER.BOTH);
        plugInDescription.setJobStopMaxItems(100000);
        return plugInDescription;
    }

    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();

        String operation = jobExecution.getInputStringParameter(cstParamOperation);
        if (CSTOPERATION_DIRECT.equals(operation))
            return operationDirectPurge(jobExecution);
        else if (CSTOPERATION_GETLIST.equals(operation))
            return getList(jobExecution);
        else if (CSTOPERATION_FROMLIST.equals(operation))
            return fromList(jobExecution);

        plugTourOutput.addEvent(new BEvent(eventUnknowOperation, "Operation[" + operation + "]"));
        plugTourOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
        return plugTourOutput;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * Direct Purge
     * /*
     */
    /* ******************************************************************************** */

    public MilkJobOutput operationDirectPurge(MilkJobExecution jobExecution) {
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();

        Integer maximumArchiveDeletionPerRound = jobExecution.getJobStopAfterMaxItems();
        // default value is 1 Million
        if (maximumArchiveDeletionPerRound == null || maximumArchiveDeletionPerRound.equals(MilkJob.CSTDEFAULT_STOPAFTER_MAXITEMS))
            maximumArchiveDeletionPerRound = 1000000;

        SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, (int) maximumArchiveDeletionPerRound + 1);

        try {
            ListProcessesResult listProcessResult = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, false, searchActBuilder, ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, jobExecution.getApiAccessor().getProcessAPI());

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                plugTourOutput.addEvents(listProcessResult.listEvents);
                plugTourOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return plugTourOutput;
            }

            DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamDelay, new Date(), false);
            if (BEventFactory.isError(delayResult.listEvents)) {
                plugTourOutput.addEvents(delayResult.listEvents);
                plugTourOutput.executionStatus = ExecutionStatus.ERROR;
                return plugTourOutput;
            }
            if (delayResult.delayInMs == 0) {
                purgeArchiveNoDelay(listProcessResult, jobExecution, plugTourOutput);
            } else {
                purgeArchivesWithDelay(listProcessResult, delayResult, jobExecution, searchActBuilder, plugTourOutput);
            }
            if (plugTourOutput.nbItemsProcessed == 0 && plugTourOutput.executionStatus == ExecutionStatus.SUCCESS)
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
        } catch (SearchException e1) {
            plugTourOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;

        }
        return plugTourOutput;
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
     * DeleteArchiveNoDelay
     * 
     * @param listProcessResult
     * @param jobExecution
     * @param jobOutput
     */
    private void purgeArchiveNoDelay(ListProcessesResult listProcessResult, MilkJobExecution jobExecution, MilkJobOutput jobOutput) {
        DeletionPerProcess deletionPerProcess = new DeletionPerProcess();

        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        Integer maximumArchiveDeletionPerRound = jobExecution.getJobStopAfterMaxItems();
        // default value is 1 Million
        if (maximumArchiveDeletionPerRound == null || maximumArchiveDeletionPerRound.equals(MilkJob.CSTDEFAULT_STOPAFTER_MAXITEMS))
            maximumArchiveDeletionPerRound = 1000000;
        int totalNumberCaseDeleted = 0;
        String currentProcessToLog = "";
        List<ProcessDeploymentInfo> listProcesses = listProcessResult.listProcessDeploymentInfo;
        // base on the original filter : the filter can be set by no process found (process deleted)
        if (listProcessResult.listProcessNameVersion.isEmpty()) {
            // if listProcessResult.listProcessDeploymentInfo is empty, that's mean all process info
            listProcesses = processAPI.getProcessDeploymentInfos(0, 10000, ProcessDeploymentInfoCriterion.NAME_ASC);
        }

        try {

            int countNumberThisPass = 1;
            while (totalNumberCaseDeleted < maximumArchiveDeletionPerRound && countNumberThisPass > 0) {
                if (jobExecution.pleaseStop())
                    break;
                countNumberThisPass = 0;

                for (ProcessDeploymentInfo processDeploymentInfo : listProcesses) {
                    if (jobExecution.pleaseStop())
                        break;

                    currentProcessToLog = processDeploymentInfo.getName() + "(" + processDeploymentInfo.getVersion() + ")";
                    int realCasePerDeletion = (int) (maximumArchiveDeletionPerRound - totalNumberCaseDeleted);
                    if (realCasePerDeletion > casePerDeletion)
                        realCasePerDeletion = casePerDeletion;

                    DeletionItemProcess deletionItemProcess = deletionPerProcess.getDeletionProcessFromProcessDeployment(processDeploymentInfo.getProcessId(), processDeploymentInfo);

                    // to estimate the number of case deleted, first get the list
                    SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 1);
                    sob.filter(ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
                    SearchResult<ArchivedProcessInstance> searchResultArchived = processAPI.searchArchivedProcessInstances(sob.done());
                    long beforeNbCase = searchResultArchived.getCount();

                    // we can't trust this information: by default, it's 3 per case 
                    // BUT if the case has sub process, it will be 3 per sub process 
                    // Example : a case call 10 times a subprocess? It will be 3+10*3 for one case
                    long numberArchivedDeleted = processAPI.deleteArchivedProcessInstances(processDeploymentInfo.getProcessId(), 0, realCasePerDeletion * 3) / 3;

                    // execute again
                    searchResultArchived = processAPI.searchArchivedProcessInstances(sob.done());
                    long afterNbCase = searchResultArchived.getCount();
                    if (numberArchivedDeleted > 0)
                        numberArchivedDeleted = afterNbCase - beforeNbCase;

                    deletionItemProcess.nbCaseDeleted += numberArchivedDeleted;

                    totalNumberCaseDeleted += numberArchivedDeleted;
                    countNumberThisPass += numberArchivedDeleted;
                }
            }
            if (totalNumberCaseDeleted == 0)
                jobOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
            else
                jobOutput.executionStatus = ExecutionStatus.SUCCESS;

        } catch (DeletionException | SearchException e) {
            logger.severe(LOGGER_LABEL + ".purgeNoDelay: Error Delete Archived ProcessDefinition=[" + currentProcessToLog + "] Error[" + e.getMessage() + "]");
            jobOutput.executionStatus = ExecutionStatus.ERROR;
            jobOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + currentProcessToLog));
        }
        // produce the report now
        jobOutput.setMesure(cstMesureCasePurged, totalNumberCaseDeleted);

        jobOutput.setNbItemsProcessed(totalNumberCaseDeleted);
        deletionPerProcess.addInReport(jobOutput);

    }

    /**
     * deleteArchivesWithDelay
     * 
     * @param listProcessResult
     * @param delayResult
     * @param jobExecution
     * @param searchActBuilder
     */
    private void purgeArchivesWithDelay(ListProcessesResult listProcessResult, DelayResult delayResult, MilkJobExecution jobExecution, SearchOptionsBuilder searchActBuilder, MilkJobOutput jobOutput) {
        List<Long> sourceProcessInstanceIds = new ArrayList<>();
        DeletionPerProcess deletionPerProcess = new DeletionPerProcess();

        try {
            long timeSearch = delayResult.delayDate.getTime();
            SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance;

            ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();

            // Now, search items to delete
            searchActBuilder.lessOrEquals(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, timeSearch);
            searchActBuilder.sort(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, Order.ASC);

            searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());
            if (searchArchivedProcessInstance.getCount() == 0) {
                jobOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
                jobOutput.nbItemsProcessed = 0;
                return;
            }

            // do the purge now
            Long beginTime = System.currentTimeMillis();

            for (ArchivedProcessInstance archivedProcessInstance : searchArchivedProcessInstance.getResult()) {
                if (jobExecution.pleaseStop())
                    break;
                // proceed page per page
                sourceProcessInstanceIds.add(archivedProcessInstance.getSourceObjectId());

                DeletionItemProcess deletionItemprocess = deletionPerProcess.getDeletionProcessFromProcessId(archivedProcessInstance.getProcessDefinitionId(), processAPI);
                deletionItemprocess.nbCaseDeleted++;

                if (sourceProcessInstanceIds.size() == 50) {
                    processAPI.deleteArchivedProcessInstancesInAllStates(sourceProcessInstanceIds);
                    jobOutput.nbItemsProcessed += sourceProcessInstanceIds.size();
                    sourceProcessInstanceIds.clear();
                }
            }
            // reliquat
            if (!sourceProcessInstanceIds.isEmpty()) {
                processAPI.deleteArchivedProcessInstancesInAllStates(sourceProcessInstanceIds);
                jobOutput.nbItemsProcessed += sourceProcessInstanceIds.size();
            }
            Long endTime = System.currentTimeMillis();

            jobOutput.addEvent(new BEvent(eventDeletionSuccess, "Purge:" + jobOutput.nbItemsProcessed + " in " + TypesCast.getHumanDuration(endTime - beginTime, true)));
            jobOutput.executionStatus = ExecutionStatus.SUCCESS;

        } catch (SearchException e1) {
            jobOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
            jobOutput.executionStatus = ExecutionStatus.ERROR;
        } catch (DeletionException e) {
            logger.severe(LOGGER_LABEL + ".purgeArchivesWithDelay: Error Delete Archived ProcessInstance=[" + sourceProcessInstanceIds + "] Error[" + e.getMessage() + "]");
            jobOutput.executionStatus = ExecutionStatus.ERROR;
            jobOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + sourceProcessInstanceIds));
        }
        deletionPerProcess.addInReport(jobOutput);
        jobOutput.setMesure(cstMesureCasePurged, jobOutput.nbItemsProcessed);

    }
    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * DeletionMethod
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
            milkJobOutput.addReportTable(new String[] { "Process Name", "Version", "Number of deletion" });
            for (DeletionItemProcess deletionItemProcess : mapDeletionProcess.values()) {
                if (deletionItemProcess.nbCaseDeleted > 0) {
                    if (deletionItemProcess.processDeploymentInfo != null)
                        milkJobOutput.addReportLine(new Object[] { deletionItemProcess.processDeploymentInfo.getName(), deletionItemProcess.processDeploymentInfo.getVersion(), deletionItemProcess.nbCaseDeleted });
                    else if (deletionItemProcess.processDefinition != null)
                        milkJobOutput.addReportLine(new Object[] { deletionItemProcess.processDefinition.getName(), deletionItemProcess.processDefinition.getVersion(), deletionItemProcess.nbCaseDeleted });
                }
                milkJobOutput.addReportEndTable();
            }
        }

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * GetList
     * /*
     */
    /* ******************************************************************************** */
    protected static String cstColCaseId = "caseid";
    protected static String cstColProcessName = "processname";
    protected static String cstColProcessVersion = "processversion";
    protected static String cstColArchivedDate = "archiveddate";

    protected static String cstColStatus = "status";
    protected static String cstColStatus_V_ALREADYDELETED = "ALREADYDELETED";
    protected static String cstColStatus_V_DELETE = "DELETE";

    private MilkJobOutput getList(MilkJobExecution jobExecution) {
        MilkJobOutput milkJobOutput = jobExecution.getMilkJobOutput();

        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();

        // get Input 
        String separatorCSV = jobExecution.getInputStringParameter(cstParamSeparatorCSV);

        // 20 for the preparation, 100 to collect cases
        // Time to run the query take time, and we don't want to show 0% for a long time
        jobExecution.setAvancementTotalStep(120);
        try {

            SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, jobExecution.getJobStopAfterMaxItems() + 1);

            ListProcessesResult listProcessResult = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, false, searchActBuilder, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                milkJobOutput.addEvents(listProcessResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return milkJobOutput;
            }

            DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamDelay, new Date(), false);
            if (BEventFactory.isError(delayResult.listEvents)) {
                milkJobOutput.addEvents(delayResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                return milkJobOutput;
            }
            long timeSearch = delayResult.delayDate.getTime();

            jobExecution.setAvancementStep(5);

            // -------------- now get the list of cases
            searchActBuilder.lessOrEquals(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, timeSearch);
            searchActBuilder.sort(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, Order.ASC);
            SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance;
            searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());

            /** ok, we did 15 step */
            jobExecution.setAvancementStep(20);

            Map<Long, ProcessDefinition> cacheProcessDefinition = new HashMap<>();

            CSVOperation csvOperationOuput = new CSVOperation();
            csvOperationOuput.writeCsvDocument(new String[] { cstColCaseId, cstColProcessName, cstColProcessVersion, cstColArchivedDate, cstColStatus }, separatorCSV);

            // loop on archive
            int countInArchive = 0;
            for (ArchivedProcessInstance archivedProcessInstance : searchArchivedProcessInstance.getResult()) {

                if (jobExecution.pleaseStop())
                    break;
                countInArchive++;

                Map<String, String> recordCsv = new HashMap<>();
                recordCsv.put(cstColCaseId, String.valueOf(archivedProcessInstance.getSourceObjectId()));

                long processId = archivedProcessInstance.getProcessDefinitionId();
                if (!cacheProcessDefinition.containsKey(processId)) {
                    try {
                        ProcessDefinition processDefinition = processAPI.getProcessDefinition(processId);
                        cacheProcessDefinition.put(processId, processDefinition);
                    } catch (Exception e) {
                        cacheProcessDefinition.put(processId, null);
                    }
                }
                recordCsv.put(cstColProcessName, cacheProcessDefinition.get(processId) == null ? "" : cacheProcessDefinition.get(processId).getName());
                recordCsv.put(cstColProcessVersion, cacheProcessDefinition.get(processId) == null ? "" : cacheProcessDefinition.get(processId).getVersion());
                recordCsv.put(cstColArchivedDate, TypesCast.sdfCompleteDate.format(archivedProcessInstance.getArchiveDate()));
                recordCsv.put(cstColStatus, ""); // status
                csvOperationOuput.writeRecord(recordCsv);

                jobExecution.addManagedItems(1);

                jobExecution.setAvancementStep(20L + (long) (100 * countInArchive / searchArchivedProcessInstance.getResult().size()));

            }

            milkJobOutput.nbItemsProcessed = countInArchive;
            milkJobOutput.setMesure(cstMesureCaseDetected, searchArchivedProcessInstance.getCount());

            csvOperationOuput.closeAndWriteToParameter(milkJobOutput, cstParamListOfCasesDocumment);

            milkJobOutput.addReportTable(new String[] { "Label", "Value" });
            milkJobOutput.addReportLine(new Object[] { "Total cases detected", searchArchivedProcessInstance.getCount() });
            milkJobOutput.addReportLine(new Object[] { "Total cases in CSV file", countInArchive });
            milkJobOutput.addReportEndTable();

            milkJobOutput.addEvent(new BEvent(eventSynthesisReport, "Total cases:" + searchArchivedProcessInstance.getCount() + ", In list:" + countInArchive));

            if (searchArchivedProcessInstance.getCount() == 0) {
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
                milkJobOutput.nbItemsProcessed = 0;
                return milkJobOutput;
            }

            milkJobOutput.executionStatus = jobExecution.pleaseStop() ? ExecutionStatus.SUCCESSPARTIAL : ExecutionStatus.SUCCESS;
        }

        catch (SearchException e1) {
            milkJobOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
        } catch (Exception e1) {
            milkJobOutput.addEvent(new BEvent(eventWriteReportError, e1, ""));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
        }

        return milkJobOutput;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * From List
     * /*
     */
    /* ******************************************************************************** */
    private @Data class Statistic {

        long pleaseStopAfterManagedItems = 0;
        long countIgnored = 0;
        long countAlreadyDone = 0;
        long countBadDefinition = 0;
        long countStillToAnalyse = 0;
        long countNbItems = 0;
        long totalLineCsv = 0;
        long sumTimeSearch = 0;
        long sumTimeDeleted = 0;
        long sumTimeManipulateCsv = 0;
        long nbCasesDeleted = 0;
    }

    /**
     * @param jobExecution
     * @return
     */
    public MilkJobOutput fromList(MilkJobExecution jobExecution) {

        MilkJobOutput milkJobOutput = jobExecution.getMilkJobOutput();

        long beginExecution = System.currentTimeMillis();

        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        // get Input 
        long beginManipulateCsv = System.currentTimeMillis();

        String separatorCSV = jobExecution.getInputStringParameter(cstParamSeparatorCSV);

        Statistic statistic = new Statistic();

        long endManipulateCsv = System.currentTimeMillis();
        statistic.sumTimeManipulateCsv = endManipulateCsv - beginManipulateCsv;

        statistic.pleaseStopAfterManagedItems = jobExecution.getJobStopAfterMaxItems();

        List<Long> sourceProcessInstanceIds = new ArrayList<>();
        long nbAnalyseAlreadyReported = 0;
        try {
            CSVOperation csvOperationInput = new CSVOperation();

            csvOperationInput.loadCsvDocument(jobExecution, cstParamListOfCasesDocumment, separatorCSV);
            statistic.totalLineCsv = csvOperationInput.getCount();
            if (statistic.totalLineCsv == 0) {
                // no document uploaded
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
                return milkJobOutput;
            }

            // update the report : prepare the production
            CSVOperation csvOperationOuput = new CSVOperation();
            csvOperationOuput.writeCsvDocument(new String[] { cstColCaseId, cstColProcessName, cstColProcessVersion, cstColArchivedDate, cstColStatus }, separatorCSV);

            jobExecution.setAvancementTotalStep(statistic.totalLineCsv);

            long lineNumber = 1;
            StringBuilder analysis = new StringBuilder();
            Map<String, String> record;
            while ((record = csvOperationInput.getNextRecord()) != null) {

                if (jobExecution.pleaseStop()) {
                    csvOperationOuput.writeRecord(record);
                    analysis.append("Stop asked;");
                    break;
                }

                jobExecution.setAvancementStep(lineNumber);
                lineNumber++;

                Long caseId = TypesCast.getLong(record.get(MilkPurgeArchivedListGetList.cstColCaseId), null);
                String status = TypesCast.getString(record.get(MilkPurgeArchivedListGetList.cstColStatus), null);

                if (caseId == null) {
                    if (analysis.length() < 300)
                        analysis.append("Line[" + lineNumber + "] " + MilkPurgeArchivedListGetList.cstColCaseId + " undefined;");
                    else
                        nbAnalyseAlreadyReported++;
                    statistic.countBadDefinition++;
                } else if (status == null) {
                    statistic.countIgnored++;
                } else if (cstColStatus_V_ALREADYDELETED.equalsIgnoreCase(status)) {
                    statistic.countAlreadyDone++;
                } else if (cstColStatus_V_DELETE.equalsIgnoreCase(status)) {

                    // delete it
                    sourceProcessInstanceIds.add(caseId);
                    // use the preparation item to stop at the exact requested number
                    jobExecution.addPreparationItems(1);
                    if (sourceProcessInstanceIds.size() > 50) {
                        // purge now
                        long nbCaseDeleted = purgeList(sourceProcessInstanceIds, statistic, processAPI);
                        milkJobOutput.nbItemsProcessed = statistic.countNbItems;
                        jobExecution.addManagedItems(nbCaseDeleted);
                        jobExecution.setPreparationItems(0);
                        sourceProcessInstanceIds.clear();
                    }
                    // actually, deletion is not made, but we can trust the Bonita API
                    record.put(cstColStatus, cstColStatus_V_ALREADYDELETED);
                } else
                    statistic.countIgnored++;

                csvOperationOuput.writeRecord(record);
            } // end loop

            // the end, purge a last time 
            if (!sourceProcessInstanceIds.isEmpty()) {
                long nbCaseDeleted = purgeList(sourceProcessInstanceIds, statistic, processAPI);
                jobExecution.addManagedItems(nbCaseDeleted);
                sourceProcessInstanceIds.clear();
            }
            milkJobOutput.nbItemsProcessed = statistic.countNbItems;

            long endExecution = System.currentTimeMillis();

            // update the report now
            // We have to rewrite the complete document, with no change
            while ((record = csvOperationInput.getNextRecord()) != null) {
                csvOperationOuput.writeRecord(record);
            }
            csvOperationOuput.closeAndWriteToParameter(milkJobOutput, cstParamListOfCasesDocumment);

            // calculated the last ignore            
            statistic.countStillToAnalyse = statistic.totalLineCsv - lineNumber;
            milkJobOutput.addReportTable(new String[] { "Mesure", "Value" });

            StringBuilder reportEvent = new StringBuilder();
            milkJobOutput.addReportLine(new Object[] { "Already done", statistic.countAlreadyDone });

            // reportEvent.append("AlreadyDone: " + statistic.countAlreadyDone + "; ");
            if (statistic.countIgnored > 0) {
                milkJobOutput.addReportLine(new Object[] { "Ignored(no status DELETE):", statistic.countIgnored });
                // reportEvent.append("Ignored(no status DELETE):" + statistic.countIgnored + ";");
            }

            if (statistic.countStillToAnalyse > 0) {
                milkJobOutput.addReportLine(new Object[] { "StillToAnalyse",statistic.countStillToAnalyse });
                // reportEvent.append("StillToAnalyse:" + statistic.countStillToAnalyse+";");
            }

            if (statistic.countBadDefinition > 0) {
                milkJobOutput.addReportLine(new Object[] { "Bad Definition(noCaseid):", statistic.countBadDefinition });
                milkJobOutput.addReportLine(new Object[] { "Analysis:", analysis.toString() });

                // reportEvent.append("Bad Definition(noCaseid):" + statistic.countBadDefinition + " : " + analysis.toString()+";");
            }

            if (nbAnalyseAlreadyReported > 0) {
                milkJobOutput.addReportLine(new Object[] { "More errors", nbAnalyseAlreadyReported, "" });
                // reportEvent.append("(" + nbAnalyseAlreadyReported + ") more errors;");
            }

            // add Statistics
            reportEvent.append("Cases Deleted:" + statistic.countNbItems + " in " + statistic.sumTimeDeleted + " ms ");
            if (statistic.countNbItems > 0)
                reportEvent.append("( " + ((int) (statistic.sumTimeDeleted / statistic.countNbItems)) + " ms/case)");

            reportEvent.append("SearchCase time:" + statistic.sumTimeSearch + " ms;");
            reportEvent.append("Manipulate CSV time:" + statistic.sumTimeManipulateCsv + " ms;");

            reportEvent.append("total Execution time:" + (endExecution - beginExecution) + " ms;");

            if (jobExecution.pleaseStop())
                reportEvent.append("Stop asked;");
            if (statistic.countNbItems >= statistic.pleaseStopAfterManagedItems)
                reportEvent.append("Reach the NumberOfItem;");

            milkJobOutput.addReportEndTable();

            milkJobOutput.setMesure(cstMesureCasePurged, statistic.countNbItems);

            BEvent eventFinal = (statistic.countBadDefinition == 0) ? new BEvent(eventDeletionSuccess, reportEvent.toString()) : new BEvent(eventReportError, reportEvent.toString());

            milkJobOutput.addEvent(eventFinal);

            milkJobOutput.executionStatus = (jobExecution.pleaseStop() || statistic.countStillToAnalyse > 0) ? ExecutionStatus.SUCCESSPARTIAL : ExecutionStatus.SUCCESS;
            if (statistic.countBadDefinition > 0) {
                milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            }

        } catch (Exception e) {
            logger.severe(LOGGER_LABEL + ".fromList: Error Delete Archived ProcessInstance=[" + sourceProcessInstanceIds + "] Error[" + e.getMessage() + "]");
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + sourceProcessInstanceIds));

        }

        return milkJobOutput;
    }

    /**
     * methid processAPI.deleteArchivedProcessInstancesInAllStates(sourceProcessInstanceIds) is very long, even if there are nothing to purge
     * so, let's first search the real number to purge, and do the purge only on real case.
     * 
     * @return
     */

    public int purgeList(List<Long> sourceProcessInstanceIds, Statistic statistic, ProcessAPI processAPI) throws DeletionException, SearchException {
        long startTimeSearch = System.currentTimeMillis();
        SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, sourceProcessInstanceIds.size());
        for (int i = 0; i < sourceProcessInstanceIds.size(); i++) {
            if (i > 0)
                searchActBuilder.or();
            searchActBuilder.filter(ArchivedProcessInstancesSearchDescriptor.SOURCE_OBJECT_ID, sourceProcessInstanceIds.get(i));
        }
        SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());
        long endTimeSearch = System.currentTimeMillis();
        statistic.sumTimeSearch += endTimeSearch - startTimeSearch;

        List<Long> listRealId = new ArrayList<>();
        for (ArchivedProcessInstance archived : searchArchivedProcessInstance.getResult()) {
            listRealId.add(archived.getSourceObjectId());
        }

        // we know how many item we don't need to process in this batch
        // BATCH TO STUDY                   : sourceProcessInstanceIds
        // Case To delete in this batch     : realId.size()
        // Already Managed in this job      : statistic.countNbItems
        // Already Done in a previous job   : sourceProcessInstanceIds.size() - realId.size() to add to countAlreadyDone
        // if (Already Managed in this job) + (Case To Delete in the bath) > pleaseStopAfterManagedItem then we have to limit our number of deletion 

        statistic.countAlreadyDone += sourceProcessInstanceIds.size() - listRealId.size();
        // ok, the point is now maybe we don't want to process ALL this process to delete, due to the limitation
        if (statistic.countNbItems + listRealId.size() > statistic.pleaseStopAfterManagedItems) {
            // too much, we need to reduce the number
            long maximumToManage = statistic.pleaseStopAfterManagedItems - statistic.countNbItems;
            listRealId = listRealId.subList(0, (int) maximumToManage);
        }

        long startTimeDelete = System.currentTimeMillis();
        long nbCaseDeleted = 0;
        if (!listRealId.isEmpty()) {
            // we can't trust this information
            nbCaseDeleted += processAPI.deleteArchivedProcessInstancesInAllStates(listRealId);
            statistic.nbCasesDeleted += listRealId.size();
        }
        long endTimeDelete = System.currentTimeMillis();

        logger.info(LOGGER_LABEL + ".purgeList: search in " + (endTimeSearch - startTimeSearch) + " ms , delete in " + (endTimeDelete - startTimeDelete) + " ms for " + listRealId.size() + " nbCaseDeleted=" + nbCaseDeleted);
        logger.fine(LOGGER_LABEL + ".purgeList: InternalCaseDeletion=" + statistic.nbCasesDeleted);
        statistic.countNbItems += listRealId.size();
        statistic.sumTimeDeleted += endTimeDelete - startTimeDelete;
        return listRealId.size();

    }

    /**
     * count the number of line in the
     * 
     * @param outputByte
     * @return
     */
    public long nbLinesInCsv(ByteArrayOutputStream outputByte) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(outputByte.toByteArray())));
            long nbLine = 0;
            while (reader.readLine() != null) {
                nbLine++;
            }
            return nbLine;
        } catch (Exception e) {
            return 0;
        }

    }
}
