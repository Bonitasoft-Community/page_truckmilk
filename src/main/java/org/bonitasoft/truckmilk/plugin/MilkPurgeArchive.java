package org.bonitasoft.truckmilk.plugin;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.bonitasoft.truckmilk.engine.MilkJobOutput.Chronometer;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter.FilterProcess;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.ReplacementPlugIn;
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

    
    public final static String CST_PLUGIN_NAME= "PurgeArchivedCase";
    
    
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

    private static PlugInParameter cstParamDelay = PlugInParameter.createInstanceDelay("delayinday", "Delay", DELAYSCOPE.MONTH,3, "The case must be older than this number, in days. 0 means all archived case is immediately in the perimeter")
            .withMandatory(true)
            .withVisibleCondition("milkJob.parametersvalue[ 'operation' ] != '" + CSTOPERATION_FROMLIST + "'");

    private static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Process Filter", TypeParameter.ARRAYPROCESSNAME, null, "Give a list of process name. Name must be exact, no version is given (all versions will be purged)")            
            .withVisibleCondition("milkJob.parametersvalue[ 'operation' ] != '" + CSTOPERATION_FROMLIST + "'")
            .withFilterProcess(FilterProcess.ALL);

    private static PlugInParameter cstParamSeparatorCSV = PlugInParameter.createInstance("separatorCSV", "Separator CSV", TypeParameter.STRING, ",", "CSV use a separator. May be ; or ,").withVisibleCondition("milkJob.parametersvalue[ 'operation' ] != '" + CSTOPERATION_DIRECT + "'");

    private static PlugInParameter cstParamListOfCasesDocument = PlugInParameter.createInstanceFile("report", "List of cases", TypeParameter.FILEREADWRITE, null, "List is calculated and saved in this parameter", "ListToPurge.csv", "application/CSV")
            .withVisibleCondition("milkJob.parametersvalue[ 'operation' ] != '" + CSTOPERATION_DIRECT + "'");

    private final static PlugInMeasurement cstMesureCasePurged = PlugInMeasurement.createInstance("CasePurged", "cases purged", "Number of case purged in this execution");
    private final static PlugInMeasurement cstMesureCaseDetected = PlugInMeasurement.createInstance("CaseDetected", "cases detected", "Number of case detected in the scope");

    public MilkPurgeArchive() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    
    @Override
    public ReplacementPlugIn getReplacement( String plugInName, Map<String,Object> parameters) {

        if ("ListPurgeCase".equals(plugInName))  {
            ReplacementPlugIn replacementPlugIn = new ReplacementPlugIn(this, parameters);
            replacementPlugIn.parameters.put(cstParamOperation.name, CSTOPERATION_GETLIST);
            return replacementPlugIn;
        }
        if ("PurgeCaseFromList".equals(plugInName))  {
            ReplacementPlugIn replacementPlugIn = new ReplacementPlugIn(this, parameters);
            replacementPlugIn.parameters.put(cstParamOperation.name, CSTOPERATION_FROMLIST);
            return replacementPlugIn;
        }            
        
        return null;

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
        plugInDescription.setName( CST_PLUGIN_NAME );
        plugInDescription.setLabel("Purge Archived Case");
        plugInDescription.setCategory(CATEGORY.CASES);
        plugInDescription.setExplanation("3 operations: PURGE/ GET LIST / PURGE FROM LIST. Purge (or get the list of) archived case according the filter. Filter based on different process, and purge cases older than the delai. At each round with Purge / Purge From list, a maximum case are deleted. If the maximum is over than 100000, it's reduce to this limit.");
        plugInDescription.setWarning("A case purged can't be retrieved. Operation is final. Use with caution.");
        plugInDescription.addParameter(cstParamOperation);
        plugInDescription.addParameter(cstParamDelay);
        plugInDescription.addParameter(cstParamProcessFilter);
        plugInDescription.addParameter(cstParamSeparatorCSV);
        plugInDescription.addParameter(cstParamListOfCasesDocument);

        plugInDescription.addMesure(cstMesureCasePurged);
        plugInDescription.addMesure(cstMesureCaseDetected);

        plugInDescription.setStopJob(JOBSTOPPER.BOTH);
        plugInDescription.setJobStopMaxItems(100000);
        return plugInDescription;
    }

    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {

        String operation = jobExecution.getInputStringParameter(cstParamOperation);
        if (CSTOPERATION_DIRECT.equals(operation))
            return operationDirectPurge(jobExecution);
        else if (CSTOPERATION_GETLIST.equals(operation))
            return getList(jobExecution);
        else if (CSTOPERATION_FROMLIST.equals(operation))
            return fromList(jobExecution);


        MilkJobOutput milkJobOutput = jobExecution.getMilkJobOutput();
        milkJobOutput.addEvent(new BEvent(eventUnknowOperation, "Operation[" + operation + "]"));
        milkJobOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
        return milkJobOutput;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * Direct Purge
     * /*
     */
    /* ******************************************************************************** */

    public MilkJobOutput operationDirectPurge(MilkJobExecution jobExecution) {
        MilkJobOutput milkJobOutput = jobExecution.getMilkJobOutput();

        Integer maximumArchiveDeletionPerRound = jobExecution.getJobStopAfterMaxItems();
        // default value is 1 Million
        if (maximumArchiveDeletionPerRound == null || maximumArchiveDeletionPerRound.equals(MilkJob.CSTDEFAULT_STOPAFTER_MAXITEMS))
            maximumArchiveDeletionPerRound = 1000000;

        SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, (int) maximumArchiveDeletionPerRound + 1);

        try {
            
            // List of process ENABLE AND DISABLE
            ListProcessesResult listProcessResult = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, false, searchActBuilder, ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, jobExecution.getApiAccessor().getProcessAPI());

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                milkJobOutput.addEvents(listProcessResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return milkJobOutput;
            }
            
            
            // Delay
            DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamDelay, new Date(), false);
            if (BEventFactory.isError(delayResult.listEvents)) {
                milkJobOutput.addEvents(delayResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                return milkJobOutput;
            }
            if (delayResult.delayInMs == 0) {
                purgeArchiveNoDelay(listProcessResult, jobExecution, milkJobOutput);
            } else {
                
                purgeArchivesWithDelay(listProcessResult, delayResult, jobExecution, searchActBuilder, milkJobOutput);
            }
            if (milkJobOutput.nbItemsProcessed == 0 && milkJobOutput.executionStatus == ExecutionStatus.SUCCESS)
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
        } catch (SearchException e1) {
            milkJobOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;

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
     * DeleteArchiveNoDelay
     * 
     * @param listProcessResult
     * @param jobExecution
     * @param milkJobOutput
     */
    private void purgeArchiveNoDelay(ListProcessesResult listProcessResult, MilkJobExecution jobExecution, MilkJobOutput milkJobOutput) {
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
        StringBuilder buildTheSQLRequest = new StringBuilder();
        
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
                    buildTheSQLRequest.append("Process ["+processDeploymentInfo.getName()+"("+processDeploymentInfo.getVersion()+") id=["+processDeploymentInfo.getProcessId()+"] ");
                    SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 1);
                    sob.filter(ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
                    SearchResult<ArchivedProcessInstance> searchResultArchived = processAPI.searchArchivedProcessInstances(sob.done());

                    long beforeNbCase = searchResultArchived.getCount();
                    buildTheSQLRequest.append("Nb Archives="+beforeNbCase+"] ");
                    
                    // we can't trust this information: by default, it's 3 per case 
                    // BUT if the case has sub process, it will be 3 per sub process 
                    // Example : a case call 10 times a subprocess? It will be 3+10*3 for one case
                    long numberArchivedDeleted = processAPI.deleteArchivedProcessInstances(processDeploymentInfo.getProcessId(), 0, realCasePerDeletion * 3) / 3;

                    // execute again
                    searchResultArchived = processAPI.searchArchivedProcessInstances(sob.done());
                    long afterNbCase = searchResultArchived.getCount();
                    if (numberArchivedDeleted > 0)
                        numberArchivedDeleted = beforeNbCase - afterNbCase;
                    buildTheSQLRequest.append("After="+afterNbCase+"] = "+numberArchivedDeleted+";<br>");
                    
                    deletionItemProcess.nbCaseDeleted += numberArchivedDeleted;

                    totalNumberCaseDeleted += numberArchivedDeleted;
                    countNumberThisPass += numberArchivedDeleted;
                }
            }
            milkJobOutput.addReportInHtml(buildTheSQLRequest.toString());
            
            if (totalNumberCaseDeleted == 0)
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
            else
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;

        } catch (DeletionException | SearchException e) {
            logger.severe(LOGGER_LABEL + ".purgeNoDelay: Error Delete Archived ProcessDefinition=[" + currentProcessToLog + "] Error[" + e.getMessage() + "]");
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + currentProcessToLog));
        }
        // produce the report now
        milkJobOutput.setMesure(cstMesureCasePurged, totalNumberCaseDeleted);

        milkJobOutput.setNbItemsProcessed(totalNumberCaseDeleted);
        deletionPerProcess.addInReport(milkJobOutput);

    }

    /**
     * deleteArchivesWithDelay
     * 
     * @param listProcessResult
     * @param delayResult
     * @param jobExecution
     * @param searchActBuilder
     */
    private void purgeArchivesWithDelay(ListProcessesResult listProcessResult, DelayResult delayResult, MilkJobExecution jobExecution, SearchOptionsBuilder searchActBuilder, MilkJobOutput milkJobOutput) {
        List<Long> sourceProcessInstanceIds = new ArrayList<>();
        DeletionPerProcess deletionPerProcess = new DeletionPerProcess();
        StringBuilder buildTheSQLRequest=new StringBuilder();
        try {
            // ---------------------  Deployment
            buildTheSQLRequest.append("select * from ARCH_PROCESS_INSTANCE where ");
            if (! listProcessResult.listProcessDeploymentInfo.isEmpty()) {
                buildTheSQLRequest.append(" (");
                for (int i=0;i<listProcessResult.listProcessDeploymentInfo.size();i++) {
                    if (i>0)
                        buildTheSQLRequest.append(" or ");
                    buildTheSQLRequest.append( "PROCESSDEFINITIONID = "+listProcessResult.listProcessDeploymentInfo.get(i).getProcessId());
                }
                buildTheSQLRequest.append(" ) and ");
            }

            
            
            long timeSearch = delayResult.delayDate.getTime();
            SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance;

            ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();

            Chronometer startSearch = milkJobOutput.beginChronometer("SearchArchivedCase");
            // Now, search items to delete
            searchActBuilder.lessOrEquals(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, timeSearch);
            searchActBuilder.sort(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, Order.ASC);
            buildTheSQLRequest.append(" ARCHIVEDATE <= "+timeSearch );
            buildTheSQLRequest.append(" and TENANTID="+jobExecution.getTenantId());
            buildTheSQLRequest.append(" and STATEID=6"); // only archive case
            buildTheSQLRequest.append(" and ROOTPROCESSINSTANCEID = SOURCEOBJECTID"); // only root case

            searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());
            if (searchArchivedProcessInstance.getCount() == 0) {
                milkJobOutput.addReportInHtml(buildTheSQLRequest.toString());

                milkJobOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
                milkJobOutput.nbItemsProcessed = 0;
                return;
            }
            milkJobOutput.endChronometer(startSearch);
            // do the purge now
            
            Chronometer startMarker = milkJobOutput.beginChronometer("Purge");
            buildTheSQLRequest.append(" nbResult="+searchArchivedProcessInstance.getCount());
            
            for (ArchivedProcessInstance archivedProcessInstance : searchArchivedProcessInstance.getResult()) {
                if (jobExecution.pleaseStop())
                    break;
                // proceed page per page
                sourceProcessInstanceIds.add(archivedProcessInstance.getSourceObjectId());

                DeletionItemProcess deletionItemprocess = deletionPerProcess.getDeletionProcessFromProcessId(archivedProcessInstance.getProcessDefinitionId(), processAPI);
                deletionItemprocess.nbCaseDeleted++;

                if (sourceProcessInstanceIds.size() == 50) {
                    Chronometer startDeletion = milkJobOutput.beginChronometer("Deletion");

                    processAPI.deleteArchivedProcessInstancesInAllStates(sourceProcessInstanceIds);
                    milkJobOutput.nbItemsProcessed += sourceProcessInstanceIds.size();
                    sourceProcessInstanceIds.clear();
                    milkJobOutput.endChronometer(startDeletion);

                }
            }
            // reliquat
            if (!sourceProcessInstanceIds.isEmpty()) {
                Chronometer startDeletion = milkJobOutput.beginChronometer("Deletion");
                processAPI.deleteArchivedProcessInstancesInAllStates(sourceProcessInstanceIds);
                milkJobOutput.endChronometer(startDeletion);

                milkJobOutput.nbItemsProcessed += sourceProcessInstanceIds.size();
            }
            milkJobOutput.endChronometer( startMarker );

            milkJobOutput.addReportInHtml(buildTheSQLRequest.toString());

            milkJobOutput.addEvent(new BEvent(eventDeletionSuccess, "Purge:" + milkJobOutput.nbItemsProcessed + " in " + TypesCast.getHumanDuration(startMarker.getTimeExecution(), true)));
            
            milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;

        } catch (SearchException e1) {
            milkJobOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
        } catch (DeletionException e) {
            logger.severe(LOGGER_LABEL + ".purgeArchivesWithDelay: Error Delete Archived ProcessInstance=[" + sourceProcessInstanceIds + "] Error[" + e.getMessage() + "]");
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + sourceProcessInstanceIds));
        }
        deletionPerProcess.addInReport(milkJobOutput);
        milkJobOutput.setMesure(cstMesureCasePurged, milkJobOutput.nbItemsProcessed);

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
            boolean reportSomething = false;

            for (DeletionItemProcess deletionItemProcess : mapDeletionProcess.values()) {
                if (deletionItemProcess.nbCaseDeleted > 0) {
                    if (deletionItemProcess.processDeploymentInfo != null) {
                        milkJobOutput.addReportLine(new Object[] { deletionItemProcess.processDeploymentInfo.getName(), deletionItemProcess.processDeploymentInfo.getVersion(), deletionItemProcess.nbCaseDeleted });
                        reportSomething = true;
                    } else if (deletionItemProcess.processDefinition != null) {
                        milkJobOutput.addReportLine(new Object[] { deletionItemProcess.processDefinition.getName(), deletionItemProcess.processDefinition.getVersion(), deletionItemProcess.nbCaseDeleted });
                        reportSomething = true;
                    }
                }
            }
            if (!reportSomething)
                milkJobOutput.addReportLine(new Object[] { "No deletion performed", "", "" });
            milkJobOutput.addReportEndTable();
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
            StringBuilder buildTheSQLRequest = new StringBuilder();
            buildTheSQLRequest.append("select * from ARCH_PROCESS_INSTANCE where ");
            if (! listProcessResult.listProcessDeploymentInfo.isEmpty()) {
                buildTheSQLRequest.append(" (");
                for (int i=0;i<listProcessResult.listProcessDeploymentInfo.size();i++) {
                    if (i>0)
                        buildTheSQLRequest.append(" or ");
                    buildTheSQLRequest.append( "PROCESSDEFINITIONID = "+listProcessResult.listProcessDeploymentInfo.get(i).getProcessId());
                }
                buildTheSQLRequest.append(" ) and ");
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
            
            buildTheSQLRequest.append(" ARCHIVEDATE <= "+timeSearch );
            buildTheSQLRequest.append(" and TENANTID="+jobExecution.getTenantId());
            buildTheSQLRequest.append(" and STATEID=6"); // only archive case
            buildTheSQLRequest.append(" and ROOTPROCESSINSTANCEID = SOURCEOBJECTID"); // only root case

    
            
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

            csvOperationOuput.closeAndWriteToParameter(milkJobOutput, cstParamListOfCasesDocument);

            milkJobOutput.addReportInHtml(buildTheSQLRequest.toString());

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

        Chronometer purgeMarker = milkJobOutput.beginChronometer("PurgeFromList");

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

            csvOperationInput.loadCsvDocument(jobExecution, cstParamListOfCasesDocument, separatorCSV);
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

            // track time per status
            Chronometer currentMarker = null;
            String currentStatus = null;

            while ((record = csvOperationInput.getNextRecord()) != null) {

                if (jobExecution.pleaseStop()) {
                    csvOperationOuput.writeRecord(record);
                    analysis.append("Stop asked;");
                    break;
                }

                jobExecution.setAvancementStep(lineNumber);
                lineNumber++;

                Long caseId = TypesCast.getLong(record.get(cstColCaseId), null);
                String status = TypesCast.getString(record.get(cstColStatus), "");
                if (currentStatus == null)
                    currentStatus = status;
                if (currentMarker == null)
                    currentMarker = milkJobOutput.beginChronometer("Manipulate " + currentStatus);

                if (caseId == null) {
                    if (analysis.length() < 300)
                        analysis.append("Line[" + lineNumber + "] " + cstColCaseId + " undefined;");
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
                        long nbCaseDeleted = purgeList(sourceProcessInstanceIds, statistic, milkJobOutput, processAPI);
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

                // update the accumulate marker
                if (currentStatus != null && !currentStatus.equals(status)) {
                    // we change the type, so accumulate in the current status
                    milkJobOutput.endChronometer(currentMarker);
                    currentStatus = status;
                    currentMarker = milkJobOutput.beginChronometer("Manipulate " + status);
                }

            } // end loop

            // last accumulation
            if (currentMarker != null)
                milkJobOutput.endChronometer(currentMarker);

            // the end, purge a last time 
            if (!sourceProcessInstanceIds.isEmpty()) {
                currentMarker = milkJobOutput.beginChronometer("Manipulate " + cstColStatus_V_DELETE);

                long nbCaseDeleted = purgeList(sourceProcessInstanceIds, statistic, milkJobOutput, processAPI);
                jobExecution.addManagedItems(nbCaseDeleted);
                sourceProcessInstanceIds.clear();

                milkJobOutput.endChronometer(currentMarker);
            }
            milkJobOutput.nbItemsProcessed = statistic.countNbItems;

            // update the report now
            // We have to rewrite the complete document, with no change
            currentMarker = milkJobOutput.beginChronometer("Manipulate TOANALYSE");
            while ((record = csvOperationInput.getNextRecord()) != null) {
                csvOperationOuput.writeRecord(record);
            }
            milkJobOutput.endChronometer(currentMarker);

            csvOperationOuput.closeAndWriteToParameter(milkJobOutput, cstParamListOfCasesDocument);

            milkJobOutput.endChronometer(purgeMarker);

            // -------------------------------------------------- reorting
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
                milkJobOutput.addReportLine(new Object[] { "StillToAnalyse", statistic.countStillToAnalyse });
                // reportEvent.append("StillToAnalyse:" + statistic.countStillToAnalyse+";");
            }

            if (statistic.countBadDefinition > 0) {
                milkJobOutput.addReportLine(new Object[] { "Bad Definition(noCaseid):", statistic.countBadDefinition });
                milkJobOutput.addReportLine(new Object[] { "Analysis:", analysis.toString() });

                // reportEvent.append("Bad Definition(noCaseid):" + statistic.countBadDefinition + " : " + analysis.toString()+";");
            }

            if (nbAnalyseAlreadyReported > 0) {
                milkJobOutput.addReportLine(new Object[] { "More errors:", nbAnalyseAlreadyReported, "" });
                // reportEvent.append("(" + nbAnalyseAlreadyReported + ") more errors;");
            }

            // add Statistics
            // reportEvent.append("Cases Deleted:" + statistic.countNbItems + " in " + statistic.sumTimeDeleted + " ms ");
            milkJobOutput.addReportLine(new Object[] { "Cases Deleted:", statistic.countNbItems });
            if (statistic.countNbItems > 0) {
                milkJobOutput.addReportLine(new Object[] { "Average/case (ms/case):", ((int) (statistic.sumTimeDeleted / statistic.countNbItems)), "" });
                // reportEvent.append("( " + ((int) (statistic.sumTimeDeleted / statistic.countNbItems)) + " ms/case)");
            }
            milkJobOutput.addReportLine(new Object[] { "SearchCase time:", statistic.sumTimeSearch });
            milkJobOutput.addReportLine(new Object[] { "Manipulate CSV time (ms):", statistic.sumTimeManipulateCsv });
            milkJobOutput.addReportLine(new Object[] { "Total Execution time (ms):", purgeMarker.getTimeExecution() });

            // reportEvent.append("SearchCase time:" + statistic.sumTimeSearch + " ms;");
            // reportEvent.append("Manipulate CSV time:" + statistic.sumTimeManipulateCsv + " ms;");
            // reportEvent.append("Total Execution time:" + (endExecution - beginExecution) + " ms;");

            if (jobExecution.pleaseStop())
                reportEvent.append("Stop asked;");
            if (statistic.countNbItems >= statistic.pleaseStopAfterManagedItems)
                reportEvent.append("Reach the NumberOfItem;");

            milkJobOutput.addReportEndTable();

            milkJobOutput.addChronometersInReport(false, false);

            milkJobOutput.setMesure(cstMesureCasePurged, statistic.countNbItems);

            BEvent eventFinal = (statistic.countBadDefinition == 0) ? new BEvent(eventDeletionSuccess, reportEvent.toString()) : new BEvent(eventReportError, reportEvent.toString());

            milkJobOutput.addEvent(eventFinal);

            milkJobOutput.executionStatus = (jobExecution.pleaseStop() || statistic.countStillToAnalyse > 0) ? ExecutionStatus.SUCCESSPARTIAL : ExecutionStatus.SUCCESS;
            if (statistic.countBadDefinition > 0) {
                milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            }

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + ".fromList: Error Delete Archived ProcessInstance=[" + sourceProcessInstanceIds + "] Error[" + e.getMessage() + "] at " + exceptionDetails);
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

    public int purgeList(List<Long> sourceProcessInstanceIds, Statistic statistic, MilkJobOutput milkJobOutput, ProcessAPI processAPI) throws DeletionException, SearchException {
        Chronometer searchArchivedMarker = milkJobOutput.beginChronometer("searchArchived");
        long startTimeSearch = System.currentTimeMillis();
        SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, sourceProcessInstanceIds.size());
        for (int i = 0; i < sourceProcessInstanceIds.size(); i++) {
            if (i > 0)
                searchActBuilder.or();
            searchActBuilder.filter(ArchivedProcessInstancesSearchDescriptor.SOURCE_OBJECT_ID, sourceProcessInstanceIds.get(i));
        }
        SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());
        statistic.sumTimeSearch += milkJobOutput.endChronometer(searchArchivedMarker);

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

        Chronometer deleteMarker = milkJobOutput.beginChronometer("deleteArchived");
        long nbCaseDeleted = 0;
        if (!listRealId.isEmpty()) {
            // we can't trust this information
            nbCaseDeleted += processAPI.deleteArchivedProcessInstancesInAllStates(listRealId);
            statistic.nbCasesDeleted += listRealId.size();
        }
        milkJobOutput.endChronometer(deleteMarker);

        logger.info(LOGGER_LABEL + ".purgeList: search in " + searchArchivedMarker.getTimeExecution() + " ms , delete in " + deleteMarker.getTimeExecution() + " ms for " + listRealId.size() + " nbCaseDeleted=" + nbCaseDeleted);
        logger.fine(LOGGER_LABEL + ".purgeList: InternalCaseDeletion=" + statistic.nbCasesDeleted);
        statistic.countNbItems += listRealId.size();
        statistic.sumTimeDeleted += deleteMarker.getTimeExecution();
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
