package org.bonitasoft.truckmilk.plugin;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ActivationState;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessActivationException;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfoSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
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
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter.FilterProcess;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.ListProcessesResult;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.CSVOperation;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

import com.bonitasoft.engine.bpm.process.impl.ProcessInstanceSearchDescriptor;

public class MilkDeleteProcesses extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkDeleteProcesses.class.getName());
    private final static String LOGGER_LABEL = "MilkDeleteProcesses ";

    private final static String CSTCOL_PROCESSNAME = "Process";
    private final static String CSTCOL_PROCESSVERSION = "Version";
    private final static String CSTCOL_PROCESSRANGEINTHEVERSION = "Range Version";
    private final static String CSTCOL_PROCESSID = "PID";
    private final static String CSTCOL_ACTIVECASE = "Cases Active";
    private final static String CSTCOL_ARCHIVECASE = "Cases Archived";
    private final static String CSTCOL_STATUS = "Status";
    private final static String CSTCOL_STATUSLEVEL = "StatusLevel";

    private final static BEvent eventSearchFailed = new BEvent(MilkDeleteProcesses.class.getName(), 1, Level.ERROR,
            "Search failed", "Search failed task return an error", "No retry can be performed", "Check the error");

    private final static BEvent eventDisableFailed = new BEvent(MilkDeleteProcesses.class.getName(), 2, Level.ERROR,
            "Process can't be disabled", "A proceaa can't be disabled", "The disabled failed. Process can't be deleted", "Check the error, try to disabled the process");

    private final static BEvent eventDeletionFailed = new BEvent(MilkDeleteProcesses.class.getName(), 3, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion of process", "Process are not deleted", "That's possible when the process contains sub case.Then, the root case must be deleted too");

    private final static BEvent eventDeletionSuccess = new BEvent(MilkDeleteProcesses.class.getName(), 4, Level.SUCCESS,
            "Deletion done with success", "Archived Cases are deleted with success");

    private final static BEvent eventDeletionCaseFailed = new BEvent(MilkDeleteProcesses.class.getName(), 5, Level.ERROR,
            "Error during Case deletion", "An error arrived during the deletion of cases", "Cases are not deleted", "Check why cases can't be deleted");

    private final static BEvent eventDeletionArchivedCaseFailed = new BEvent(MilkDeleteProcesses.class.getName(), 6, Level.ERROR,
            "Error during Archive Case deletion", "An error arrived during the deletion of archive cases", "Cases are not deleted", "Check why cases can't be deleted");

    private final static String CSTCASE_NOPURGE = "No Purge";
    private final static String CSTCASE_PURGEARCHIVED = "Purge Archived Cases";
    private final static String CSTCASE_PURGEALL = "Purge Active/Archived Cases";

    private final static String CSTVERSION_ONLYDISABLED = "Only Disabled";
    private final static String CSTVERSION_DISABLEANDENABLE = "Disabled and Enabled";

    // private final static String CSTROOTCASE_PURGEARCHIVED = "Purge Archived";
    // private final static String CSTROOTCASE_PURGEACTIVE = "Purge Archived and Active";

    private final static String CSTOPERATION_DETECTION = "Only detection";
    private final static String CSTOPERATION_DELETION = "Deletion";

    private static PlugInParameter cstParamOperation = PlugInParameter.createInstanceListValues("operation", "Operation",
            new String[] { CSTOPERATION_DETECTION, CSTOPERATION_DELETION },
            CSTOPERATION_DETECTION,
            "Detect or delete processes "
                    + CSTOPERATION_DETECTION + ": Only detect. "
                    + CSTOPERATION_DELETION + ": Purge records.");

    private static PlugInParameter cstParamScopeProcessFilter = PlugInParameter.createInstance("processfilter", "Process in the perimeter", TypeParameter.ARRAYPROCESSNAME, null, "Job manage only process which mach the filter. If no filter is given, all processes are inspected")
            .withMandatory(false)
            .withFilterProcess(FilterProcess.ALL);
    private static PlugInParameter cstParamExclusionProcessFilter = PlugInParameter.createInstance("exclusionProcessfilter", "Exclusion list", TypeParameter.ARRAYPROCESSNAME, null, "This processes are not in the scope of the job")
            .withMandatory(false)
            .withFilterProcess(FilterProcess.ALL);
    private static PlugInParameter cstParamSeparatorCSV = PlugInParameter.createInstance("separatorCSV", "Separator CSV", TypeParameter.STRING, ",", "CSV use a separator. May be ; or ,");
    private static PlugInParameter cstParamListOfProcessesDocument = PlugInParameter.createInstanceFile("report", "List of Processes", TypeParameter.FILEWRITE, null, "List of processes is is calculated and saved in this parameter", "ListOfProcessesToDelete.csv", "application/CSV");

    //---------------------- process disabled rule
    private static PlugInParameter cstParamDisablead = PlugInParameter.createInstance("processdiseabled", "Disabled processes", TypeParameter.BOOLEAN, Boolean.FALSE, "Delete all Diseable processes");

    private static PlugInParameter cstParamDisabledPurgeCasePolicy = PlugInParameter.createInstanceListValues("disabledPurge", "Purge Cases",
            new String[] { CSTCASE_NOPURGE, CSTCASE_PURGEARCHIVED, CSTCASE_PURGEALL }, CSTCASE_NOPURGE,
            "Purge cases policy. If the process is called as a sub process, sub case is not purged")
            .withVisibleConditionParameterValueEqual(cstParamDisablead, true);
    private static PlugInParameter cstParamDisableDelay = PlugInParameter.createInstanceDelay("DisabledDelay", "No activity in this period", DELAYSCOPE.MONTH, 3, "Process is deleted only if there is not operation in this delay (based on the case STARTED or Last Update Modification - an task executed after is not take into account)")
            .withVisibleConditionParameterValueEqual(cstParamDisablead, true);
    private static PlugInParameter cstParamDisabledKeepLastVersion = PlugInParameter.createInstance("DisableKeepLastVersion", "Keep Last Version of the process", TypeParameter.BOOLEAN, Boolean.FALSE, "Keep the last version of the process")
            .withVisibleConditionParameterValueEqual(cstParamDisablead, true);

    //-------------------- not used process
    private static PlugInParameter cstParamNotUsed = PlugInParameter.createInstance("processNotUsed", "Process not used", TypeParameter.BOOLEAN, Boolean.FALSE, "Delete all Process not used in the delay (No Active/Archived case STARTED in the period)");
    private static PlugInParameter cstParamNotUsedDelay = PlugInParameter.createInstanceDelay("processNotUsedDelay", "No activity in this period", DELAYSCOPE.MONTH, 3, "Process is deleted only if there is not operation in this delay (based on the case STARTED or Last Update Modification - an task executed after is not take into account)")
            .withVisibleConditionParameterValueEqual(cstParamNotUsed, true);
    private static PlugInParameter cstParamNotUsedPurgeCasePolicy = PlugInParameter.createInstanceListValues("disabledPurge", "Purge Cases",
            new String[] { CSTCASE_NOPURGE, CSTCASE_PURGEARCHIVED, CSTCASE_PURGEALL }, CSTCASE_NOPURGE,
            "Purge cases policy. If the process is called as a sub process, sub case is not purged")
            .withVisibleConditionParameterValueEqual(cstParamNotUsed, true);
    private static PlugInParameter cstParamNotUsedKeepLastVersion = PlugInParameter.createInstance("processKeepLastVersion", "Keep Last Version of the process", TypeParameter.BOOLEAN, Boolean.FALSE, "Keep the last version of the process")
            .withVisibleConditionParameterValueEqual(cstParamNotUsed, true);

    // ------------------ Max version
    private static PlugInParameter cstParamMaxVersion = PlugInParameter.createInstance("processMaxVersion", "Max version", TypeParameter.BOOLEAN, Boolean.FALSE, "Keep a number of version for a process. Older version than the max are deleted.");
    private static PlugInParameter cstParamMaxVersionDisableNumber = PlugInParameter.createInstance("DisableMaxVersion", "Disable after version number", TypeParameter.LONG, Long.valueOf(3), "Keep a number of version for a process. Older version than the max are disabled. Example, set to 3, you keep only the 3 first enable, process, after it will be disabled");

    private static PlugInParameter cstParamMaxVersionDeleteNumber = PlugInParameter.createInstance("MaxVersion", "Delete after version number", TypeParameter.LONG, Long.valueOf(5), "All processes under this number for a process are deleted (based on the deployment date). Example, set to 5, you keep only the 5 first process, after it will be deleted")
            .withVisibleConditionParameterValueEqual(cstParamMaxVersion, true);

    private static PlugInParameter cstParamMaxVersionPolicy = PlugInParameter.createInstanceListValues("VersionScope", "Scope",
            new String[] { CSTVERSION_ONLYDISABLED, CSTVERSION_DISABLEANDENABLE }, CSTVERSION_ONLYDISABLED,
            "Delete process policy. With a ONLYDISABLED process, before deleting a process, check if Only Disabled process can be deleted, or Enable process as well")
            .withVisibleConditionParameterValueEqual(cstParamMaxVersion, true);
    private static PlugInParameter cstParamMaxVersionPurgeCasePolicy = PlugInParameter.createInstanceListValues("VersionPurgeCases", "Purge Cases",
            new String[] { CSTCASE_NOPURGE, CSTCASE_PURGEARCHIVED, CSTCASE_PURGEALL }, CSTCASE_NOPURGE,
            "Purge case policy. If the process contains cases, it will be purged according the policy")
            .withVisibleConditionParameterValueEqual(cstParamMaxVersion, true);

    private static PlugInParameter cstParamRootCase = PlugInParameter.createInstance("subProcess", "Sub Process", TypeParameter.BOOLEAN, null, "A process can't be purge if it is called as a sub process. You allowed the tool to purge the Root cases");

    private static final String CST_NUMBER_OF_PROCESS = "Number of process: ";
    private static final String CST_ERROR = "] Error[";

    /*
     * private static PlugInParameter cstParamRootCasePolicy = PlugInParameter.createInstanceListValues("subProcessRootPolicy", "Purge the Root case",
     * new String[] { CSTROOTCASE_PURGEARCHIVED, CSTROOTCASE_PURGEACTIVE }, CSTROOTCASE_PURGEARCHIVED,
     * "Purge Root cases policy. Purge the root case only if it is archived, or Active and Archived cases")
     * .withVisibleConditionParameterValueEqual(cstParamRootCase, true);
     */
    private static PlugInParameter cstParamRootDelay = PlugInParameter.createInstanceDelay("SubProcessDelay", "Delay", DELAYSCOPE.MONTH, 3, "Root Case created more than this delay can be purged")
            .withVisibleConditionParameterValueEqual(cstParamRootCase, true);

    public MilkDeleteProcesses() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the deleteCase, nothing is required
     */
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    @Override
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        // is the command Exist ? 
        return new ArrayList<>();
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName("DeleteProcesses");
        plugInDescription.setLabel("Delete Processes (ENABLED or DISABLED)");
        plugInDescription.setExplanation("Delete processes. If process has Active or Archived cases, they may be purged too");
        plugInDescription.setWarning("This plugin delete ENABLED and DISABLED processes, ACTIVES and ARCHIVED cases. A process, a case deleted can't be retrieved. Operation is final. Use with caution.");

        plugInDescription.setCategory(CATEGORY.PROCESSES);
        plugInDescription.setStopJob(JOBSTOPPER.BOTH);

        plugInDescription.addParameterSeparator("Scope", "Scope of deletion");
        plugInDescription.addParameter(cstParamOperation);
        plugInDescription.addParameter(cstParamScopeProcessFilter);
        plugInDescription.addParameter(cstParamExclusionProcessFilter);
        plugInDescription.addParameter(cstParamSeparatorCSV);
        plugInDescription.addParameter(cstParamListOfProcessesDocument);

        /*
         * plugInDescription.addParameterSeparator("Root Case Policy",
         * "A process can be deleted only if no sub cases exists. Specify the policity for the root case");
         * plugInDescription.addParameter(cstParamRootCase);
         * plugInDescription.addParameter(cstParamRootCasePolicy);
         * plugInDescription.addParameter(cstParamRootDelay);
         */
        plugInDescription.addParameterSeparator("Disabled", "Deletion based on processes DISABLED");
        plugInDescription.addParameter(cstParamDisablead);
        plugInDescription.addParameter(cstParamDisabledPurgeCasePolicy);
        plugInDescription.addParameter(cstParamDisableDelay);
        plugInDescription.addParameter(cstParamDisabledKeepLastVersion);

        plugInDescription.addParameterSeparator("Unused", "Deletion based on processes not used");
        plugInDescription.addParameter(cstParamNotUsed);
        plugInDescription.addParameter(cstParamNotUsedPurgeCasePolicy);
        plugInDescription.addParameter(cstParamNotUsedDelay);
        plugInDescription.addParameter(cstParamNotUsedKeepLastVersion);

        plugInDescription.addParameterSeparator("Maximum Version", "Deletion based on process version");
        plugInDescription.addParameter(cstParamMaxVersion);
        plugInDescription.addParameter(cstParamMaxVersionDisableNumber);
        plugInDescription.addParameter(cstParamMaxVersionDeleteNumber);
        plugInDescription.addParameter(cstParamMaxVersionPurgeCasePolicy);
        plugInDescription.addParameter(cstParamMaxVersionPolicy);

        return plugInDescription;
    }

    @Override
    public int getDefaultNbSavedExecution() {
        return 7;
    }

    @Override
    public int getDefaultNbHistoryMesures() {
        return 14;
    }

    private class StatsResult {

        /**
         * this a a CASE, not a process Instance.
         * on a sub process, there is ONE case, MULTIPLE process instance
         */
        public long countCasesPurged = 0;
        public long countArchiveCasesPurged = 0;
        public long countActiveRootCasesPurged = 0;
        public long countArchivedRootCasesPurged = 0;
        public int processesPurged = 0;

        boolean existParentCase = false;

        // in a detection, inScope return if the process is in the scope to be delete, or not.
        // Example, purge Disable process, no case purge : if the process still have case, it is out of the scope
        boolean inScope = true;
        public String status;
        public STATUSLEVEL statusLevel;

        /**
         * This is process Instance : maybe a sub process process instance
         */
        public long processesWithActiveInstance = 0;
        public long processesWithNoActiveInstance = 0;

        public void add(StatsResult stats) {
            countCasesPurged += stats.countCasesPurged;
            countArchiveCasesPurged += stats.countArchiveCasesPurged;
            countActiveRootCasesPurged += stats.countActiveRootCasesPurged;
            countArchivedRootCasesPurged += stats.countArchivedRootCasesPurged;

            processesPurged += stats.processesPurged;
        }
    }

    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput milkJobOutput = jobExecution.getMilkJobOutput();
        logger.info(LOGGER_LABEL + " Start to MilkDeleteProcess ");

        //preparation
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        StatsResult statsResult = new StatsResult();

        // StringBuilder finalReport = new StringBuilder();
        List<Object[]>  listFinalReport = new ArrayList<>();
        CSVOperation csvOperationOuput = new CSVOperation();
        // collect the number of Active and Disable process at beginning
        long nbEnableProcessBefore = countNumberOfProcesses(Boolean.TRUE, processAPI);
        long nbDisabledProcessBefore = countNumberOfProcesses(Boolean.FALSE, processAPI);

        String operation = jobExecution.getInputStringParameter(cstParamOperation);
        boolean doTheDeletion = CSTOPERATION_DELETION.equals(operation);

        try {
            Boolean disabled = jobExecution.getInputBooleanParameter(cstParamDisablead);
            Boolean notUsed = jobExecution.getInputBooleanParameter(cstParamNotUsed);
            Boolean maxVersion = jobExecution.getInputBooleanParameter(cstParamMaxVersion);

            String separatorCSV = jobExecution.getInputStringParameter(cstParamSeparatorCSV);

            csvOperationOuput.writeCsvDocument(new String[] { CSTCOL_PROCESSNAME, CSTCOL_PROCESSVERSION, CSTCOL_PROCESSID, CSTCOL_PROCESSRANGEINTHEVERSION, CSTCOL_ACTIVECASE, CSTCOL_ARCHIVECASE, CSTCOL_STATUS, CSTCOL_STATUSLEVEL }, separatorCSV);

            List<ProcessDeploymentInfo> listProcessPerimeters;
            SearchOptionsBuilder searchBuilderCase = new SearchOptionsBuilder(0, jobExecution.getJobStopAfterMaxItems() + 1);
            ListProcessesResult listProcess = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamScopeProcessFilter, false, searchBuilderCase, ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);
            if (BEventFactory.isError(listProcess.listEvents)) {
                milkJobOutput.addEvents(listProcess.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return milkJobOutput;
            }
            if (listProcess.listProcessDeploymentInfo.isEmpty()) {
                // search get ALL process
                // Attention, according the policy, we should search only some kind of process
                searchBuilderCase = new SearchOptionsBuilder(0, jobExecution.isLimitationOnMaxItem() ? jobExecution.getJobStopAfterMaxItems() + 1 : 50000);
                if (!(Boolean.TRUE.equals(notUsed) || Boolean.TRUE.equals(maxVersion)))
                    searchBuilderCase.filter(ProcessDeploymentInfoSearchDescriptor.ACTIVATION_STATE, ActivationState.DISABLED.toString());
                searchBuilderCase.sort(ProcessDeploymentInfoSearchDescriptor.NAME, Order.ASC);
                searchBuilderCase.sort(ProcessDeploymentInfoSearchDescriptor.DEPLOYMENT_DATE, Order.ASC);

                SearchResult<ProcessDeploymentInfo> searchResultCurrentProcess = processAPI.searchProcessDeploymentInfos(searchBuilderCase.done());
                listProcessPerimeters = searchResultCurrentProcess.getResult();
            } else
                listProcessPerimeters = listProcess.listProcessDeploymentInfo;

            ListProcessesResult listProcessExclude = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamExclusionProcessFilter, false, searchBuilderCase, ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);
            Set<Long> setExclude = new HashSet<>();
            for (ProcessDeploymentInfo processDeploymentInfo : listProcessExclude.listProcessDeploymentInfo) {
                setExclude.add(processDeploymentInfo.getProcessId());
            }

            // --------------------------  Options
            if (Boolean.TRUE.equals(disabled)) {
                StatsResult statsResultOp = deleteBasedOnDisabled(jobExecution, doTheDeletion, listProcessPerimeters, setExclude, csvOperationOuput, milkJobOutput);
                listFinalReport.add( new Object[] { "<div class='label label-info'>Criteria Delete Disabled</div> "+(doTheDeletion ? " deleted" : " detected"), statsResultOp.processesPurged });
                listFinalReport.add( new Object[] { "<li>Process with    active Process Instance :", statsResultOp.processesWithActiveInstance });
                listFinalReport.add( new Object[] { "<li>Process with No active Process Instance :", statsResultOp.processesWithNoActiveInstance });

                statsResult.add(statsResultOp);
            }

            if (Boolean.TRUE.equals(notUsed)) {
                StatsResult statsResultOp = deleteBasedOnNotUsed(jobExecution, doTheDeletion, listProcessPerimeters, setExclude, csvOperationOuput, milkJobOutput);
                listFinalReport.add( new Object[] { "<div class='label label-info'>Criteria Delete Not Used</div> "+ (doTheDeletion ? "deleted" : " detected"), statsResultOp.processesPurged  });
                listFinalReport.add( new Object[] { "<li>Process with    active Process Instance :", statsResultOp.processesWithActiveInstance });
                listFinalReport.add( new Object[] { "<li>Process with No active Process Instance :", statsResultOp.processesWithNoActiveInstance });
                statsResult.add(statsResultOp);
            }

            if (Boolean.TRUE.equals(maxVersion)) {
                StatsResult statsResultOp = deleteBasedOnMaxVersion(jobExecution, doTheDeletion, listProcessPerimeters, setExclude, csvOperationOuput, milkJobOutput);
                listFinalReport.add( new Object[] { "<div class='label label-info'>Criteria Delete Max Version</div> "+(doTheDeletion ? "deleted" : " detected"), statsResultOp.processesPurged  });
                listFinalReport.add( new Object[] { "<li>Process with    active Process Instance :", statsResultOp.processesWithActiveInstance });
                listFinalReport.add( new Object[] { "<li>Process with No active Process Instance :", statsResultOp.processesWithNoActiveInstance });
                statsResult.add(statsResultOp);
            }
            csvOperationOuput.closeAndWriteToParameter(milkJobOutput, cstParamListOfProcessesDocument);

        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventSearchFailed, e, ""));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + " Error[" + e.getMessage() + "] at " + exceptionDetails);
            return milkJobOutput;
        }

        milkJobOutput.addReportInHtml("<h3>Synthesis</h3>");

        milkJobOutput.addReportTableBegin(new String[] { "", "" });
        for (Object[] line : listFinalReport)
            milkJobOutput.addReportTableLine( line );
        listFinalReport.add( new Object[] { "<div class='label label-info'>Conclusion</div> :", ""});

        if (doTheDeletion) {
            long nbEnableProcessAfter = countNumberOfProcesses(Boolean.TRUE, processAPI);
            long nbDisabledProcessAfter = countNumberOfProcesses(Boolean.FALSE, processAPI);
            milkJobOutput.addReportTableLine(new Object[] { "Total Number of Enable  processes Before", nbEnableProcessBefore });
            milkJobOutput.addReportTableLine(new Object[] { "Total Number of Disable processes Before:", nbDisabledProcessBefore });
            milkJobOutput.addReportTableLine(new Object[] { "Number of Active   cases deleted:", statsResult.countCasesPurged });
            milkJobOutput.addReportTableLine(new Object[] { "Number of Archived cases deleted ", statsResult.countArchiveCasesPurged });
            milkJobOutput.addReportTableLine(new Object[] { "Number of Active   Root cases deleted ", statsResult.countActiveRootCasesPurged });
            milkJobOutput.addReportTableLine(new Object[] { "Number of Archived Root cases deleted ", statsResult.countArchivedRootCasesPurged });
            milkJobOutput.addReportTableLine(new Object[] { "Number of Process deleted ", statsResult.processesPurged });
            milkJobOutput.addReportTableLine(new Object[] { "Total Number of Enable  processes After ", nbEnableProcessAfter });
            milkJobOutput.addReportTableLine(new Object[] { "Total Number of Disable processes After", nbDisabledProcessAfter });

        } else {

            milkJobOutput.addReportTableLine(new Object[] { "Total Number of Enable  processes ", nbEnableProcessBefore });
            milkJobOutput.addReportTableLine(new Object[] { "Total Number of Disable processes ", nbDisabledProcessBefore });
            milkJobOutput.addReportTableLine(new Object[] { "Number of Processes detected to be deleted ", statsResult.processesPurged });
        }
        milkJobOutput.addReportTableEnd();

        milkJobOutput.addChronometersInReport(false, true);

        return milkJobOutput;
    }

    /**
     * @param jobExecution
     * @param milkJobOutput
     */
    private StatsResult deleteBasedOnDisabled(MilkJobExecution jobExecution, boolean doTheDeletion, List<ProcessDeploymentInfo> listProcess, Set<Long> setExclude, CSVOperation csvOperation, MilkJobOutput milkJobOutput) {
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        StatsResult statsResult = new StatsResult();
        milkJobOutput.addReportInHtml("<h3>Disabled</h3>");

        writeRecordHeader(csvOperation, "Disabled Processes");

        String policyOnCase = jobExecution.getInputStringParameter(cstParamDisabledPurgeCasePolicy);

        DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamDisableDelay, new Date(), false);
        if (BEventFactory.isError(delayResult.listEvents)) {
            milkJobOutput.addEvents(delayResult.listEvents);
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            return statsResult;
        }
        Boolean keepLastVersion = jobExecution.getInputBooleanParameter(cstParamDisabledKeepLastVersion);

        // root policy
        // Boolean rootCasePurge = jobExecution.getInputBooleanParameter(cstParamRootCase);

        jobExecution.setAvancement(0);
        int count = 0;

        milkJobOutput.addReportTableBegin(new String[] { CSTCOL_PROCESSNAME, CSTCOL_PROCESSVERSION, CSTCOL_PROCESSID, CSTCOL_ACTIVECASE, CSTCOL_ARCHIVECASE, CSTCOL_STATUS }, 20);
        for (ProcessDeploymentInfo processDeploymentInfo : listProcess) {
            if (jobExecution.pleaseStop())
                break;
            jobExecution.setAvancement(0 + (int) (30.0 * count / listProcess.size()));
            count++;
            if (setExclude.contains(processDeploymentInfo.getProcessId()))
                continue;
            if (ActivationState.ENABLED.equals(processDeploymentInfo.getActivationState()))
                continue;

            if (Boolean.TRUE.equals(keepLastVersion) && isLastVersionProcess(processDeploymentInfo, processAPI)) {
                continue;
            }

            if (delayResult.delayInMs < 0) {
                Long lastActivity = getLastTimeActivityOnProcess(processDeploymentInfo, milkJobOutput, processAPI);
                if (lastActivity != null && lastActivity > delayResult.delayDate.getTime())
                    continue;

            }
            // Ok, this process is maybe in the perimeters
            StatsResult statsResultProcess = deleteProcessAndCases(processDeploymentInfo, null,
                    doTheDeletion,
                    policyOnCase,
                    processAPI,
                    csvOperation,
                    jobExecution, milkJobOutput);
            if (!statsResultProcess.inScope)
                continue;

            statsResult.add(statsResultProcess);
            if (STATUSLEVEL.SUCCESS.equals(statsResult.statusLevel)) {
                milkJobOutput.nbItemsProcessed++;
            }

        } // end loop
        milkJobOutput.addReportTableEnd();
        milkJobOutput.addReportInHtml(CST_NUMBER_OF_PROCESS + statsResult.processesPurged);

        jobExecution.setAvancement(30);
        return statsResult;

    }

    /**
     * Delete process inactif
     * 
     * @param jobExecution
     * @param listProcess
     * @param setExclude
     * @param milkJobOutput
     * @return
     */
    private StatsResult deleteBasedOnNotUsed(MilkJobExecution jobExecution, boolean doTheDeletion, List<ProcessDeploymentInfo> listProcess, Set<Long> setExclude, CSVOperation csvOperation, MilkJobOutput milkJobOutput) {
        jobExecution.setAvancement(30);
        StatsResult statsResult = new StatsResult();
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        milkJobOutput.addReportInHtml("<h3>Not used</h3>");
        writeRecordHeader(csvOperation, "Processes Not used");

        String policyOnCase = jobExecution.getInputStringParameter(cstParamNotUsedPurgeCasePolicy);

        DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamNotUsedDelay, new Date(), false);
        if (BEventFactory.isError(delayResult.listEvents)) {
            milkJobOutput.addEvents(delayResult.listEvents);
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            return statsResult;
        }
        Boolean keepLastVersion = jobExecution.getInputBooleanParameter(cstParamNotUsedKeepLastVersion);

        milkJobOutput.addReportTableBegin(new String[] { CSTCOL_PROCESSNAME, CSTCOL_PROCESSVERSION, CSTCOL_PROCESSID, CSTCOL_ACTIVECASE, CSTCOL_ARCHIVECASE, CSTCOL_STATUS }, 20);

        int count = 0;

        for (ProcessDeploymentInfo processDeploymentInfo : listProcess) {
            if (jobExecution.pleaseStop())
                break;
            jobExecution.setAvancement(60 + (int) (30.0 * count / listProcess.size()));
            
            count++;

            if (Boolean.TRUE.equals(keepLastVersion)) {
                if (isLastVersionProcess(processDeploymentInfo, processAPI))
                    continue;
            }

            if (delayResult.delayInMs < 0) {
                // check the last update date

                //  search the last processInstance / last Archive process instance in this process
                Long lastActivity = getLastTimeActivityOnProcess(processDeploymentInfo, milkJobOutput, processAPI);
                if (lastActivity != null && lastActivity > delayResult.delayDate.getTime())
                    continue;
            }

            StatsResult statsResultProcess = deleteProcessAndCases(processDeploymentInfo, null,
                    doTheDeletion,
                    policyOnCase,
                    processAPI,
                    csvOperation,
                    jobExecution, milkJobOutput);
            if (!statsResultProcess.inScope)
                continue;

            statsResult.add(statsResultProcess);
            if (STATUSLEVEL.SUCCESS.equals(statsResult.statusLevel)) {
                milkJobOutput.nbItemsProcessed++;
            }

        }
        milkJobOutput.addReportTableEnd();
        milkJobOutput.addReportInHtml(CST_NUMBER_OF_PROCESS + statsResult.processesPurged);

        jobExecution.setAvancement(60);
        return statsResult;
    }

    /**
     * @param jobExecution
     * @param listProcess
     * @param setExclude
     * @param milkJobOutput
     * @return
     */
    private StatsResult deleteBasedOnMaxVersion(MilkJobExecution jobExecution, boolean doTheDeletion, List<ProcessDeploymentInfo> listProcess, Set<Long> setExclude, CSVOperation csvOperation, MilkJobOutput milkJobOutput) {
        jobExecution.setAvancement(60);
        StatsResult statsResult = new StatsResult();
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        milkJobOutput.addReportInHtml("<h3>Version</h3>");
        writeRecordHeader(csvOperation, "Too many versions");

        long versionDisableNumber = jobExecution.getInputLongParameter(cstParamMaxVersionDisableNumber);
        long versionDeleteNumber = jobExecution.getInputLongParameter(cstParamMaxVersionDeleteNumber);
        if (versionDisableNumber == 0)
            versionDisableNumber = 100000;
        if (versionDeleteNumber == 0)
            versionDeleteNumber = 100000;
        // normally, disableNumber < deleteNumber

        String versionPolicy = jobExecution.getInputStringParameter(cstParamMaxVersionPolicy);
        String policyOnCase = jobExecution.getInputStringParameter(cstParamMaxVersionPurgeCasePolicy);

        // first pass, let's collect all process name
        List<String> listProcessName = new ArrayList<>();

        for (ProcessDeploymentInfo processDeploymentInfo : listProcess) {
            if (!listProcessName.contains(processDeploymentInfo.getName()))
                listProcessName.add(processDeploymentInfo.getName());
        }

        Collections.sort(listProcessName);

        milkJobOutput.addReportTableBegin(new String[] { CSTCOL_PROCESSNAME, CSTCOL_PROCESSVERSION, CSTCOL_PROCESSID, CSTCOL_PROCESSRANGEINTHEVERSION, CSTCOL_ACTIVECASE, CSTCOL_ARCHIVECASE, CSTCOL_STATUS }, 20);

        // process name per name
        int count = 0;

        for (String processName : listProcessName) {
            if (jobExecution.pleaseStop())
                break;
            jobExecution.setAvancement(60 + (int) (60.0 * count / listProcessName.size()));
            count++;
            SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 100000);
            sob.filter(ProcessDeploymentInfoSearchDescriptor.NAME, processName);
            sob.sort(ProcessDeploymentInfoSearchDescriptor.DEPLOYMENT_DATE, Order.DESC);
            try {
                SearchResult<ProcessDeploymentInfo> search = processAPI.searchProcessDeploymentInfos(sob.done());
                // protection, but anormal : we should find the process as minimum

                // ok, delete all After the first version
                for (long i = 0; i < search.getCount(); i++) {
                    if (i < versionDisableNumber && i < versionDeleteNumber)
                        continue;
                    ProcessDeploymentInfo processDeploymentInfo = search.getResult().get((int) i);

                    // we are here because it trigger the disable OR the delete OR both
                    // disable ==> disable
                    // delete ==> delete
                    // disable AND delete ==> delete
                    if (i >= versionDisableNumber && i < versionDeleteNumber) {
                        // just disable this one
                        StatsResult statsResultProcess = new StatsResult();
                        CountCases countCases = countCasesInProcess(processDeploymentInfo, true, true, false, processAPI);

                        try {
                            processAPI.disableProcess(processDeploymentInfo.getProcessId());
                            statsResult.status = "Deactivation";
                            statsResultProcess.statusLevel = STATUSLEVEL.SUCCESS;

                        } catch (Exception e) {
                            milkJobOutput.addEvent(new BEvent(eventDisableFailed, e, getReportProcessName(processDeploymentInfo) + " Exception " + e.getMessage()));
                            statsResult.status = "Deactivation error";
                            statsResultProcess.statusLevel = STATUSLEVEL.ERROR;
                        }
                        reportLine(processDeploymentInfo, i, countCases.countActives, countCases.countArchived, statsResultProcess.status, statsResultProcess.statusLevel, csvOperation, milkJobOutput);
                        statsResult.add(statsResultProcess);

                        continue;
                    }
                    // disableor delete : delete

                    if (CSTVERSION_ONLYDISABLED.equals(versionPolicy) && ActivationState.ENABLED.equals(processDeploymentInfo.getActivationState())) {
                        // milkJobOutput.addReportInHtml("<tr><td> Version["+i+"] " + getReportProcessName(processDeploymentInfo) + "</td><td> - </td><td> - </td><td>" + getStatusForReport("is ENABLED", STATUSLEVEL.INFO) + "</td></tr>");
                        continue;
                    }

                    StatsResult statsResultProcess = deleteProcessAndCases(processDeploymentInfo, i,
                            doTheDeletion,
                            policyOnCase,
                            processAPI,
                            csvOperation,
                            jobExecution, milkJobOutput);
                    if (!statsResultProcess.inScope)
                        continue;

                    statsResult.add(statsResultProcess);
                    if (STATUSLEVEL.SUCCESS.equals(statsResult.statusLevel)) {
                        milkJobOutput.nbItemsProcessed++;
                    }
                }
            } catch (Exception e) {
                milkJobOutput.addEvent(new BEvent(eventSearchFailed, e, " Exception " + e.getMessage()));
                milkJobOutput.addReportTableLine(new Object[] { processName, " - ", " - ", getStatusForReport("Search failed " + e.getMessage(), STATUSLEVEL.ERROR) });

                milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String exceptionDetails = sw.toString();
                logger.severe(LOGGER_LABEL + " Error[" + e.getMessage() + "] at " + exceptionDetails);
            }

        }
        milkJobOutput.addReportTableEnd();
        milkJobOutput.addReportInHtml(CST_NUMBER_OF_PROCESS + statsResult.processesPurged);

        jobExecution.setAvancement(100);
        return statsResult;
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* process operation */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

    /**
     * A process is ready to be delete.
     * Different parameters
     * 
     * @param processDeploymentInfo
     * @param doTheDeletion if true, the deletion is done, else not
     * @policy : CSTDISABLED_PURGEARCHIVED, CSTDISABLED_PURGEALL
     * @param processAPI
     * @param jobExecution
     * @param milkJobOutput
     * @return
     */

    private StatsResult deleteProcessAndCases(ProcessDeploymentInfo processDeploymentInfo,
            Long rangeInTheVersion,
            boolean doTheDeletion,
            String policyOnCase,
            ProcessAPI processAPI,
            CSVOperation csvOperation,
            MilkJobExecution jobExecution,
            MilkJobOutput milkJobOutput) {
        StatsResult statsResult = new StatsResult();
        
        jobExecution.setAvancementInformation("Study [" + getReportProcessName(processDeploymentInfo) + "]");
        
        // According the PurgeCase Policy, calculate if the process is in the scope or not
        CountCases countCases = countCasesInProcess(processDeploymentInfo, true, true, false, processAPI);

        if (CSTCASE_PURGEARCHIVED.equals(policyOnCase) && countCases.countActives > 0) {
            statsResult.inScope = false;
        }
        if (CSTCASE_NOPURGE.equals(policyOnCase) && countCases.getTotal() > 0) {
            statsResult.inScope = false;
        }

        if (!statsResult.inScope)
            return statsResult;

        if (countCases.countActives > 0)
            statsResult.processesWithActiveInstance++;
        else
            statsResult.processesWithNoActiveInstance++;

        // --------------------- Detection
        if (!doTheDeletion) {
            statsResult.processesPurged++;
            milkJobOutput.nbItemsProcessed++;
            reportLine(processDeploymentInfo, rangeInTheVersion, countCases.countActives, countCases.countArchived, "To be delete", STATUSLEVEL.INFO, csvOperation, milkJobOutput);
            return statsResult;
        }

        // --------------------- Deletion
        boolean parentCaseDetected = false;

        // the purgeCasesPurge is now
        /*
         * if (Boolean.TRUE.equals(rootCasePurge)) {
         * statsResult.add(purgeRootCase(jobExecution, processDeploymentInfo, milkJobOutput));
         * }
         */

        // First, delete active cases
        if (CSTCASE_PURGEALL.equals(policyOnCase)) {
            jobExecution.setAvancementInformation("Purge Active case in [" + getReportProcessName(processDeploymentInfo) + "]");
            StatsResult operationPurgeCase = purgeActiveCases(jobExecution, processDeploymentInfo, milkJobOutput);
            if (operationPurgeCase.existParentCase)
                parentCaseDetected = true;
            statsResult.countCasesPurged += operationPurgeCase.countCasesPurged;
        }
        // Double check : there is still active process in this process ?
        CountCases countCasesVerification = countCasesInProcess(processDeploymentInfo, true, false, false, processAPI);

        // no sense to purge archived if there is still active case
        if (countCasesVerification.countActives > 0 || parentCaseDetected) {
            // we have to report this line. At this point, we should not have any actives cases
            reportLine(processDeploymentInfo, rangeInTheVersion, countCases.countActives, 0, "Still Active cases (Sub process)", STATUSLEVEL.WARNING, csvOperation, milkJobOutput);
            return statsResult;
        }

        // same with  the archive now 
        // test for the form: here, if the policy is "NOPURGE", we should not be here
        if (CSTCASE_PURGEALL.equals(policyOnCase) || (CSTCASE_PURGEARCHIVED.equals(policyOnCase))) {
            jobExecution.setAvancementInformation("Purge Archive case in [" + getReportProcessName(processDeploymentInfo) + "]");
            StatsResult operationPurgeCase = purgeArchivedCases(jobExecution, processDeploymentInfo, milkJobOutput);
            if (operationPurgeCase.existParentCase)
                parentCaseDetected = true;
            statsResult.countArchiveCasesPurged += operationPurgeCase.countCasesPurged;
        }
        countCasesVerification = countCasesInProcess(processDeploymentInfo, true, true, false, processAPI);

        if (countCasesVerification.countArchived > 0 || parentCaseDetected) {
            // we have to report this line. At this point, we should not have any archive cases
            reportLine(processDeploymentInfo, rangeInTheVersion, countCases.countActives, countCases.countArchived, "Still archived case", STATUSLEVEL.WARNING, csvOperation, milkJobOutput);
            return statsResult;
        }

        // ---------------- ready to delete
        try {
            jobExecution.setAvancementInformation("Delete Process in [" + getReportProcessName(processDeploymentInfo) + "]");

            // Now it's possible to purge the process - first disable it
            if (ActivationState.ENABLED.equals(processDeploymentInfo.getActivationState())) {
                processAPI.disableProcess(processDeploymentInfo.getProcessId());
            }
            Chronometer startDeletion = milkJobOutput.beginChronometer("DeletionProcessDefinition");
            processAPI.deleteProcessDefinition(processDeploymentInfo.getProcessId());
            milkJobOutput.endChronometer(startDeletion);
            statsResult.processesPurged++;
            milkJobOutput.nbItemsProcessed++;

            statsResult.status = "Deleted";
            statsResult.statusLevel = STATUSLEVEL.SUCCESS;

            logger.info(LOGGER_LABEL + " Deleted " + getReportProcessName(processDeploymentInfo));
            milkJobOutput.addEvent(new BEvent(eventDeletionSuccess, getReportProcessName(processDeploymentInfo)));
            reportLine(processDeploymentInfo, rangeInTheVersion, countCases.countActives, countCases.countArchived, statsResult.status, statsResult.statusLevel, csvOperation, milkJobOutput);

        } catch (Exception e) {
            if (e instanceof ProcessActivationException) {
                milkJobOutput.addEvent(new BEvent(eventDisableFailed, e, getReportProcessName(processDeploymentInfo) + " Exception " + e.getMessage()));
                statsResult.status = "Deactivation error";
            } else if (e instanceof DeletionException) {
                milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, getReportProcessName(processDeploymentInfo) + " Exception " + e.getMessage()));
                statsResult.status = "Deletion error";
            } else {
                milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, getReportProcessName(processDeploymentInfo) + " Exception " + e.getMessage()));
                statsResult.status = "Error " + e.getMessage();
            }
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + getReportProcessName(processDeploymentInfo) + CST_ERROR + e.getMessage() + "] at " + exceptionDetails);
            statsResult.statusLevel = STATUSLEVEL.ERROR;
            reportLine(processDeploymentInfo, rangeInTheVersion, countCases.countActives, countCases.countArchived, statsResult.status, statsResult.statusLevel, csvOperation, milkJobOutput);

        }

        return statsResult;
    }

    /**
     * keep the last version, for a process name
     */
    public Map<String, String> cacheLastProcessVersion = new HashMap<>();

    /**
     * Is the last version of the process?
     * 
     * @param processDeploymentInfo
     * @param processAPI
     * @return
     */
    public boolean isLastVersionProcess(ProcessDeploymentInfo processDeploymentInfo, ProcessAPI processAPI) {
        if (cacheLastProcessVersion.containsKey(processDeploymentInfo.getName())) {
            return cacheLastProcessVersion.get(processDeploymentInfo.getName()).equals(processDeploymentInfo.getVersion());
        }
        // search the last version
        SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 1);
        sob.filter(ProcessDeploymentInfoSearchDescriptor.NAME, processDeploymentInfo.getName());
        sob.sort(ProcessDeploymentInfoSearchDescriptor.DEPLOYMENT_DATE, Order.DESC);
        try {
            SearchResult<ProcessDeploymentInfo> search = processAPI.searchProcessDeploymentInfos(sob.done());
            // protection, but abnormal : we should find the process as minimum
            if (search.getCount() == 0)
                return true;
            cacheLastProcessVersion.put(search.getResult().get(0).getName(), search.getResult().get(0).getVersion());
            return search.getResult().get(0).getVersion().equals(processDeploymentInfo.getVersion());
        } catch (Exception e) {
            return true;
        }

    }

    /**
     * return null if no activity detected, else the last time
     */
    public Long getLastTimeActivityOnProcess(ProcessDeploymentInfo processDeploymentInfo, MilkJobOutput milkJobOutput, ProcessAPI processAPI) {
        List<Long> listActivityTime = new ArrayList<>();

        if (processDeploymentInfo.getLastUpdateDate() != null)
            listActivityTime.add(processDeploymentInfo.getLastUpdateDate().getTime());

        try {
            SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 1);
            sob.filter(ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
            sob.sort(ProcessInstanceSearchDescriptor.LAST_UPDATE, Order.DESC);
            SearchResult<ProcessInstance> searchProcessInstance = processAPI.searchProcessInstances(sob.done());
            if (searchProcessInstance.getCount() > 0) {
                ProcessInstance processInstance = searchProcessInstance.getResult().get(0);
                Date lastUpdate = processInstance.getLastUpdate();
                if (lastUpdate == null) {
                    milkJobOutput.addReportTableLine(new Object[] { getReportProcessName(processDeploymentInfo), " - ", " - ", getStatusForReport("ArchiveCase[" + processInstance.getId() + "]: No LastUpdateDate, ignored", STATUSLEVEL.WARNING) });

                } else {
                    listActivityTime.add(lastUpdate.getTime());
                }
            }

            sob = new SearchOptionsBuilder(0, 1);
            sob.filter(ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
            sob.sort(ArchivedProcessInstancesSearchDescriptor.LAST_UPDATE, Order.DESC);
            SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(sob.done());
            if (searchArchivedProcessInstance.getCount() > 0) {
                ArchivedProcessInstance processInstance = searchArchivedProcessInstance.getResult().get(0);
                Date lastUpdate = processInstance.getLastUpdate();
                if (lastUpdate == null) {
                    milkJobOutput.addReportTableLine(new Object[] { getReportProcessName(processDeploymentInfo), " - ", " - ", getStatusForReport("ArchiveCase[" + processInstance.getSourceObjectId() + "]: No LastUpdateDate, ignored", STATUSLEVEL.WARNING) });

                } else
                    listActivityTime.add(lastUpdate.getTime());
            }
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + getReportProcessName(processDeploymentInfo) + CST_ERROR + e.getMessage() + "] at " + exceptionDetails);
        }

        // collect the MAX of values
        long maxValue = 0;
        for (long value : listActivityTime)
            maxValue = Math.max(maxValue, value);

        if (maxValue == 0)
            return null;
        return maxValue;
    }

    /**
     * Count the number of processes
     * 
     * @param activationStateActive null : all process, TRUE : ENABLE, FALSE: DISABLED
     * @param processAPI
     * @return
     */
    public long countNumberOfProcesses(Boolean activationStateActive, ProcessAPI processAPI) {
        SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 1);
        if (activationStateActive != null) {
            sob.filter(ProcessDeploymentInfoSearchDescriptor.ACTIVATION_STATE, activationStateActive ? ActivationState.ENABLED.toString() : ActivationState.DISABLED.toString());
        }
        SearchResult<ProcessDeploymentInfo> searchProcess;
        try {
            searchProcess = processAPI.searchProcessDeploymentInfos(sob.done());
            return searchProcess.getCount();
        } catch (SearchException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + " Error countNumberOfProcesses[" + e.getMessage() + "] at " + exceptionDetails);
            return 0;
        }

    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Purge Case function */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

    /** Oracle limit the deletion at 1000 */
    private int cstMaxDeletion = 900;

    /**
     * @param jobExecution
     * @param processDefinitionId
     * @param processAPI
     * @param milkJobOutput
     */
    private StatsResult purgeActiveCases(MilkJobExecution jobExecution, ProcessDeploymentInfo processDeploymentInfo, MilkJobOutput milkJobOutput) {
        StatsResult statsResult = new StatsResult();

        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();

        jobExecution.setAvancementInformation("Purge Active cases in [" + getReportProcessName(processDeploymentInfo) + "]");
        long result = 0;
        try {
            do {
                if (jobExecution.pleaseStop())
                    break;

                SearchOptionsBuilder sob = new SearchOptionsBuilder(0, cstMaxDeletion + 1);
                sob.filter(ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
                Chronometer startSearchDeletion = milkJobOutput.beginChronometer("DeletionSearchArchivedCase");
                SearchResult<ProcessInstance> resultSearch = processAPI.searchProcessInstances(sob.done());
                result = resultSearch.getCount();
                milkJobOutput.endChronometer(startSearchDeletion);

                Chronometer startDeletion = milkJobOutput.beginChronometer("DeletionArchivedCase");
                for (ProcessInstance processInstance : resultSearch.getResult()) {
                    if (processInstance.getRootProcessInstanceId() == processInstance.getId())
                        processAPI.deleteProcessInstance(processInstance.getId());
                    else
                        statsResult.existParentCase = true;
                }
                milkJobOutput.endChronometer(startDeletion, resultSearch.getResult().size());
                statsResult.countCasesPurged += resultSearch.getResult().size();

            } while (result > 0 && result > cstMaxDeletion - 1);
            logger.info(LOGGER_LABEL + " Purge [" + statsResult.countCasesPurged + "] Active case in " + getReportProcessName(processDeploymentInfo));

        } catch (SearchException | DeletionException e) {
            milkJobOutput.addEvent(new BEvent(eventDeletionCaseFailed, e, "Process[" + processDeploymentInfo.getName() + "(" + processDeploymentInfo.getVersion() + ") ProcessId[" + processDeploymentInfo.getProcessId() + "]"));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            // this is possible if the process contains Subcase. So, it's not an SEVERE error because that may appends.
            logger.info(LOGGER_LABEL + getReportProcessName(processDeploymentInfo) + CST_ERROR + e.getMessage() + "] at " + exceptionDetails);
        }
        jobExecution.setAvancementInformation("");
        return statsResult;
    }

    /*
     * 
     */
    private StatsResult purgeArchivedCases(MilkJobExecution jobExecution, ProcessDeploymentInfo processDeploymentInfo, MilkJobOutput milkJobOutput) {
        StatsResult purgeCaseResult = new StatsResult();
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();

        long result = 0;
        jobExecution.setAvancementInformation("Purge Active cases in [" + getReportProcessName(processDeploymentInfo) + "]");
        try {
            do {
                if (jobExecution.pleaseStop())
                    break;
                SearchOptionsBuilder sob = new SearchOptionsBuilder(0, cstMaxDeletion + 1);
                sob.filter(ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
                Chronometer startSearchDeletion = milkJobOutput.beginChronometer("DeletionSearchArchivedCase");
                SearchResult<ArchivedProcessInstance> resultSearch = processAPI.searchArchivedProcessInstances(sob.done());
                result = resultSearch.getCount();
                milkJobOutput.endChronometer(startSearchDeletion);

                Chronometer startDeletion = milkJobOutput.beginChronometer("DeletionArchivedCase");
                List<Long> listArchivedProcessInstance = new ArrayList<>();
                for (ArchivedProcessInstance archivedProcessInstance : resultSearch.getResult()) {
                    if (archivedProcessInstance.getRootProcessInstanceId() == archivedProcessInstance.getSourceObjectId())
                        listArchivedProcessInstance.add(archivedProcessInstance.getSourceObjectId());
                    else
                        purgeCaseResult.existParentCase = true;
                }

                if (!listArchivedProcessInstance.isEmpty())
                    processAPI.deleteArchivedProcessInstancesInAllStates(listArchivedProcessInstance);
                milkJobOutput.endChronometer(startDeletion, listArchivedProcessInstance.size());
                purgeCaseResult.countCasesPurged += listArchivedProcessInstance.size();

                milkJobOutput.nbItemsProcessed += result;
            } while (result > 0 && result > cstMaxDeletion - 1);
            logger.fine(LOGGER_LABEL + " Purge [" + purgeCaseResult.countCasesPurged + "] Archive case in " + getReportProcessName(processDeploymentInfo));
        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventDeletionArchivedCaseFailed, e, "Process[" + processDeploymentInfo.getName() + "(" + processDeploymentInfo.getVersion() + ") ProcessId[" + processDeploymentInfo.getProcessId() + "] " + e.getMessage()));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + getReportProcessName(processDeploymentInfo) + CST_ERROR + e.getMessage() + "] at " + exceptionDetails);
        }
        jobExecution.setAvancementInformation("");
        return purgeCaseResult;

    }

    /**
     * @param jobExecution
     * @param processDeploymentInfo
     * @param milkJobOutput
     * @return
     */
    private StatsResult purgeRootCase(MilkJobExecution jobExecution, ProcessDeploymentInfo processDeploymentInfo, MilkJobOutput milkJobOutput) {

        // ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        StatsResult statsResult = new StatsResult();

        // String policyRoot = jobExecution.getInputStringParameter(cstParamRootCasePolicy);
        DelayResult delayRoot = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamRootDelay, new Date(), false);
        if (BEventFactory.isError(delayRoot.listEvents)) {
            milkJobOutput.addEvents(delayRoot.listEvents);
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            return statsResult;
        }
        // search all root cases in this process
        return statsResult;
    }

    /**
     * @param processDeploymentInfo
     * @param active
     * @param archive
     * @param subCase
     * @param processAPI
     * @return
     */
    private static class CountCases {

        public long countActives;
        public long countArchived;

        public long getTotal() {
            return countActives + countArchived;
        }
    }

    private CountCases countCasesInProcess(ProcessDeploymentInfo processDeploymentInfo, boolean active, boolean archive, boolean subCase, ProcessAPI processAPI) {

        CountCases countCases = new CountCases();
        if (active) {
            try {
                SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 1);
                sob.filter(ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
                SearchResult<ProcessInstance> search = processAPI.searchProcessInstances(sob.done());
                countCases.countActives = search.getCount();
            } catch (Exception e) {
                // No report here, consider the result as 0
            }
        }
        if (archive) {
            try {
                SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 1);
                sob.filter(ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
                SearchResult<ArchivedProcessInstance> search = processAPI.searchArchivedProcessInstances(sob.done());
                countCases.countArchived = search.getCount();
            } catch (Exception e) {
                // No report here, consider the result as 0
            }
        }
        return countCases;

    }

    /**
     * @param processDeploymentInfo
     * @return
     */
    private String getReportProcessName(ProcessDeploymentInfo processDeploymentInfo) {
        return " " + processDeploymentInfo.getName() + "(" + processDeploymentInfo.getVersion() + ") PID[" + processDeploymentInfo.getProcessId() + "]";
    }

    public enum STATUSLEVEL {
        SUCCESS, INFO, WARNING, ERROR
    }

    /**
     * @param status
     * @param level
     * @return
     */
    private String getStatusForReport(String status, STATUSLEVEL level) {
        if (level.equals(STATUSLEVEL.ERROR))
            return "<label class=\"label label-danger\">" + status + "</label>";
        if (level.equals(STATUSLEVEL.WARNING))
            return "<label class=\"label label-warning\">" + status + "</label>";
        if (level.equals(STATUSLEVEL.INFO))
            return "<label class=\"label label-primary\">" + status + "</label>";
        if (level.equals(STATUSLEVEL.SUCCESS))
            return "<label class=\"label label-success\">" + status + "</label>";
        return status;
    }

    /**
     * calculate the list of object to return
     * 
     * @param inHtml
     * @param processDeploymentInfo
     * @param rangeInTheVersion
     * @param activeCases
     * @param archiveCases
     * @param status
     * @param level
     * @return
     */

    private Map<String, String> getRecordForTableReport(ProcessDeploymentInfo processDeploymentInfo, Long rangeInTheVersion, long activeCases, long archiveCases, String status, STATUSLEVEL level) {
        Map<String, String> record = new HashMap<>();
        record.put(CSTCOL_PROCESSNAME, processDeploymentInfo.getName());
        record.put(CSTCOL_PROCESSVERSION, processDeploymentInfo.getVersion());
        record.put(CSTCOL_PROCESSID, String.valueOf(processDeploymentInfo.getProcessId()));
        record.put(CSTCOL_PROCESSRANGEINTHEVERSION, rangeInTheVersion == null ? "" : String.valueOf(rangeInTheVersion));
        record.put(CSTCOL_ACTIVECASE, String.valueOf(activeCases));
        record.put(CSTCOL_ARCHIVECASE, String.valueOf(archiveCases));
        record.put(CSTCOL_STATUS, status);
        record.put(CSTCOL_STATUSLEVEL, level.toString());
        return record;
    }

    /**
     * @param processDeploymentInfo
     * @param rangeInTheVersion
     * @param activeCases
     * @param archiveCases
     * @param status
     * @param level
     * @param csvOperation
     * @param milkJobOutput
     */
    private void reportLine(ProcessDeploymentInfo processDeploymentInfo, Long rangeInTheVersion, long activeCases, long archiveCases, String status, STATUSLEVEL level, CSVOperation csvOperation, MilkJobOutput milkJobOutput) {
        if (rangeInTheVersion == null) {
            milkJobOutput.addReportTableLine(new Object[] { processDeploymentInfo.getName(),
                    processDeploymentInfo.getVersion(),
                    processDeploymentInfo.getProcessId(),
                    activeCases,
                    archiveCases,
                    getStatusForReport(status, level) });

        } else {
            milkJobOutput.addReportTableLine(new Object[] { processDeploymentInfo.getName(),
                    processDeploymentInfo.getVersion(),
                    processDeploymentInfo.getProcessId(),
                    rangeInTheVersion,
                    activeCases,
                    archiveCases,
                    getStatusForReport(status, level) });
        }
        try {
            csvOperation.writeRecord(getRecordForTableReport(processDeploymentInfo, rangeInTheVersion, activeCases, archiveCases, status, level));
        } catch (Exception e) {
            // no special treatment here
        }
    }

    private void writeRecordHeader(CSVOperation csvOperation, String header) {
        try {
            Map<String, String> mapHeader = new HashMap<>();
            mapHeader.put(CSTCOL_PROCESSNAME, "#####  " + header);
            csvOperation.writeRecord(mapHeader);
        } catch (Exception e) {
            // do not trace, continue in case of error
        }

    }

}
