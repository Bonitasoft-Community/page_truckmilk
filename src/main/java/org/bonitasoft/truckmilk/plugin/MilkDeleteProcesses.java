package org.bonitasoft.truckmilk.plugin;

import java.io.PrintWriter;
import java.io.StringWriter;
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
import org.bonitasoft.truckmilk.toolbox.MilkLog;

import com.bonitasoft.engine.bpm.process.impl.ProcessInstanceSearchDescriptor;

public class MilkDeleteProcesses extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkDeleteProcesses.class.getName());
    private final static String LOGGER_LABEL = "MilkDeleteProcesses ";

    private final static String HEADER_REPORT = "<table class=\"table table-striped table-hover table-condensed\"><tr><th>Process</th><th>Active</th><th>Archive</th><th>Status</th></tr>";
    private static BEvent eventSearchFailed = new BEvent(MilkDeleteProcesses.class.getName(), 1, Level.ERROR,
            "Search failed", "Search failed task return an error", "No retry can be performed", "Check the error");

    private static BEvent eventDisableFailed = new BEvent(MilkDeleteProcesses.class.getName(), 2, Level.ERROR,
            "Process can't be disabled", "A proceaa can't be disabled", "The disabled failed. Process can't be deleted", "Check the error, try to disabled the process");

    private static BEvent eventDeletionFailed = new BEvent(MilkDeleteProcesses.class.getName(), 3, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion of process", "Process are not deleted", "That's possible when the process contains sub case.Then, the root case must be deleted too");

    private static BEvent eventDeletionSuccess = new BEvent(MilkDeleteProcesses.class.getName(), 4, Level.SUCCESS,
            "Deletion done with success", "Archived Cases are deleted with success");

    private static BEvent eventDeletionCaseFailed = new BEvent(MilkDeleteProcesses.class.getName(), 5, Level.ERROR,
            "Error during Case deletion", "An error arrived during the deletion of cases", "Cases are not deleted", "Check why cases can't be deleted");

    private static BEvent eventDeletionArchivedCaseFailed = new BEvent(MilkDeleteProcesses.class.getName(), 6, Level.ERROR,
            "Error during Archive Case deletion", "An error arrived during the deletion of archive cases", "Cases are not deleted", "Check why cases can't be deleted");

    private final static String CSTDISABLED_NO = "No Purge";
    private final static String CSTDISABLED_PURGEARCHIVED = "Purge Archived Cases";
    private final static String CSTDISABLED_PURGEALL = "Purge Active/Archived Cases";

    private final static String CSTVERSION_ONLYDISABLED = "Only Disabled";
    private final static String CSTVERSION_DISABLEANDENABLE = "Disabled and Enabled";

    private final static String CSTROOTCASE_PURGEARCHIVED = "Purge Archived";
    private final static String CSTROOTCASE_PURGEACTIVE = "Purge Archived and Active";

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

    // process disabled rule
    private static PlugInParameter cstParamDisablead = PlugInParameter.createInstance("processdiseabled", "Disabled processes", TypeParameter.BOOLEAN, Boolean.FALSE, "Delete all Diseable processes");

    private static PlugInParameter cstParamDisabledPurgePolicy = PlugInParameter.createInstanceListValues("disabledPurge", "Purge",
            new String[] { CSTDISABLED_NO, CSTDISABLED_PURGEARCHIVED, CSTDISABLED_PURGEALL }, CSTDISABLED_NO,
            "Purge cases policy. If the process is called as a sub process, sub case is not purged")
            .withVisibleConditionParameterValueEqual(cstParamDisablead, true);
    private static PlugInParameter cstParamDisableDelay = PlugInParameter.createInstanceDelay("DisabledDelay", "No activity in this period", DELAYSCOPE.MONTH, 3, "Process is deleted only after this delay (based on the last update modification)")
            .withVisibleConditionParameterValueEqual(cstParamDisablead, true);
    private static PlugInParameter cstParamDisabledKeepLastVersion = PlugInParameter.createInstance("DisableKeepLastVersion", "Keep Last Version of the process", TypeParameter.BOOLEAN, Boolean.FALSE, "Keep the last version of the process")
            .withVisibleConditionParameterValueEqual(cstParamDisablead, true);

    // not used process
    private static PlugInParameter cstParamNotUsed = PlugInParameter.createInstance("processNotUsed", "Process not used", TypeParameter.BOOLEAN, Boolean.FALSE, "Delete all Process not used in the delay (No Active/Archived case in the period)");
    private static PlugInParameter cstParamNotUsedDelay = PlugInParameter.createInstanceDelay("processNotUsedDelay", "No activity in this period", DELAYSCOPE.MONTH, 3, "Process is deleted only if there is not operation in this delay")
            .withVisibleConditionParameterValueEqual(cstParamNotUsed, true);
    private static PlugInParameter cstParamNotUsedKeepLastVersion = PlugInParameter.createInstance("processKeepLastVersion", "Keep Last Version of the process", TypeParameter.BOOLEAN, Boolean.FALSE, "Keep the last version of the process")
            .withVisibleConditionParameterValueEqual(cstParamNotUsed, true);

    private static PlugInParameter cstParamMaxVersion = PlugInParameter.createInstance("processMaxVersion", "Max version", TypeParameter.BOOLEAN, Boolean.FALSE, "Keep a number of version for a process. Older version than the max are deleted.");
    private static PlugInParameter cstParamVersionNumber = PlugInParameter.createInstance("MaxVersion", "Maximum version kept", TypeParameter.LONG, Long.valueOf(3), "All processes under this number for a process are deleted (based on the deployment date)")
            .withVisibleConditionParameterValueEqual(cstParamMaxVersion, true);
    private static PlugInParameter cstParamVersionPolicy = PlugInParameter.createInstanceListValues("VersionScope", "Scope",
            new String[] { CSTVERSION_ONLYDISABLED, CSTVERSION_DISABLEANDENABLE }, CSTVERSION_ONLYDISABLED,
            "Purge process policy. Only Disabled process can be deleted, or Enable process as well")
            .withVisibleConditionParameterValueEqual(cstParamMaxVersion, true);
    private static PlugInParameter cstParamVersionPurge = PlugInParameter.createInstanceListValues("VersionPurgeCases", "Purge Cases",
            new String[] { CSTDISABLED_PURGEARCHIVED, CSTDISABLED_PURGEALL }, CSTDISABLED_PURGEARCHIVED,
            "Purge case policy. If the process contains cases, it will be purged according the policy")
            .withVisibleConditionParameterValueEqual(cstParamMaxVersion, true);

    private static PlugInParameter cstParamRootCase = PlugInParameter.createInstance("subProcess", "Sub Process", TypeParameter.BOOLEAN, null, "A process can't be purge if it is called as a sub process. You allowed the tool to purge the Root cases");
    private static PlugInParameter cstParamRootCasePolicy = PlugInParameter.createInstanceListValues("subProcessRootPolicy", "Purge the Root case",
            new String[] { CSTROOTCASE_PURGEARCHIVED, CSTROOTCASE_PURGEACTIVE }, CSTROOTCASE_PURGEARCHIVED,
            "Purge Root cases policy. Purge the root case only if it is archived, or Active and Archived cases")
            .withVisibleConditionParameterValueEqual(cstParamRootCase, true);
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

        /*
         * plugInDescription.addParameterSeparator("Root Case Policy",
         * "A process can be deleted only if no sub cases exists. Specify the policity for the root case");
         * plugInDescription.addParameter(cstParamRootCase);
         * plugInDescription.addParameter(cstParamRootCasePolicy);
         * plugInDescription.addParameter(cstParamRootDelay);
         */
        plugInDescription.addParameterSeparator("Disabled", "Deletion based on processes DISABLED");
        plugInDescription.addParameter(cstParamDisablead);
        plugInDescription.addParameter(cstParamDisabledPurgePolicy);
        plugInDescription.addParameter(cstParamDisableDelay);
        plugInDescription.addParameter(cstParamDisabledKeepLastVersion);

        plugInDescription.addParameterSeparator("Unused", "Deletion based on processes not used");
        plugInDescription.addParameter(cstParamNotUsed);
        plugInDescription.addParameter(cstParamNotUsedDelay);
        plugInDescription.addParameter(cstParamNotUsedKeepLastVersion);

        plugInDescription.addParameterSeparator("Version", "Deletion based on process version");
        plugInDescription.addParameter(cstParamMaxVersion);
        plugInDescription.addParameter(cstParamVersionNumber);
        plugInDescription.addParameter(cstParamVersionPolicy);
        plugInDescription.addParameter(cstParamVersionPurge);

        return plugInDescription;
    }

    private class StatsResult {

        public long countCasesPurged = 0;
        public long countArchiveCasesPurged = 0;
        public long rootCountCasesPurged = 0;
        public long rootArchivedCountCasesPurged = 0;
        public int processesPurged = 0;
        public String status;

        public void add(StatsResult stats) {
            countCasesPurged += stats.countCasesPurged;
            countArchiveCasesPurged += stats.countArchiveCasesPurged;
            rootCountCasesPurged += stats.rootCountCasesPurged;
            rootArchivedCountCasesPurged += stats.rootArchivedCountCasesPurged;

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
        
        StringBuilder finalReport = new StringBuilder();

        try {
            String operation = jobExecution.getInputStringParameter(cstParamOperation);
            Boolean disabled = jobExecution.getInputBooleanParameter(cstParamDisablead);
            Boolean notUsed = jobExecution.getInputBooleanParameter(cstParamNotUsed);
            Boolean maxVersion = jobExecution.getInputBooleanParameter(cstParamMaxVersion);

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
                StatsResult statsResultOp = deleteBasedOnDisabled(jobExecution, CSTOPERATION_DELETION.equals(operation), listProcessPerimeters, setExclude, milkJobOutput);
                finalReport.append("Delete Disabled :"+statsResultOp.processesPurged+"<br>");
                statsResult.add( statsResultOp );
            }

            if (Boolean.TRUE.equals(notUsed)) {
                StatsResult statsResultOp =deleteBasedOnNotUsed(jobExecution, CSTOPERATION_DELETION.equals(operation), listProcessPerimeters, setExclude, milkJobOutput);
                finalReport.append("Delete Not Used :"+statsResultOp.processesPurged+"<br>");
                statsResult.add( statsResultOp );
            }

            if (Boolean.TRUE.equals(maxVersion)) {
                StatsResult statsResultOp = deleteBasedOnMaxVersion(jobExecution, CSTOPERATION_DELETION.equals(operation), listProcessPerimeters, setExclude, milkJobOutput);
                finalReport.append("Delete Max Version :"+statsResultOp.processesPurged+"<br>");
                statsResult.add( statsResultOp );
            }
        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventSearchFailed, e, ""));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + " Error[" + e.getMessage() + "] at " + exceptionDetails);
            return milkJobOutput;
        }
        milkJobOutput.addReportInHtml( "<h3>Synthesis</h3>");
        milkJobOutput.addReportInHtmlCR( finalReport.toString() );
        milkJobOutput.addReportInHtmlCR( "Number of Active   cases deleted " + statsResult.countCasesPurged );
        milkJobOutput.addReportInHtmlCR( "Number of Archived cases deleted " + statsResult.countArchiveCasesPurged);
        milkJobOutput.addReportInHtmlCR( "Number of Active   Root cases deleted " + statsResult.rootCountCasesPurged);
        milkJobOutput.addReportInHtmlCR( "Number of Archived Root cases deleted " + statsResult.rootArchivedCountCasesPurged);
        milkJobOutput.addReportInHtmlCR( "Number of Process deleted " + statsResult.processesPurged );
        milkJobOutput.addChronometersInReport(false, true);

        milkJobOutput.setNbItemsProcessed(statsResult.processesPurged);

        return milkJobOutput;
    }

    /**
     * @param jobExecution
     * @param milkJobOutput
     */
    private StatsResult deleteBasedOnDisabled(MilkJobExecution jobExecution, boolean doTheDeletion, List<ProcessDeploymentInfo> listProcess, Set<Long> setExclude, MilkJobOutput milkJobOutput) {
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        StatsResult statsResult = new StatsResult();
        milkJobOutput.addReportInHtml("<h3>Disabled</h3>");

        String policy = jobExecution.getInputStringParameter(cstParamDisabledPurgePolicy);

        DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamDisableDelay, new Date(), false);
        if (BEventFactory.isError(delayResult.listEvents)) {
            milkJobOutput.addEvents(delayResult.listEvents);
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            return statsResult;
        }
        Boolean keepLastVersion = jobExecution.getInputBooleanParameter(cstParamDisabledKeepLastVersion);

        // root policy
        Boolean rootCasePurge = jobExecution.getInputBooleanParameter(cstParamRootCase);

        jobExecution.setAvancement(0);
        int count = 0;
        int nbOfProcessesToDelete = 0;
        milkJobOutput.addReportInHtml(HEADER_REPORT);
        for (ProcessDeploymentInfo processDeploymentInfo : listProcess) {
            if (jobExecution.pleaseStop())
                break;
            jobExecution.setAvancement(0 + (int) (30.0 * count / listProcess.size()));
            count++;
            if (setExclude.contains(processDeploymentInfo.getProcessId()))
                continue;
            if (ActivationState.ENABLED.equals(processDeploymentInfo.getActivationState()))
                continue;

            if (Boolean.TRUE.equals(keepLastVersion)) {
                if (isLastVersionProcess(processDeploymentInfo, processAPI))
                    continue;
            }

            if (delayResult.delayInMs < 0) {
                Long lastActivity = getLastTimeActivityOnProcess(processDeploymentInfo, milkJobOutput, processAPI);
                if (lastActivity != null && lastActivity > delayResult.delayDate.getTime())
                    continue;

            }

            // Only Detection
            if (!doTheDeletion) {
                nbOfProcessesToDelete++;

                int countActiveInstance = countInstancesInProcess(processDeploymentInfo, true, false, false, processAPI);
                int countArchiveInstance = countInstancesInProcess(processDeploymentInfo, false, true, false, processAPI);
                milkJobOutput.addReportInHtml("<tr><td>" + getReportProcessName(processDeploymentInfo) + "</td><td>" + countActiveInstance + "</td><td>" + countArchiveInstance + "</td><td>" + getStatusForReport("To be delete", STATUSLEVEL.INFO) + "</td></tr>");
                continue;
            }

            // Delete process
            // according the policy, purge cases
            logger.info(LOGGER_LABEL + " Start to delete " + getReportProcessName(processDeploymentInfo));
            long countCasesPurged = 0;
            long countArchivedCasesPurged = 0;
            if (CSTDISABLED_PURGEALL.equals(policy)) {
                countCasesPurged = purgeActiveCases(jobExecution, processDeploymentInfo, milkJobOutput);
                statsResult.countCasesPurged += countCasesPurged;
            }
            // there is still active process in this process ?
            int countActiveInstance = countInstancesInProcess(processDeploymentInfo, true, false, false, processAPI);

            // no sens to purge archived if there is still active case
            if (countActiveInstance > 0) {
                milkJobOutput.addReportInHtml("<tr><td>" + getReportProcessName(processDeploymentInfo) + "</td><td>" + countArchivedCasesPurged + "</td><td> - </td><td>" + getStatusForReport("Still active case", STATUSLEVEL.WARNING) + "</td></tr>");
                continue;
            }
            if (CSTDISABLED_PURGEARCHIVED.equals(policy) || (CSTDISABLED_PURGEALL.equals(policy))) {
                countArchivedCasesPurged = purgeArchivedCases(jobExecution, processDeploymentInfo, milkJobOutput);
                statsResult.countArchiveCasesPurged += countArchivedCasesPurged;
            }
            int countArchiveInstance = countInstancesInProcess(processDeploymentInfo, false, true, false, processAPI);

            if (countArchiveInstance > 0) {
                milkJobOutput.addReportInHtml(getReportProcessName(processDeploymentInfo) + " cases:" + countActiveInstance + ", no Deletion<br>");
                milkJobOutput.addReportInHtml("<tr><td>" + getReportProcessName(processDeploymentInfo) + "</td><td>" + countCasesPurged + "</td><td>" + countArchivedCasesPurged + "</td><td>" + getStatusForReport("Still archived case", STATUSLEVEL.WARNING) + "</td></tr>");
                continue;
            }

            if (rootCasePurge) {
                statsResult.add(purgeRootCase(jobExecution, processDeploymentInfo, milkJobOutput));
            }

            // ---------------- ready to delete
            nbOfProcessesToDelete++;

            StatsResult statsResultProcess = deleteProcess(processDeploymentInfo, processAPI, milkJobOutput);
            milkJobOutput.addReportInHtml("<tr><td>" + getReportProcessName(processDeploymentInfo) + "</td><td>" + countCasesPurged + "</td><td>" + countArchivedCasesPurged + "</td><td>" + statsResultProcess.status + "</td></tr>");
            statsResult.add(statsResultProcess);
        } // end loop
        milkJobOutput.addReportInHtml("</table>Number of process: " + nbOfProcessesToDelete);

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
    private StatsResult deleteBasedOnNotUsed(MilkJobExecution jobExecution, boolean doTheDeletion, List<ProcessDeploymentInfo> listProcess, Set<Long> setExclude, MilkJobOutput milkJobOutput) {
        jobExecution.setAvancement(30);
        StatsResult statsResult = new StatsResult();
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        milkJobOutput.addReportInHtml("<h3>Not used</h3>");

        DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamNotUsedDelay, new Date(), false);
        if (BEventFactory.isError(delayResult.listEvents)) {
            milkJobOutput.addEvents(delayResult.listEvents);
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            return statsResult;
        }
        Boolean keepLastVersion = jobExecution.getInputBooleanParameter(cstParamNotUsedKeepLastVersion);

        milkJobOutput.addReportInHtml(HEADER_REPORT);

        int count = 0;
        int nbOfProcessesToDelete = 0;

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
                if (processDeploymentInfo.getLastUpdateDate() != null && processDeploymentInfo.getLastUpdateDate().getTime() > delayResult.delayDate.getTime())
                    continue;

                //  search the last processInstance / last Archive process instance in this process
                Long lastActivity = getLastTimeActivityOnProcess(processDeploymentInfo, milkJobOutput, processAPI);
                if (lastActivity != null && lastActivity > delayResult.delayDate.getTime())
                    continue;
            }
            //------------------------ we are still here ? Let delete the process
            nbOfProcessesToDelete++;
            if (!doTheDeletion) {
                int countActiveInstance = countInstancesInProcess(processDeploymentInfo, true, false, false, processAPI);
                int countArchiveInstance = countInstancesInProcess(processDeploymentInfo, false, true, false, processAPI);
                milkJobOutput.addReportInHtml("<tr><td>" + getReportProcessName(processDeploymentInfo) + "</td><td>" + countActiveInstance + "</td><td>" + countArchiveInstance + "</td><td>" + getStatusForReport("To be delete", STATUSLEVEL.INFO) + "</td></tr>");
                continue;
            }

            long countCasesPurged = purgeActiveCases(jobExecution, processDeploymentInfo, milkJobOutput);
            long countArchiveCasesPurged = purgeArchivedCases(jobExecution, processDeploymentInfo, milkJobOutput);
            StatsResult statsResultProcess = deleteProcess(processDeploymentInfo, processAPI, milkJobOutput);
            milkJobOutput.addReportInHtml("<tr><td>" + getReportProcessName(processDeploymentInfo) + "</td><td>" + countCasesPurged + "</td><td>" + countArchiveCasesPurged + "</td><td>" + statsResultProcess.status + "</td></tr>");
            statsResult.countCasesPurged += countCasesPurged;
            statsResult.countArchiveCasesPurged += countArchiveCasesPurged;
            statsResult.add(statsResultProcess);
        }
        milkJobOutput.addReportInHtml("</table>Number of process: " + nbOfProcessesToDelete);

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
    private StatsResult deleteBasedOnMaxVersion(MilkJobExecution jobExecution, boolean doTheDeletion, List<ProcessDeploymentInfo> listProcess, Set<Long> setExclude, MilkJobOutput milkJobOutput) {
        jobExecution.setAvancement(60);
        StatsResult statsResult = new StatsResult();
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        milkJobOutput.addReportInHtml("<h3>Version</h3>");
        long numberOfVersion = jobExecution.getInputLongParameter(cstParamVersionNumber);
        String versionPolicy = jobExecution.getInputStringParameter(cstParamVersionPolicy);
        String versionPurge = jobExecution.getInputStringParameter(cstParamVersionPurge);

        // first pass, let's collect all process name
        List<String> listProcessName = new ArrayList<>();

        for (ProcessDeploymentInfo processDeploymentInfo : listProcess) {
            if (!listProcessName.contains(processDeploymentInfo.getName()))
                listProcessName.add(processDeploymentInfo.getName());
        }

        Collections.sort(listProcessName);

        milkJobOutput.addReportInHtml(HEADER_REPORT);

        // process name per name
        int count = 0;
        int nbOfProcessesToDelete = 0;

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
                if (search.getCount() <= numberOfVersion)
                    continue;

                // ok, delete all After the first version
                for (long i = numberOfVersion; i < search.getCount(); i++) {
                    ProcessDeploymentInfo processDeploymentInfo = search.getResult().get((int) i);
                    if (CSTVERSION_ONLYDISABLED.equals(versionPolicy) && ActivationState.ENABLED.equals(processDeploymentInfo.getActivationState())) {
                        // milkJobOutput.addReportInHtml("<tr><td> Version["+i+"] " + getReportProcessName(processDeploymentInfo) + "</td><td> - </td><td> - </td><td>" + getStatusForReport("is ENABLED", STATUSLEVEL.INFO) + "</td></tr>");
                        continue;
                    }
                    if (CSTDISABLED_PURGEARCHIVED.equals(versionPurge)) {
                        int countActiveInstance = countInstancesInProcess(processDeploymentInfo, true, false, false, processAPI);
                        if (countActiveInstance > 0) {
                            // milkJobOutput.addReportInHtml("<tr><td> Version["+i+"] " + getReportProcessName(processDeploymentInfo) + "</td><td>" + countActiveInstance + "</td><td> - </td><td>" + getStatusForReport("Contains active cases", STATUSLEVEL.INFO) + "</td></tr>");
                            continue;
                        }
                    }

                    // ------------------- do the deletion now
                    nbOfProcessesToDelete++;

                    if (!doTheDeletion) {
                        int countActiveInstance = countInstancesInProcess(processDeploymentInfo, true, false, false, processAPI);
                        int countArchiveInstance = countInstancesInProcess(processDeploymentInfo, false, true, false, processAPI);
                        milkJobOutput.addReportInHtml("<tr><td> Version[" + i + "] " + getReportProcessName(processDeploymentInfo) + "</td><td>" + countActiveInstance + "</td><td>" + countArchiveInstance + "</td><td>" + getStatusForReport("To be deleted", STATUSLEVEL.INFO) + "</td></tr>");
                        continue;
                    }
                    long countCasesPurged = 0;
                    long countArchiveCasesPurged = 0;
                    countCasesPurged = purgeActiveCases(jobExecution, processDeploymentInfo, milkJobOutput);
                    countArchiveCasesPurged = purgeArchivedCases(jobExecution, processDeploymentInfo, milkJobOutput);
                    StatsResult statsResultProcess = deleteProcess(processDeploymentInfo, processAPI, milkJobOutput);
                    milkJobOutput.addReportInHtml("<tr><td>" + getReportProcessName(processDeploymentInfo) + "</td><td>" + countCasesPurged + "</td><td>" + countArchiveCasesPurged + "</td><td>" + statsResultProcess.status + "</td></tr>");
                    statsResult.countCasesPurged += countCasesPurged;
                    statsResult.countArchiveCasesPurged += countArchiveCasesPurged;
                    statsResult.add(statsResultProcess);

                }
            } catch (Exception e) {
                milkJobOutput.addEvent(new BEvent(eventSearchFailed, e, " Exception " + e.getMessage()));
                milkJobOutput.addReportInHtml("<tr><td>" + processName + "</td><td> - </td><td> - </td><td>" + getStatusForReport("Search failed " + e.getMessage(), STATUSLEVEL.ERROR) + "</td></tr>");

                milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String exceptionDetails = sw.toString();
                logger.severe(LOGGER_LABEL + " Error[" + e.getMessage() + "] at " + exceptionDetails);
            }

        }
        milkJobOutput.addReportInHtml("</table>Number of process: " + nbOfProcessesToDelete);

        jobExecution.setAvancement(100);
        return statsResult;
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Delete process */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

    private StatsResult deleteProcess(ProcessDeploymentInfo processDeploymentInfo, ProcessAPI processAPI, MilkJobOutput milkJobOutput) {
        StatsResult statsResult = new StatsResult();

        try {
            int countActiveInstance = countInstancesInProcess(processDeploymentInfo, true, true, false, processAPI);
            if (countActiveInstance > 0) {
                // do not delete it. API will allow to do it, but then we generated a lot of Dross data in database
                milkJobOutput.addEvent(new BEvent(eventDeletionSuccess, getReportProcessName(processDeploymentInfo)));
                statsResult.status = getStatusForReport("Cases in the process, no deletion", STATUSLEVEL.WARNING);
                return statsResult;
            }
            // Now it's possible to purge the process - may be
            if (ActivationState.ENABLED.equals(processDeploymentInfo.getActivationState())) {
                processAPI.disableProcess(processDeploymentInfo.getProcessId());
            }
            Chronometer startDeletion = milkJobOutput.beginChronometer("DeletionProcessDefinition");
            processAPI.deleteProcessDefinition(processDeploymentInfo.getProcessId());
            milkJobOutput.endChronometer(startDeletion);
            milkJobOutput.nbItemsProcessed += 1; // count case and process
            statsResult.processesPurged++;
            milkJobOutput.addEvent(new BEvent(eventDeletionSuccess, getReportProcessName(processDeploymentInfo)));
            statsResult.status = getStatusForReport("Deleted", STATUSLEVEL.SUCCESS);
            logger.info(LOGGER_LABEL + " Deleted " + getReportProcessName(processDeploymentInfo));

        } catch (ProcessActivationException e) {
            milkJobOutput.addEvent(new BEvent(eventDisableFailed, e, getReportProcessName(processDeploymentInfo) + " Exception " + e.getMessage()));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + getReportProcessName(processDeploymentInfo) + "] Error[" + e.getMessage() + "] at " + exceptionDetails);
            statsResult.status = getStatusForReport("Deactivation error", STATUSLEVEL.ERROR);
        } catch (DeletionException e) {
            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, getReportProcessName(processDeploymentInfo) + " Exception " + e.getMessage()));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + getReportProcessName(processDeploymentInfo) + "] Error[" + e.getMessage() + "] at " + exceptionDetails);
            statsResult.status = getStatusForReport("Deletion error", STATUSLEVEL.ERROR);
        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, getReportProcessName(processDeploymentInfo) + " Exception " + e.getMessage()));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + getReportProcessName(processDeploymentInfo) + "] Error[" + e.getMessage() + "] at " + exceptionDetails);
            statsResult.status = getStatusForReport("Error " + e.getMessage(), STATUSLEVEL.ERROR);
        }
        return statsResult;
    }

    /**
     * keep the last version, for a process name
     */
    public Map<String, String> cacheLastProcessVersion = new HashMap<String, String>();

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
            // protection, but anormal : we should find the process as minimum
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
        Long lastActiveActivity = null;
        Long lastArchivedActivity = null;
        try {
            SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 1);
            sob.filter(ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
            sob.sort(ProcessInstanceSearchDescriptor.LAST_UPDATE, Order.DESC);
            SearchResult<ProcessInstance> searchProcessInstance = processAPI.searchProcessInstances(sob.done());
            if (searchProcessInstance.getCount() > 0) {
                ProcessInstance processInstance = searchProcessInstance.getResult().get(0);
                Date lastUpdate = processInstance.getLastUpdate();
                if (lastUpdate == null) {
                    milkJobOutput.addReportInHtml("<tr><td>" + getReportProcessName(processDeploymentInfo) + "</td><td> - </td><td> - </td><td>ArchiveCase[" + processInstance.getId() + "]: No LastUpdateDate, ignored</td></tr>");
                } else {
                    lastActiveActivity = lastUpdate.getTime();
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
                    milkJobOutput.addReportInHtml("<tr><td>" + getReportProcessName(processDeploymentInfo) + "</td><td> - </td><td> - </td><td>ArchiveCase[" + processInstance.getSourceObjectId() + "]: No LastUpdateDate, ignored</td></tr>");
                } else
                    lastArchivedActivity = lastUpdate.getTime();
            }
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + getReportProcessName(processDeploymentInfo) + "] Error[" + e.getMessage() + "] at " + exceptionDetails);
        }
        Long lastUpdateDate = processDeploymentInfo.getLastUpdateDate()==null ? 0L : processDeploymentInfo.getLastUpdateDate().getTime();
        
        // collect the MAX of the tree value 
        if (lastActiveActivity==null)
            lastActiveActivity=0L;
        if (lastArchivedActivity==null)
            lastArchivedActivity=0L;
        long maxValue= Math.max(lastActiveActivity, Math.max( lastArchivedActivity,lastUpdateDate));
        if (maxValue==0)
            return null;
        return maxValue;
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
    private long purgeActiveCases(MilkJobExecution jobExecution, ProcessDeploymentInfo processDeploymentInfo, MilkJobOutput milkJobOutput) {
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();

        long result = 0;
        long countCasesPurged = 0;
        try {
            do {
                if (jobExecution.pleaseStop())
                    break;
                Chronometer startDeletion = milkJobOutput.beginChronometer("DeletionActiveCase");
                result = processAPI.deleteProcessInstances(processDeploymentInfo.getProcessId(), 0, cstMaxDeletion);
                countCasesPurged += result;
                milkJobOutput.endChronometer(startDeletion);
                milkJobOutput.nbItemsProcessed += result;
            } while (result > 0 && result > cstMaxDeletion - 1);
            logger.info(LOGGER_LABEL + " Purge [" + countCasesPurged + "] Active case in " + getReportProcessName(processDeploymentInfo));

        } catch (DeletionException e) {
            milkJobOutput.addEvent(new BEvent(eventDeletionCaseFailed, e, "Process[" + processDeploymentInfo.getName() + "(" + processDeploymentInfo.getVersion() + ") ProcessId[" + processDeploymentInfo.getProcessId() + "]"));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + getReportProcessName(processDeploymentInfo) + "] Error[" + e.getMessage() + "] at " + exceptionDetails);
        }
        return countCasesPurged;
    }

    /*
     * 
     */
    private long purgeArchivedCases(MilkJobExecution jobExecution, ProcessDeploymentInfo processDeploymentInfo, MilkJobOutput milkJobOutput) {
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();

        long result = 0;
        long countCasesPurged = 0;

        try {
            do {
                if (jobExecution.pleaseStop())
                    break;
                Chronometer startDeletion = milkJobOutput.beginChronometer("DeletionArchivedCase");
                result = processAPI.deleteArchivedProcessInstances(processDeploymentInfo.getProcessId(), 0, cstMaxDeletion);
                countCasesPurged += result;
                milkJobOutput.endChronometer(startDeletion);
                milkJobOutput.nbItemsProcessed += result;
            } while (result > 0 && result > cstMaxDeletion - 1);
            logger.info(LOGGER_LABEL + " Purge [" + countCasesPurged + "] Archive case in " + getReportProcessName(processDeploymentInfo));
        } catch (DeletionException e) {
            milkJobOutput.addEvent(new BEvent(eventDeletionArchivedCaseFailed, e, "Process[" + processDeploymentInfo.getName() + "(" + processDeploymentInfo.getVersion() + ") ProcessId[" + processDeploymentInfo.getProcessId() + "]"));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + getReportProcessName(processDeploymentInfo) + "] Error[" + e.getMessage() + "] at " + exceptionDetails);

        }
        return countCasesPurged;

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
    private int countInstancesInProcess(ProcessDeploymentInfo processDeploymentInfo, boolean active, boolean archive, boolean subCase, ProcessAPI processAPI) {
        int count = 0;
        if (active) {
            try {
                SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 1);
                sob.filter(ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
                SearchResult<ProcessInstance> search = processAPI.searchProcessInstances(sob.done());
                count += search.getCount();
            } catch (Exception e) {
            } ;
        }
        if (archive) {
            try {
                SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 1);
                sob.filter(ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
                SearchResult<ArchivedProcessInstance> search = processAPI.searchArchivedProcessInstances(sob.done());
                count += search.getCount();
            } catch (Exception e) {
            } ;
        }
        return count;

    }

    private String getReportProcessName(ProcessDeploymentInfo processDeploymentInfo) {
        return " " + processDeploymentInfo.getName() + "(" + processDeploymentInfo.getVersion() + ") PID[" + processDeploymentInfo.getProcessId() + "]";
    }

    public enum STATUSLEVEL {
        SUCCESS, INFO, WARNING, ERROR
    }

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
}
