package org.bonitasoft.truckmilk.plugin;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
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
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkJobOutput.Chronometer;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.DELAYSCOPE;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TYPE_PLUGIN;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter.FilterProcess;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.ListProcessesResult;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

import com.bonitasoft.engine.bpm.process.impl.ProcessInstanceSearchDescriptor;

public class MilkDeleteProcesses extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkDeleteProcesses.class.getName());
    private final static String LOGGER_LABEL = "MilkDeleteProcesses ";

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
    private static PlugInParameter cstParamDisableDelay = PlugInParameter.createInstanceDelay("DisabledDelay", "Delay", DELAYSCOPE.MONTH, 3, "Process is deleted only after this delay (based on the last update modification)")
            .withVisibleConditionParameterValueEqual(cstParamDisablead, true);
// not used process
    private static PlugInParameter cstParamNotUsed = PlugInParameter.createInstance("processNotUsed", "Process not used", TypeParameter.BOOLEAN, Boolean.FALSE, "Delete all Process not used in the delay (No Active/Archived case in the period)");
    private static PlugInParameter cstParamNotUsedDelay = PlugInParameter.createInstanceDelay("DisabledDelay", "Delay", DELAYSCOPE.MONTH, 3, "Process is deleted only if there is not operation in this delay")
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
            "Purge case policy. If the process contains cases, it willm be purged according the policy")
            .withVisibleConditionParameterValueEqual(cstParamMaxVersion, true);

    private static PlugInParameter cstParamRootCase = PlugInParameter.createInstance("subProcess", "Sub Process", TypeParameter.BOOLEAN, null, "A process can't be purge if it is called as a sub process. You allowed the tool to purge the Root cases");
    private static PlugInParameter cstParamRootCasePolicy = PlugInParameter.createInstanceListValues("subProcessRootPolicy", "Purge the Root case",
            new String[] { CSTROOTCASE_PURGEARCHIVED, CSTROOTCASE_PURGEACTIVE }, CSTROOTCASE_PURGEARCHIVED,
            "Purge Root cases policy. Purge the root case only if it is archived, or Active and Archived cases")
            .withVisibleConditionParameterValueEqual(cstParamRootCase, true);
    private static PlugInParameter cstParamRootDelay = PlugInParameter.createInstanceDelay("SubProcessDelay", "Delay", DELAYSCOPE.MONTH, 3, "Root Case created more than this delay can be purged")
            .withVisibleConditionParameterValueEqual(cstParamRootCase, true);

    /**
     * it's faster to delete 100 per 100
     */
    private static int casePerDeletion = 500;

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
        plugInDescription.setWarning("This plugin delete ENABLED and DISABLED processes, ACTIVES and archived cases. A process, a case deleted can't be retrieved. Operation is final. Use with caution.");

        plugInDescription.setCategory(CATEGORY.OTHER);
        plugInDescription.setStopJob(JOBSTOPPER.BOTH);

        plugInDescription.addParameterSeparator("Scope", "Scope of deletion");
        plugInDescription.addParameter(cstParamScopeProcessFilter);
        plugInDescription.addParameter(cstParamExclusionProcessFilter);

        plugInDescription.addParameterSeparator("Root Case Policy", "A process can be deleted only if no sub cases exists. Specify the policity for the root case");
        plugInDescription.addParameter(cstParamRootCase);
        plugInDescription.addParameter(cstParamRootCasePolicy);
        plugInDescription.addParameter(cstParamRootDelay);

        plugInDescription.addParameterSeparator("Disabled", "Deletion based on processes DISABLED");
        plugInDescription.addParameter(cstParamDisablead);
        plugInDescription.addParameter(cstParamDisabledPurgePolicy);
        // plugInDescription.addParameter(cstParamDisableDelay);

        plugInDescription.addParameterSeparator("Unused", "Deletion based on processes not used");
        plugInDescription.addParameter(cstParamNotUsed);
        plugInDescription.addParameter(cstParamNotUsedDelay);

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
        logger.info(LOGGER_LABEL+" Start to MilkDeleteProcess ");

        //preparation
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        StatsResult statsResult = new StatsResult();

        try {
            Boolean disabled = jobExecution.getInputBooleanParameter(cstParamDisablead);
            Boolean notUsed = jobExecution.getInputBooleanParameter(cstParamNotUsed);
            Boolean maxVersion = jobExecution.getInputBooleanParameter(cstParamMaxVersion);

            List<ProcessDeploymentInfo> listProcessPerimeters;
            SearchOptionsBuilder searchBuilderCase = new SearchOptionsBuilder(0, jobExecution.getJobStopAfterMaxItems() + 1);
            ListProcessesResult listProcess = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamScopeProcessFilter, false, searchBuilderCase, ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);
            if (BEventFactory.isError(listProcess.listEvents))
            {
                milkJobOutput.addEvents(listProcess.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return milkJobOutput;
            }
            if (listProcess.listProcessDeploymentInfo.isEmpty())
            {
                // search get ALL process
                // Attention, according the policy, we should search only some kind of process
                searchBuilderCase = new SearchOptionsBuilder(0, jobExecution.isLimitationOnMaxItem() ? jobExecution.getJobStopAfterMaxItems() + 1 : 50000);
                if (! (Boolean.TRUE.equals(notUsed) || Boolean.TRUE.equals(maxVersion) ))
                    searchBuilderCase.filter( ProcessDeploymentInfoSearchDescriptor.ACTIVATION_STATE, ActivationState.DISABLED.toString());
                
                SearchResult<ProcessDeploymentInfo> searchResultCurrentProcess = processAPI.searchProcessDeploymentInfos(searchBuilderCase.done());
                listProcessPerimeters = searchResultCurrentProcess.getResult(); 
            }
            else
                listProcessPerimeters= listProcess.listProcessDeploymentInfo;
                
            ListProcessesResult listProcessExclude = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamExclusionProcessFilter, false, searchBuilderCase, ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);
            Set<Long> setExclude = new HashSet<>();
            for (ProcessDeploymentInfo processDeploymentInfo : listProcessExclude.listProcessDeploymentInfo) {
                setExclude.add(processDeploymentInfo.getProcessId());
            }

            // --------------------------  Options
            if (Boolean.TRUE.equals(disabled)) {
                statsResult.add( deleteBasedOnDisabled(jobExecution, listProcessPerimeters, setExclude, milkJobOutput));
            }

            if (Boolean.TRUE.equals(notUsed)) {
                statsResult.add( deleteBasedOnNotUsed(jobExecution, listProcessPerimeters, setExclude, milkJobOutput) ); 
            }

            if (Boolean.TRUE.equals(maxVersion)) {
                statsResult.add( deleteBasedOnMaxVersion(jobExecution, listProcessPerimeters, setExclude, milkJobOutput));
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
        milkJobOutput.addReportInHtml("Number of Active   cases deleted "+statsResult.countCasesPurged+"<br>");
        milkJobOutput.addReportInHtml("Number of Archived cases deleted "+statsResult.countArchiveCasesPurged+"<br>");
        milkJobOutput.addReportInHtml("Number of Active   Root cases deleted "+statsResult.rootCountCasesPurged+"<br>");
        milkJobOutput.addReportInHtml("Number of Archived Root cases deleted "+statsResult.rootArchivedCountCasesPurged+"<br>");
        milkJobOutput.addReportInHtml("Number of Process deleted "+statsResult.processesPurged+"<br>");
        milkJobOutput.addChronometersInReport(false, true);
        
        milkJobOutput.setNbItemsProcessed(statsResult.processesPurged);
        
        return milkJobOutput;
    }

    /**
     * @param jobExecution
     * @param milkJobOutput
     */
    private StatsResult deleteBasedOnDisabled(MilkJobExecution jobExecution, List<ProcessDeploymentInfo> listProcess, Set<Long> setExclude, MilkJobOutput milkJobOutput) {
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        StatsResult statsResult = new StatsResult();

        String policy = jobExecution.getInputStringParameter(cstParamDisabledPurgePolicy);

        DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamDisableDelay, new Date(), false);
        if (BEventFactory.isError(delayResult.listEvents)) {
            milkJobOutput.addEvents(delayResult.listEvents);
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            return statsResult;
        }

        // root policy
        Boolean rootCasePurge = jobExecution.getInputBooleanParameter(cstParamRootCase);

        jobExecution.setAvancement(0);
        int count = 0;

        for (ProcessDeploymentInfo processDeploymentInfo : listProcess) {
            if (jobExecution.pleaseStop())
                break;
            jobExecution.setAvancement(0 + (int) (30.0 * count / listProcess.size()));
            count++;
            if (setExclude.contains(processDeploymentInfo.getProcessId()))
                continue;
            if (ActivationState.ENABLED.equals(processDeploymentInfo.getActivationState()))
                continue;
            // according the policy, purge cases
            logger.info(LOGGER_LABEL+" Start to delete "+getReportProcessName(processDeploymentInfo));
            if (CSTDISABLED_PURGEALL.equals(policy)) {
                statsResult.countCasesPurged += purgeActiveCases(jobExecution, processDeploymentInfo, milkJobOutput);
            }
            // there is still active process in this process ? 
            int countActiveInstance  = countInstancesInProcess(processDeploymentInfo, true, false, false, processAPI );
            // no sens to purge archived if there is still active case
            if (countActiveInstance>0)
            {
                milkJobOutput.addReportInHtml( getReportProcessName(processDeploymentInfo)+" Active cases:"+countActiveInstance+", no Deletion<br>");
                continue;
            }
            if (CSTDISABLED_PURGEARCHIVED.equals(policy) || (CSTDISABLED_PURGEALL.equals(policy))) {
                statsResult.countArchiveCasesPurged += purgeArchivedCases(jobExecution, processDeploymentInfo, milkJobOutput);
                countActiveInstance  = countInstancesInProcess(processDeploymentInfo, true, true, false, processAPI );
            }

            if (countActiveInstance>0)
            {
                milkJobOutput.addReportInHtml(getReportProcessName(processDeploymentInfo)+" cases:"+countActiveInstance+", no Deletion<br>");
                continue;
            }

            if (rootCasePurge) {
                statsResult.add(purgeRootCase(jobExecution, processDeploymentInfo, milkJobOutput));
            }

            statsResult.add( deleteProcess( processDeploymentInfo, processAPI,milkJobOutput));

        } // end loop
        jobExecution.setAvancement(30);
        return statsResult;

    }

    // First, purge cases
    // cstParamDisabledPurgePolicy

    private StatsResult deleteBasedOnNotUsed(MilkJobExecution jobExecution, List<ProcessDeploymentInfo> listProcess, Set<Long> setExclude, MilkJobOutput milkJobOutput) {
        jobExecution.setAvancement(30);
        StatsResult statsResult = new StatsResult();

        // ListProcessesResult listProcess = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, false, searchActBuilder, ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, jobExecution.getApiAccessor().getProcessAPI());
        // ListProcessesResult listProcessExclude = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, false, searchActBuilder, ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, jobExecution.getApiAccessor().getProcessAPI());

        jobExecution.setAvancement(60);
        return statsResult;
    }

    private StatsResult deleteBasedOnMaxVersion(MilkJobExecution jobExecution,  List<ProcessDeploymentInfo> listProcess, Set<Long> setExclude, MilkJobOutput milkJobOutput) {
        jobExecution.setAvancement(60);
        StatsResult statsResult = new StatsResult();

        // ListProcessesResult listProcess = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, false, searchActBuilder, ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, jobExecution.getApiAccessor().getProcessAPI());
        // ListProcessesResult listProcessExclude = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, false, searchActBuilder, ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, jobExecution.getApiAccessor().getProcessAPI());
        jobExecution.setAvancement(100);
        return statsResult;
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Instanciate */
    /*                                                                      */
    /* -------------------------------------------------------------------- */


    private StatsResult deleteProcess(ProcessDeploymentInfo processDeploymentInfo, ProcessAPI processAPI,MilkJobOutput milkJobOutput) {
        StatsResult statsResult = new StatsResult();
    
    try {
        int countActiveInstance  = countInstancesInProcess(processDeploymentInfo, true, true, false, processAPI );
        if (countActiveInstance > 0)
        {
            // do not delete it. API will allow to do it, but then we generated a lot of Dross data in database
            milkJobOutput.addEvent(new BEvent(eventDeletionSuccess, getReportProcessName(processDeploymentInfo)));
            milkJobOutput.addReportInHtml(getReportProcessName(processDeploymentInfo)+" cases (active or archived):"+countActiveInstance+", no Deletion<br>");
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
        logger.info(LOGGER_LABEL+" Deleted "+getReportProcessName(processDeploymentInfo));

    } catch (ProcessActivationException e) {
        milkJobOutput.addEvent(new BEvent(eventDisableFailed, e, getReportProcessName( processDeploymentInfo )+" Exception "+e.getMessage()));
        milkJobOutput.executionStatus = ExecutionStatus.ERROR;
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String exceptionDetails = sw.toString();
        logger.severe(LOGGER_LABEL + getReportProcessName( processDeploymentInfo )+ "] Error[" + e.getMessage() + "] at " + exceptionDetails);

    } catch (DeletionException e) {
        milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, getReportProcessName( processDeploymentInfo )+" Exception "+e.getMessage()));
        milkJobOutput.executionStatus = ExecutionStatus.ERROR;
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String exceptionDetails = sw.toString();
        logger.severe(LOGGER_LABEL + getReportProcessName( processDeploymentInfo )+ "] Error[" + e.getMessage() + "] at " + exceptionDetails);

    } catch (Exception e) {
        milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, getReportProcessName( processDeploymentInfo )+" Exception "+e.getMessage()));
        milkJobOutput.executionStatus = ExecutionStatus.ERROR;
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String exceptionDetails = sw.toString();
        logger.severe(LOGGER_LABEL + getReportProcessName( processDeploymentInfo )+ "] Error[" + e.getMessage() + "] at " + exceptionDetails);
    }
    return statsResult;
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
            } while (result > 0 && result>cstMaxDeletion-1);
            logger.info(LOGGER_LABEL+" Purge ["+countCasesPurged+"] Active case in "+getReportProcessName(processDeploymentInfo));

        } catch (DeletionException e) {
            milkJobOutput.addEvent(new BEvent(eventDeletionCaseFailed, e, "Process[" + processDeploymentInfo.getName() + "(" + processDeploymentInfo.getVersion() + ") ProcessId[" + processDeploymentInfo.getProcessId() + "]"));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + getReportProcessName( processDeploymentInfo )+ "] Error[" + e.getMessage() + "] at " + exceptionDetails);
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
            } while (result > 0 && result > cstMaxDeletion-1);
            logger.info(LOGGER_LABEL+" Purge ["+countCasesPurged+"] Archive case in "+getReportProcessName(processDeploymentInfo));
        } catch (DeletionException e) {
            milkJobOutput.addEvent(new BEvent(eventDeletionArchivedCaseFailed, e, "Process[" + processDeploymentInfo.getName() + "(" + processDeploymentInfo.getVersion() + ") ProcessId[" + processDeploymentInfo.getProcessId() + "]"));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + getReportProcessName( processDeploymentInfo )+ "] Error[" + e.getMessage() + "] at " + exceptionDetails);

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

        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        StatsResult statsResult = new StatsResult();

        String policyRoot = jobExecution.getInputStringParameter(cstParamRootCasePolicy);
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
     * 
     * @param processDeploymentInfo
     * @param active
     * @param archive
     * @param subCase
     * @param processAPI
     * @return
     */
    private int countInstancesInProcess(ProcessDeploymentInfo processDeploymentInfo, boolean active, boolean archive, boolean subCase, ProcessAPI processAPI )
    {
        int count=0;
        if (active) {
            try {
            SearchOptionsBuilder sob = new SearchOptionsBuilder(0,1);
            sob.filter(ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
            SearchResult<ProcessInstance> search = processAPI.searchProcessInstances(sob.done());
            count+= search.getCount();
            }catch(Exception e) {};
        }
        if (archive) {
            try {
                SearchOptionsBuilder sob = new SearchOptionsBuilder(0,1);
            sob.filter(ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
            SearchResult<ArchivedProcessInstance> search = processAPI.searchArchivedProcessInstances(sob.done());
            count+= search.getCount();
            }catch(Exception e) {};
        }
        return count;
        
    }

    private String getReportProcessName(ProcessDeploymentInfo processDeploymentInfo) {
        return " " + processDeploymentInfo.getName() + "(" + processDeploymentInfo.getVersion() + ") PID[" + processDeploymentInfo.getProcessId() + "]";
    }
}
