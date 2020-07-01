package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.engine.bpm.process.ProcessInstanceSearchDescriptor;
import org.bonitasoft.engine.exception.DeletionException;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptions;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.engine.search.Sort;
import org.bonitasoft.engine.search.impl.SearchFilter;
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
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

/**
 * Delete all cases in a process
 */
public class MilkDeleteCases extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkDeleteCases.class.getName());

    private static BEvent eventDeletionSuccess = new BEvent(MilkDeleteCases.class.getName(), 1, Level.SUCCESS,
            "Deletion done with success", "Archived Cases are deleted with success");

    private static BEvent eventDeletionFailed = new BEvent(MilkDeleteCases.class.getName(), 2, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion of archived cases", "Cases are not deleted", "Check the exception");

    private static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Filter on process", TypeParameter.ARRAYPROCESSNAME, null, "Job manage only process which mach the filter. If no filter is given, all processes are inspected")
            .withMandatory(false)
            .withFilterProcess(FilterProcess.ALL);

    private static PlugInParameter cstParamDelay = PlugInParameter.createInstanceDelay("Delay", "Delay to delete case, based on the Create date", DELAYSCOPE.MONTH, 6, "All cases started before this delay will be purged");

    /**
     * it's faster to delete 100 per 100
     */
    private static int casePerDeletion = 500;

    public MilkDeleteCases() {
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
        plugInDescription.setName("DeleteCases");
        plugInDescription.setLabel("Delete Cases (active and archived)");
        plugInDescription.setExplanation("Delete all cases in the given process, by a limitation given in parameters");
        plugInDescription.setWarning("This plugin delete ACTIVES and archived cases. A case deleted can't be retrieved. Operation is final. Use with caution.");

        plugInDescription.setCategory(CATEGORY.CASES);
        plugInDescription.setStopJob(JOBSTOPPER.BOTH);
        plugInDescription.addParameter(cstParamProcessFilter);
        plugInDescription.addParameter(cstParamDelay);

        return plugInDescription;
    }

    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput milkJobOutput = jobExecution.getMilkJobOutput();

        DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamDelay, new Date(), false);
        if (BEventFactory.isError(delayResult.listEvents)) {
            milkJobOutput.addEvents(delayResult.listEvents);
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            return milkJobOutput;
        }

        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        // get Input 
        SearchOptionsBuilder searchBuilderCase = new SearchOptionsBuilder(0, jobExecution.getJobStopAfterMaxItems() + 1);

        Chronometer deletionMarker = milkJobOutput.beginChronometer("deletion");
        try {
            ListProcessesResult listProcessResult = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, false, searchBuilderCase, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                milkJobOutput.addEvents(listProcessResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return milkJobOutput;
            }
            /*
             * long totalCaseToDelete=0;
             * for (Long processDefinitionId : listProcessDefinitionId)
             * {
             * totalCaseToDelete+= getNumberProcessInstance( processDefinitionId, processAPI );
             * }
             */

            int totalCaseDeleted;
            if (delayResult.delayInMs == 0)
                totalCaseDeleted = deleteCasesNoDelay(jobExecution, listProcessResult, milkJobOutput);
            else
                totalCaseDeleted = deleteCasesWithDelay(jobExecution, listProcessResult, delayResult, milkJobOutput);

            milkJobOutput.setNbItemsProcessed(totalCaseDeleted);

            milkJobOutput.endChronometer(deletionMarker);

            milkJobOutput.addEvent(new BEvent(eventDeletionSuccess, "Purge:" + totalCaseDeleted + " in " + deletionMarker.getTimeExecution() + " ms"));
            milkJobOutput.addChronometersInReport(false, true);

            if (totalCaseDeleted == 0)
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
            else if (jobExecution.pleaseStop())
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;
            else
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;

        } catch (Exception e1) {
            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e1, ""));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
        }

        return milkJobOutput;
    }

    /**
     *   
     */
    private int deleteCasesNoDelay(MilkJobExecution jobExecution, ListProcessesResult listProcessResult, MilkJobOutput milkJobOutput) {
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();

        int maximumArchiveDeletionPerRound = jobExecution.getJobStopAfterMaxItems();
        if (maximumArchiveDeletionPerRound > 100000)
            maximumArchiveDeletionPerRound = 100000;

        // Faster by group  of casePerDeletion cases            
        int count = 0;
        int totalNumberCaseDeleted = 0;
        StringBuilder reportOperation = new StringBuilder();
        List<ProcessDeploymentInfo> listProcessesInScope;
        try {

            if (!listProcessResult.listProcessDeploymentInfo.isEmpty()) {
                listProcessesInScope = listProcessResult.listProcessDeploymentInfo;

            } else {
                SearchResult<ProcessDeploymentInfo> searchProcess = processAPI.searchProcessDeploymentInfos(new SearchOptionsBuilder(0, 100000).done());
                listProcessesInScope = searchProcess.getResult();
            }
            reportOperation.append("Number of process:" + listProcessesInScope.size());

            for (ProcessDeploymentInfo processDefinition : listProcessResult.listProcessDeploymentInfo) {
                if (jobExecution.pleaseStop())
                    return totalNumberCaseDeleted;

                int countNumberThisPass = 1;
                while (count < maximumArchiveDeletionPerRound && countNumberThisPass > 0) {
                    if (jobExecution.pleaseStop())
                        return totalNumberCaseDeleted;
                    countNumberThisPass = 0;

                    int realCasePerDeletion = maximumArchiveDeletionPerRound - count;
                    if (realCasePerDeletion > casePerDeletion)
                        realCasePerDeletion = casePerDeletion;

                    long caseBefore = getNumberProcessInstance(processDefinition.getProcessId(), processAPI);
                    if (caseBefore == 0)
                        break;
                    long caseAfter = caseBefore;
                    try {
                        reportOperation.append("Detect [" + caseBefore + "] in [" + processDefinition.getProcessId() + "] : " + processDefinition.getName() + "(" + processDefinition.getVersion() + ")");
                        // we can't trust the result
                        Chronometer processInstanceMarker = milkJobOutput.beginChronometer("deleteProcessInstance-" + processDefinition.getName() + "(" + processDefinition.getVersion() + ")");
                        long numberActiveDeleted = processAPI.deleteProcessInstances(processDefinition.getProcessId(), 0, realCasePerDeletion * 3);
                        milkJobOutput.endChronometer(processInstanceMarker);

                        Chronometer processArchiveMarker = milkJobOutput.beginChronometer("deleteArchiveProcessInstance-" + processDefinition.getName() + "(" + processDefinition.getVersion() + ")");
                        long numberArchivedDeleted = processAPI.deleteArchivedProcessInstances(processDefinition.getProcessId(), 0, realCasePerDeletion * 3);
                        milkJobOutput.endChronometer(processArchiveMarker);

                        caseAfter = getNumberProcessInstance(processDefinition.getProcessId(), processAPI);
                        reportOperation.append("Deleted " + (caseBefore - caseAfter) + ") in " + (processInstanceMarker.getTimeExecution() + processArchiveMarker.getTimeExecution()) + " ms");

                        logger.fine("MilkDeleteCase - delete " + (numberActiveDeleted + numberArchivedDeleted) + " in " + processArchiveMarker.getTimeExecution() + " ms (Bonita record calculation:" + (numberActiveDeleted + numberArchivedDeleted) + ")");
                    } catch (Exception e) {
                        milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + processDefinition.getName() + "(" + processDefinition.getVersion() + ") PID:" + processDefinition.getProcessId()));
                    }
                    totalNumberCaseDeleted += (caseBefore - caseAfter);
                    countNumberThisPass += (caseBefore - caseAfter);

                } // end loop delete count item in the process
                count += casePerDeletion;
            } // end loop on processes
        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "Exception :" + e.toString()));
        }
        milkJobOutput.addReportInHtml(reportOperation.toString());

        return totalNumberCaseDeleted;
    }

    /**
     * @param processDefinitionId
     * @param processAPI
     * @return
     */
    private int deleteCasesWithDelay(MilkJobExecution jobExecution, ListProcessesResult listProcessResult, DelayResult delayResult, MilkJobOutput milkJobOutput) {
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();

        List<Long> processInstances = new ArrayList<>();
        List<Long> archivedProcessInstances = new ArrayList<>();
        int totalNumberCaseDeleted = 0;
        StringBuilder buildTheSQLRequest = new StringBuilder();
        List<BEvent> listEventsDeleted = new ArrayList<>();
        PurgeCasesResult purgeCasesResult;
        try {

            long timeSearch = delayResult.delayDate.getTime();

            SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance;

            // ------------- Active
            Integer maximumArchiveDeletionPerRoundInt = jobExecution.getJobStopAfterMaxItems();
            // default value is 1 Million
            if (maximumArchiveDeletionPerRoundInt == null || maximumArchiveDeletionPerRoundInt.equals(MilkJob.CSTDEFAULT_STOPAFTER_MAXITEMS))
                maximumArchiveDeletionPerRoundInt = 1000000;
            int maximumArchiveDeletionPerRound = maximumArchiveDeletionPerRoundInt;

            SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, maximumArchiveDeletionPerRound + 1);
            buildTheSQLRequest.append("select * from process_instance where ");

            if (!listProcessResult.listProcessDeploymentInfo.isEmpty()) {
                searchActBuilder.leftParenthesis();
                buildTheSQLRequest.append(" (");

                for (int i = 0; i < listProcessResult.listProcessDeploymentInfo.size(); i++) {
                    if (i > 0) {
                        searchActBuilder.or();
                        buildTheSQLRequest.append("  or ");
                    }
                    searchActBuilder.filter(ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, listProcessResult.listProcessDeploymentInfo.get(i).getProcessId());
                    buildTheSQLRequest.append(" processdefinitionid=" + listProcessResult.listProcessDeploymentInfo.get(i).getProcessId());

                }
                searchActBuilder.rightParenthesis();
                searchActBuilder.and();
                buildTheSQLRequest.append(") and ");
            }

            searchActBuilder.lessOrEquals(ProcessInstanceSearchDescriptor.START_DATE, timeSearch);
            buildTheSQLRequest.append(" startdate <= " + timeSearch);

            searchActBuilder.sort(ProcessInstanceSearchDescriptor.START_DATE, Order.ASC);
            buildTheSQLRequest.append(" order by startdate asc;<br>");

            Chronometer startSearchProcessInstance = milkJobOutput.beginChronometer("searchProcessInstance");
            SearchResult<ProcessInstance> searchProcessInstance = processAPI.searchProcessInstances(searchActBuilder.done());
            buildTheSQLRequest.append("Nb result : " + searchProcessInstance.getCount() + "<br>");
            milkJobOutput.endChronometer(startSearchProcessInstance);
            jobExecution.setAvancement(10);

            // do the purge now = from 10 to 50 %
            int localCount = 0;
            for (ProcessInstance processInstance : searchProcessInstance.getResult()) {
                if (jobExecution.pleaseStop())
                    return totalNumberCaseDeleted;
                // proceed page per page
                processInstances.add(processInstance.getId());
                if (processInstances.size() == 50) {
                    purgeCasesResult = purgeCases(processInstances, archivedProcessInstances, milkJobOutput, processAPI);
                    totalNumberCaseDeleted += purgeCasesResult.getNbCasesPurged();
                    listEventsDeleted.addAll(purgeCasesResult.listEvents);

                    localCount += processInstances.size();
                    jobExecution.setAvancement(10 + (int) (40.0 * localCount / searchProcessInstance.getCount()));
                }
            }

            // relicat
            purgeCasesResult = purgeCases(processInstances, archivedProcessInstances, milkJobOutput, processAPI);
            totalNumberCaseDeleted += purgeCasesResult.getNbCasesPurged();
            listEventsDeleted.addAll(purgeCasesResult.listEvents);

            processInstances.clear();

            jobExecution.setAvancement(50);

            maximumArchiveDeletionPerRound -= totalNumberCaseDeleted;
            if (maximumArchiveDeletionPerRound <= 0)
                return totalNumberCaseDeleted;

            // ------------- Archived
            searchActBuilder = new SearchOptionsBuilder(0, maximumArchiveDeletionPerRound + 1);
            buildTheSQLRequest.append("select * from arch_process_instance where ");

            if (!listProcessResult.listProcessDeploymentInfo.isEmpty()) {
                buildTheSQLRequest.append("(");
                searchActBuilder.leftParenthesis();
                for (int i = 0; i < listProcessResult.listProcessDeploymentInfo.size(); i++) {
                    if (i > 0) {
                        searchActBuilder.or();
                        buildTheSQLRequest.append(" or ");
                    }
                    searchActBuilder.filter(ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, listProcessResult.listProcessDeploymentInfo.get(i).getProcessId());
                    buildTheSQLRequest.append(" processdefinitionid = " + listProcessResult.listProcessDeploymentInfo.get(i).getProcessId());
                }
                searchActBuilder.rightParenthesis();
                searchActBuilder.and();
            }
            searchActBuilder.lessOrEquals(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, timeSearch);
            buildTheSQLRequest.append(" archivedate <= " + timeSearch);

            searchActBuilder.sort(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, Order.ASC);
            buildTheSQLRequest.append(" order by archivedate asc;<br>");

            Chronometer startSearchArchivedProcessInstance = milkJobOutput.beginChronometer("searchArchivedProcessInstance");
            searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());
            buildTheSQLRequest.append("Nb result : " + searchProcessInstance.getCount() + "<br>");
            milkJobOutput.endChronometer(startSearchArchivedProcessInstance);
            jobExecution.setAvancement(60);

            // do the purge now
            localCount = 0;
            for (ArchivedProcessInstance archivedProcessInstance : searchArchivedProcessInstance.getResult()) {
                if (jobExecution.pleaseStop())
                    return totalNumberCaseDeleted;
                // proceed page per page
                archivedProcessInstances.add(archivedProcessInstance.getSourceObjectId());
                if (archivedProcessInstances.size() == 50) {
                    purgeCasesResult = purgeCases(processInstances, archivedProcessInstances, milkJobOutput, processAPI);
                    totalNumberCaseDeleted += purgeCasesResult.getNbCasesPurged();
                    listEventsDeleted.addAll(purgeCasesResult.listEvents);

                    localCount += processInstances.size();
                    jobExecution.setAvancement(60 + (int) (40.0 * localCount / searchProcessInstance.getCount()));

                }
            }

            // reliquat
            purgeCasesResult = purgeCases(processInstances, archivedProcessInstances, milkJobOutput, processAPI);
            totalNumberCaseDeleted += purgeCasesResult.getNbCasesPurged();
            listEventsDeleted.addAll(purgeCasesResult.listEvents);

            jobExecution.setAvancement(100);

        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge"));
        }

        milkJobOutput.addReportInHtml(buildTheSQLRequest.toString());
        if (!listEventsDeleted.isEmpty()) {
            //we have to report it max 3 items
            for (int i = 0; i < 3; i++) {
                if (i < listEventsDeleted.size())
                    milkJobOutput.addEvent(listEventsDeleted.get(i));
            }
            milkJobOutput.addReportInHtml(listEventsDeleted.size() + " Error(s) deletion cases");

        }

        return totalNumberCaseDeleted;

    }

    /**
     * @param sourceProcessInstanceIds
     * @param sourceArchiveProcessInstanceIds
     * @param milkJobOutput
     * @return
     * @throws DeletionException
     */
    private class PurgeCasesResult {

        public int nbActiveCasePurged = 0;
        public int nbArchivedCasePurged = 0;
        public List<BEvent> listEvents = new ArrayList<BEvent>();

        public int getNbCasesPurged() {
            return nbActiveCasePurged + nbArchivedCasePurged;
        }
    }

    private PurgeCasesResult purgeCases(List<Long> processInstances, List<Long> archivedProcessInstances, MilkJobOutput milkJobOutput, ProcessAPI processAPI) throws DeletionException {
        PurgeCasesResult purgeCasesResult = new PurgeCasesResult();

        if (processInstances != null) {
            Chronometer processInstanceMarker = milkJobOutput.beginChronometer("deleteProcessInstance");
            for (Long processInstance : processInstances)
                try {
                    processAPI.deleteProcessInstance(processInstance);
                    purgeCasesResult.nbActiveCasePurged++;
                } catch (Exception e) {
                    purgeCasesResult.listEvents.add(new BEvent(eventDeletionFailed, "CaseId[" + processInstance + "] " + e.getMessage()));
                }
            milkJobOutput.endChronometer(processInstanceMarker, purgeCasesResult.nbActiveCasePurged);
            processInstances.clear();
        }
        if (archivedProcessInstances != null) {
            Chronometer processInstanceMarker = milkJobOutput.beginChronometer("deleteProcessInstance");
            try {
                processAPI.deleteArchivedProcessInstancesInAllStates(archivedProcessInstances);
                milkJobOutput.endChronometer(processInstanceMarker, archivedProcessInstances.size());
                purgeCasesResult.nbActiveCasePurged += archivedProcessInstances.size();
            } catch (Exception e) {
                purgeCasesResult.listEvents.add(new BEvent(eventDeletionFailed, "ListArchiveId: " + archivedProcessInstances.toString() + " " + e.getMessage()));
            }

            archivedProcessInstances.clear();
        }

        return purgeCasesResult;
    }

    /**
     * @param processDefinitionId
     * @param processAPI
     * @return
     */
    private long getNumberProcessInstance(long processDefinitionId, ProcessAPI processAPI) {

        try {
            SearchOptionsBuilder searchOption = new SearchOptionsBuilder(0, 10);
            searchOption.filter(ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processDefinitionId);
            SearchResult<ArchivedProcessInstance> searchArch = processAPI.searchArchivedProcessInstances(searchOption.done());
            long archived = searchArch.getCount();

            searchOption = new SearchOptionsBuilder(0, 10);
            searchOption.filter(ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDefinitionId);
            SearchResult<ProcessInstance> searchActive = processAPI.searchProcessInstances(searchOption.done());
            long active = searchActive.getCount();
            return archived + active;
        } catch (SearchException e) {
            return 0;
        }

    }

}
