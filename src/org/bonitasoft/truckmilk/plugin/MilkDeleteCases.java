package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.List;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfoSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.engine.bpm.process.ProcessInstanceSearchDescriptor;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

/**
 * Delete all cases in a process
 */
public class MilkDeleteCases extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkDeleteCases.class.getName());

    private static BEvent eventNoProcessMatchFilter = new BEvent(MilkDeleteCases.class.getName(), 1,
            Level.APPLICATIONERROR,
            "No process match filter", "No process is found with the given filter", "This filter does not apply.",
            "Check the process name");

    private static BEvent eventNoProcessForFilter = new BEvent(MilkDeleteCases.class.getName(), 2,
            Level.APPLICATIONERROR,
            "Filter is not active", "No processes was found for all the filter, search will not run",
            "No filter at all apply, assuming configuration want to apply only on some process",
            "Check the process name");

    private static BEvent eventDeletionSuccess = new BEvent(MilkDeleteCases.class.getName(), 3, Level.SUCCESS,
            "Deletion done with success", "Archived Cases are deleted with success");

    private static BEvent eventDeletionFailed = new BEvent(MilkDeleteCases.class.getName(), 4, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion of archived cases", "Cases are not deleted", "Check the exception");

    private static PlugInParameter cstParamMaximumDeletion = PlugInParameter.createInstance("maximumdeletion", "Maximum cases Deletion", TypeParameter.LONG, 1000L, "Maximum of case to delete. When this number is reach, job stops");
    private static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Filter on process", TypeParameter.ARRAY, null, "Job manage only process which mach the filter. If no filter is given, all processes are inspected");

    /**
     * it's faster to delete 100 per 100
     */
    private static int casePerDeletion = 100;

    public MilkDeleteCases() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the deleteCase, nothing is required
     */
    public List<BEvent> checkPluginEnvironment(long tenantId, APIAccessor apiAccessor) {
        return new ArrayList<>();
    };

    @Override
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        // is the command Exist ? 
        return new ArrayList<>();
    }

    @Override
    public MilkJobOutput execute(MilkJobExecution input, APIAccessor apiAccessor) {
        MilkJobOutput plugTourOutput = input.getMilkJobOutput();

        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        // get Input 
        @SuppressWarnings("unchecked")
        List<String> listProcessName = (List<String>) input.getInputListParameter(cstParamProcessFilter);
        Long maximumArchiveDeletionPerRound = input.getInputLongParameter(cstParamMaximumDeletion);
        if (maximumArchiveDeletionPerRound > 10000)
            maximumArchiveDeletionPerRound = 10000L;
        try {

            List<Long> listProcessDefinitionId = new ArrayList<Long>();

            if (listProcessName != null && listProcessName.size() > 0) {

                for (String processName : listProcessName) {
                    SearchOptionsBuilder searchOptionBuilder = new SearchOptionsBuilder(0, 1000);
                    searchOptionBuilder.filter(ProcessDeploymentInfoSearchDescriptor.NAME, processName);
                    SearchResult<ProcessDeploymentInfo> searchProcessDeployment = processAPI
                            .searchProcessDeploymentInfos(searchOptionBuilder.done());
                    if (searchProcessDeployment.getCount() == 0) {
                        plugTourOutput.addEvent(new BEvent(eventNoProcessMatchFilter, "Filter[" + processName + "]"));

                    }
                    for (ProcessDeploymentInfo processInfo : searchProcessDeployment.getResult()) {
                        listProcessDefinitionId.add(processInfo.getProcessId());
                    }
                }
            }

            if (listProcessDefinitionId.size() == 0) {
                // filter given, no process found : stop now
                plugTourOutput.addEvent(eventNoProcessForFilter);
                plugTourOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return plugTourOutput;
            }

            /*
             * long totalCaseToDelete=0;
             * for (Long processDefinitionId : listProcessDefinitionId)
             * {
             * totalCaseToDelete+= getNumberProcessInstance( processDefinitionId, processAPI );
             * }
             */

            // Faster by group  of casePerDeletion cases            
            int count = 0;
            int totalNumberCaseDeleted = 0;
            long totalTime = 0;
            while (count < maximumArchiveDeletionPerRound) {
                for (Long processDefinitionId : listProcessDefinitionId) {
                    try {
                        long beginTime = System.currentTimeMillis();

                        int realCasePerDeletion = (int) (maximumArchiveDeletionPerRound - count);
                        if (realCasePerDeletion > casePerDeletion)
                            realCasePerDeletion = casePerDeletion;
                        long numberActiveDeleted = processAPI.deleteProcessInstances(processDefinitionId, 0, realCasePerDeletion * 3) / 3;
                        totalNumberCaseDeleted += numberActiveDeleted;

                        long numberArchivedDeleted = processAPI.deleteArchivedProcessInstances(processDefinitionId, 0, realCasePerDeletion * 3) / 3;
                        long endTime = System.currentTimeMillis();
                        logger.info("MilkDeleteCase - delete " + (numberActiveDeleted + numberArchivedDeleted) + " in " + (endTime - beginTime) + " ms");
                        totalNumberCaseDeleted += numberArchivedDeleted;
                        totalTime += (endTime - beginTime);

                    } catch (Exception e) {
                        plugTourOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + processDefinitionId));
                    }
                }
                count += casePerDeletion;
            }

            long totalCaseAfter = 0;
            for (Long processDefinitionId : listProcessDefinitionId) {
                totalCaseAfter += getNumberProcessInstance(processDefinitionId, processAPI);
            }

            plugTourOutput.addEvent(new BEvent(eventDeletionSuccess, "Purge:" + totalNumberCaseDeleted + " in " + totalTime + " ms, still " + totalCaseAfter + " cases to deleted"));
            plugTourOutput.executionStatus = totalNumberCaseDeleted == 0 ? ExecutionStatus.SUCCESSNOTHING : ExecutionStatus.SUCCESS;

        } catch (Exception e1) {
            plugTourOutput.addEvent(new BEvent(eventDeletionFailed, e1, ""));
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
        }

        return plugTourOutput;
    }

    @Override
    public PlugInDescription getDefinitionDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();
        plugInDescription.name = "DeleteCases";
        plugInDescription.category = CATEGORY.CASES;
        plugInDescription.label = "Delete Cases (active and archived)";
        plugInDescription.description = "Delete all cases in the given process, by a limitation given in parameters";
        plugInDescription.addParameter(cstParamMaximumDeletion);
        plugInDescription.addParameter(cstParamProcessFilter);

        /*
         * plugInDescription.addParameterFromMapJson(
         * "{\"delayinmn\":10,\"maxtentative\":12,\"processfilter\":[]}");
         */
        return plugInDescription;
    }

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
            return -1;
        }

    }

}
