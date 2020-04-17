package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.List;

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
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.ListProcessesResult;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;

/**
 * Delete all cases in a process
 */
public class MilkDeleteCases extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkDeleteCases.class.getName());

    private static BEvent eventDeletionSuccess = new BEvent(MilkDeleteCases.class.getName(), 1, Level.SUCCESS,
            "Deletion done with success", "Archived Cases are deleted with success");

    private static BEvent eventDeletionFailed = new BEvent(MilkDeleteCases.class.getName(), 2, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion of archived cases", "Cases are not deleted", "Check the exception");

    private static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Filter on process", TypeParameter.ARRAYPROCESSNAME, null, "Job manage only process which mach the filter. If no filter is given, all processes are inspected", true);

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
    public List<BEvent> checkPluginEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();
    };

    @Override
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution) {
        // is the command Exist ? 
        return new ArrayList<>();
    }
    @Override
    public MilkPlugInDescription getDefinitionDescription() {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName( "DeleteCases");
        plugInDescription.setLabel( "Delete Cases (active and archived)");
        plugInDescription.setExplanation( "Delete all cases in the given process, by a limitation given in parameters");
        plugInDescription.setWarning("This plugin delete ACTIVES and archived cases. A case deleted can't be retrieved. Operation is final. Use with caution.");

        plugInDescription.setCategory( CATEGORY.CASES);
        plugInDescription.setStopJob( JOBSTOPPER.BOTH );
        plugInDescription.addParameter(cstParamProcessFilter);
        
        return plugInDescription;
    }
    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();

        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        // get Input 
        SearchOptionsBuilder searchBuilderCase = new SearchOptionsBuilder(0, jobExecution.getJobStopAfterMaxItems() + 1);

        int maximumArchiveDeletionPerRound = jobExecution.getJobStopAfterMaxItems();
        if (maximumArchiveDeletionPerRound > 100000)
            maximumArchiveDeletionPerRound = 100000;
        try {
            ListProcessesResult listProcessResult = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, true, searchBuilderCase, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                plugTourOutput.addEvents(listProcessResult.listEvents);
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
            int countNumberThisPass=1;
            while (count < maximumArchiveDeletionPerRound && countNumberThisPass>0) {
                if ( jobExecution.pleaseStop())
                    break;
                countNumberThisPass=0;
                for (ProcessDeploymentInfo processDefinition : listProcessResult.listProcessDeploymentInfo) {
                    if ( jobExecution.pleaseStop())
                        break;
                    try {
                        long beginTime = System.currentTimeMillis();

                        int realCasePerDeletion = (int) (maximumArchiveDeletionPerRound - count);
                        if (realCasePerDeletion > casePerDeletion)
                            realCasePerDeletion = casePerDeletion;
                        long numberActiveDeleted = processAPI.deleteProcessInstances(processDefinition.getProcessId(), 0, realCasePerDeletion * 3) / 3;
                        totalNumberCaseDeleted += numberActiveDeleted;
                        countNumberThisPass+= numberActiveDeleted;
                        long numberArchivedDeleted = processAPI.deleteArchivedProcessInstances(processDefinition.getProcessId(), 0, realCasePerDeletion * 3) / 3;
                        long endTime = System.currentTimeMillis();
                        logger.info("MilkDeleteCase - delete " + (numberActiveDeleted + numberArchivedDeleted) + " in " + (endTime - beginTime) + " ms");
                        totalNumberCaseDeleted += numberArchivedDeleted;
                        countNumberThisPass+= numberArchivedDeleted;
                        totalTime += (endTime - beginTime);

                    } catch (Exception e) {
                        plugTourOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + processDefinition.getName()+"("+processDefinition.getVersion()+")"));
                    }
                }
                count += casePerDeletion;
            }

            long totalCaseAfter = 0;
            for (ProcessDeploymentInfo processDefinition : listProcessResult.listProcessDeploymentInfo) {
                totalCaseAfter += getNumberProcessInstance(processDefinition.getProcessId(), processAPI);
            }

            plugTourOutput.addEvent(new BEvent(eventDeletionSuccess, "Purge:" + totalNumberCaseDeleted + " in " + totalTime + " ms, still " + totalCaseAfter + " cases to deleted"));
            if (totalNumberCaseDeleted == 0)
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
            else if (jobExecution.pleaseStop())
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;
            else    
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
            
            
        } catch (Exception e1) {
            plugTourOutput.addEvent(new BEvent(eventDeletionFailed, e1, ""));
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
        }

        return plugTourOutput;
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
