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
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkJobOutput.Chronometer;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter.FilterProcess;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.ListProcessesResult;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
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
            .withMandatory(true)
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
        plugInDescription.setName( "DeleteCases");
        plugInDescription.setLabel( "Delete Cases (active and archived)");
        plugInDescription.setExplanation( "Delete all cases in the given process, by a limitation given in parameters");
        plugInDescription.setWarning("This plugin delete ACTIVES and archived cases. A case deleted can't be retrieved. Operation is final. Use with caution.");

        plugInDescription.setCategory( CATEGORY.CASES);
        plugInDescription.setStopJob( JOBSTOPPER.BOTH );
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
            ListProcessesResult listProcessResult = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, true, searchBuilderCase, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);

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
            if (delayResult.delayInMs ==0)
                totalCaseDeleted= deleteCasesNoDelay(jobExecution, listProcessResult, milkJobOutput );
            else
                totalCaseDeleted = deleteCasesWithDelay(jobExecution, listProcessResult, delayResult, milkJobOutput);
            

            milkJobOutput.setNbItemsProcessed(totalCaseDeleted);
          
            milkJobOutput.endChronometer( deletionMarker );
            
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
    private int deleteCasesNoDelay(MilkJobExecution jobExecution, ListProcessesResult listProcessResult, MilkJobOutput milkJobOutput ) {
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();

        int maximumArchiveDeletionPerRound = jobExecution.getJobStopAfterMaxItems();
        if (maximumArchiveDeletionPerRound > 100000)
            maximumArchiveDeletionPerRound = 100000;
        
        
        // Faster by group  of casePerDeletion cases            
        int count = 0;
        int totalNumberCaseDeleted = 0;
        int countNumberThisPass=1;
        while (count < maximumArchiveDeletionPerRound && countNumberThisPass>0) {
            if ( jobExecution.pleaseStop())
                return totalNumberCaseDeleted;
            countNumberThisPass=0;
            for (ProcessDeploymentInfo processDefinition : listProcessResult.listProcessDeploymentInfo) {
                if ( jobExecution.pleaseStop())
                    return totalNumberCaseDeleted;
                try {

                    int realCasePerDeletion = maximumArchiveDeletionPerRound - count;
                    if (realCasePerDeletion > casePerDeletion)
                        realCasePerDeletion = casePerDeletion;
                        
                        long caseBefore=getNumberProcessInstance(processDefinition.getProcessId(), processAPI);
                    
                        // we can't trust the result
                        Chronometer processInstanceMarker = milkJobOutput.beginChronometer("deleteProcessInstance-"+processDefinition.getName()+"("+processDefinition.getVersion()+")");
                        long numberActiveDeleted = processAPI.deleteProcessInstances(processDefinition.getProcessId(), 0, realCasePerDeletion * 3);
                        milkJobOutput.endChronometer(processInstanceMarker);
                        
                        
                        Chronometer processArchiveMarker = milkJobOutput.beginChronometer("deleteArchiveProcessInstance-"+processDefinition.getName()+"("+processDefinition.getVersion()+")");
                        long numberArchivedDeleted = processAPI.deleteArchivedProcessInstances(processDefinition.getProcessId(), 0, realCasePerDeletion * 3);
                        milkJobOutput.endChronometer(processArchiveMarker);

                        long caseAfter=getNumberProcessInstance(processDefinition.getProcessId(), processAPI);
                        
                        
                        logger.fine("MilkDeleteCase - delete " + (numberActiveDeleted + numberArchivedDeleted) + " in " + processArchiveMarker.getTimeExecution() + " ms (Bonita record calculation:"+(numberActiveDeleted+numberArchivedDeleted)+")");
                        totalNumberCaseDeleted += (caseBefore - caseAfter);
                        countNumberThisPass+= (caseBefore - caseAfter);
                } catch (Exception e) {
                    milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + processDefinition.getName()+"("+processDefinition.getVersion()+")"));
                }
            }
            count += casePerDeletion;
        }
        return totalNumberCaseDeleted;
    }

    
    /**
     * 
     * @param processDefinitionId
     * @param processAPI
     * @return
     */
    private int deleteCasesWithDelay(MilkJobExecution jobExecution, ListProcessesResult listProcessResult, DelayResult delayResult, MilkJobOutput milkJobOutput) {
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();

        List<Long> processInstances = new ArrayList<>();
        List<Long> archivedProcessInstances = new ArrayList<>();
        int totalNumberCaseDeleted = 0;
        
    
        try {

            long timeSearch = delayResult.delayDate.getTime();
            
            for (ProcessDeploymentInfo processDefinition : listProcessResult.listProcessDeploymentInfo) {
                if ( jobExecution.pleaseStop())
                    return totalNumberCaseDeleted;
                SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance;

                // ------------- Active

                SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0,jobExecution.getJobStopAfterMaxItems()+1); 
                searchActBuilder.filter(ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDefinition.getProcessId());
                searchActBuilder.lessOrEquals(ProcessInstanceSearchDescriptor.START_DATE, timeSearch);
                searchActBuilder.sort(ProcessInstanceSearchDescriptor.START_DATE, Order.ASC);

                SearchResult<ProcessInstance> searchProcessInstance = processAPI.searchProcessInstances(searchActBuilder.done());

                // do the purge now
                for (ProcessInstance processInstance : searchProcessInstance.getResult()) {
                    if (jobExecution.pleaseStop())
                        return totalNumberCaseDeleted;
                    // proceed page per page
                    processInstances.add(processInstance.getId());
                    if (processInstances.size() == 50) {
                        totalNumberCaseDeleted += purgeCases( processInstances, archivedProcessInstances, milkJobOutput, processAPI );
                    }
                }
                
            // ------------- Archived
            searchActBuilder = new SearchOptionsBuilder(0,jobExecution.getJobStopAfterMaxItems()+1); 
            searchActBuilder.filter(ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, processDefinition.getProcessId());
            searchActBuilder.lessOrEquals(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, timeSearch);
            searchActBuilder.sort(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, Order.ASC);

            searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());

            // do the purge now
            for (ArchivedProcessInstance archivedProcessInstance : searchArchivedProcessInstance.getResult()) {
                if (jobExecution.pleaseStop())
                    return totalNumberCaseDeleted;
                // proceed page per page
                archivedProcessInstances.add(archivedProcessInstance.getSourceObjectId());
                if (archivedProcessInstances.size() == 50) {
                    totalNumberCaseDeleted += purgeCases( processInstances, archivedProcessInstances, milkJobOutput, processAPI );
                }
            }
            }
            
            
            // reliquat
            totalNumberCaseDeleted += purgeCases( processInstances, archivedProcessInstances, milkJobOutput, processAPI );
        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge"));
        }
            return totalNumberCaseDeleted;
            
    }
    
    
    /**
     * 
     * @param sourceProcessInstanceIds
     * @param sourceArchiveProcessInstanceIds
     * @param milkJobOutput
     * @return
     * @throws DeletionException 
     */
    private int purgeCases(List<Long> processInstances, List<Long> archivedProcessInstances,MilkJobOutput milkJobOutput, ProcessAPI processAPI ) throws DeletionException {
       int totalNumberCaseDeleted = 0; 

        if (processInstances != null && processInstances.size() == 50) {
            Chronometer processInstanceMarker = milkJobOutput.beginChronometer("deleteProcessInstance");
            for (Long processInstance : processInstances)
                processAPI.deleteProcessInstance( processInstance );
            milkJobOutput.endChronometer(processInstanceMarker);
            totalNumberCaseDeleted += processInstances.size();
            processInstances.clear();
        }
    if (archivedProcessInstances != null && archivedProcessInstances.size() == 50) {
        Chronometer processInstanceMarker = milkJobOutput.beginChronometer("deleteProcessInstance");
        processAPI.deleteArchivedProcessInstancesInAllStates(archivedProcessInstances);
        milkJobOutput.endChronometer(processInstanceMarker);
        totalNumberCaseDeleted += archivedProcessInstances.size();
        archivedProcessInstances.clear();
    }
    return totalNumberCaseDeleted;
    }
    
    /**
     * 
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
