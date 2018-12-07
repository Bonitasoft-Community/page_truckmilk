package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ActivationState;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfoSearchDescriptor;
import org.bonitasoft.engine.exception.DeletionException;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;

public class MilkPurgeArchive extends MilkPlugIn {

  static Logger logger = Logger.getLogger(MilkPurgeArchive.class.getName());

  private static BEvent eventNoProcessMatchFilter = new BEvent(MilkPurgeArchive.class.getName(), 1,
      Level.APPLICATIONERROR,
      "No process match filter", "No process is found with the given filter", "This filter does not apply.",
      "Check the process name");

  private static BEvent eventNoProcessForFilter = new BEvent(MilkPurgeArchive.class.getName(), 2,
      Level.APPLICATIONERROR,
      "Filter is not active", "No processes was found for all the filter, search will not run",
      "No filter at all apply, assuming configuration want to apply only on some process",
      "Check the process name");

  private static BEvent eventDeletionSuccess = new BEvent(MilkPurgeArchive.class.getName(), 3, Level.SUCCESS,
      "Deletion done with success", "Archived Cases are deleted with success");

  private static BEvent eventDeletionFailed = new BEvent(MilkPurgeArchive.class.getName(), 4, Level.ERROR,
      "Error during deletion", "An error arrived during the deletion of archived cases", "Cases are not deleted", "Check the exception");

  private static BEvent eventSearchFailed = new BEvent(MilkPurgeArchive.class.getName(), 5, Level.ERROR,
      "Search failed", "Search failed task return an error", "No retry can be performed", "Check the error");

  private static PlugInParameter cstParamDelayInDay = PlugInParameter.createInstance("delayinday", TypeParameter.LONG, 10L, null);
  private static PlugInParameter cstParamMaximumdeletion = PlugInParameter.createInstance("maximumdeletion", TypeParameter.LONG, 10L, null);
  private static PlugInParameter cstParamProcessfilter = PlugInParameter.createInstance("processfilter", TypeParameter.ARRAY, null, null);

  /**
   * it's embedded
   */
  public boolean isEmbeded() {
    return true;
  };

  /**
   * check the environment : for the milkEmailUsersTasks, we require to be able to send an email
   */
  public List<BEvent> checkEnvironment(long tenantId) {
    return new ArrayList<BEvent>();
  };

  @SuppressWarnings("unchecked")
  @Override
  public PlugTourOutput execute(PlugTourInput input, APIAccessor apiAccessor) {
    PlugTourOutput plugTourOutput = input.getPlugTourOutput();

    ProcessAPI processAPI = apiAccessor.getProcessAPI();
    // get Input 
    List<String> listProcessName = (List<String>) input.getInputListParameter(cstParamProcessfilter);
    Long delayInDay = input.getInputLongParameter(cstParamDelayInDay);
    Long maximumArchiveDeletionPerRound = input.getInputLongParameter(cstParamMaximumdeletion);
    if (maximumArchiveDeletionPerRound > 50)
      maximumArchiveDeletionPerRound = 20L;
    try {

      List<Long> listProcessDefinitionId = new ArrayList<Long>();

      // Filter on process?
      SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, maximumArchiveDeletionPerRound.intValue());

      if (listProcessName != null && listProcessName.size() > 0) {

        for (String processName : listProcessName) {
          SearchOptionsBuilder searchOptionBuilder = new SearchOptionsBuilder(0, 1000);
          searchOptionBuilder.filter(ProcessDeploymentInfoSearchDescriptor.NAME, processName);
          searchOptionBuilder.filter(ProcessDeploymentInfoSearchDescriptor.ACTIVATION_STATE,
              ActivationState.ENABLED.name());
          SearchResult<ProcessDeploymentInfo> searchProcessDeployment = processAPI
              .searchProcessDeploymentInfos(searchOptionBuilder.done());
          if (searchProcessDeployment.getCount() == 0) {
            plugTourOutput.addEvent(new BEvent(eventNoProcessMatchFilter, "Filter[" + processName + "]"));

          }
          for (ProcessDeploymentInfo processInfo : searchProcessDeployment.getResult()) {
            listProcessDefinitionId.add(processInfo.getProcessId());
          }
        }
        if (listProcessDefinitionId.size() == 0) {
          // filter given, no process found : stop now
          plugTourOutput.addEvent(eventNoProcessForFilter);
          plugTourOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
          return plugTourOutput;
        }
        searchActBuilder.leftParenthesis();
        for (int i = 0; i < listProcessDefinitionId.size(); i++) {
          if (i > 0)
            searchActBuilder.or();

          searchActBuilder.filter(ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID,
              listProcessDefinitionId.get(i));
        }
        searchActBuilder.rightParenthesis();
        searchActBuilder.and();

      } // end list process name

      Date currentDate = new Date();
      long timeSearch = currentDate.getTime() - delayInDay * 1000 * 60 * 60 * 24;

      searchActBuilder.greaterOrEquals(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, timeSearch);
      searchActBuilder.sort(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, Order.ASC);
      SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance;
      searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());
      if (searchArchivedProcessInstance.getCount() == 0) {
        plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
        return plugTourOutput;
      }
      
      List<Long> sourceProcessInstanceIds = new ArrayList<Long>();

      for (ArchivedProcessInstance archivedProcessInstance : searchArchivedProcessInstance.getResult()) {
        sourceProcessInstanceIds.add(archivedProcessInstance.getSourceObjectId());
      }
      try {
        processAPI.deleteArchivedProcessInstancesInAllStates(sourceProcessInstanceIds);
        plugTourOutput.addEvent(new BEvent(eventDeletionSuccess, "Purge:" + sourceProcessInstanceIds.size()));

      } catch (DeletionException e) {
        logger.severe("Error Delete Archived ProcessInstance=[" + sourceProcessInstanceIds + "] Error[" + e.getMessage() + "]");
        plugTourOutput.executionStatus = ExecutionStatus.ERROR;
        plugTourOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + sourceProcessInstanceIds));
      }
      plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;

    } catch (SearchException e1) {
      plugTourOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
      plugTourOutput.executionStatus = ExecutionStatus.ERROR;
    }

    return plugTourOutput;
  }

  @Override
  public PlugInDescription getDescription() {
    PlugInDescription plugInDescription = new PlugInDescription();
    plugInDescription.name = "PurgeArchivedCase";
    plugInDescription.displayName = "Purge Archived Case";
    plugInDescription.description = "Purge all archived case according the filter. Filter based on different process, and purge cases older than the delai. At each round, a maximum case are deleted. If the maximum is over than 20, it's reduce to 20.";
    plugInDescription.addParameter(cstParamDelayInDay);
    plugInDescription.addParameter(cstParamMaximumdeletion);
    plugInDescription.addParameter(cstParamProcessfilter);

    /*
     * plugInDescription.addParameterFromMapJson(
     * "{\"delayinmn\":10,\"maxtentative\":12,\"processfilter\":[]}");
     */
    return plugInDescription;
  }
}
