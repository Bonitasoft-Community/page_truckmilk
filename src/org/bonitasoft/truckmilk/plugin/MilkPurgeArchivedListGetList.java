package org.bonitasoft.truckmilk.plugin;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ActivationState;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfoSearchDescriptor;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

/**
 * this job calculate the list of case to purge, and save the list in a the FileParameters
 */
public class MilkPurgeArchivedListGetList extends MilkPlugIn {

    static Logger logger = Logger.getLogger(MilkPurgeArchive.class.getName());

    private static BEvent eventNoProcessMatchFilter = new BEvent(MilkPurgeArchivedListGetList.class.getName(), 1,
            Level.APPLICATIONERROR,
            "No process match filter", "No process is found with the given filter", "This filter does not apply.",
            "Check the process name");

    private static BEvent eventNoProcessForFilter = new BEvent(MilkPurgeArchivedListGetList.class.getName(), 2,
            Level.APPLICATIONERROR,
            "Filter is not active", "No processes was found for all the filter, search will not run",
            "No filter at all apply, assuming configuration want to apply only on some process",
            "Check the process name");

    private static BEvent eventSearchFailed = new BEvent(MilkPurgeArchivedListGetList.class.getName(), 5, Level.ERROR,
            "Search failed", "Search failed task return an error", "No retry can be performed", "Check the error");

    private static BEvent EVENT_WRITEREPORT_ERROR = new BEvent(MilkPurgeArchivedListGetList.class.getName(), 6, Level.ERROR,
            "Report generation error", "Error arrived during the generation of the report", "No report is available", "Check the error");

    private static PlugInParameter cstParamDelayInDay = PlugInParameter.createInstance("delayinday", "Delai in days", TypeParameter.LONG, 10L, "Only cases archived before this delay are in the perimeter");
    private static PlugInParameter cstParamMaximumInReport = PlugInParameter.createInstance("maximuminreport", "Maximum in report", TypeParameter.LONG, 10000L, "Job stops when then number of case to delete reach this limit, in order to not create a very huge file");
    private static PlugInParameter cstParamMaximumInMinutes = PlugInParameter.createInstance("maximuminminutes", "Maximum in minutes", TypeParameter.LONG, 3L, "Job stops when the execution reach this limit, in order to not overload the server.");
    private static PlugInParameter cstParamProcessfilter = PlugInParameter.createInstance("processfilter", "Process filter", TypeParameter.ARRAY, null, "Only processes in the list will be in the perimeter. No filter means all processes will be in the perimeter");
    private static PlugInParameter cstParamReport = PlugInParameter.createInstanceFile("report", "Report", TypeParameter.FILEWRITE, null, "List is calculated and saved in this parameter", "ListToPurge.csv", "text/csv");

    public MilkPurgeArchivedListGetList() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    protected static String cstColCaseId = "caseid";
    protected static String cstColStatus = "status";

    /**
     * check the environment : for the milkEmailUsersTasks, we require to be able to send an email
     */
    public List<BEvent> checkEnvironment(long tenantId, APIAccessor apiAccessor) {
        return new ArrayList<BEvent>();
    };

    @SuppressWarnings("unchecked")
    @Override
    public PlugTourOutput execute(MilkJobExecution plugInTourExecution, APIAccessor apiAccessor) {
        PlugTourOutput plugTourOutput = plugInTourExecution.getPlugTourOutput();

        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        // get Input 
        List<String> listProcessName = (List<String>) plugInTourExecution.getInputListParameter(cstParamProcessfilter);

        long delayInDay = plugInTourExecution.getInputLongParameter(cstParamDelayInDay);
        plugInTourExecution.setPleaseStopAfterManagedItems(plugInTourExecution.getInputLongParameter(cstParamMaximumInReport), 1000000L);
        plugInTourExecution.setPleaseStopAfterTime(plugInTourExecution.getInputLongParameter(cstParamMaximumInMinutes), 24 * 60L);
        try {

            List<Long> listProcessDefinitionId = new ArrayList<Long>();

            // Filter on process?
            SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, plugInTourExecution.getPleaseStopAfterManagerItems().intValue());

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

            // -------------- now get the list of cases
            Date currentDate = new Date();
            long timeSearch = currentDate.getTime() - delayInDay * 1000 * 60 * 60 * 24;

            searchActBuilder.lessOrEquals(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, timeSearch);
            searchActBuilder.sort(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, Order.ASC);
            SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance;
            searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());

            Map<Long, String> cacheProcessDefinition = new HashMap<Long, String>();

            ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
            Writer w = new OutputStreamWriter(arrayOutputStream);

            w.write(cstColCaseId + ";processname;processversion;archiveddate;" + cstColStatus + "\n");

            // loop on archive
            for (ArchivedProcessInstance archivedProcessInstance : searchArchivedProcessInstance.getResult()) {
                if (plugInTourExecution.pleaseStop())
                    break;
                String line = String.valueOf(archivedProcessInstance.getSourceObjectId()) + ";";
                long processId = archivedProcessInstance.getProcessDefinitionId();
                if (!cacheProcessDefinition.containsKey(processId)) {
                    try {
                        ProcessDefinition processDefinition = processAPI.getProcessDefinition(processId);
                        cacheProcessDefinition.put(processId, processDefinition.getName() + ";" + processDefinition.getVersion() + ";");
                    } catch (Exception e) {
                        cacheProcessDefinition.put(processId, " ; ;");
                    }
                }
                line += cacheProcessDefinition.get(processId);
                line += TypesCast.sdfCompleteDate.format(archivedProcessInstance.getArchiveDate());
                line += ";";
                w.write(line + "\n");
                plugInTourExecution.addManagedItem( 1 );
            }
            w.flush();
            plugTourOutput.nbItemsProcessed = searchArchivedProcessInstance.getCount();
            plugTourOutput.setParameterStream(cstParamReport, new ByteArrayInputStream(arrayOutputStream.toByteArray()));

            if (searchArchivedProcessInstance.getCount() == 0) {
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
                plugTourOutput.nbItemsProcessed = 0;
                return plugTourOutput;
            }

            plugTourOutput.executionStatus = plugInTourExecution.pleaseStop()? ExecutionStatus.SUCCESSPARTIAL : ExecutionStatus.SUCCESS;
        }

        catch (SearchException e1) {
            plugTourOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
        } catch (Exception e1) {
            plugTourOutput.addEvent(new BEvent(EVENT_WRITEREPORT_ERROR, e1, ""));
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
        }

        return plugTourOutput;
    }

    @Override
    public PlugInDescription getDefinitionDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();
        plugInDescription.name = "ListPurgeCase";
        plugInDescription.label = "Purge Archived Cases: get List (no purge)";
        plugInDescription.description = "Calculate the list of case to be purge, and update the report with the list (CSV Format).";
        plugInDescription.addParameter(cstParamDelayInDay);
        plugInDescription.addParameter(cstParamMaximumInReport);
        plugInDescription.addParameter(cstParamMaximumInMinutes);
        plugInDescription.addParameter(cstParamProcessfilter);
        plugInDescription.addParameter(cstParamReport);
        return plugInDescription;
    }
}
