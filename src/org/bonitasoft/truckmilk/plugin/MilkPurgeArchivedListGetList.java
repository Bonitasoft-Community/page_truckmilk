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
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

/**
 * this job calculate the list of case to purge, and save the list in a the FileParameters
 */
public class MilkPurgeArchivedListGetList extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkPurgeArchive.class.getName());

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

    private static BEvent EVENT_SYNTHESISREPORT = new BEvent(MilkPurgeArchivedListGetList.class.getName(), 7, Level.INFO,
            "Report Synthesis", "Result of search", "", "");

    private static PlugInParameter cstParamDelayInDay = PlugInParameter.createInstance("delayinday", "Delai in days", TypeParameter.LONG, 10L, "Only cases archived before this delay are in the perimeter");
    private static PlugInParameter cstParamMaximumInReport = PlugInParameter.createInstance("maximuminreport", "Maximum in report", TypeParameter.LONG, 10000L, "Job stops when then number of case to delete reach this limit, in order to not create a very huge file");
    private static PlugInParameter cstParamMaximumInMinutes = PlugInParameter.createInstance("maximuminminutes", "Maximum in minutes", TypeParameter.LONG, 3L, "Job stops when the execution reach this limit, in order to not overload the server.");
    private static PlugInParameter cstParamProcessfilter = PlugInParameter.createInstance("processfilter", "Process filter", TypeParameter.ARRAY, null, "Only processes in the list will be in the perimeter. No filter means all processes will be in the perimeter. Tips: give 'processName;version' to specify a special version of the process, else all versions of the process are processed");
    private static PlugInParameter cstParamSeparatorCSV = PlugInParameter.createInstance("separatorCSV", "Separator CSV", TypeParameter.STRING, ",", "CSV use a separator. May be ; or ,");
    private static PlugInParameter cstParamReport = PlugInParameter.createInstanceFile("report", "List of cases", TypeParameter.FILEWRITE, null, "List is calculated and saved in this parameter", "ListToPurge.csv", "application/CSV");

    public MilkPurgeArchivedListGetList() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    protected static String cstColCaseId = "caseid";
    protected static String cstColStatus = "status";

    /**
     * check the environment : for the milkEmailUsersTasks, we require to be able to send an email
     */
    public List<BEvent> checkPluginEnvironment(long tenantId, APIAccessor apiAccessor) {
        return new ArrayList<BEvent>();
    };

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        return listEvents;
    };

    @SuppressWarnings("unchecked")
    @Override
    public PlugTourOutput execute(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        PlugTourOutput plugTourOutput = jobExecution.getPlugTourOutput();

        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        // get Input 
        List<String> listProcessName = (List<String>) jobExecution.getInputListParameter(cstParamProcessfilter);
        String separatorCSV = jobExecution.getInputStringParameter(cstParamSeparatorCSV);

        long delayInDay = jobExecution.getInputLongParameter(cstParamDelayInDay);
        jobExecution.setPleaseStopAfterManagedItems(jobExecution.getInputLongParameter(cstParamMaximumInReport), 1000000L);
        jobExecution.setPleaseStopAfterTime(jobExecution.getInputLongParameter(cstParamMaximumInMinutes), 24 * 60L);

        // 20 for the preparation, 100 to collect cases
        // Time to run the query take time, and we don't want to show 0% for a long time
        jobExecution.setAvancementTotalStep(120);
        try {

            List<Long> listProcessDefinitionId = new ArrayList<Long>();

            // Filter on process?
            SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, jobExecution.getPleaseStopAfterManagerItems().intValue());

            if (listProcessName != null && listProcessName.size() > 0) {

                for (String fullProcessName : listProcessName) {
                    SearchOptionsBuilder searchOptionBuilder = new SearchOptionsBuilder(0, 1000);
                    String processName = fullProcessName;
                    String version = null;
                    if (fullProcessName.lastIndexOf(";") != -1) {
                        int pos = fullProcessName.lastIndexOf(";");
                        processName = fullProcessName.substring(0, pos);
                        version = fullProcessName.substring(pos + 1);
                    }
                    searchOptionBuilder.filter(ProcessDeploymentInfoSearchDescriptor.NAME, processName);
                    if (version != null)
                        searchOptionBuilder.filter(ProcessDeploymentInfoSearchDescriptor.VERSION, version);
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

            jobExecution.setAvancementStep(5);

            // -------------- now get the list of cases
            Date currentDate = new Date();
            long timeSearch = currentDate.getTime() - delayInDay * 1000 * 60 * 60 * 24;

            searchActBuilder.lessOrEquals(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, timeSearch);
            searchActBuilder.sort(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, Order.ASC);
            SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance;
            searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());

            /** ok, we did 15 step */
            jobExecution.setAvancementStep(20);

            Map<Long, String> cacheProcessDefinition = new HashMap<Long, String>();

            ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
            Writer w = new OutputStreamWriter(arrayOutputStream);

            w.write(cstColCaseId + separatorCSV + "processname" + separatorCSV + "processversion" + separatorCSV + "archiveddate" + separatorCSV + cstColStatus + "\n");

            // loop on archive
            int countInArchive = 0;
            for (ArchivedProcessInstance archivedProcessInstance : searchArchivedProcessInstance.getResult()) {

                if (jobExecution.pleaseStop())
                    break;
                countInArchive++;
                String line = String.valueOf(archivedProcessInstance.getSourceObjectId()) + separatorCSV;
                long processId = archivedProcessInstance.getProcessDefinitionId();
                if (!cacheProcessDefinition.containsKey(processId)) {
                    try {
                        ProcessDefinition processDefinition = processAPI.getProcessDefinition(processId);
                        cacheProcessDefinition.put(processId, processDefinition.getName() + separatorCSV + processDefinition.getVersion() + separatorCSV);
                    } catch (Exception e) {
                        cacheProcessDefinition.put(processId, separatorCSV + " " + separatorCSV);
                    }
                }
                line += cacheProcessDefinition.get(processId);
                line += TypesCast.sdfCompleteDate.format(archivedProcessInstance.getArchiveDate());
                line += ";";
                w.write(line + "\n");
                jobExecution.addManagedItem(1);

                jobExecution.setAvancementStep(20 + (int) (100 * countInArchive / searchArchivedProcessInstance.getResult().size()));

            }
            w.flush();
            plugTourOutput.nbItemsProcessed = searchArchivedProcessInstance.getCount();
            plugTourOutput.setParameterStream(cstParamReport, new ByteArrayInputStream(arrayOutputStream.toByteArray()));

            plugTourOutput.addEvent(new BEvent(EVENT_SYNTHESISREPORT, "Total cases:" + searchArchivedProcessInstance.getCount() + ", In list:" + countInArchive));

            if (searchArchivedProcessInstance.getCount() == 0) {
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
                plugTourOutput.nbItemsProcessed = 0;
                return plugTourOutput;
            }

            plugTourOutput.executionStatus = jobExecution.pleaseStop() ? ExecutionStatus.SUCCESSPARTIAL : ExecutionStatus.SUCCESS;
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
        plugInDescription.addParameter(cstParamSeparatorCSV);

        plugInDescription.addParameter(cstParamReport);
        return plugInDescription;
    }
}
