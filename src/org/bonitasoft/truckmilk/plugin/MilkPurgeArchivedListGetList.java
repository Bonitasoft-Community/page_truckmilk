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
import org.bonitasoft.engine.bpm.process.ProcessInstanceSearchDescriptor;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.ExecutionStatus;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.ListProcessesResult;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

/**
 * this job calculate the list of case to purge, and save the list in a the FileParameters
 */
public class MilkPurgeArchivedListGetList extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkPurgeArchivedListGetList.class.getName());


    private static BEvent eventSearchFailed = new BEvent(MilkPurgeArchivedListGetList.class.getName(), 1, Level.ERROR,
            "Search failed", "Search failed task return an error", "No retry can be performed", "Check the error");

    private static BEvent eventWriteReportError = new BEvent(MilkPurgeArchivedListGetList.class.getName(), 2, Level.ERROR,
            "Report generation error", "Error arrived during the generation of the report", "No report is available", "Check the error");

    private static BEvent eventSynthesisReport = new BEvent(MilkPurgeArchivedListGetList.class.getName(), 3, Level.INFO,
            "Report Synthesis", "Result of search", "", "");

    private static PlugInParameter cstParamDelayInDay = PlugInParameter.createInstance("delayinday", "Delai in days", TypeParameter.DELAY, MilkPlugInToolbox.DELAYSCOPE.YEAR + ":1", "Only cases archived before this delay are in the perimeter");
    private static PlugInParameter cstParamMaximumInReport = PlugInParameter.createInstance("maximuminreport", "Maximum in report", TypeParameter.LONG, 10000L, "Job stops when then number of case to delete reach this limit, in order to not create a very huge file");
    private static PlugInParameter cstParamMaximumInMinutes = PlugInParameter.createInstance("maximuminminutes", "Maximum in minutes", TypeParameter.LONG, 3L, "Job stops when the execution reach this limit, in order to not overload the server.");
    private static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Process filter", TypeParameter.ARRAYPROCESSNAME, null, "Only processes in the list will be in the perimeter. No filter means all processes will be in the perimeter. Tips: give 'processName;version' to specify a special version of the process, else all versions of the process are processed");
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
        return new ArrayList<>();
    };

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        return new ArrayList<>();

    };

    @Override
    public PlugTourOutput execute(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        PlugTourOutput plugTourOutput = jobExecution.getPlugTourOutput();

        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        // get Input 

        String separatorCSV = jobExecution.getInputStringParameter(cstParamSeparatorCSV);

        long maximumInReport = jobExecution.getInputLongParameter(cstParamMaximumInReport);
        jobExecution.setPleaseStopAfterManagedItems(maximumInReport, 1000000L);
        jobExecution.setPleaseStopAfterTime(jobExecution.getInputLongParameter(cstParamMaximumInMinutes), 24 * 60L);

        // 20 for the preparation, 100 to collect cases
        // Time to run the query take time, and we don't want to show 0% for a long time
        jobExecution.setAvancementTotalStep(120);
        try {

            SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, (int) maximumInReport);

            ListProcessesResult listProcessResult = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, true, searchActBuilder, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, apiAccessor.getProcessAPI());

            if (listProcessResult.listProcessDeploymentInfo.isEmpty()) {
                // filter given, no process found : stop now
                plugTourOutput.addEvents(listProcessResult.listEvents);
                plugTourOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return plugTourOutput;
            }

            


            DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamDelayInDay, new Date(), false);
            if (BEventFactory.isError( delayResult.listEvents)) {
                plugTourOutput.addEvents( delayResult.listEvents );
                plugTourOutput.executionStatus = ExecutionStatus.ERROR;
                return plugTourOutput;
            }
            long timeSearch = delayResult.delayDate.getTime();


            jobExecution.setAvancementStep(5);

            // -------------- now get the list of cases
            searchActBuilder.lessOrEquals(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, timeSearch);
            searchActBuilder.sort(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, Order.ASC);
            SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance;
            searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());

            /** ok, we did 15 step */
            jobExecution.setAvancementStep(20);

            Map<Long, String> cacheProcessDefinition = new HashMap<>();

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

            plugTourOutput.addEvent(new BEvent(eventSynthesisReport, "Total cases:" + searchArchivedProcessInstance.getCount() + ", In list:" + countInArchive));

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
            plugTourOutput.addEvent(new BEvent(eventWriteReportError, e1, ""));
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
        plugInDescription.addParameter(cstParamProcessFilter);
        plugInDescription.addParameter(cstParamSeparatorCSV);

        plugInDescription.addParameter(cstParamReport);
        return plugInDescription;
    }
}
