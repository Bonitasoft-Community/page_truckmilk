package org.bonitasoft.truckmilk.plugin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.flownode.ActivityStates;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.properties.BonitaEngineConnection;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkJobOutput.Chronometer;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInMeasurement;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.ListProcessesResult;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

public class MilkDeleteDuplicateTasks extends MilkPlugIn {

    static Logger logger = Logger.getLogger(MilkDeleteDuplicateTasks.class.getName());
    private final static String LOGGER_LABEL = "MilkDeleteDuplicateTasks ";

    private final static String CSTOPERATION_GETLIST = "Get List (No operation)";
    private final static String CSTOPERATION_DELETE = "Delete tasks";

    private final static BEvent eventErrorDeleteTask = new BEvent(MilkDeleteDuplicateTasks.class.getName(), 1,
            Level.APPLICATIONERROR,
            "Error deleted a task", "Deleted a task failed", "Task is not deleted",
            "Check exception");
    private static BEvent eventSearchFailed = new BEvent(MilkDeleteDuplicateTasks.class.getName(), 2, Level.ERROR,
            "Search failed", "Search failed task return an error", "No retry can be performed", "Check the error");

    private static PlugInParameter cstParamOperation = PlugInParameter.createInstanceListValues("operation", "operation",
            new String[] { CSTOPERATION_GETLIST, CSTOPERATION_DELETE }, CSTOPERATION_GETLIST, "Detect the list of duplicate task or delete them");

    private static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Process Filter", TypeParameter.ARRAYPROCESSNAME, null, "Give a list of process name. Name must be exact, no version is given (all versions will be purged)").withMandatory(true);

    private final static PlugInMeasurement cstMesureTasksDeleted = PlugInMeasurement.createInstance("TasksDeleted", "Tasks deleted", "Number of task deleted");
    private final static PlugInMeasurement cstMesureTasksError = PlugInMeasurement.createInstance("TasksError", "Task deletion error", "Number of deletion error");

    public MilkDeleteDuplicateTasks() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the milkEmailUsersTasks, we require to be able to send an email
     */
    public List<BEvent> checkPluginEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();
    }

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription() {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName("DeleteDuplicateTasks");
        plugInDescription.setLabel("Delete Duplicate Tasks");
        plugInDescription.setCategory(CATEGORY.TASKS);
        plugInDescription.setExplanation("3 operations: PURGE/ GET LIST / PURGE FROM LIST. Purge (or get the list of) archived case according the filter. Filter based on different process, and purge cases older than the delai. At each round with Purge / Purge From list, a maximum case are deleted. If the maximum is over than 100000, it's reduce to this limit.");
        plugInDescription.setWarning("A case purged can't be retrieved. Operation is final. Use with caution.");
        plugInDescription.addParameter(cstParamOperation);
        plugInDescription.addParameter(cstParamProcessFilter);

        plugInDescription.addMesure(cstMesureTasksDeleted);
        plugInDescription.addMesure(cstMesureTasksError);

        plugInDescription.setStopJob(JOBSTOPPER.BOTH);
        plugInDescription.setJobStopMaxItems(100000);
        return plugInDescription;
    }

    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput milkJobOutput = jobExecution.getMilkJobOutput();

        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();

        String policy = jobExecution.getInputStringParameter(cstParamOperation);

        List<Object> parameters = new ArrayList<>();
        StringBuilder sqlRequest = new StringBuilder();
        sqlRequest.append("select distinct b.id, b.rootcontainerid, b.name, b.reachedstatedate, pd.name as PROCESSNAME, pd.version as PROCESSVERSION ,pd.processid");
        sqlRequest.append(" from flownode_instance a, flownode_instance b, process_instance pi, process_definition pd");
        sqlRequest.append(" where a.rootcontainerid = b.rootcontainerid");
        sqlRequest.append(" and a.rootcontainerid = pi.id");
        sqlRequest.append(" and pi.processdefinitionid = pd.processid");

        sqlRequest.append("  and a.id < b.id ");
        sqlRequest.append("  and a.name = b.name and a.statename='ready' and b.statename='ready'");
        sqlRequest.append("  and a.tenantid = ? ");
        parameters.add( jobExecution.getTenantId());
        SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 100);

        try {
            ListProcessesResult listProcessResult = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, false, sob, ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, jobExecution.getApiAccessor().getProcessAPI());

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                milkJobOutput.addEvents(listProcessResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return milkJobOutput;
            }
            StringBuilder listProcessesId = new StringBuilder();
            for (ProcessDeploymentInfo processDeployment : listProcessResult.listProcessDeploymentInfo) {
                if (listProcessesId.length() > 0)
                    listProcessesId.append(", ");
                listProcessesId.append("?");
                parameters.add(processDeployment.getProcessId());
            }
            sqlRequest.append(" and pd.processid in (" + listProcessesId.toString() + ")");
            sqlRequest.append(" order by b.id");

            milkJobOutput.addReportInHtml(sqlRequest.toString() + "<br>");
            milkJobOutput.addReportInHtml("List processId :"+parameters.toString()+"<br>");

            Chronometer searchMarker = milkJobOutput.beginChronometer("searchTasks");
            List<Map<String, Object>> listResult = BonitaEngineConnection.executeSqlRequest(sqlRequest.toString(), parameters, jobExecution.getJobStopAfterMaxItems() + 1);
            milkJobOutput.endChronometer(searchMarker);

            if (CSTOPERATION_GETLIST.equals(policy)) {
                milkJobOutput.addReportTable(new String[] { "Case Id", "TaskId", "Task Name", "Process Name", "Process Version" });
                int max = listResult.size();
                if (max > jobExecution.getJobStopAfterMaxItems())
                    max = jobExecution.getJobStopAfterMaxItems();
                for (int i = 0; i < max; i++) {
                    Map<String, Object> report = listResult.get(i);
                    milkJobOutput.addReportLine(new Object[] { report.get("ROOTCONTAINERID"), report.get("ID"), report.get("NAME"), report.get("PROCESSNAME"), report.get("PROCESSVERSION") });
                }
                if (listResult.size() > jobExecution.getJobStopAfterMaxItems())
                    milkJobOutput.addReportLine("<td><colspan=\"5\">More object...</td></tr>");
                milkJobOutput.addReportEndTable();

                milkJobOutput.setNbItemsProcessed(max);
            } else if (CSTOPERATION_DELETE.equals(policy)) {

                int max = listResult.size();
                if (max > jobExecution.getJobStopAfterMaxItems())
                    max = jobExecution.getJobStopAfterMaxItems();

                boolean oneErrorDetected = false;
                int countCorrects = 0;
                int countErrors = 0;
                jobExecution.setAvancementTotalStep(max);

                for (int i = 0; i < max; i++) {
                    if (jobExecution.pleaseStop())
                        break;
                    Map<String, Object> report = listResult.get(i);
                    jobExecution.setAvancementStep(i);

                    // delete  pending_mapping where activityid=60025;
                    // delete  data_instance where containerid= 60025;
                    // delete  flownode_instance where id=60025;
                    Long taskId = TypesCast.getLong(report.get("ID"), null);
                    Long caseId = TypesCast.getLong(report.get("ROOTCONTAINERID"), null);
                    if (taskId == null) {
                        continue;
                    }

                    try (Connection con = BonitaEngineConnection.getConnection()) {
                        Chronometer purgeTasksMarker = milkJobOutput.beginChronometer("purgeTask");

                        // add the tenantId
                        executeQuery("update flownode_instance set STATECATEGORY='CANCELLING' where id=?", taskId, jobExecution.getTenantId(), con);
                        con.commit();
                        
                        processAPI.setActivityStateByName(taskId, ActivityStates.CANCELLED_STATE);
                        
                        /*
                        executeQuery("delete pending_mapping where activityid=?", taskId, con);

                        executeQuery("delete data_instance where containerid=?", taskId, con);

                        executeQuery("delete flownode_instance where id=?", taskId, con);

                        con.commit();
                        */
                        milkJobOutput.endChronometer(purgeTasksMarker);
                        countCorrects++;
                        jobExecution.addManagedItems(1);

                    } catch (Exception e) {
                        milkJobOutput.addEvent(new BEvent(eventErrorDeleteTask, "TaskId[" + taskId + "] caseId[" + caseId + "] " + e.getMessage()));
                        countErrors++;
                    } finally {
                    }

                }
                jobExecution.setAvancementStep(max);
                milkJobOutput.addReportTable(new String[] { "Indicator", "Value" });
                milkJobOutput.addReportLine(new Object[] { "Number of tasks detected", listResult.size() });
                milkJobOutput.addReportLine(new Object[] { "Task deleted", countCorrects });
                milkJobOutput.addReportLine(new Object[] { "Task deletion error", countErrors });
                milkJobOutput.addReportEndTable();

                milkJobOutput.setMesure(cstMesureTasksDeleted, countCorrects);
                milkJobOutput.setMesure(cstMesureTasksError, countErrors);
                milkJobOutput.setNbItemsProcessed(countCorrects);

                if (oneErrorDetected)
                    milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                else if (listResult.size() > jobExecution.getJobStopAfterMaxItems())
                    milkJobOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;
                else
                    milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;

                if (listResult.size() > jobExecution.getJobStopAfterMaxItems())
                    milkJobOutput.addReportLine("<td><colspan=\"5\">More object...</td></tr>");
                milkJobOutput.addReportEndTable();

                milkJobOutput.setNbItemsProcessed(max);

            }

        } catch (SearchException e1) {
            milkJobOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;

        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventSearchFailed, e, ""));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;

        }
        milkJobOutput.addChronometersInReport(true, false);

        return milkJobOutput;
    }

    private void executeQuery(String sqlUpdateRequest, Long taskId, Long tenantId, Connection con) throws Exception {
        try (PreparedStatement pstmt = con.prepareStatement(sqlUpdateRequest)) {
            pstmt.setObject(1, taskId);
            pstmt.setObject(2, tenantId);
            pstmt.executeUpdate();
            pstmt.close();
        } catch (Exception e) {
            throw e;
        }
    }
}
