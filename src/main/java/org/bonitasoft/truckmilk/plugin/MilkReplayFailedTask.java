package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.comment.Comment;
import org.bonitasoft.engine.bpm.comment.SearchCommentsDescriptor;
import org.bonitasoft.engine.bpm.flownode.ActivityInstance;
import org.bonitasoft.engine.bpm.flownode.ActivityInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.flownode.ActivityStates;
import org.bonitasoft.engine.bpm.process.ActivationState;
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
import org.bonitasoft.truckmilk.engine.MilkCmdControl;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.ListProcessesResult;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;

public class MilkReplayFailedTask extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkReplayFailedTask.class.getName());

    private static BEvent eventRetryFailed = new BEvent(MilkCmdControl.class.getName(), 1, Level.APPLICATIONERROR,
            "Retry failed", "Some retry failed", "Some task retry failed", "Check the log");

    private static BEvent eventRetrySuccess = new BEvent(MilkCmdControl.class.getName(), 2, Level.SUCCESS,
            "Retry success", "All retry was done with success");

    private static BEvent eventRetrySuccessButSkip = new BEvent(MilkCmdControl.class.getName(), 3, Level.SUCCESS,
            "Retry success, but some tasks were skipped", "All retry was done with success, some task were skipped due to the maxtentative retry");

    private static BEvent eventSearchFailed = new BEvent(MilkCmdControl.class.getName(), 4, Level.ERROR,
            "Search failed", "Search failed task return an error", "No retry can be performed", "Check the error");

    private static PlugInParameter cstParamDelay = PlugInParameter.createInstance("delay", "Delay", TypeParameter.DELAY, MilkPlugInToolbox.DELAYSCOPE.MN + ":5", "Delay before asking to replay a failed task");
    private static PlugInParameter cstParamMaximumTentatives = PlugInParameter.createInstance("maxtentative", "Max tentatives", TypeParameter.LONG, 5, "Maximum tentative to replay a failed task. After this number of tentative, job will not try to replay it.");
    private static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Process Filter", TypeParameter.ARRAYPROCESSNAME, null, "List of processes in the perimeter. If no filter is given, all processes are in the perimeter");
    private static PlugInParameter cstParamNumberOfTasks = PlugInParameter.createInstance("NumberOfTasks", "Number of tasks", TypeParameter.LONG, 1000, "Number of failed tasks detected, and replayed.");
    private static PlugInParameter cstParamOnlyDetection = PlugInParameter.createInstance("OnlyDetection", "Only Detection", TypeParameter.BOOLEAN, Boolean.FALSE, "Only detection, do not replay tasks");
    private static PlugInParameter cstParamTasksInReport = PlugInParameter.createInstance("TasksInReport", "Task and case in report", TypeParameter.BOOLEAN, Boolean.FALSE, "In the report, the list of Task/Case processed is saved");

    public MilkReplayFailedTask() {
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
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();

        // get Input 
        @SuppressWarnings("unchecked")

        Long maxTentatives = jobExecution.getInputLongParameter(cstParamMaximumTentatives);
        long retryFailed = 0;
        long retrySuccess = 0;
        long retrySkip = 0;
        Long numberOfTasks = jobExecution.getInputLongParameter(cstParamNumberOfTasks);
        Boolean onlyDetection = jobExecution.getInputBooleanParameter(cstParamOnlyDetection);
        Boolean tasksInReport = jobExecution.getInputBooleanParameter(cstParamTasksInReport);

        try {

            List<Long> listProcessDefinitionId = new ArrayList<>();

            // Filter on process?
            SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, numberOfTasks == null ? 1000 : numberOfTasks.intValue());

            ListProcessesResult listProcessResult = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessFilter, true, searchActBuilder, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, jobExecution.getApiAccessor().getProcessAPI());
            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                plugTourOutput.addEvents(listProcessResult.listEvents);
                plugTourOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return plugTourOutput;
            }

            DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamDelay, new Date(), false);
            if (BEventFactory.isError(delayResult.listEvents)) {
                plugTourOutput.addEvents(delayResult.listEvents);
                plugTourOutput.executionStatus = ExecutionStatus.ERROR;
                return plugTourOutput;
            }
            long timeSearch = delayResult.delayDate.getTime();

            searchActBuilder.filter(ActivityInstanceSearchDescriptor.STATE_NAME, ActivityStates.FAILED_STATE);
            searchActBuilder.lessOrEquals(ActivityInstanceSearchDescriptor.LAST_MODIFICATION_DATE, timeSearch);

            searchActBuilder.sort(ActivityInstanceSearchDescriptor.LAST_MODIFICATION_DATE, Order.ASC);
            SearchResult<ActivityInstance> searchFailedActivityInstance;
            searchFailedActivityInstance = processAPI.searchActivities(searchActBuilder.done());
            if (searchFailedActivityInstance.getCount() == 0) {
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
                return plugTourOutput;
            }
            StringBuilder listTasksCases = new StringBuilder();
            listTasksCases.append("TaskId/CaseId:");
            Exception collectFirstException = null;
            for (ActivityInstance activityInstance : searchFailedActivityInstance.getResult()) {
                try {
                    String commentString = "PlugInMilk - reexecute  " + activityInstance.getName() + "(" + activityInstance.getParentContainerId() + ")";
                    // we need to first check how many time this activity was executed
                    long numberReexecution = 0;
                    SearchOptionsBuilder searchOptionCommentBuilder = new SearchOptionsBuilder(0, 1000);
                    searchOptionCommentBuilder.filter(SearchCommentsDescriptor.PROCESS_INSTANCE_ID, activityInstance.getParentProcessInstanceId());
                    SearchResult<Comment> searchComment = processAPI.searchComments(searchOptionCommentBuilder.done());
                    for (Comment comment : searchComment.getResult()) {
                        if (comment.getContent().startsWith(commentString))
                            numberReexecution++;
                    }
                    if (numberReexecution > maxTentatives) {
                        retrySkip++;
                    } else {
                        if ((onlyDetection != null) && Boolean.FALSE.equals(onlyDetection)) {
                            // add the comment
                            processAPI.addProcessComment(activityInstance.getRootContainerId(), commentString);
                            processAPI.retryTask(activityInstance.getId());
                            listTasksCases.append(activityInstance.getId() + "/" + activityInstance.getRootContainerId() + ", ");
                            retrySuccess++;
                        }
                    }
                } catch (Exception e) {
                    logger.severe("Error Retry rootContainerId=[" + activityInstance.getRootContainerId() + "] ActiId[" + activityInstance.getId() + "] Error[" + e.getMessage() + "]");
                    if (collectFirstException == null)
                        collectFirstException = e;
                    retryFailed++;
                }
            }
            if (retryFailed > 0) {
                plugTourOutput.addEvent(new BEvent(eventRetryFailed,
                        collectFirstException,
                        "Failed retry/total " + retryFailed + ":" + (retryFailed + retrySuccess) + " (skip after " + maxTentatives + ": " + retrySkip + ")" + (Boolean.TRUE.equals(tasksInReport) ? listTasksCases.toString() : "")));
                plugTourOutput.executionStatus = ExecutionStatus.ERROR;
            } else if (retrySkip > 0) {
                plugTourOutput.addEvent(new BEvent(eventRetrySuccessButSkip, "Retry " + retrySuccess + " (skip after " + maxTentatives + ": " + retrySkip + ")" + (Boolean.TRUE.equals(tasksInReport) ? listTasksCases.toString() : "")));
                plugTourOutput.executionStatus = ExecutionStatus.WARNING;

            } else {
                plugTourOutput.addEvent(new BEvent(eventRetrySuccess, "Retry " + retrySuccess + " (skip after " + maxTentatives + ": " + retrySkip + ")" + (Boolean.TRUE.equals(tasksInReport) ? listTasksCases.toString() : "")));
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
            }
        } catch (SearchException e1) {
            plugTourOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
        }

        return plugTourOutput;
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription() {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName( "ReplayFailedTask");
        plugInDescription.setLabel( "Replay Failed Task");
        plugInDescription.setDescription( "Monitor all failed tasks. Then after a delay, replay them, if the number of tentative is not reach");
        plugInDescription.setCategory( CATEGORY.TASKS);
        plugInDescription.addParameter(cstParamDelay);
        plugInDescription.addParameter(cstParamMaximumTentatives);
        plugInDescription.addParameter(cstParamProcessFilter);
        plugInDescription.addParameter(cstParamNumberOfTasks);
        plugInDescription.addParameter(cstParamOnlyDetection);
        plugInDescription.addParameter(cstParamTasksInReport);
        return plugInDescription;
    }

}
