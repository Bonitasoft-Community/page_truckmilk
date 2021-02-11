package org.bonitasoft.truckmilk.plugin.tasks;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.comment.Comment;
import org.bonitasoft.engine.bpm.comment.SearchCommentsDescriptor;
import org.bonitasoft.engine.bpm.flownode.ActivityInstance;
import org.bonitasoft.engine.bpm.flownode.ActivityInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.flownode.ActivityStates;
import org.bonitasoft.engine.bpm.process.ProcessInstanceSearchDescriptor;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkCmdControl;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter.FilterProcess;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJobExecution.DelayResult;
import org.bonitasoft.truckmilk.job.MilkJobExecution.ListProcessesResult;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

public class MilkReplayFailedTasks extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkReplayFailedTasks.class.getName());

    private static BEvent eventRetryFailed = new BEvent(MilkCmdControl.class.getName(), 1, Level.APPLICATIONERROR,
            "Retry failed", "Some retry failed", "Some task retry failed", "Check the log");

    private static BEvent eventRetrySuccess = new BEvent(MilkCmdControl.class.getName(), 2, Level.SUCCESS,
            "Retry success", "All retry was done with success");

    private static BEvent eventRetrySuccessButSkip = new BEvent(MilkCmdControl.class.getName(), 3, Level.SUCCESS,
            "Retry success, but some tasks were skipped", "All retry was done with success, some task were skipped due to the maxtentative retry");

    private static BEvent eventSearchFailed = new BEvent(MilkCmdControl.class.getName(), 4, Level.ERROR,
            "Search failed", "Search failed task return an error", "No retry can be performed", "Check the error");

    private final static String CSTACTION_REPLAY = "Replay";
    private final static String CSTACTION_SKIP = "Skip";

    
    private static PlugInParameter cstParamDelay = PlugInParameter.createInstanceDelay("delay", "Delay", DELAYSCOPE.MN, 5, "Delay before asking to replay a failed task");
    private static PlugInParameter cstParamMaximumTentatives = PlugInParameter.createInstance("maxtentative", "Max tentatives", TypeParameter.LONG, 5, "Maximum tentative to replay a failed task. After this number of tentative, job will not try to replay it.");
    private static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Process Filter", TypeParameter.ARRAYPROCESSNAME, null, "List of processes in the perimeter. If no filter is given, all processes are in the perimeter")
            .withFilterProcess(FilterProcess.ONLYENABLED);

    private static PlugInParameter cstParamNumberOfTasks = PlugInParameter.createInstance("NumberOfTasks", "Number of tasks", TypeParameter.LONG, 1000, "Number of failed tasks detected, and replayed.");
    private static PlugInParameter cstParamOnlyDetection = PlugInParameter.createInstance("OnlyDetection", "Only Detection", TypeParameter.BOOLEAN, Boolean.FALSE, "Only detection, do not replay tasks");
    private static PlugInParameter cstParamActionOnTasks = PlugInParameter.createInstanceListValues("actiononTasks", "Action on tasks : Replay or Skip",
            new String[] { CSTACTION_REPLAY, CSTACTION_SKIP }, CSTACTION_REPLAY, "Task are replay, or skipped")
            .withVisibleConditionParameterValueEqual(cstParamOnlyDetection, false);


    private static PlugInParameter cstParamTasksInReport = PlugInParameter.createInstance("TasksInReport", "Task and case in report", TypeParameter.BOOLEAN, Boolean.FALSE, "In the report, the list of Task/Case processed is saved");

    public MilkReplayFailedTasks() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the milkEmailUsersTasks, we require to be able to send an email
     */
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    @Override
    public MilkJobOutput executeJob(MilkJobExecution milkJobExecution) {
        ProcessAPI processAPI = milkJobExecution.getApiAccessor().getProcessAPI();
        MilkJobOutput plugTourOutput = milkJobExecution.getMilkJobOutput();

        // get Input 
        Long maxTentatives = milkJobExecution.getInputLongParameter(cstParamMaximumTentatives);
        long retryFailed = 0;
        long retrySuccess = 0;
        long retrySkip = 0;
        Long numberOfTasks = milkJobExecution.getInputLongParameter(cstParamNumberOfTasks);
        
        Boolean onlyDetection = milkJobExecution.getInputBooleanParameter(cstParamOnlyDetection);
        String policy = milkJobExecution.getInputStringParameter(cstParamActionOnTasks);
        Boolean tasksInReport = milkJobExecution.getInputBooleanParameter(cstParamTasksInReport);

        try {

            // Filter on process?
            SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, numberOfTasks == null ? 1000 : numberOfTasks.intValue());

            ListProcessesResult listProcessResult = milkJobExecution.getInputArrayProcess( cstParamProcessFilter, false, searchActBuilder, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, milkJobExecution.getApiAccessor().getProcessAPI());
            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                plugTourOutput.addEvents(listProcessResult.listEvents);
                plugTourOutput.setExecutionStatus( ExecutionStatus.BADCONFIGURATION );
                return plugTourOutput;
            }

            DelayResult delayResult = milkJobExecution.getInputDelayParameter( cstParamDelay, new Date(), false);
            if (BEventFactory.isError(delayResult.listEvents)) {
                plugTourOutput.addEvents(delayResult.listEvents);
                plugTourOutput.setExecutionStatus( ExecutionStatus.ERROR );
                return plugTourOutput;
            }
            long timeSearch = delayResult.delayDate.getTime();

            searchActBuilder.filter(ActivityInstanceSearchDescriptor.STATE_NAME, ActivityStates.FAILED_STATE);
            searchActBuilder.lessOrEquals(ActivityInstanceSearchDescriptor.LAST_MODIFICATION_DATE, timeSearch);

            searchActBuilder.sort(ActivityInstanceSearchDescriptor.LAST_MODIFICATION_DATE, Order.ASC);
            SearchResult<ActivityInstance> searchFailedActivityInstance;
            searchFailedActivityInstance = processAPI.searchActivities(searchActBuilder.done());
            if (searchFailedActivityInstance.getCount() == 0) {
                plugTourOutput.setExecutionStatus( ExecutionStatus.SUCCESSNOTHING );
                return plugTourOutput;
            }
            StringBuilder listTasksCases = new StringBuilder();
            listTasksCases.append("TaskId/CaseId:");
            Exception collectFirstException = null;
            for (ActivityInstance activityInstance : searchFailedActivityInstance.getResult()) {
                try {
                    StringBuilder commentString = new StringBuilder();
                    commentString.append("TruckMilk - reexecute[" + activityInstance.getName() + "], RootCaseId:" + activityInstance.getParentContainerId() + ", ");
                    // we need to first check how many time this activity was executed
                    long numberReexecution = 0;
                    SearchOptionsBuilder searchOptionCommentBuilder = new SearchOptionsBuilder(0, 1000);
                    searchOptionCommentBuilder.filter(SearchCommentsDescriptor.PROCESS_INSTANCE_ID, activityInstance.getParentProcessInstanceId());
                    SearchResult<Comment> searchComment = processAPI.searchComments(searchOptionCommentBuilder.done());
                    for (Comment comment : searchComment.getResult()) {
                        if (comment.getContent().startsWith(commentString.toString()))
                            numberReexecution++;
                    }
                    commentString.append( "Tentative: "+(numberReexecution+1) );
                    if (numberReexecution > maxTentatives) {
                        retrySkip++;
                    } else {
                        if ((onlyDetection != null) && Boolean.FALSE.equals(onlyDetection)) {
                            // add the comment
                            processAPI.addProcessComment(activityInstance.getRootContainerId(), commentString.toString());
                            if (CSTACTION_REPLAY.equals(policy))
                                    processAPI.retryTask(activityInstance.getId());
                            else
                                processAPI.setActivityStateByName(activityInstance.getId(), ActivityStates.SKIPPED_STATE);
                            listTasksCases.append(activityInstance.getId() + "/" + activityInstance.getRootContainerId() + ", ");
                            retrySuccess++;
                        }
                        else
                            retrySuccess++;
                    }
                } catch (Exception e) {
                    logger.severe("Error Retry rootContainerId=[" + activityInstance.getRootContainerId() + "] ActiId[" + activityInstance.getId() + "] Error[" + e.getMessage() + "]");
                    if (collectFirstException == null)
                        collectFirstException = e;
                    retryFailed++;
                }
            }
            plugTourOutput.nbItemsProcessed=retrySuccess;
            if (retryFailed > 0) {
                plugTourOutput.addEvent(new BEvent(eventRetryFailed,
                        collectFirstException,
                        "Failed retry/total " + retryFailed + ":" + (retryFailed + retrySuccess) + " (skip after " + maxTentatives + ": " + retrySkip + ")" + (Boolean.TRUE.equals(tasksInReport) ? listTasksCases.toString() : "")));
                plugTourOutput.setExecutionStatus( ExecutionStatus.ERROR );
            } else if (retrySkip > 0) {
                plugTourOutput.addEvent(new BEvent(eventRetrySuccessButSkip, "Retry " + retrySuccess + " (skip after " + maxTentatives + ": " + retrySkip + ")" + (Boolean.TRUE.equals(tasksInReport) ? listTasksCases.toString() : "")));
                plugTourOutput.setExecutionStatus( ExecutionStatus.WARNING );

            } else {
                plugTourOutput.addEvent(new BEvent(eventRetrySuccess, "Retry " + retrySuccess + " (skip after " + maxTentatives + ": " + retrySkip + ")" + (Boolean.TRUE.equals(tasksInReport) ? listTasksCases.toString() : "")));
                plugTourOutput.setExecutionStatus( ExecutionStatus.SUCCESS );
            }
        } catch (SearchException e1) {
            plugTourOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
            plugTourOutput.setExecutionStatus( ExecutionStatus.ERROR );
        }

        return plugTourOutput;
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName("ReplayFailedTasks");
        plugInDescription.setLabel("Replay Failed Tasks");
        plugInDescription.setExplanation("Monitor all failed tasks. Then after a delay, replay them, if the number of tentative is not reach");
        plugInDescription.setCategory(CATEGORY.TASKS);
        plugInDescription.addParameter(cstParamDelay);
        plugInDescription.addParameter(cstParamMaximumTentatives);
        plugInDescription.addParameter(cstParamProcessFilter);
        plugInDescription.addParameter(cstParamNumberOfTasks);
        plugInDescription.addParameter(cstParamOnlyDetection);
        plugInDescription.addParameter(cstParamActionOnTasks);
        plugInDescription.addParameter(cstParamTasksInReport);

        plugInDescription.setJobCanBeStopped(JOBSTOPPER.BOTH);
        
        return plugInDescription;
    }

}
