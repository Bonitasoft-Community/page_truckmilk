package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.comment.Comment;
import org.bonitasoft.engine.bpm.comment.SearchCommentsDescriptor;
import org.bonitasoft.engine.bpm.flownode.ActivityInstance;
import org.bonitasoft.engine.bpm.flownode.ActivityInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.flownode.ActivityStates;
import org.bonitasoft.engine.bpm.process.ActivationState;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfoSearchDescriptor;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkCmdControl;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

public class MilkReplayFailedTask extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkReplayFailedTask.class.getName());

    private static BEvent eventNoProcessMatchFilter = new BEvent(MilkCmdControl.class.getName(), 1,
            Level.APPLICATIONERROR,
            "No process match filter", "No process is found with the given filter", "This filter does not apply.",
            "Check the process name");

    private static BEvent eventNoProcessForFilter = new BEvent(MilkCmdControl.class.getName(), 2,
            Level.APPLICATIONERROR,
            "Filter is not active", "No processes was found for all the filter, search will not run",
            "No filter at all apply, assuming configuration want to apply only on some process",
            "Check the process name");

    private static BEvent eventRetryFailed = new BEvent(MilkCmdControl.class.getName(), 3, Level.APPLICATIONERROR,
            "Retry failed", "Some retry failed", "Some task retry failed", "Check the log");

    private static BEvent eventRetrySuccess = new BEvent(MilkCmdControl.class.getName(), 4, Level.SUCCESS,
            "Retry success", "All retry was done with success");

    private static BEvent eventRetrySuccessButSkip = new BEvent(MilkCmdControl.class.getName(), 5, Level.SUCCESS,
            "Retry success, but some tasks were skipped", "All retry was done with success, some task were skipped due to the maxtentative retry");

    private static BEvent eventSearchFailed = new BEvent(MilkCmdControl.class.getName(), 6, Level.ERROR,
            "Search failed", "Search failed task return an error", "No retry can be performed", "Check the error");

    private static PlugInParameter cstParamDelayInMinutes = PlugInParameter.createInstance("delayinmn", "Delay in minutes", TypeParameter.LONG, 5, "Delay before asking to replay a failed task");
    private static PlugInParameter cstParamMaximumTentative = PlugInParameter.createInstance("maxtentative", "Max tentative", TypeParameter.LONG, 5, "Maximum tentative to replay a failed task. After this number of tentative, job will not try to replay it.");
    private static PlugInParameter cstParamProcessfilter = PlugInParameter.createInstance("processfilter", "Process Filter", TypeParameter.ARRAY, null, "List of processes in the perimeter. If no filter is given, all processes are in the perimeter");

    public MilkReplayFailedTask() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the milkEmailUsersTasks, we require to be able to send an email
     */
    public List<BEvent> checkEnvironment(long tenantId, APIAccessor apiAccessor) {
        return new ArrayList<BEvent>();
    };

    @Override
    public PlugTourOutput execute(MilkJobExecution input, APIAccessor apiAccessor) {
        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        PlugTourOutput plugTourOutput = input.getPlugTourOutput();

        // get Input 
        @SuppressWarnings("unchecked")
        List<String> listProcessName = (List<String>) input.getInputListParameter(cstParamProcessfilter);
        Long delayInMin = input.getInputLongParameter(cstParamDelayInMinutes);
        Long maxTentative = input.getInputLongParameter(cstParamMaximumTentative);
        long retryFailed = 0;
        long retrySuccess = 0;
        long retrySkip = 0;

        try {

            List<Long> listProcessDefinitionId = new ArrayList<Long>();

            // Filter on process?
            SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, 1000);

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

                    searchActBuilder.filter(ActivityInstanceSearchDescriptor.PROCESS_DEFINITION_ID,
                            listProcessDefinitionId.get(i));
                }
                searchActBuilder.rightParenthesis();
                searchActBuilder.and();

            } // end list process name

            Date currentDate = new Date();
            long timeSearch = currentDate.getTime() - delayInMin * 60 * 1000;

            searchActBuilder.filter(ActivityInstanceSearchDescriptor.STATE_NAME, ActivityStates.FAILED_STATE);
            searchActBuilder.lessOrEquals(ActivityInstanceSearchDescriptor.LAST_MODIFICATION_DATE, timeSearch);

            searchActBuilder.sort(ActivityInstanceSearchDescriptor.LAST_MODIFICATION_DATE, Order.ASC);
            SearchResult<ActivityInstance> searchFailedActivityInstance;
            searchFailedActivityInstance = processAPI.searchActivities(searchActBuilder.done());
            if (searchFailedActivityInstance.getCount() == 0) {
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
                return plugTourOutput;
            }
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
                    if (numberReexecution > maxTentative) {
                        retrySkip++;
                    } else {
                        // add the comment
                        processAPI.addProcessComment(activityInstance.getRootContainerId(), commentString);
                        processAPI.retryTask(activityInstance.getId());
                        retrySuccess++;
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
                        "Failed retry/total " + retryFailed + ":" + (retryFailed + retrySuccess) + " (skip after " + maxTentative + ": " + retrySkip + ")"));
                plugTourOutput.executionStatus = ExecutionStatus.ERROR;
            } else if (retrySkip > 0) {
                plugTourOutput.addEvent(new BEvent(eventRetrySuccessButSkip, "Retry " + retrySuccess + " (skip after " + maxTentative + ": " + retrySkip + ")"));
                plugTourOutput.executionStatus = ExecutionStatus.WARNING;

            } else {
                plugTourOutput.addEvent(new BEvent(eventRetrySuccess, "Retry " + retrySuccess + " (skip after " + maxTentative + ": " + retrySkip + ")"));
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
            }
        } catch (SearchException e1) {
            plugTourOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
        }

        return plugTourOutput;
    }

    @Override
    public PlugInDescription getDefinitionDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();
        plugInDescription.name = "ReplayFailedTask";
        plugInDescription.label = "Replay Failed Task";
        plugInDescription.description = "Monitor all failed tasks. Then after a delay, replay them, if the number of tentative is not reach";
        plugInDescription.addParameter(cstParamDelayInMinutes);
        plugInDescription.addParameter(cstParamMaximumTentative);
        plugInDescription.addParameter(cstParamProcessfilter);

        /*
         * plugInDescription.addParameterFromMapJson(
         * "{\"delayinmn\":10,\"maxtentative\":12,\"processfilter\":[]}");
         */
        return plugInDescription;
    }

}
