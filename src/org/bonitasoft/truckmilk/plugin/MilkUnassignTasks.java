package org.bonitasoft.truckmilk.plugin;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.exception.UpdateException;
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.identity.UserNotFoundException;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.ListProcessesResult;
import org.bonitasoft.truckmilk.job.MilkJobExecution;

import groovy.time.TimeCategory;
import groovy.time.TimeDuration;

/* ******************************************************************************** */
/*                                                                                  */
/* MilkUnassignTasks */
/*                                                                                  */
/* Unassign tasks */
/*                                                                                  */
/* ******************************************************************************** */

public class MilkUnassignTasks extends MilkPlugIn {

    Logger logger = Logger.getLogger(MilkUnassignTasks.class.getName());

    private static PlugInParameter cstParamTaskName = PlugInParameter.createInstance("taskName", "Tasks Name", TypeParameter.STRING, null, "The task name to search for. If empty, all tasks in process are concerned. A list of tasks, separate by , can be provided too.");
    private static PlugInParameter cstParamCheckoutTime = PlugInParameter.createInstance("checkoutTime", "Checkout time execution (in mn)", TypeParameter.LONG, 15, "How many minutes until the item should be unassigned");
    private static PlugInParameter cstParamProcessName = PlugInParameter.createInstance("ProcessName", "Process Name", TypeParameter.PROCESSNAME, "", "Process name is mandatory. You can specify the process AND the version, or only the process name: all versions of this process is then checked");

    private static BEvent eventTaskReported = new BEvent(MilkUnassignTasks.class.getName(), 1, Level.INFO,
            "Unassign Task !", "The unassign task job is executed correctly");
    private static BEvent eventTaskNameInvalid = new BEvent(MilkUnassignTasks.class.getName(), 2, Level.APPLICATIONERROR,
            "Task name not found", "Task name is required");
    private static BEvent userIdNotFound = new BEvent(MilkUnassignTasks.class.getName(), 3, Level.ERROR,
            "AssigneeID Not found", "Could not locate the AssigneeID via IdentityAPI");

    private static BEvent eventNoprocessFilter = new BEvent(MilkUnassignTasks.class.getName(), 4,
            Level.APPLICATIONERROR,
            "No process filter", "The process filter is empty", "SLA can't run.",
            "Give a process name");

    private static BEvent eventNoprocessMatchFilter = new BEvent(MilkUnassignTasks.class.getName(), 5,
            Level.APPLICATIONERROR,
            "No process found", "No process is found with the given filter", "This filter does not apply.",
            "Check the process name (you must give as minimum one process)");

    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    public MilkUnassignTasks() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * plug in can check its environment, to detect if you missed something. An external component may
     * be required and are not installed.
     * 
     * @return a list of Events.
     */
    @Override
    public List<BEvent> checkPluginEnvironment(long tenantId, APIAccessor apiAccessor) {
        return new ArrayList<>();
    }

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        return new ArrayList<>();

    };

    /**
     * return the description of ping job
     */
    @Override
    public PlugInDescription getDefinitionDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();

        plugInDescription.name = "Unassign Tasks";
        plugInDescription.description = "Unassign tasks if not resolved after a specified time.";
        plugInDescription.label = "Unassign Tasks";
        plugInDescription.addParameter(cstParamProcessName);
        plugInDescription.addParameter(cstParamTaskName);
        plugInDescription.addParameter(cstParamCheckoutTime);
        return plugInDescription;
    }

    /**
     * execution of the job. Just calculated the result according the parameters, and return it.
     */
    @Override
    public PlugTourOutput execute(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        PlugTourOutput plugTourOutput = jobExecution.getPlugTourOutput();

        // task name is required
        String taskNameList = jobExecution.getInputStringParameter(cstParamTaskName);

        Long checkoutTime = jobExecution.getInputLongParameter(cstParamCheckoutTime);

        try {
            // one process name is required
            SearchOptionsBuilder searchTasks = new SearchOptionsBuilder(0, 10000);
            ListProcessesResult listProcessResult = MilkPlugInToolbox.completeListProcess(jobExecution, cstParamProcessName, false, searchTasks, HumanTaskInstanceSearchDescriptor.PROCESS_DEFINITION_ID, apiAccessor.getProcessAPI());

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                plugTourOutput.addEvents(listProcessResult.listEvents);
                return plugTourOutput;
            }

            // search for assigned tasks        
            listProcessResult.sob.and();
            listProcessResult.sob.differentFrom(HumanTaskInstanceSearchDescriptor.ASSIGNEE_ID, 0); // unassigned
            listProcessResult.sob.filter(HumanTaskInstanceSearchDescriptor.STATE_NAME, "ready");
            if (taskNameList != null && taskNameList.trim().length() > 0) {
                StringTokenizer st = new StringTokenizer(taskNameList, ",");
                listProcessResult.sob.leftParenthesis();
                int countTask = 0;
                while (st.hasMoreTokens()) {
                    if (countTask > 0)
                        listProcessResult.sob.or();
                    countTask++;
                    listProcessResult.sob.filter(HumanTaskInstanceSearchDescriptor.NAME, st.nextToken());
                }
                listProcessResult.sob.rightParenthesis();
            }

            List<HumanTaskInstance> tasks = apiAccessor.getProcessAPI().searchHumanTaskInstances(listProcessResult.sob.done()).getResult();
            jobExecution.setAvancementTotalStep((long) tasks.size());
            logger.info("MilkUnassignTasks: unassign Task Count: " + tasks.size());

            // loop through task list and determine if checkout duration meets threshold
            Map<String, String> listUnassignTasks = new HashMap<>();
            long count = 0;
            for (HumanTaskInstance task : tasks) {

                if (jobExecution.pleaseStop()) {
                    break;
                }
                jobExecution.setAvancementStep(count);
                count++;

                TimeDuration duration = TimeCategory.minus(new Date(), task.getClaimedDate());
                if (duration.getMinutes() >= checkoutTime) {
                    logger.fine("Unassigning task id: " + task.getId());
                    try {
                        User assigneeUser = apiAccessor.getIdentityAPI().getUser(task.getAssigneeId());
                        apiAccessor.getProcessAPI().assignUserTask(task.getId(), 0);
                        String userKey = assigneeUser.getFirstName() + " " + assigneeUser.getLastName();
                        String taskReporting = listUnassignTasks.get(userKey);
                        taskReporting = (taskReporting == null ? "" : taskReporting + ", ") + task.getName() + "[" + task.getId() + "]";
                        listUnassignTasks.put(userKey, taskReporting);

                    } catch (UserNotFoundException e1) {
                        plugTourOutput.addEvent(new BEvent(userIdNotFound, "User Id[" + task.getAssigneeId() + "]"));
                    } catch (UpdateException e) {
                        plugTourOutput.addEvent(new BEvent(eventTaskReported, "Error un-assigning taskId: " + task.getId()));
                    }
                }
            }
            for (Entry<String, String> entry : listUnassignTasks.entrySet()) {
                plugTourOutput.addEvent(new BEvent(eventTaskReported, entry.getKey() + ":" + entry.getValue()));
            }

        } catch (SearchException e) {
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
            plugTourOutput.addEvent(new BEvent(eventTaskNameInvalid, e.getMessage()));
            return plugTourOutput;
        }

        logger.fine("Finished checking tasks to unassign");
        plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;

        if (jobExecution.pleaseStop())
            plugTourOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;

        return plugTourOutput;
    }

}
