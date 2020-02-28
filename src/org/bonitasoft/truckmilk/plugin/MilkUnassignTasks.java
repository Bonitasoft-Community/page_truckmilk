package org.bonitasoft.truckmilk.plugin;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.job.MilkJobExecution;

import groovy.time.TimeCategory;
import groovy.time.TimeDuration;

/* ******************************************************************************** */
/*                                                                                  */
/* Ping */
/*                                                                                  */
/* this class may be use as a skeleton for a new plug in */
/*                                                                                  */
/* ******************************************************************************** */

public class MilkUnassignTasks extends MilkPlugIn {

	Logger logger = Logger.getLogger( MilkUnassignTasks.class.getName() );
	
	// private static PlugInParameter cstParamProcessName = PlugInParameter.createInstance("processName", "Process Name", TypeParameter.STRING, null, "List of processes to run");
	// private static PlugInParameter cstParamProcessVersion = PlugInParameter.createInstance("processVersion", "Process Version", TypeParameter.STRING, null, "Version of process to run. Leave blank for all versions.");
    private static PlugInParameter cstParamTaskName = PlugInParameter.createInstance("taskName", "Task Name", TypeParameter.STRING, null, "The task name to search for");
    private static PlugInParameter cstParamCheckoutTime = PlugInParameter.createInstance("checkoutTime", "Checkout time execution (in mn)", TypeParameter.LONG, 15, "How many minutes until the item should be unassigned");
    private static PlugInParameter cstParamProcessName = PlugInParameter.createInstance("ProcessName", "Process Name", TypeParameter.PROCESSNAME, "", "Process name is mandatory. You can specify the process AND the version, or only the process name: all versions of this process is then checked");

    private static BEvent eventTaskReported = new BEvent(MilkUnassignTasks.class.getName(), 1, Level.INFO, 
    		"Unassign Task !", "The unassign task job is executed correctly");
    private static BEvent eventTaskNameInvalid = new BEvent(MilkUnassignTasks.class.getName(), 2, Level.APPLICATIONERROR,
            "Task name not found", "Task name is required");
    private static BEvent userIdNotFound = new BEvent(MilkUnassignTasks.class.getName(), 3, Level.ERROR, 
    		"AssigneeID Not found", "Could not locate the AssigneeID via IdentityAPI");
    
    private static BEvent EVENT_NO_PROCESS_FILTER = new BEvent(MilkUnassignTasks.class.getName(), 4,
            Level.APPLICATIONERROR,
            "No process filter", "The process filter is empty", "SLA can't run.",
            "Give a process name");

    private static BEvent EVENT_NO_PROCESS_MATCH_FILTER = new BEvent(MilkUnassignTasks.class.getName(), 5,
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
        return new ArrayList<BEvent>();
    }

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        return listEvents;
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
        String taskName = jobExecution.getInputStringParameter(cstParamTaskName.name, null);
        if(taskName == null) {
        	plugTourOutput.addEvent(eventTaskNameInvalid);
        	plugTourOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
        	return plugTourOutput;
        }
        Long checkoutTime = jobExecution.getInputLongParameter(cstParamCheckoutTime.name, 15L);
        String processName = jobExecution.getInputStringParameter(cstParamProcessName);
        logger.info("MilkUnassignTasks: ProcessName: ["+processName+"] TaskName: ["+taskName+"] checkouttime: ["+checkoutTime+"]");
        
        try {
            // process name is required
            
            if (processName == null || processName.trim().length() == 0) {
                plugTourOutput.addEvent(EVENT_NO_PROCESS_FILTER);
                return plugTourOutput;
            }
            SearchResult<ProcessDeploymentInfo> procList = getListProcessDefinitionId(jobExecution, cstParamProcessName, apiAccessor.getProcessAPI());
            if (procList.getCount() == 0) {
                plugTourOutput.addEvent(new BEvent(EVENT_NO_PROCESS_MATCH_FILTER, "ProcessName[" + processName + "]"));
                return plugTourOutput;
            }
            
            
        	
        	for(ProcessDeploymentInfo proc : procList.getResult()) {
        		logger.info("MilkUnassignTasks: ProcessName: ["+proc.getName()+"] Version: ["+proc.getVersion()+"] Id: ["+proc.getProcessId()+"]");
            	
            	// search for assigned tasks        
                SearchOptionsBuilder searchTasks = new SearchOptionsBuilder(0, 10000);
                searchTasks.filter(HumanTaskInstanceSearchDescriptor.PROCESS_DEFINITION_ID, proc.getProcessId());
                searchTasks.differentFrom(HumanTaskInstanceSearchDescriptor.ASSIGNEE_ID, 0); // unassigned
                searchTasks.filter(HumanTaskInstanceSearchDescriptor.STATE_NAME, "ready");
        		searchTasks.filter(HumanTaskInstanceSearchDescriptor.DISPLAY_NAME, taskName);

            	List<HumanTaskInstance> tasks = apiAccessor.getProcessAPI().searchHumanTaskInstances(searchTasks.done()).getResult();
            	jobExecution.setAvancementTotalStep(Long.valueOf(tasks.size()));
            	logger.info("MilkUnassignTasks: unassign Task Count: " + tasks.size());
            	     
            	// loop through task list and determine if checkout duration meets threshold
            	Map<String, String> listUnassignTasks = new HashMap<String, String>();
            	for(HumanTaskInstance task : tasks) {
            		
            		if(jobExecution.pleaseStop()) {
        				break;
        			}
        			jobExecution.setAvancementStep(1);
        			
        			TimeDuration duration = TimeCategory.minus(new Date(), task.getClaimedDate());
    				if(duration.getMinutes() >= checkoutTime) {		
    					//logger.info("Unassigning task id: " + task.getId());
    					try {
    						User assigneeUser = apiAccessor.getIdentityAPI().getUser(task.getAssigneeId());
                            apiAccessor.getProcessAPI().assignUserTask(task.getId(), 0);
    						//plugTourOutput.addEvent(new BEvent(eventTaskReported, taskName + " task has been unassigned from UserID: " + task.getAssigneeId() + " , " + assigneeUser.getFirstName() + " " + assigneeUser.getLastName()));
    						
    						String taskReporting = listUnassignTasks.get(assigneeUser.getFirstName() + " " + assigneeUser.getLastName());
    						taskReporting = (taskReporting==null ? "" : ",") + task.getId();
    						listUnassignTasks.put(assigneeUser.getFirstName() + " " + assigneeUser.getLastName(), taskReporting);
    						for (String key : listUnassignTasks.keySet())
    						{
    							plugTourOutput.addEvent(new BEvent(eventTaskReported, key +":"+listUnassignTasks.get( key )));
    						}
    					}
    					catch (UserNotFoundException e1) {
    						plugTourOutput.addEvent(userIdNotFound);
    						plugTourOutput.addEvent( new BEvent( userIdNotFound, "User Id["+ task.getAssigneeId() +"]"));
    					}		
    					catch (UpdateException e) {
    						 plugTourOutput.addEvent(new BEvent(eventTaskReported, "Error un-assigning taskId: " + task.getId()));
    					}
    				}
            	}
        	}
        	
        }
        catch(SearchException e) {
        	plugTourOutput.executionStatus = ExecutionStatus.ERROR;
        	plugTourOutput.addEvent(new BEvent(eventTaskNameInvalid, e.getMessage()));
        	return plugTourOutput;
        }
        
        
        plugTourOutput.addEvent(new BEvent(eventTaskReported, "Finished checking tasks to unassign"));
        plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
        
        if (jobExecution.pleaseStop())
            plugTourOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;

        return plugTourOutput;
    }

}
