package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.radar.RadarFactory;
import org.bonitasoft.radar.workers.RadarWorkers;
import org.bonitasoft.radar.workers.RadarWorkers.StuckFlowNodes;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.TypesCast;
public class MilkRestartFlowNodes  extends MilkPlugIn {

    private final static String RADAR_NAME_WORKER = "workers";
    
    static MilkLog logger = MilkLog.getLogger(MilkRestartFlowNodes.class.getName());

    private final static BEvent eventErrorExecuteFlownode = new BEvent(MilkRestartFlowNodes.class.getName(), 1,
            Level.APPLICATIONERROR,
            "Error executed a flow node", "Executing a flow node failed", "Flow node is not restarted",
            "Check exception");
    
    private final static BEvent eventErrorNoRadarWorker = new BEvent(MilkRestartFlowNodes.class.getName(), 2,
            Level.ERROR,
            "No radar Worker found", "A radar definition is missing", "Flow nodes to restart can't be detected",
            "Check exception");

    private final static String CSTOPERATION_GETINFORMATION = "Get information (No operation)";
    private final static String CSTOPERATION_EXECUTE = "Execute FlowNodes";
    
    private static PlugInParameter cstParamDelay = PlugInParameter.createInstanceDelay("Delay", "Delay consider task is stuck", DELAYSCOPE.MN, 5, "In the report, the list of Task/Case processed is saved");
    private static PlugInParameter cstParamPolicy = PlugInParameter.createInstanceListValues("policy", "Task and case in report", new String[] { CSTOPERATION_GETINFORMATION, CSTOPERATION_EXECUTE }, CSTOPERATION_GETINFORMATION,  "In the report, the list of Task/Case processed is saved");

    private final static PlugInMeasurement cstMesureTasksExecuted = PlugInMeasurement.createInstance("TasksExecuted", "Tasks Executed", "Number of task executed");
    private final static PlugInMeasurement cstMesureTasksError = PlugInMeasurement.createInstance("TasksError", "Task execution error", "Number of task in error when a re-execution was asked");
    
    
    public MilkRestartFlowNodes() {
        super(TYPE_PLUGIN.EMBEDED);
    }


    @Override
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    @Override
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName( "ReplayFlowNodes");
        plugInDescription.setLabel( "Replay (Stuck) Flow Nodes");
        plugInDescription.setExplanation( "Check all flow nodes in the database, and if this number is over the number of Pending flownodes, restart them");
        plugInDescription.setCategory( CATEGORY.TASKS );

        plugInDescription.setJobCanBeStopped(JOBSTOPPER.BOTH);
        
        plugInDescription.addParameter(cstParamDelay);
        plugInDescription.addParameter(cstParamPolicy);
        
        plugInDescription.addMesure(cstMesureTasksExecuted);
        plugInDescription.addMesure(cstMesureTasksError);
        
        return plugInDescription;
        }

    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput milkJobOutput = jobExecution.getMilkJobOutput();
        
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        
        DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamDelay, new Date(), false);
        if (BEventFactory.isError(delayResult.listEvents)) {
            milkJobOutput.addEvents(delayResult.listEvents);
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            return milkJobOutput;
        }
        String policy = jobExecution.getInputStringParameter(cstParamPolicy);
        
        // How many flownode do we have to re-execute?
        RadarFactory radarFactory = RadarFactory.getInstance();
        
        

        RadarWorkers radarWorkers = (RadarWorkers) radarFactory.getInstance(RADAR_NAME_WORKER, RadarWorkers.CLASS_RADAR_NAME, jobExecution.getTenantId(), jobExecution.getApiAccessor());

        
        if (radarWorkers==null)
        {
            milkJobOutput.addEvent(new BEvent(eventErrorNoRadarWorker, "Radar Worker["+RadarWorkers.CLASS_RADAR_NAME+"] not found"));
            milkJobOutput.executionStatus =ExecutionStatus.ERROR;
            return milkJobOutput;
            
        }
        // +1 then we can detect if we re-execute ALL flow nodes, or less that we have in database
        /*
         * SELECT f.*
FROM flownode_instance AS f 
WHERE (f.state_Executing = 1 OR f.stable = 0 OR f.terminal = 1 OR f.stateCategory = 'ABORTING' OR f.stateCategory = 'CANCELLING') 
ORDER BY id;
         */
        
        StuckFlowNodes stuckFlowNodes = radarWorkers.getOldestFlowNodesWaitingForExecution(jobExecution.getTenantId(), jobExecution.getJobStopAfterMaxItems() +100, delayResult.delayInMs );
        milkJobOutput.addEvents( stuckFlowNodes.listEvents);
        milkJobOutput.addReportInHtml("SqlQuery to detect Stuck Flow node:"+stuckFlowNodes.sqlQuery);
        
        if (stuckFlowNodes.listStuckFlowNode.isEmpty() || CSTOPERATION_GETINFORMATION.equals(policy))
        {
            milkJobOutput.executionStatus =ExecutionStatus.SUCCESSNOTHING;
            milkJobOutput.setMesure(cstMesureTasksExecuted, 0);
            milkJobOutput.setMesure(cstMesureTasksError, 0);
            milkJobOutput.addReportTable( new String[] {"Indicator", "Value"});
            milkJobOutput.addReportLine( new Object[] {"Number of tasks detected", stuckFlowNodes.listStuckFlowNode.size() });
            // show up the fist lines
            int max = jobExecution.getJobStopAfterMaxItems() ;
            if (max>50)
                max=50;
            for (Map<String,Object> flowNode : stuckFlowNodes.listStuckFlowNode) {
                milkJobOutput.addReportLine( new Object[] {"CaseId/FlowId", flowNode.get("ROOTCONTAINERID")+"/"+flowNode.get("ID") });
            }
            if (max < jobExecution.getJobStopAfterMaxItems())
                milkJobOutput.addReportLine( new Object[] {"... more objects", "" });
            
            milkJobOutput.addReportEndTable();
            
            return milkJobOutput;
        }
        
        // restart somes nodes
        jobExecution.setAvancementTotalStep(stuckFlowNodes.listStuckFlowNode.size());
        boolean oneErrorDetected=false;
        int countCorrects=0;
        int countErrors=0; 
        for (int i=0;i<stuckFlowNodes.listStuckFlowNode.size();i++)
        {
            if (jobExecution.pleaseStop())
                break;
            jobExecution.setAvancementStep( i );

            Long flowNodeId = TypesCast.getLong(stuckFlowNodes.listStuckFlowNode.get( i ).get("ID"), null);
            Long rootContainerId = TypesCast.getLong( stuckFlowNodes.listStuckFlowNode.get( i ).get("ROOTCONTAINERID"), null);
            if (flowNodeId!=null)
                try {
                    processAPI.executeFlowNode( flowNodeId);
                    jobExecution.addManagedItems( 1 );
                    countCorrects++;
                } catch (Exception e) {
                    
                    // the flow node may notexist, because a worker executed it in the mean time
                    oneErrorDetected=true;
                    milkJobOutput.addEvent(new BEvent(eventErrorExecuteFlownode, "FlowNodeId["+flowNodeId+"] caseId["+rootContainerId+"] "+e.getMessage()));
                    countErrors++;
                } 
            else {
                milkJobOutput.addEvent(new BEvent(eventErrorExecuteFlownode, "FlowNodeId is not a Long["+stuckFlowNodes.listStuckFlowNode.get( i ).get("ID")+"] CaseId["+stuckFlowNodes.listStuckFlowNode.get( i ).get("ROOTCONTAINERID")+"]"));
                countErrors++;
            }
        }
        milkJobOutput.addReportTable( new String[] {"Indicator", "Value"});
        milkJobOutput.addReportLine( new Object[] {"Number of tasks detected", stuckFlowNodes.listStuckFlowNode.size() });
        milkJobOutput.addReportLine( new Object[] {"Task executed", countCorrects});
        milkJobOutput.addReportLine( new Object[] {"Task execution error", countErrors});
        milkJobOutput.addReportEndTable();
        
        milkJobOutput.setMesure(cstMesureTasksExecuted, countCorrects);
        milkJobOutput.setMesure(cstMesureTasksError, countErrors);
        milkJobOutput.setNbItemsProcessed(countCorrects);
        
        if (oneErrorDetected)
            milkJobOutput.executionStatus =ExecutionStatus.ERROR;
        else if (stuckFlowNodes.listStuckFlowNode.size() > jobExecution.getJobStopAfterMaxItems())
            milkJobOutput.executionStatus =ExecutionStatus.SUCCESSPARTIAL;
        else
            milkJobOutput.executionStatus =ExecutionStatus.SUCCESS;
        return milkJobOutput;
    }

}
