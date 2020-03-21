package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bonitasoft.deepmonitoring.radar.RadarFactory;
import org.bonitasoft.deepmonitoring.radar.workers.RadarWorkers;
import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.flownode.FlowNodeExecutionException;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

public class MilkRestartFlowNodes  extends MilkPlugIn {

    public MilkRestartFlowNodes() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    static MilkLog logger = MilkLog.getLogger(MilkRestartFlowNodes.class.getName());

    private final static BEvent eventErrorExecuteFlownode = new BEvent(MilkRestartFlowNodes.class.getName(), 1,
            Level.APPLICATIONERROR,
            "Error executed a flow node", "Executing a flow node failed", "Flow node is not restarted",
            "Check exception");
    
    private final static BEvent eventErrorNoRadarWorker = new BEvent(MilkRestartFlowNodes.class.getName(), 2,
            Level.ERROR,
            "No radar Worker found", "A radar definition is missing", "Flow nodes to restart can't be detected",
            "Check exception");
    private static PlugInParameter cstParamNumberOfFlowNodesToRestart = PlugInParameter.createInstance("NumberOfFlowNodesToRestart", "Number Of Flow Nodes to restart", TypeParameter.LONG, 5000, "The x oldest flow nodes are restarted");
    
    @Override
    public List<BEvent> checkPluginEnvironment(long tenantId, APIAccessor apiAccessor) {
        return new ArrayList<>();
    }

    @Override
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        return new ArrayList<>();
    }

    @Override
    public PlugInDescription getDefinitionDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();
        plugInDescription.name = "ReplayFlowNodes";
        plugInDescription.label = "Replay (Stuck) Flow Nodes";
        plugInDescription.category = CATEGORY.TASKS;
        plugInDescription.description = "Check all flow nodes in the database, and if this number is over the number of Pending flownodes, restart them";
        plugInDescription.addParameter(cstParamNumberOfFlowNodesToRestart);
        return plugInDescription;
        }

    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();
        
        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        
        // How many flownode do we have to re-execute?
        RadarFactory radarFactory = RadarFactory.getInstance();
        radarFactory.initialisation(jobExecution.tenantId, apiAccessor);
        
        RadarWorkers radarWorkers = (RadarWorkers) radarFactory.getRadars(RadarWorkers.CLASS_RADAR_NAME);
        if (radarWorkers==null)
        {
            plugTourOutput.addEvent(new BEvent(eventErrorNoRadarWorker, "Radar Worker["+RadarWorkers.CLASS_RADAR_NAME+"] not found"));
            plugTourOutput.executionStatus =ExecutionStatus.ERROR;
            return plugTourOutput;
            
        }
        // +1 then we can detect if we re-execute ALL flow nodes, or less that we have in database
        /*
         * SELECT f.*
FROM flownode_instance AS f 
WHERE (f.state_Executing = 1 OR f.stable = 0 OR f.terminal = 1 OR f.stateCategory = 'ABORTING' OR f.stateCategory = 'CANCELLING') 
ORDER BY id;
         */
        List<Map<String,Object>> listFlowNodes = radarWorkers.getOldestFlowNodesWaitingForExecution(jobExecution.tenantId, 1+ jobExecution.getInputLongParameter(cstParamNumberOfFlowNodesToRestart));
        if (listFlowNodes.isEmpty())
        {
            plugTourOutput.executionStatus =ExecutionStatus.SUCCESSNOTHING;
            return plugTourOutput;
        }
        // restart somes nodes
        jobExecution.setAvancementTotalStep(listFlowNodes.size());
        boolean oneErrorDetected=false;
        
        for (int i=0;i<listFlowNodes.size();i++)
        {
            if (jobExecution.pleaseStop())
                break;
            jobExecution.setAvancementStep( i );

            Long flowNodeId = (Long) listFlowNodes.get( i ).get("ID");
            Long rootContainerId = (Long) listFlowNodes.get( i ).get("ROOTCONTAINERID");
            
            try {
                processAPI.executeFlowNode( flowNodeId);
            } catch (FlowNodeExecutionException e) {
                
                // the flow node may notexist, because a worker executed it in the mean time
                oneErrorDetected=true;
                plugTourOutput.addEvent(new BEvent(eventErrorExecuteFlownode, "FlowNodeId["+flowNodeId+"] caseId["+rootContainerId+"]"));

            }
        }
        if (oneErrorDetected)
            plugTourOutput.executionStatus =ExecutionStatus.ERROR;
        else if (listFlowNodes.size() > jobExecution.getInputLongParameter(cstParamNumberOfFlowNodesToRestart))
            plugTourOutput.executionStatus =ExecutionStatus.SUCCESSPARTIAL;
        else
            plugTourOutput.executionStatus =ExecutionStatus.SUCCESS;
        return plugTourOutput;
    }

}
