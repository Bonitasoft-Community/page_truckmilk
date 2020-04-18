package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bonitasoft.deepmonitoring.radar.RadarFactory;
import org.bonitasoft.deepmonitoring.radar.connector.RadarTimeTrackerConnector;
import org.bonitasoft.deepmonitoring.radar.workers.RadarWorkers;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.flownode.FlowNodeExecutionException;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInMesure;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.TypesCast;
import org.hibernate.internal.jaxb.mapping.orm.JaxbInheritanceType;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
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
  
    private final static PlugInMesure cstMesureTasksExecuted = PlugInMesure.createInstance("TasksExecuted", "Tasks Executed", "Number of task executed");
    private final static PlugInMesure cstMesureTasksError = PlugInMesure.createInstance("TasksError", "Task execution error", "Number of task in error when a re-execution was asked");

    public MilkRestartFlowNodes() {
        super(TYPE_PLUGIN.EMBEDED);
    }


    @Override
    public List<BEvent> checkPluginEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();
    }

    @Override
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription() {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName( "ReplayFlowNodes");
        plugInDescription.setLabel( "Replay (Stuck) Flow Nodes");
        plugInDescription.setExplanation( "Check all flow nodes in the database, and if this number is over the number of Pending flownodes, restart them");
        plugInDescription.setCategory( CATEGORY.TASKS );

        plugInDescription.setJobCanBeStopped(JOBSTOPPER.BOTH);
        
          
        plugInDescription.addMesure(cstMesureTasksExecuted);
        plugInDescription.addMesure(cstMesureTasksError);
        
        return plugInDescription;
        }

    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();
        
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        
        // How many flownode do we have to re-execute?
        RadarFactory radarFactory = RadarFactory.getInstance();
        
        

        RadarWorkers radarWorkers = (RadarWorkers) radarFactory.getInstance(RADAR_NAME_WORKER, RadarWorkers.CLASS_RADAR_NAME, jobExecution.getTenantId(), jobExecution.getApiAccessor());

        
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
        
        List<Map<String,Object>> listFlowNodes = radarWorkers.getOldestFlowNodesWaitingForExecution(jobExecution.getTenantId(), jobExecution.getJobStopAfterMaxItems() +100 );
        if (listFlowNodes.isEmpty())
        {
            plugTourOutput.executionStatus =ExecutionStatus.SUCCESSNOTHING;
            plugTourOutput.setMesure(cstMesureTasksExecuted, 0);
            plugTourOutput.setMesure(cstMesureTasksError, 0);
            plugTourOutput.addReportTable( new String[] {"Indicator", "Value"});
            plugTourOutput.addReportLine( new Object[] {"Number of tasks detected", listFlowNodes.size() });
            plugTourOutput.addReportEndTable();
            
            return plugTourOutput;
        }
        // restart somes nodes
        jobExecution.setAvancementTotalStep(listFlowNodes.size());
        boolean oneErrorDetected=false;
        int countCorrects=0;
        int countErrors=0; 
        for (int i=0;i<listFlowNodes.size();i++)
        {
            // PB  ca ne s'arrete pas au max

            if (jobExecution.pleaseStop())
                break;
            jobExecution.setAvancementStep( i );

            Long flowNodeId = TypesCast.getLong(listFlowNodes.get( i ).get("ID"), null);
            Long rootContainerId = TypesCast.getLong( listFlowNodes.get( i ).get("ROOTCONTAINERID"), null);
            if (flowNodeId!=null)
                try {
                    processAPI.executeFlowNode( flowNodeId);
                    countCorrects++;
                } catch (Exception e) {
                    
                    // the flow node may notexist, because a worker executed it in the mean time
                    oneErrorDetected=true;
                    plugTourOutput.addEvent(new BEvent(eventErrorExecuteFlownode, "FlowNodeId["+flowNodeId+"] caseId["+rootContainerId+"] "+e.getMessage()));
                    countErrors++;
                } 
            else {
                plugTourOutput.addEvent(new BEvent(eventErrorExecuteFlownode, "FlowNodeId is not a Long["+listFlowNodes.get( i ).get("ID")+"] CaseId["+listFlowNodes.get( i ).get("ROOTCONTAINERID")+"]"));
                countErrors++;
            }
        }
        plugTourOutput.addReportTable( new String[] {"Indicator", "Value"});
        plugTourOutput.addReportLine( new Object[] {"Number of tasks detected", listFlowNodes.size() });
        plugTourOutput.addReportLine( new Object[] {"Task executed", countCorrects});
        plugTourOutput.addReportLine( new Object[] {"Task execution error", countErrors});
        plugTourOutput.addReportEndTable();
        
        plugTourOutput.setMesure(cstMesureTasksExecuted, countCorrects);
        plugTourOutput.setMesure(cstMesureTasksError, countErrors);
        plugTourOutput.setNbItemsProcessed(countCorrects);
        
        if (oneErrorDetected)
            plugTourOutput.executionStatus =ExecutionStatus.ERROR;
        else if (listFlowNodes.size() > jobExecution.getJobStopAfterMaxItems())
            plugTourOutput.executionStatus =ExecutionStatus.SUCCESSPARTIAL;
        else
            plugTourOutput.executionStatus =ExecutionStatus.SUCCESS;
        return plugTourOutput;
    }

}
