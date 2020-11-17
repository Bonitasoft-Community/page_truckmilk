package org.bonitasoft.truckmilk.plugin.other;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.meteor.MeteorAPI.StatusParameters;
import org.bonitasoft.meteor.MeteorClientAPI;
import org.bonitasoft.meteor.MeteorSimulation;
import org.bonitasoft.meteor.cmd.CmdMeteor;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;

public class MilkMeteor extends MilkPlugIn {

    Logger logger = Logger.getLogger( MilkMeteor.class.getName() );
    
    private static PlugInParameter cstParamScenarioName = PlugInParameter.createInstance("scenarioName", "Scenario name", TypeParameter.STRING, "", "Give the scenario name in Meteor");

    private static BEvent eventCantStartTest = new BEvent(MilkMeteor.class.getName(), 1, Level.APPLICATIONERROR,
            "Scenario can't start", "An error is detected at startup. The scenario may not exists","The startup failed", "Check the error reported" );
    private static BEvent eventExecutionSuccessfull= new BEvent(MilkMeteor.class.getName(), 2, Level.SUCCESS,       "Scenario executed with success", "A Scenario is executed, and finished with success" );
    private static BEvent eventExecutionNoRobot= new BEvent(MilkMeteor.class.getName(), 3, Level.APPLICATIONERROR,          "Bad Scenario definition", "The scenario have nothing to start.", "No execution is possible", "Check the scenario definition" );
    private static BEvent eventExecutionDontFinish= new BEvent(MilkMeteor.class.getName(), 4, Level.INFO,            "Scenario does not finish on time", "The scenario's execution does not finish in the given time","Scenario still running", "Check the scenario execution, give more time to execute" );
    
    
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    public MilkMeteor() {
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

        plugInDescription.setName( "Meteor");
        plugInDescription.setLabel( "Meteor");
        plugInDescription.setExplanation( "Call a Meteor scenario");
        plugInDescription.setCategory( CATEGORY.OTHER);
        plugInDescription.setStopJob( JOBSTOPPER.MAXMINUTES );
        plugInDescription.addParameter(cstParamScenarioName);

        
        return plugInDescription;
    }

    @Override
    public MilkJobOutput executeJob(MilkJobExecution jobExecution) {
       
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();

        String scenarioName = jobExecution.getInputStringParameter(cstParamScenarioName.name, null);

        MeteorClientAPI meteorClientAPI = new MeteorClientAPI();
        
        Map<String, Object> resultCommand = meteorClientAPI.startFromScenarioName(scenarioName, jobExecution.getApiAccessor().getProcessAPI(), jobExecution.getApiAccessor().getCommandAPI(), jobExecution.getTenantId());
        
        logger.info("~~~~~~~~~~ MilkMeteor.startFromName() : END " + resultCommand);
    
        // get the Simulation Id
        String simulationId = (String) resultCommand.get( CmdMeteor.CSTPARAM_RESULTSIMULATIONID);
        if (simulationId == null)
        {
            // job can't start
            plugTourOutput.addEvent(new BEvent( eventCantStartTest, "Scenario name["+scenarioName+"] : "+(String) resultCommand.get( CmdMeteor.CSTPARAM_RESULTLISTEVENTSST)));
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
            return plugTourOutput;
        }
        // get the status
        boolean stillWait=true;
        while ( stillWait && ! jobExecution.isStopRequired()) 
        {
            // check the result of the simulation
            StatusParameters statusParameters = new StatusParameters();
            statusParameters.simulationId = Long.valueOf(simulationId);
            
            Map<String, Object> resultStatus = meteorClientAPI.getStatus(statusParameters, jobExecution.getApiAccessor().getProcessAPI(),  jobExecution.getApiAccessor().getCommandAPI(), jobExecution.getTenantId());
            String statusSimulation = (String) resultStatus.get( MeteorSimulation.CSTJSON_STATUS);
            Integer percentAdvance = (Integer) resultStatus.get( MeteorSimulation.CSTJSON_PERCENTADVANCE );
            if (percentAdvance!=null)
                jobExecution.setAvancement( percentAdvance);
            
            if (MeteorSimulation.STATUS.DONE.toString().equals( statusSimulation )) {
                stillWait=false;
                String total = (String) resultStatus.get( "total");

                plugTourOutput.addEvent(new BEvent( eventExecutionSuccessfull, "Test executed with success : SimulationId["+simulationId+"] "+total ));

                plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
            
            }
            if (MeteorSimulation.STATUS.NOROBOT.toString().equals( statusSimulation ) || MeteorSimulation.STATUS.NOSIMULATION.toString().equals( statusSimulation )) {
                stillWait=false;
                plugTourOutput.addEvent(new BEvent( eventExecutionNoRobot, "Scenario does not have any robot to execute: SimulationId["+simulationId+"] ScenarioName["+scenarioName+"]" ));
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
            }
            
                
            if (stillWait) {
                try {
                    Thread.sleep( (long) 1000*30); // 30 secondes
                } catch(Exception e) {
                }
            }
        }
        if (jobExecution.isStopRequired())
        {
            plugTourOutput.addEvent(new BEvent( eventExecutionDontFinish, "SimulationId["+simulationId+"] ScenarioName["+scenarioName+"]" ));
            plugTourOutput.executionStatus = ExecutionStatus.ERROR; // scenarion isn't finish in the expected time, this is an error

        }
        return plugTourOutput;
    }

}
