package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bonitasoft.deepmonitoring.radar.RadarFactory;
import org.bonitasoft.deepmonitoring.radar.RadarPhoto;
import org.bonitasoft.deepmonitoring.radar.connector.RadarTimeTrackerConnector;
import org.bonitasoft.deepmonitoring.radar.workers.RadarWorkers;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

public class MilkRadarBonitaEngine extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkRadarBonitaEngine.class.getName());

    
    private final static String RADAR_NAME_CONNECTORTIMETRACKER= "connectorTimeTracker";
    
    private static BEvent eventErrorNoRadarTrackerConnector = new BEvent(MilkRadarBonitaEngine.class.getName(), 1, Level.ERROR,
            "Tracker Connector radar not found", "The Radar Tracker Connector is not found", "Connector Too Long can't be calculated", "Check library and dependency");


    private static PlugInParameter cstParamMonitorConnector = PlugInParameter.createInstance("Connector", "Connector Too Long", TypeParameter.BOOLEAN, true, "Monitor connector execution. When an execution is too long, return it");
    private static PlugInParameter cstParamConnectorDuration = PlugInParameter.createInstance("Connectorduration", "Connector Limit Duration (seconds)", TypeParameter.LONG, true, "Report all connector execution longer than this value");
    private static PlugInParameter cstParamConnectorFrame = PlugInParameter.createInstance("Connectframe", "Connector Frame (in minutes)", TypeParameter.DELAY, MilkPlugInToolbox.DELAYSCOPE.HOUR + ":12", "Search in all connector execution between now and now - frame");

    public MilkRadarBonitaEngine() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription() {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName("RadarBonitaEngine");
        plugInDescription.setLabel("Radar Bonita Engine");
        plugInDescription.setDescription("Monitor different indicator");
        plugInDescription.setCategory(CATEGORY.MONITOR);
        plugInDescription.addParameter(cstParamMonitorConnector);
        plugInDescription.addParameter(cstParamConnectorDuration);
        plugInDescription.addParameter(cstParamConnectorFrame);
        return plugInDescription;
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
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();


        // How many flownode do we have to re-execute?
        RadarFactory radarFactory = RadarFactory.getInstance();
        radarFactory.initialisation(jobExecution.getTenantId(), jobExecution.getApiAccessor());
        List<RadarPhoto> listPhoto = new ArrayList<>();
        
        
        if (Boolean.TRUE.equals(jobExecution.getInputBooleanParameter(cstParamMonitorConnector))) {
            RadarTimeTrackerConnector radarTimeTrackerConnector = (RadarTimeTrackerConnector) radarFactory.getInstance(RADAR_NAME_CONNECTORTIMETRACKER, RadarTimeTrackerConnector.CLASS_RADAR_NAME, jobExecution.getTenantId(), jobExecution.getApiAccessor());
            
            if (radarTimeTrackerConnector == null) {
                plugTourOutput.addEvent(new BEvent(eventErrorNoRadarTrackerConnector, "Radar Worker[" + RadarWorkers.CLASS_RADAR_NAME + "] not found"));
                plugTourOutput.executionStatus = ExecutionStatus.ERROR;
            } else {
                
                // update the configuration
                radarTimeTrackerConnector.setThresholdDuration( jobExecution.getInputLongParameter( cstParamConnectorDuration ) );
                DelayResult delay = jobExecution.getInputDelayParameter(cstParamConnectorFrame, new Date(), false);
                long delayFrameInMs = - delay.delayInMs;
                radarTimeTrackerConnector.setFrameMonitorInMs( delayFrameInMs );
                radarTimeTrackerConnector.setSpringAccessor(jobExecution.getTenantServiceAccessor());
                if (! radarTimeTrackerConnector.isStarted())
                {
                    radarTimeTrackerConnector.start();                    
                }

                // take a photo now
                listPhoto.addAll( radarTimeTrackerConnector.takePhoto());
            }
        }

        plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
        return plugTourOutput;
    }

}
