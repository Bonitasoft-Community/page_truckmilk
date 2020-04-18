package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bonitasoft.deepmonitoring.radar.Radar.RadarPhotoResult;
import org.bonitasoft.deepmonitoring.radar.Radar.RadarResult;
import org.bonitasoft.deepmonitoring.radar.RadarFactory;
import org.bonitasoft.deepmonitoring.radar.RadarPhoto;
import org.bonitasoft.deepmonitoring.radar.RadarPhoto.IndicatorPhoto;
import org.bonitasoft.deepmonitoring.radar.connector.RadarTimeTrackerConnector;
import org.bonitasoft.deepmonitoring.radar.connector.RadarTimeTrackerConnector.TimeTrackerParameter;
import org.bonitasoft.deepmonitoring.radar.workers.RadarWorkers;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

public class MilkRadarBonitaEngine extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkRadarBonitaEngine.class.getName());

    private final static String RADAR_NAME_CONNECTORTIMETRACKER = "connectorTimeTracker";

    private static BEvent eventErrorNoRadarTrackerConnector = new BEvent(MilkRadarBonitaEngine.class.getName(), 1, Level.ERROR,
            "Tracker Connector radar not found", "The Radar Tracker Connector is not found", "Connector Too Long can't be calculated", "Check library and dependency");
    private static BEvent eventConnectorTooLongStatus = new BEvent(MilkRadarBonitaEngine.class.getName(), 2, Level.INFO,
            "Connector", "Status on the connector too long radar" );

    private static PlugInParameter cstParamMonitorConnector = PlugInParameter.createInstance("Connector", "Connector Too Long", TypeParameter.BOOLEAN, true, "Monitor connector execution. When an execution is too long, return it");
    private static PlugInParameter cstParamConnectorDurationInSecond = PlugInParameter.createInstance("Connectorduration", "Connector Limit Duration (seconds)", TypeParameter.LONG, 50L, "Report all connector execution longer than this value");
    private static PlugInParameter cstParamConnectorFrame = PlugInParameter.createInstance("Connectframe", "Connector Frame (in minutes)", TypeParameter.DELAY, MilkPlugInToolbox.DELAYSCOPE.HOUR + ":12", "Search in all connector execution between now and now - frame");

    private static PlugInParameter cstParamMonitorWorker = PlugInParameter.createInstance("Worker", "Workers", TypeParameter.BOOLEAN, true, "Monitor workers. Return number of workers running, waiting");

    private final static PlugInMesure cstMesureConnectorConnectorCall = PlugInMesure.createInstance(RadarTimeTrackerConnector.CSTPHOTO_CONNECTORSCALL, "Number of connector called", "Give the number of connector called in the period");
    private final static PlugInMesure cstMesureConnectorConnectorOverloaded = PlugInMesure.createInstance(RadarTimeTrackerConnector.CSTPHOTO_CONNECTORSOVERLOADED, "Number of connector with a long execution", "Number of connectors with a long execution");

    public MilkRadarBonitaEngine() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription() {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName("RadarBonitaEngine");
        plugInDescription.setLabel("Radar Bonita Engine");
        plugInDescription.setExplanation("Monitor different indicators");
        plugInDescription.setCategory(CATEGORY.MONITOR);
        
        
        plugInDescription.addParameterSeparator("Radar Connector", "Monitor the connector activity");
        plugInDescription.addParameter(cstParamMonitorConnector);
        plugInDescription.addParameter(cstParamConnectorDurationInSecond);
        plugInDescription.addParameter(cstParamConnectorFrame);

        plugInDescription.addParameterSeparator("Radar Worker", "Workers activity");
        plugInDescription.addParameter(cstParamMonitorWorker);
        
        plugInDescription.addMesure(cstMesureConnectorConnectorCall);
        plugInDescription.addMesure(cstMesureConnectorConnectorOverloaded);
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

        plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;

        // we return a SUCCESSNOTHING is nothing is visible on radars
        boolean everythingIsCalm=true;
        
        // How many flownode do we have to re-execute?
        RadarFactory radarFactory = RadarFactory.getInstance();
        List<RadarPhoto> listPhoto = new ArrayList<>();
        plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;

        if (Boolean.TRUE.equals(jobExecution.getInputBooleanParameter(cstParamMonitorConnector))) {
            RadarTimeTrackerConnector radarTimeTrackerConnector = (RadarTimeTrackerConnector) radarFactory.getInstance(RADAR_NAME_CONNECTORTIMETRACKER, RadarTimeTrackerConnector.CLASS_RADAR_NAME, jobExecution.getTenantId(), jobExecution.getApiAccessor());

            if (radarTimeTrackerConnector == null) {
                plugTourOutput.addEvent(new BEvent(eventErrorNoRadarTrackerConnector, "Radar Worker[" + RadarWorkers.CLASS_RADAR_NAME + "] not found"));
                plugTourOutput.executionStatus = ExecutionStatus.ERROR;
            } else {

                // update the configuration
                radarTimeTrackerConnector.setSpringAccessor(jobExecution.getTenantServiceAccessor());

                radarTimeTrackerConnector.setThresholdDuration(jobExecution.getInputLongParameter(cstParamConnectorDurationInSecond) * 1000);
                DelayResult delay = jobExecution.getInputDelayParameter(cstParamConnectorFrame, new Date(), false);
                long delayFrameInMs = -delay.delayInMs;
                radarTimeTrackerConnector.setFrameMonitorInMs(delayFrameInMs);

                // if the tracking is not setup, do it now
                boolean isActivated = radarTimeTrackerConnector.isActivated().isActivated;
                if (!isActivated) {
                    RadarResult radarResult = radarTimeTrackerConnector.activate();
                    plugTourOutput.addEvents(radarResult.listEvents);
                    isActivated = radarResult.isActivated;
                }

                // take a photo now
                if (isActivated) {
                    
                    // Take the photo
                    TimeTrackerParameter timeTrackerParameter = new TimeTrackerParameter(); 
                    RadarPhotoResult radarPhotoResult = radarTimeTrackerConnector.takePhoto(timeTrackerParameter);
                    
                    listPhoto.addAll(radarPhotoResult.listPhotos);
                    plugTourOutput.addEvents(radarPhotoResult.listEvents);

                    // set up different mesure
                    
                    int connectorCall = (int) addMesureFromIndicator(cstMesureConnectorConnectorCall, radarPhotoResult, plugTourOutput);
                    int connectorOverload = (int) addMesureFromIndicator(cstMesureConnectorConnectorOverloaded, radarPhotoResult, plugTourOutput);
                    
                    if (connectorCall>0 || connectorOverload>0)
                        everythingIsCalm=false;
                    
                    List<IndicatorPhoto> list = radarPhotoResult.getIndicators(cstMesureConnectorConnectorOverloaded.name);


                    plugTourOutput.addReportTable(new String[] { "Connectors Radar", "Details"});
                    
                    plugTourOutput.addReportLine(new Object[] { "Connector call", connectorCall });
                    plugTourOutput.addReportLine(new Object[] { "connectorOverload", connectorOverload });
                    plugTourOutput.addReportLine(new Object[] { "Threshold (in sec)", jobExecution.getInputLongParameter(cstParamConnectorDurationInSecond) });
                    
                    for (IndicatorPhoto indicator : list )
                        plugTourOutput.addReportLine(new String[] { indicator.getName(), indicator.details==null ? "" : indicator.details });
                    
                    plugTourOutput.addReportEndTable();
                    
                    plugTourOutput.addEvent( new BEvent( eventConnectorTooLongStatus, "Connectors called["+connectorCall+"] Overload["+connectorOverload+"]" ));

                }
            }
            if (everythingIsCalm && plugTourOutput.executionStatus == ExecutionStatus.SUCCESS)
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
        }

        return plugTourOutput;
    }
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Private method */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    private double addMesureFromIndicator(PlugInMesure mesure, RadarPhotoResult radarPhotoResult, MilkJobOutput plugTourOutput) {
        List<IndicatorPhoto> list = radarPhotoResult.getIndicators(mesure.name);
        double value = 0;
        for (IndicatorPhoto photo : list) {
                value += photo.getValue();
        }
        plugTourOutput.setMesure(mesure, value);
        return value;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Notification */
    /*                                                                                  */
    /* When a job is registered, we need to start the tracking */
    /*                                                                                  */
    /* ******************************************************************************** */
    /**
     * 
     */
    @Override
    public List<BEvent> notifyRegisterAJob(MilkJob milkJob, MilkJobExecution jobExecution) {
        List<BEvent> listEvents = new ArrayList<>();        

        RadarFactory radarFactory = RadarFactory.getInstance();

        RadarTimeTrackerConnector radarTimeTrackerConnector = (RadarTimeTrackerConnector) radarFactory.getInstance(RADAR_NAME_CONNECTORTIMETRACKER, RadarTimeTrackerConnector.CLASS_RADAR_NAME, jobExecution.getTenantId(), jobExecution.getApiAccessor());

        if (radarTimeTrackerConnector == null) {
            listEvents.add(new BEvent(eventErrorNoRadarTrackerConnector, "Radar Worker[" + RadarWorkers.CLASS_RADAR_NAME + "] not found"));
            return listEvents;
        }

        // update the configuration
        radarTimeTrackerConnector.setSpringAccessor(jobExecution.getTenantServiceAccessor());

        // if the tracking is not setup, do it now
        boolean isActivated = radarTimeTrackerConnector.isActivated().isActivated;
        if (!isActivated) {
            RadarResult radarResult = radarTimeTrackerConnector.activate();
            listEvents.addAll(radarResult.listEvents);
        }
        return listEvents;
    }

    @Override
    public List<BEvent> notifyUnregisterAJob(MilkJob milkJob, MilkJobExecution jobExecution) {
        List<BEvent> listEvents = new ArrayList<>();

        boolean anAnotherJobExist = false;
        // is an another job exist ?
        for (MilkJob milkJobs : milkJob.getMilkJobFactory().getListJobs()) {
            if (milkJob.getPlugIn().getClass().equals(this.getClass())) {
                anAnotherJobExist = true;
            }
        }
        if (anAnotherJobExist)
            return listEvents;
        // deactivate
        RadarFactory radarFactory = RadarFactory.getInstance();

        RadarTimeTrackerConnector radarTimeTrackerConnector = (RadarTimeTrackerConnector) radarFactory.getInstance(RADAR_NAME_CONNECTORTIMETRACKER, RadarTimeTrackerConnector.CLASS_RADAR_NAME, jobExecution.getTenantId(), jobExecution.getApiAccessor());
        if (radarTimeTrackerConnector == null) {
            listEvents.add(new BEvent(eventErrorNoRadarTrackerConnector, "Radar Worker[" + RadarWorkers.CLASS_RADAR_NAME + "] not found"));
            return listEvents;
        }

        // update the configuration
        radarTimeTrackerConnector.setSpringAccessor(jobExecution.getTenantServiceAccessor());

        // if the tracking is not setup, do it now
        boolean isActivated = radarTimeTrackerConnector.isActivated().isActivated;
        if (isActivated) {
            RadarResult radarResult = radarTimeTrackerConnector.deactivate();
            listEvents.addAll(radarResult.listEvents);
        }
        return listEvents;

    }

}
