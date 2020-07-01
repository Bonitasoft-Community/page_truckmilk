package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.radar.Radar;
import org.bonitasoft.radar.Radar.RadarPhotoParameter;
import org.bonitasoft.radar.Radar.RadarPhotoResult;
import org.bonitasoft.radar.Radar.RadarResult;
import org.bonitasoft.radar.RadarFactory;
import org.bonitasoft.radar.RadarPhoto;
import org.bonitasoft.radar.RadarPhoto.IndicatorPhoto;
import org.bonitasoft.radar.connector.RadarTimeTrackerConnector;
import org.bonitasoft.radar.connector.RadarTimeTrackerConnector.TimeTrackerParameter;
import org.bonitasoft.radar.process.RadarCase;
import org.bonitasoft.radar.process.RadarProcess;
import org.bonitasoft.radar.process.RadarProcess.ParameterProcess;
import org.bonitasoft.radar.workers.RadarWorkers;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

public class MilkRadarBonitaEngine extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkRadarBonitaEngine.class.getName());

    private final static String RADAR_NAME_CONNECTORTIMETRACKER = "connectorTimeTracker";

    private static BEvent eventErrorNoRadarTrackerConnector = new BEvent(MilkRadarBonitaEngine.class.getName(), 1, Level.ERROR,
            "Tracker Connector radar not found", "The Radar Tracker Connector is not found", "Connector Too Long can't be calculated", "Check library and dependency");
    private static BEvent eventConnectorTooLongStatus = new BEvent(MilkRadarBonitaEngine.class.getName(), 2, Level.INFO,
            "Connector", "Status on the connector too long radar");

    private static PlugInParameter cstParamMonitorConnector = PlugInParameter.createInstance("Connector", "Connector Too Long", TypeParameter.BOOLEAN, true, "Monitor connector execution. When an execution is too long, return it");
    private static PlugInParameter cstParamConnectorDurationInSecond = PlugInParameter.createInstance("Connectorduration", "Connector Limit Duration (seconds)", TypeParameter.LONG, 50L, "Report all connector execution longer than this value");
    private static PlugInParameter cstParamConnectorFrame = PlugInParameter.createInstanceDelay("Connectframe", "Connector Frame (in minutes)", DELAYSCOPE.HOUR, 12, "Search in all connector execution between now and now - frame");

    private static PlugInParameter cstParamMonitorWorker = PlugInParameter.createInstance("Worker", "Workers", TypeParameter.BOOLEAN, true, "Monitor workers. Return number of workers running, waiting");

    private static PlugInParameter cstParamStatisticsProcess = PlugInParameter.createInstance("Process", "Process", TypeParameter.BOOLEAN, true, "Statistics on process (Number of process, number of active case)");
    private static PlugInParameter cstParamProcessDelayDeployment =   PlugInParameter.createInstanceDelay("ProcessDeployment", "Delay to collect processes deployed in this time frame", DELAYSCOPE.WEEK, 1, "Collect processes deployed in this delay");


    private final static PlugInMeasurement cstMesureConnectorConnectorCall = PlugInMeasurement.createInstance(RadarTimeTrackerConnector.CSTPHOTO_CONNECTORSCALL, "Number of connector called", "Give the number of connector called in the period").withTypeMeasure(TypeMeasure.LONG);
    private final static PlugInMeasurement cstMesureConnectorConnectorOverloaded = PlugInMeasurement.createInstance(RadarTimeTrackerConnector.CSTPHOTO_CONNECTORSOVERLOADED, "Number of connector with a long execution", "Number of connectors with a long execution").withTypeMeasure(TypeMeasure.LONG);

    public MilkRadarBonitaEngine() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
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

        plugInDescription.addParameterSeparator("Statistics", "Statistics activity");
        plugInDescription.addParameter(cstParamStatisticsProcess);
        plugInDescription.addParameter(cstParamProcessDelayDeployment);
        
        plugInDescription.addMesure(cstMesureConnectorConnectorCall);
        plugInDescription.addMesure(cstMesureConnectorConnectorOverloaded);

        return plugInDescription;
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
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput milkJobOutput = jobExecution.getMilkJobOutput();

        milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;

        // we return a SUCCESSNOTHING is nothing is visible on radars
        boolean everythingIsCalm = false;
        everythingIsCalm=true;

        // How many flownode do we have to re-execute?
        RadarFactory radarFactory = RadarFactory.getInstance();
        List<RadarPhoto> listPhoto = new ArrayList<>();
        milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;

        if (Boolean.TRUE.equals(jobExecution.getInputBooleanParameter(cstParamMonitorConnector))) {
            RadarTimeTrackerConnector radarTimeTrackerConnector = (RadarTimeTrackerConnector) radarFactory.getInstance(RADAR_NAME_CONNECTORTIMETRACKER, RadarTimeTrackerConnector.CLASS_RADAR_NAME, jobExecution.getTenantId(), jobExecution.getApiAccessor());

            if (radarTimeTrackerConnector == null) {
                milkJobOutput.addEvent(new BEvent(eventErrorNoRadarTrackerConnector, "Radar Worker[" + RadarWorkers.CLASS_RADAR_NAME + "] not found"));
                milkJobOutput.executionStatus = ExecutionStatus.ERROR;
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
                    milkJobOutput.addEvents(radarResult.listEvents);
                    isActivated = radarResult.isActivated;
                }

                // take a photo now
                if (isActivated) {

                    // Take the photo
                    TimeTrackerParameter timeTrackerParameter = new TimeTrackerParameter();
                    RadarPhotoResult radarPhotoResult = radarTimeTrackerConnector.takePhoto(timeTrackerParameter);

                    listPhoto.addAll(radarPhotoResult.listPhotos);
                    milkJobOutput.addEvents(radarPhotoResult.listEvents);

                    // set up different mesure

                    int connectorCall = (int) addMesureFromIndicator(cstMesureConnectorConnectorCall, radarPhotoResult, milkJobOutput);
                    int connectorOverload = (int) addMesureFromIndicator(cstMesureConnectorConnectorOverloaded, radarPhotoResult, milkJobOutput);

                    if (connectorCall > 0 || connectorOverload > 0)
                        everythingIsCalm = false;

                    List<IndicatorPhoto> list = radarPhotoResult.getIndicators(cstMesureConnectorConnectorOverloaded.name);

                    milkJobOutput.addReportTable(new String[] { "Connectors Radar", "Details" });

                    milkJobOutput.addReportLine(new Object[] { "Connector call", connectorCall });
                    milkJobOutput.addReportLine(new Object[] { "connectorOverload", connectorOverload });
                    milkJobOutput.addReportLine(new Object[] { "Threshold (in sec)", jobExecution.getInputLongParameter(cstParamConnectorDurationInSecond) });

                    for (IndicatorPhoto indicator : list)
                        milkJobOutput.addReportLine(new String[] { indicator.getName(), indicator.details == null ? "" : indicator.details });

                    milkJobOutput.addReportEndTable();

                    milkJobOutput.addEvent(new BEvent(eventConnectorTooLongStatus, "Connectors called[" + connectorCall + "] Overload[" + connectorOverload + "]"));

                }
            }
            if (everythingIsCalm && milkJobOutput.executionStatus == ExecutionStatus.SUCCESS)
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
        }
        if (Boolean.TRUE.equals(jobExecution.getInputBooleanParameter(cstParamStatisticsProcess))) {
            String[] listRadarsName = new String[] { RadarCase.CLASS_RADAR_NAME, RadarProcess.CLASS_RADAR_NAME };

            DelayResult delayDeploymentResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamProcessDelayDeployment, new Date(), false);
            if (BEventFactory.isError(delayDeploymentResult.listEvents)) {
                milkJobOutput.addEvents(delayDeploymentResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                return milkJobOutput;
            }
            
            
            
            milkJobOutput.addReportTable(new String[] { "Measure", "Value" });

            for (String radarName : listRadarsName) {
                Radar radar = radarFactory.getInstance(radarName, radarName, jobExecution.getTenantId(), jobExecution.getApiAccessor());

                if (radar == null) {
                    milkJobOutput.addEvent(new BEvent(eventErrorNoRadarTrackerConnector, "Radar Worker[" + RadarWorkers.CLASS_RADAR_NAME + "] not found"));
                    milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                    continue;
                }
                RadarPhotoResult result = radar.takePhoto( getRadarPhotoParameter(radar,jobExecution));
                milkJobOutput.addEvents(result.listEvents);
                for (RadarPhoto photo : result.listPhotos) {
                    for (IndicatorPhoto indicator : photo.getListIndicators()) {
                        milkJobOutput.addReportLine(new Object[] { indicator.getName(), indicator.isValue() ? indicator.getValue() : indicator.getValuePercent() });
                    }
                }
            }
            milkJobOutput.addReportEndTable();
        }

        return milkJobOutput;
    }
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Private method */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    private RadarPhotoParameter getRadarPhotoParameter( Radar radar,MilkJobExecution jobExecution ) {
        if (radar instanceof RadarProcess) {
            ParameterProcess parameter=new RadarProcess.ParameterProcess();
            DelayResult delayDeploymentResult = MilkPlugInToolbox.getTimeFromDelay(jobExecution, cstParamProcessDelayDeployment, new Date(), false);
            
            parameter.dateDeploymentAfter = delayDeploymentResult.delayDate.getTime();
            return parameter;
                    
        }
        return null;
    }
    /**
     * 
     * @param mesure
     * @param radarPhotoResult
     * @param plugTourOutput
     * @return
     */
    private double addMesureFromIndicator(PlugInMeasurement mesure, RadarPhotoResult radarPhotoResult, MilkJobOutput plugTourOutput) {
        List<IndicatorPhoto> list = radarPhotoResult.getIndicators(mesure.name);
        double value = 0;
        for (IndicatorPhoto photo : list) {
            value += photo.getValueDouble(0);
        }
        plugTourOutput.setMeasure(mesure, value);
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
