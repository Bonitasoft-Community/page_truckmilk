package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.radar.RadarFactory;
import org.bonitasoft.radar.RadarPhoto;
import org.bonitasoft.radar.Radar.RadarPhotoResult;
import org.bonitasoft.radar.Radar.RadarResult;
import org.bonitasoft.radar.RadarPhoto.IndicatorPhoto;
import org.bonitasoft.radar.archive.RadarCleanArchivedDross;
import org.bonitasoft.radar.archive.RadarCleanArchivedDross.DrossExecution;
import org.bonitasoft.radar.archive.RadarCleanArchivedDross.TypeDross;
import org.bonitasoft.radar.connector.RadarTimeTrackerConnector;
import org.bonitasoft.radar.connector.RadarTimeTrackerConnector.TimeTrackerParameter;
import org.bonitasoft.radar.workers.RadarWorkers;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.DELAYSCOPE;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInMeasurement;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TYPE_PLUGIN;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

public class MilkCleanArchivedDross extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkCleanArchivedDross.class.getName());

    private final static String RADAR_NAME_CONNECTORTIMETRACKER = "connectorTimeTracker";

    private static BEvent eventErrorNoRadarCleanArchiveDross = new BEvent(MilkRadarBonitaEngine.class.getName(), 1, Level.ERROR,
            "Clean Dross Radar not found", "The Radar Clean Dross is not found", "information can't be calculated", "Check library and dependency");

    private static String operationDetection = "Only detection";
    private static String operationPurge = "Purge";
  
    private static PlugInParameter cstParamOperation = PlugInParameter.createInstanceListValues("operation", "Operation", 
            new String[] { operationDetection, operationPurge },
            operationDetection, 
            "Detect or clean all Drosses in archived table  "
            + operationDetection + ": Only detect. "
            + operationPurge + ": Purge records.");

    public MilkCleanArchivedDross() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName("CleanArchivedDross");
        plugInDescription.setLabel("Clean Archived Dross");
        plugInDescription.setExplanation("Check dross in archived tables");
        plugInDescription.setCategory(CATEGORY.CASES);
        
        
        plugInDescription.addParameter(cstParamOperation);

        for (TypeDross typeDross : RadarCleanArchivedDross.getListTypeDross() )
        {
            plugInDescription.addMesure( PlugInMeasurement.createInstance( typeDross.name, typeDross.label, typeDross.explanation));
            String sqlQuery=typeDross.sqlQuery;
            sqlQuery = sqlQuery.replaceAll("\\?", String.valueOf( milkJobContext.getTenantId()));
            plugInDescription.addParameter( PlugInParameter.createInstanceInformation( typeDross.name, typeDross.label, 
                    typeDross.explanation+"<p>"
                    +"<div class='well'>select count(*) "+ sqlQuery+"</div>"));

        }
        
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
        MilkPlugInDescription plugInDescription = getDefinitionDescription(jobExecution.getMilkJobContext());
            
        
        milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;


        String operation = jobExecution.getInputStringParameter(cstParamOperation);
        DrossExecution drossExecution =null;
        if (operationDetection.equals( operation ))
            drossExecution = RadarCleanArchivedDross.getStatus(jobExecution.getTenantId() );
        else
            drossExecution = RadarCleanArchivedDross.deleteDross(jobExecution.getTenantId() );
        
        milkJobOutput.addEvents( drossExecution.listEvents);
        

        int totalDross=0;
        for (TypeDross typeDross : drossExecution.listDross) {
            PlugInMeasurement measure = plugInDescription.getMesureFromName( typeDross.name);
            
            milkJobOutput.setMesure(measure, typeDross.nbRecordsDetected);
            
            totalDross+= typeDross.nbRecordsDetected;

        }
        milkJobOutput.addMeasuresInReport(true);
        
        milkJobOutput.setNbItemsProcessed(totalDross);
        
        
        return milkJobOutput;
    }
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Private method */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    private double addMesureFromIndicator(PlugInMeasurement mesure, RadarPhotoResult radarPhotoResult, MilkJobOutput plugTourOutput) {
        List<IndicatorPhoto> list = radarPhotoResult.getIndicators(mesure.name);
        double value = 0;
        for (IndicatorPhoto photo : list) {
                value += photo.getValue();
        }
        plugTourOutput.setMesure(mesure, value);
        return value;
    }
}