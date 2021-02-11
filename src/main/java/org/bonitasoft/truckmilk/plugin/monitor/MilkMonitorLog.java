package org.bonitasoft.truckmilk.plugin.monitor;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.page.log.LogAccess;
import org.bonitasoft.page.log.LogAccess.FileInformation;
import org.bonitasoft.page.log.LogAccess.LogParameter;
import org.bonitasoft.page.log.LogAnalysisSynthese;
import org.bonitasoft.page.log.LogAnalysisSynthese.LogAnalyseItem;
import org.bonitasoft.page.log.LogInformation;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

public class MilkMonitorLog extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkMonitorLog.class.getName());

    private static String operationDetection = "Only detection";
    private static String operationPurge = "Purge";

    private static PlugInParameter cstParamNumberOfErrors = PlugInParameter.createInstance("nbOfLinesInTop", "Number of result in the TOP", TypeParameter.LONG, Long.valueOf(10), "Gives the number of line returned in the top x");
    private static PlugInParameter cstParamOnlyErrorsInTop = PlugInParameter.createInstance("onlyErrorsInTop", "Only errors in the TOP", TypeParameter.BOOLEAN, Boolean.TRUE, "If set, only error are returned in the TOP list");
    
    private static PlugInMeasurement nbErrors = PlugInMeasurement.createInstance("nbErrors", "Number of errors", "Number of errors detected in the file").withTypeMeasure(TypeMeasure.LONG);
    private static PlugInMeasurement nbDifferentErrors = PlugInMeasurement.createInstance("nbDifferentErrors", "Number of different errors", "One error can come multiple time. Analysis detect the duplicate to give a better situation").withTypeMeasure(TypeMeasure.LONG);
    private static PlugInMeasurement totalLines = PlugInMeasurement.createInstance("totalLines", "Total line in log", "Number of lines in the logs").withTypeMeasure(TypeMeasure.LONG);
    private static PlugInMeasurement nbItemsDetected = PlugInMeasurement.createInstance("nbItemsDetected", "Number of Items detected", "Number of errors items detected").withTypeMeasure(TypeMeasure.LONG);

    
    

    public MilkMonitorLog() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName("MonitorLog");
        plugInDescription.setLabel("Monitor Log");
        plugInDescription.setExplanation("Analyze the log of the day before and checks errors");
        plugInDescription.setCategory(CATEGORY.MONITOR);

        plugInDescription.addParameter(cstParamNumberOfErrors);
        plugInDescription.addParameter(cstParamOnlyErrorsInTop);
        
        plugInDescription.addMesure(nbErrors);
        plugInDescription.addMesure(nbDifferentErrors);
        plugInDescription.addMesure(totalLines);
        plugInDescription.addMesure(nbItemsDetected);
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
    public MilkJobOutput executeJob(MilkJobExecution jobExecution) {

        MilkJobOutput milkJobOutput = jobExecution.getMilkJobOutput();

        milkJobOutput.setExecutionStatus( ExecutionStatus.SUCCESS );

        long top = jobExecution.getInputLongParameter(cstParamNumberOfErrors);
        boolean onlyErrorInTop = jobExecution.getInputBooleanParameter(cstParamOnlyErrorsInTop);

        Calendar c = Calendar.getInstance();
        c.add(Calendar.DAY_OF_YEAR, -1);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        List<FileInformation> listfilesInformation = LogAccess.getFilesInfoLogOneDay(c.getTime());
        
        for (FileInformation fileInformation : listfilesInformation) {
            LogParameter logParameter = new LogParameter();
            logParameter.fileName = fileInformation.getFileName();
            logParameter.pathName = fileInformation.getPathName();
            logParameter.enableAnalysisError = true;
            logParameter.analysisCompactBasedOnError = true;

            milkJobOutput.addReportInHtml("File ["+fileInformation.getFileName()+"]<br>");
            
            LogInformation logInformation = LogAccess.getLog(logParameter);
            milkJobOutput.addEvents(logInformation.listEvents);
            LogAnalysisSynthese synthese = logInformation.logAnalyseError.getSynthese();
            milkJobOutput.setMeasure( nbErrors, synthese.getNbErrors());
            milkJobOutput.setMeasure( nbDifferentErrors, synthese.getNbDifferentsErrors());
            
            milkJobOutput.setMeasure( totalLines, synthese.getTotalLines() );
            milkJobOutput.setMeasure( nbItemsDetected, synthese.getNbItemsDetected());
            
            milkJobOutput.addMeasuresInReport(true, false);
            
            
            List<LogAnalyseItem> top10Synthese = synthese.getTopSyntheses((int) top, onlyErrorInTop);
            milkJobOutput.addReportTableBegin( new String[] {"Count", "Description"});
            for (LogAnalyseItem analyseItem : top10Synthese)
            {
                milkJobOutput.addReportTableLine(new String[] { String.valueOf(analyseItem.count), analyseItem.logItem.toString() } );
            }
            milkJobOutput.addReportTableEnd();
        }
        
        

        return milkJobOutput;
    }
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Private method */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

}
