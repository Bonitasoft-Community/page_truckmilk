package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.List;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.radar.archive.RadarCleanArchivedDross;
import org.bonitasoft.radar.archive.RadarCleanArchivedDross.MonitorPurge;
import org.bonitasoft.radar.archive.RadarCleanArchivedDross.TypeDrossDefinition;
import org.bonitasoft.radar.archive.RadarCleanArchivedDross.TypeDrossExecution;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkJobOutput.Chronometer;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

public class MilkCleanArchivedDross extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkCleanArchivedDross.class.getName());
    private static String loggerHeader = "CleanArchivedDross ";
    
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

        for (TypeDrossDefinition typeDross : RadarCleanArchivedDross.getListTypeDross()) {
            plugInDescription.addMesure(PlugInMeasurement.createInstance(typeDross.name, typeDross.name, typeDross.explanation).withTypeMeasure(TypeMeasure.LONG));
            String sqlQuery = typeDross.sqlQuery;
            sqlQuery = sqlQuery.replaceAll("\\?", String.valueOf(milkJobContext.getTenantId()));
            plugInDescription.addParameter(PlugInParameter.createInstanceInformation(typeDross.name,
                    typeDross.name,
                    typeDross.label + "<p>" + typeDross.explanation + "<p>"
                            + "<div class='well'>select count(*) " + sqlQuery + "</div>"));

        }
        plugInDescription.setJobCanBeStopped(JOBSTOPPER.BOTH);
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
        List<BEvent> listEvents = new ArrayList<>();

        MilkJobOutput milkJobOutput = jobExecution.getMilkJobOutput();
        MilkPlugInDescription plugInDescription = getDefinitionDescription(jobExecution.getMilkJobContext());

        milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;

        String operation = jobExecution.getInputStringParameter(cstParamOperation);
        TypeDrossDefinition[] listTypeDross = RadarCleanArchivedDross.getListTypeDross();
        // Double in case of purge
        jobExecution.setAvancementTotalStep((long) listTypeDross.length + (operationPurge.equals(operation) ? listTypeDross.length : 0L));

        List<TypeDrossExecution> listDrossExecution = new ArrayList<>();
        int advancementStep = 0;

        int totalDross = 0;

        //------------------------------- detection
        if (operationDetection.equals(operation)) {
            for (TypeDrossDefinition typeDross : listTypeDross) {
                if (jobExecution.pleaseStop())
                    break;

                advancementStep++;
                jobExecution.setAvancementStep(advancementStep);

                TypeDrossExecution typeDrossExecution = RadarCleanArchivedDross.getStatus(jobExecution.getTenantId(), typeDross);
                listEvents.addAll(typeDrossExecution.listEvents);
                listDrossExecution.add(typeDrossExecution);
            }
            // update measurement
            milkJobOutput.addReportInHtml("<h4>Detection</h4><br>");
            for (TypeDrossExecution typeDrossExecution : listDrossExecution) {
                PlugInMeasurement measure = plugInDescription.getMeasureFromName(typeDrossExecution.typeDrossDefinition.name);

                milkJobOutput.setMeasure(measure, typeDrossExecution.nbRecords);
                totalDross += typeDrossExecution.nbRecords;
            }
            milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;
        }

        // ----------------------------- Purge
        if (operationPurge.equals(operation)) {
            milkJobOutput.addReportInHtml("<h4>Purge</h4><br>");
            totalDross = 0; // we want to count the number of deletion 
            // first get the
            for (TypeDrossDefinition typeDross : listTypeDross) {
                if (jobExecution.pleaseStop()) {
                    logger.info(loggerHeader+"Stop Asked["+typeDross.name+"] "+jobExecution.getStopExplanation());
                    break;
                }

                advancementStep++;
                jobExecution.setAvancementStep(advancementStep);
                logger.info(loggerHeader+"Start type["+typeDross.name+"]");
                
                TypeDrossExecution typeDrossDeletion = RadarCleanArchivedDross.deleteDross(jobExecution.getTenantId(),
                        typeDross,
                        new MonitorPurgeBaseOnJob(jobExecution,milkJobOutput ));
                logger.info(loggerHeader+"Clean type["+typeDross.name+"] Delete["+typeDrossDeletion.nbRecords+"]");
                
                listEvents.addAll(typeDrossDeletion.listEvents);
                // Already added during the stopPurgeBaseOnJob jobExecution.addManagedItems( typeDrossDeletion.nbRecords);

                PlugInMeasurement measure = plugInDescription.getMeasureFromName(typeDross.name);
                milkJobOutput.setMeasure(measure, typeDrossDeletion.nbRecords);

                totalDross += typeDrossDeletion.nbRecords;
            }
            logger.info(loggerHeader+"Stop DrossClean: "+jobExecution.getStopExplanation());
        }

        milkJobOutput.addEvents(listEvents);
        milkJobOutput.addMeasuresInReport(true, false);
        
        milkJobOutput.addChronometersInReport(false, true);

        milkJobOutput.setNbItemsProcessed(totalDross);

        return milkJobOutput;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Private method */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public class MonitorPurgeBaseOnJob implements MonitorPurge {

        MilkJobExecution jobExecution;
        MilkJobOutput milkJobOutput;
        public MonitorPurgeBaseOnJob(MilkJobExecution jobExecution, MilkJobOutput milkJobOutput ) {
            this.jobExecution = jobExecution;
            this.milkJobOutput = milkJobOutput;            
        }

        public int getMaxNumberToProcess() {
            if (jobExecution.isLimitationOnMaxItem()) {
                return jobExecution.getJobStopAfterMaxItems() - jobExecution.getManagedItems();
            }
            // no limitation on the maxNumberToProcess
            return 100000; 
        }

        public boolean pleaseStop(int numberOfDeletion) {
            jobExecution.addManagedItems(numberOfDeletion);
            return jobExecution.pleaseStop();
        }

        @Override
        public void setTimeSelectInMs(long timeInMs) {
            milkJobOutput.addTimeChronometer("querySelect", timeInMs);
        }

        @Override
        public void setTimeCollectInMs(long timeInMs) {
            milkJobOutput.addTimeChronometer("queryCollect", timeInMs);
        }

        @Override
        public void setTimeDeleteInMs(long timeInMs) {
            milkJobOutput.addTimeChronometer("queryDelete", timeInMs);
        }

        @Override
        public void setTimeCommitInMs(long timeInMs) {
            milkJobOutput.addTimeChronometer("queryCommit", timeInMs);            
        }
    }

}
