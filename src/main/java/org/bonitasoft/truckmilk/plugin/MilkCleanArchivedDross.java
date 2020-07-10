package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.List;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.radar.archive.RadarCleanArchivedDross;
import org.bonitasoft.radar.archive.RadarCleanArchivedDross.MonitorPurge;
import org.bonitasoft.radar.archive.RadarCleanArchivedDross.TypeDrossDefinition;
import org.bonitasoft.radar.archive.RadarCleanArchivedDross.TypeDrossExecution;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkJobOutput.Chronometer;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

public class MilkCleanArchivedDross extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkCleanArchivedDross.class.getName());
    private static String loggerHeader = "CleanArchivedDross ";

    private static String cstOperationDetection = "Only detection";
    private static String cstOperationPurge = "Purge";

    private static String cstPerimeterAll = "all";
    private static String cstPerimeterArchive = "archive";
    private static String cstPerimeterActive = "active";

    private static PlugInParameter cstParamOperation = PlugInParameter.createInstanceListValues("operation", "Operation",
            new String[] { cstOperationDetection, cstOperationPurge },
            cstOperationDetection,
            "Detect or clean all Drosses in archived table  "
                    + cstOperationDetection + ": Only detect. "
                    + cstOperationPurge + ": Purge records.");

    private static PlugInParameter cstParamPerimeter = PlugInParameter.createInstanceListValues("perimeter", "Perimeter",
            new String[] { cstPerimeterAll, cstPerimeterArchive, cstPerimeterActive }, cstPerimeterArchive,
            "Perimeter of the analyse. Only archived table, or active table, or both");



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
        plugInDescription.addParameter(cstParamPerimeter);
        
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
        String perimeter = jobExecution.getInputStringParameter(cstParamPerimeter);
        TypeDrossDefinition[] listTypeDross = RadarCleanArchivedDross.getListTypeDross();
        // Double in case of purge
        jobExecution.setAvancementTotalStep((long) listTypeDross.length + (cstOperationPurge.equals(operation) ? listTypeDross.length : 0L));

        List<TypeDrossExecution> listDrossExecution = new ArrayList<>();
        int advancementStep = 0;

        int totalDross = 0;

        //------------------------------- detection
        if (cstOperationDetection.equals(operation)) {
            for (TypeDrossDefinition typeDross : listTypeDross) {
                if (jobExecution.pleaseStop())
                    break;

                advancementStep++;
                jobExecution.setAvancementStep(advancementStep);
                
                // Perimeter
                if (! cstPerimeterAll.equals(perimeter)) {
                    if (typeDross.onActiveTable &&  cstPerimeterArchive.equals(perimeter))
                        continue;
                    if (! typeDross.onActiveTable && cstPerimeterActive.equals(perimeter))
                        continue;
                }
                jobExecution.setAvancementInformation("Detect " + typeDross.name + "<br><i>select count(*) " + typeDross.sqlQuery + "</i>");

                Chronometer drossMarker = milkJobOutput.beginChronometer(typeDross.name);
                TypeDrossExecution typeDrossExecution = RadarCleanArchivedDross.getStatus(jobExecution.getTenantId(), typeDross);
                milkJobOutput.endChronometer(drossMarker);

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

        // ----------------------------- Purge now
        if (cstOperationPurge.equals(operation)) {
            milkJobOutput.addReportInHtml("<h4>Purge</h4><br>");
            totalDross = 0; // we want to count the number of deletion 
            // first get the
            for (TypeDrossDefinition typeDross : listTypeDross) {
                if (jobExecution.pleaseStop()) {
                    logger.info(loggerHeader + "Stop Asked type[" + typeDross.name + "] " + jobExecution.getStopExplanation());
                    break;
                }

                advancementStep++;
                jobExecution.setAvancementStep(advancementStep);
                
                // Perimeter
                if (! cstPerimeterAll.equals(perimeter)) {
                    if (typeDross.onActiveTable &&  cstPerimeterArchive.equals(perimeter))
                        continue;
                    if (! typeDross.onActiveTable && cstPerimeterActive.equals(perimeter))
                        continue;
                }

                jobExecution.setAvancementInformation("Purge " + typeDross.name + "<br><i>select count(*) " + typeDross.sqlQuery + "</i>");

                logger.info(loggerHeader + "Start type[" + typeDross.name + "]");

                Chronometer drossMarker = milkJobOutput.beginChronometer(typeDross.name);
                TypeDrossExecution typeDrossDeletion = RadarCleanArchivedDross.deleteDross(jobExecution.getTenantId(),
                        typeDross,
                        new MonitorPurgeBaseOnJob(jobExecution, milkJobOutput));
                milkJobOutput.endChronometer(drossMarker);

                logger.info(loggerHeader + "Clean type[" + typeDross.name + "] Delete[" + typeDrossDeletion.nbRecords + "]");

                listEvents.addAll(typeDrossDeletion.listEvents);
                // Already added during the stopPurgeBaseOnJob jobExecution.addManagedItems( typeDrossDeletion.nbRecords);

                PlugInMeasurement measure = plugInDescription.getMeasureFromName(typeDross.name);
                milkJobOutput.setMeasure(measure, typeDrossDeletion.nbRecords);

                totalDross += typeDrossDeletion.nbRecords;
            }
            logger.info(loggerHeader + "Stop DrossClean: " + jobExecution.getStopExplanation());
        }

        milkJobOutput.addEvents(listEvents);
        // according the perimeter, some measure does not have a value
        milkJobOutput.addMeasuresInReport(false, false);

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

        public MonitorPurgeBaseOnJob(MilkJobExecution jobExecution, MilkJobOutput milkJobOutput) {
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
