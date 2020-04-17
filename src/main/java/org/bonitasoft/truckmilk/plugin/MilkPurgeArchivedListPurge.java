package org.bonitasoft.truckmilk.plugin;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bonitasoft.deepmonitoring.radar.connector.RadarTimeTrackerConnector;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.exception.DeletionException;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInMesure;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.CSVOperation;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

import lombok.Data;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
public class MilkPurgeArchivedListPurge extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkPurgeArchivedListPurge.class.getName());
    private final static String LOGGER_LABEL="MilkPurgeArchivedListPurge ";

    private static BEvent eventDeletionSuccess = new BEvent(MilkPurgeArchivedListPurge.class.getName(), 1, Level.SUCCESS,
            "Deletion done with success", "Archived Cases are deleted with success");

    private static BEvent eventDeletionFailed = new BEvent(MilkPurgeArchivedListPurge.class.getName(), 2, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion of archived cases", "Cases are not deleted", "Check the exception");

    private static BEvent eventReportError = new BEvent(MilkPurgeArchivedListPurge.class.getName(), 3, Level.APPLICATIONERROR,
            "Error in source file", "The source file is not correct", "Check the source file, caseid is expected inside", "Check the error");

    private static PlugInParameter cstParamSeparatorCSV = PlugInParameter.createInstance("separatorCSV", "Separator CSV", TypeParameter.STRING, ",", "CSV use a separator. May be ; or ,");
    private static PlugInParameter cstParamInputDocument = PlugInParameter.createInstanceFile("inputdocument", "Input List (CSV)", TypeParameter.FILEREAD, null, "List is a CSV containing caseid column and status column. When the status is 'DELETE', then the case is deleted\nExample: caseId;status\n342;DELETE\n345;DELETE", "ListToPurge.csv", "text/csv");

    private final static PlugInMesure cstMesureCasePurged = PlugInMesure.createInstance("CasePurged", "cases purged", "Number of case purged in this execution");

    public MilkPurgeArchivedListPurge() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the milkEmailUsersTasks, we require to be able to send an email
     */
    @Override
    public List<BEvent> checkPluginEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();
    }

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();
    }

    private @Data class Statistic {

        long pleaseStopAfterManagedItems = 0;
        long countIgnored = 0;
        long countAlreadyDone = 0;
        long countBadDefinition = 0;
        long countStillToAnalyse = 0;
        long countNbItems = 0;
        long totalLineCsv = 0;
        long sumTimeSearch = 0;
        long sumTimeDeleted = 0;
        long sumTimeManipulateCsv=0;
        long nbCasesDeleted = 0;
    }

    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {

        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();

        long beginExecution = System.currentTimeMillis();
        
        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        // get Input 
        long beginManipulateCsv = System.currentTimeMillis();
        
        String separatorCSV = jobExecution.getInputStringParameter(cstParamSeparatorCSV);

      
        Statistic statistic = new Statistic();
        
        long endManipulateCsv = System.currentTimeMillis();
        statistic.sumTimeManipulateCsv = endManipulateCsv - beginManipulateCsv;
        
        statistic.pleaseStopAfterManagedItems = jobExecution.getJobStopAfterMaxItems();


        List<Long> sourceProcessInstanceIds = new ArrayList<>();
        long nbAnalyseAlreadyReported = 0;
        try {
            CSVOperation csvOperation = new CSVOperation();
            
            csvOperation.loadCsvDocument(jobExecution, cstParamInputDocument, separatorCSV);
            statistic.totalLineCsv = csvOperation.getCount();
            if (statistic.totalLineCsv == 0) {
                // no document uploaded
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
                return plugTourOutput;
            }
       
            jobExecution.setAvancementTotalStep(statistic.totalLineCsv);


            long lineNumber = 1;
            StringBuilder analysis = new StringBuilder();
            Map<String, String> record;
            while ( (record = csvOperation.getNextRecord())  != null) {

                if (jobExecution.pleaseStop()) {
                    analysis.append("Stop asked;");
                    break;
                }

                jobExecution.setAvancementStep(lineNumber);
                lineNumber++;

                Long caseId = TypesCast.getLong(record.get(MilkPurgeArchivedListGetList.cstColCaseId), null);
                String status = TypesCast.getString(record.get(MilkPurgeArchivedListGetList.cstColStatus), null);

                if (caseId == null) {
                    if (analysis.length() < 300)
                        analysis.append("Line[" + lineNumber + "] " + MilkPurgeArchivedListGetList.cstColCaseId + " undefined;");
                    else
                        nbAnalyseAlreadyReported++;
                    statistic.countBadDefinition++;
                } else if (status == null) {
                    statistic.countIgnored++;

                } else if ("DELETE".equalsIgnoreCase(status)) {

                    // delete it
                    sourceProcessInstanceIds.add(caseId);
                    if (sourceProcessInstanceIds.size() > 50) {
                        // purge now
                        long nbCaseDeleted = purgeList(sourceProcessInstanceIds, statistic, processAPI);
                        plugTourOutput.nbItemsProcessed = statistic.countNbItems;
                        jobExecution.addManagedItems(nbCaseDeleted);
                        sourceProcessInstanceIds.clear();
                    }
                } else
                    statistic.countIgnored++;
            }
            // the end, purge a last time 
            if (! sourceProcessInstanceIds.isEmpty()) {
                long nbCaseDeleted = purgeList(sourceProcessInstanceIds, statistic, processAPI);
                jobExecution.addManagedItems(nbCaseDeleted);
                sourceProcessInstanceIds.clear();
            }
            plugTourOutput.nbItemsProcessed = statistic.countNbItems;
            
            long endExecution = System.currentTimeMillis();
            
            // calculated the last ignore
            
            statistic.countStillToAnalyse = statistic.totalLineCsv - lineNumber;
            plugTourOutput.addReportTable( new String[] {"Mesure", "Value", "Analyse"});
            
            StringBuilder reportEvent = new StringBuilder();
            plugTourOutput.addReportLine(new String[] {"Already done", String.valueOf( statistic.countAlreadyDone), ""});
            
            // reportEvent.append("AlreadyDone: " + statistic.countAlreadyDone + "; ");
            if (statistic.countIgnored>0) {
                plugTourOutput.addReportLine(new String[] {"Ignored(no status DELETE):", String.valueOf( statistic.countIgnored), ""});
                // reportEvent.append("Ignored(no status DELETE):" + statistic.countIgnored + ";");
            }
            
            if (statistic.countStillToAnalyse>0) {
                plugTourOutput.addReportLine(new String[] {"StillToAnalyse", String.valueOf( statistic.countStillToAnalyse), ""});
                // reportEvent.append("StillToAnalyse:" + statistic.countStillToAnalyse+";");
            }
            
            if (statistic.countBadDefinition > 0) {
                plugTourOutput.addReportLine(new String[] {"Bad Definition(noCaseid):", String.valueOf( statistic.countBadDefinition), analysis.toString()});
                // reportEvent.append("Bad Definition(noCaseid):" + statistic.countBadDefinition + " : " + analysis.toString()+";");
            }
            
            if (nbAnalyseAlreadyReported > 0) {
                plugTourOutput.addReportLine(new String[] {"More errors", String.valueOf( nbAnalyseAlreadyReported), "" });
                // reportEvent.append("(" + nbAnalyseAlreadyReported + ") more errors;");
            }
            
            // add Statistics
            reportEvent.append("Cases Deleted:" + statistic.countNbItems + " in " + statistic.sumTimeDeleted + " ms ");
            if (statistic.countNbItems > 0)
                reportEvent.append("( " + ((int) (statistic.sumTimeDeleted / statistic.countNbItems)) + " ms/case)");
                    
            reportEvent.append("SearchCase time:" + statistic.sumTimeSearch+" ms;");
            reportEvent.append("Manipulate CSV time:" + statistic.sumTimeManipulateCsv+" ms;");
            
            reportEvent.append("total Execution time:" + (endExecution-beginExecution)+" ms;");
            
            if (jobExecution.pleaseStop())
                reportEvent.append("Stop asked;");
            if (statistic.countNbItems  >= statistic.pleaseStopAfterManagedItems)
                    reportEvent.append("Reach the NumberOfItem;");
            
            plugTourOutput.addReportEndTable();
            
            plugTourOutput.setMesure(cstMesureCasePurged, statistic.countNbItems);
            
            BEvent eventFinal = (statistic.countBadDefinition == 0) ? new BEvent(eventDeletionSuccess, reportEvent.toString()) : new BEvent(eventReportError, reportEvent.toString());

            plugTourOutput.addEvent(eventFinal);
            
            plugTourOutput.executionStatus = (jobExecution.pleaseStop() || statistic.countStillToAnalyse > 0) ? ExecutionStatus.SUCCESSPARTIAL : ExecutionStatus.SUCCESS;
            if (statistic.countBadDefinition > 0) {
                plugTourOutput.executionStatus = ExecutionStatus.ERROR;
            }

        } catch (Exception e) {
            logger.severe("Error Delete Archived ProcessInstance=[" + sourceProcessInstanceIds + "] Error[" + e.getMessage() + "]");
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
            plugTourOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + sourceProcessInstanceIds));

        }

        return plugTourOutput;
    }

    /**
     * 
     */
    @Override
    public MilkPlugInDescription getDefinitionDescription() {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName( "PurgeCaseFromList");
        plugInDescription.setLabel( "Purge Archived Cases: Purge from list");
        plugInDescription.setExplanation( "Get a CSV File as input. Then, delete all case in the list where the column status equals DELETED");
        plugInDescription.setWarning( "A case purged can't be retrieved. Operation is final. Use with caution.");
        plugInDescription.setCategory( CATEGORY.CASES );
        plugInDescription.setStopJob( JOBSTOPPER.BOTH );
        plugInDescription.addParameter(cstParamSeparatorCSV);
        plugInDescription.addParameter(cstParamInputDocument);
        
        plugInDescription.addMesure(cstMesureCasePurged);




        return plugInDescription;
    }

   

    /**
     * methid processAPI.deleteArchivedProcessInstancesInAllStates(sourceProcessInstanceIds) is very long, even if there are nothing to purge
     * so, let's first search the real number to purge, and do the purge only on real case.
     * 
     * @return
     */

    public int purgeList(List<Long> sourceProcessInstanceIds, Statistic statistic, ProcessAPI processAPI) throws DeletionException, SearchException {
        long startTimeSearch = System.currentTimeMillis();
        SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, sourceProcessInstanceIds.size());
        for (int i = 0; i < sourceProcessInstanceIds.size(); i++) {
            if (i > 0)
                searchActBuilder.or();
            searchActBuilder.filter(ArchivedProcessInstancesSearchDescriptor.SOURCE_OBJECT_ID, sourceProcessInstanceIds.get(i));
        }
        SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());
        long endTimeSearch = System.currentTimeMillis();
        statistic.sumTimeSearch += endTimeSearch - startTimeSearch;

        List<Long> listRealId = new ArrayList<>();
        for (ArchivedProcessInstance archived : searchArchivedProcessInstance.getResult()) {
            listRealId.add(archived.getSourceObjectId());
        }

        // we know how many item we don't need to process in this batch
        // BATCH TO STUDY                   : sourceProcessInstanceIds
        // Case To delete in this batch     : realId.size()
        // Already Managed in this job      : statistic.countNbItems
        // Already Done in a previous job   : sourceProcessInstanceIds.size() - realId.size() to add to countAlreadyDone
        // if (Already Managed in this job) + (Case To Delete in the bath) > pleaseStopAfterManagedItem then we have to limit our number of deletion 

        statistic.countAlreadyDone += sourceProcessInstanceIds.size() - listRealId.size();
        // ok, the point is now maybe we don't want to process ALL this process to delete, due to the limitation
        if (statistic.countNbItems + listRealId.size() > statistic.pleaseStopAfterManagedItems) {
            // too much, we need to reduce the number
            long maximumToManage = statistic.pleaseStopAfterManagedItems - statistic.countNbItems;
            listRealId = listRealId.subList(0, (int) maximumToManage);
        }

        long startTimeDelete = System.currentTimeMillis();
        long nbCaseDeleted = 0;
        if (!listRealId.isEmpty()) {
            nbCaseDeleted += processAPI.deleteArchivedProcessInstancesInAllStates(listRealId);
            statistic.nbCasesDeleted += nbCaseDeleted;
        }
        long endTimeDelete = System.currentTimeMillis();

        logger.info(LOGGER_LABEL+".delete: search in " + (endTimeSearch - startTimeSearch) + " ms , delete in " + (endTimeDelete - startTimeDelete) + " ms for " + listRealId.size() + " nbCaseDeleted=" + nbCaseDeleted);
        logger.fine(LOGGER_LABEL+".delete InternalCaseDeletion="+statistic.nbCasesDeleted);
        statistic.countNbItems += listRealId.size();
        statistic.sumTimeDeleted += endTimeDelete - startTimeDelete;
        return listRealId.size();

    }

    /**
     * count the number of line in the
     * 
     * @param outputByte
     * @return
     */
    public long nbLinesInCsv(ByteArrayOutputStream outputByte) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(outputByte.toByteArray())));
            long nbLine = 0;
            while (reader.readLine() != null) {
                nbLine++;
            }
            return nbLine;
        } catch (Exception e) {
            return 0;
        }

    }

}
