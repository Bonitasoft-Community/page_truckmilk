/* ******************************************************************************** */
/*                                                                                  */
/*    PurgeList                                                                     */
/*   purge operation based on list                                                  */
/*                                                                                  */
/*                                                                                  */
/* ******************************************************************************** */
package org.bonitasoft.truckmilk.plugin.cases.purge;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessInstanceSearchDescriptor;
import org.bonitasoft.engine.exception.DeletionException;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkJobOutput.Chronometer;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJobExecution.DelayResult;
import org.bonitasoft.truckmilk.job.MilkJobExecution.ListProcessesResult;
import org.bonitasoft.truckmilk.plugin.cases.MilkPurgeArchivedCases;
import org.bonitasoft.truckmilk.plugin.cases.purge.PurgeOperation.ManagePurgeResult;
import org.bonitasoft.truckmilk.plugin.cases.purge.PurgeOperation.Statistic;
import org.bonitasoft.truckmilk.plugin.cases.purge.PurgeOperation.SubProcessOperation;
import org.bonitasoft.truckmilk.toolbox.CSVOperation;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

public class PurgeList {

    static Logger logger = Logger.getLogger(PurgeList.class.getName());
    private static final String LOGGER_LABEL = "MilkPurgeArchive.PurgeList ";

    private static BEvent eventSynthesisReport = new BEvent(PurgeList.class.getName(), 1, Level.INFO,
            "Report Synthesis", "Result of search", "", "");
 
    private static BEvent eventSearchFailed = new BEvent(PurgeList.class.getName(), 2, Level.ERROR,
            "Search failed", "Search failed task return an error", "No retry can be performed", "Check the error");

    private static BEvent eventWriteReportError = new BEvent(PurgeList.class.getName(), 3, Level.ERROR,
            "Report generation error", "Error arrived during the generation of the report", "No report is available", "Check the error");

    private static BEvent eventDeletionSuccess = new BEvent(PurgeList.class.getName(), 4, Level.SUCCESS,
            "Deletion done with success", "Archived Cases are deleted with success");

    private static BEvent eventDeletionFailed = new BEvent(PurgeList.class.getName(), 5, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion of archived cases", "Cases are not deleted", "Check the exception");

    private static BEvent eventReportError = new BEvent(PurgeList.class.getName(), 6, Level.APPLICATIONERROR,
            "Error in source file", "The source file is not correct", "Check the source file, caseid is expected inside", "Check the error");

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * GetList
     * /*
     */
    /* ******************************************************************************** */
    protected static String cstColCaseId = "caseid";
    protected static String cstColProcessDefinitionId = "processDefinitionId";
    protected static String cstColProcessName = "processname";
    protected static String cstColProcessVersion = "processversion";
    protected static String cstColArchivedDate = "archiveddate";

    protected static String cstColStatus = "status";
    protected static String cstColStatus_V_ALREADYDELETED = "ALREADYDELETED";
    protected static String cstColStatus_V_DELETE = "DELETE";

    public MilkJobOutput getList(MilkJobExecution milkJobExecution) {
        MilkJobOutput milkJobOutput = milkJobExecution.getMilkJobOutput();

        ProcessAPI processAPI = milkJobExecution.getApiAccessor().getProcessAPI();

        // get Input 
        String separatorCSV = milkJobExecution.getInputStringParameter( MilkPurgeArchivedCases.cstParamSeparatorCSV);

        // 20 for the preparation, 100 to collect cases
        // Time to run the query take time, and we don't want to show 0% for a long time
        milkJobExecution.setAvancementTotalStep(140);
        try {
            Map<Long, ProcessDefinition> cacheProcessDefinition = new HashMap<>();
            CSVOperation csvOperationOuput = new CSVOperation();
            SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, milkJobExecution.getJobStopAfterMaxItems() + 1);
            ListProcessesResult listProcessResult = milkJobExecution.getInputArrayProcess(MilkPurgeArchivedCases.cstParamProcessFilter, false, searchActBuilder, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);
            StringBuilder finalReport = new StringBuilder();
            DelayResult delayResult = milkJobExecution.getInputDelayParameter(MilkPurgeArchivedCases.cstParamDelay, new Date(), false);
            if (BEventFactory.isError(delayResult.listEvents)) {
                milkJobOutput.addEvents(delayResult.listEvents);
                milkJobOutput.setExecutionStatus(ExecutionStatus.ERROR);
                return milkJobOutput;
            }
            long timeSearch = delayResult.delayDate.getTime();

            long totalCasesDetected = 0;

            String scopeDetection = milkJobExecution.getInputStringParameter(MilkPurgeArchivedCases.cstParamOperationScopeProcess);

            if (MilkPurgeArchivedCases.CSTSCOPE_ROOTPROCESS.equals(scopeDetection) || MilkPurgeArchivedCases.CSTSCOPE_BOTH.equals(scopeDetection)) {

                if (BEventFactory.isError(listProcessResult.listEvents)) {
                    // filter given, no process found : stop now
                    milkJobOutput.addEvents(listProcessResult.listEvents);
                    milkJobOutput.setExecutionStatus(ExecutionStatus.BADCONFIGURATION);
                    return milkJobOutput;
                }
                finalReport.append("select * from ARCH_PROCESS_INSTANCE where ");
                if (!listProcessResult.listProcessDeploymentInfo.isEmpty()) {
                    finalReport.append(" (");
                    for (int i = 0; i < listProcessResult.listProcessDeploymentInfo.size(); i++) {
                        if (i > 0)
                            finalReport.append(" or ");
                        finalReport.append("PROCESSDEFINITIONID = " + listProcessResult.listProcessDeploymentInfo.get(i).getProcessId());
                    }
                    finalReport.append(" ) and ");
                }

                milkJobExecution.setAvancementStep(5);

                // -------------- now get the list of cases
                searchActBuilder.lessOrEquals(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, timeSearch);
                searchActBuilder.sort(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, Order.ASC);

                finalReport.append(" ARCHIVEDATE <= " + timeSearch);
                finalReport.append(" and TENANTID=" + milkJobExecution.getTenantId());
                finalReport.append(" and STATEID in (3,4,6)"); // only archive case
                finalReport.append(" and ROOTPROCESSINSTANCEID = SOURCEOBJECTID"); // only root case

                SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance;
                searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());

                /** ok, we did 15 step */
                milkJobExecution.setAvancementStep(20);

                csvOperationOuput.writeCsvDocument(new String[] { cstColCaseId, cstColProcessName, cstColProcessVersion, cstColArchivedDate, cstColStatus }, separatorCSV);

                // loop on archive
                milkJobOutput.nbItemsProcessed = 0;
                for (ArchivedProcessInstance archivedProcessInstance : searchArchivedProcessInstance.getResult()) {

                    if (milkJobExecution.isStopRequired())
                        break;
                    milkJobOutput.nbItemsProcessed++;

                    Map<String, String> recordCsv = new HashMap<>();
                    recordCsv.put(cstColCaseId, String.valueOf(archivedProcessInstance.getSourceObjectId()));

                    long processId = archivedProcessInstance.getProcessDefinitionId();
                    if (!cacheProcessDefinition.containsKey(processId)) {
                        try {
                            ProcessDefinition processDefinition = processAPI.getProcessDefinition(processId);
                            cacheProcessDefinition.put(processId, processDefinition);
                        } catch (Exception e) {
                            cacheProcessDefinition.put(processId, null);
                        }
                    }
                    recordCsv.put(cstColProcessName, cacheProcessDefinition.get(processId) == null ? "" : cacheProcessDefinition.get(processId).getName());
                    recordCsv.put(cstColProcessVersion, cacheProcessDefinition.get(processId) == null ? "" : cacheProcessDefinition.get(processId).getVersion());
                    recordCsv.put(cstColArchivedDate, TypesCast.sdfCompleteDate.format(archivedProcessInstance.getArchiveDate()));
                    recordCsv.put(cstColStatus, ""); // status
                    csvOperationOuput.writeRecord(recordCsv);

                    milkJobExecution.addManagedItems(1);

                    milkJobExecution.setAvancementStep(20L + (long) (100 * milkJobOutput.nbItemsProcessed / searchArchivedProcessInstance.getResult().size()));

                }
                milkJobOutput.setMeasure(MilkPurgeArchivedCases.cstMesureCaseDetected, searchArchivedProcessInstance.getCount());
                totalCasesDetected = searchArchivedProcessInstance.getCount();
            }

            milkJobExecution.setAvancementStep(120);

            // Manage now the sub process purge
            if (MilkPurgeArchivedCases.CSTSCOPE_TRANSIENTONLY.equals(scopeDetection) || MilkPurgeArchivedCases.CSTSCOPE_BOTH.equals(scopeDetection)) {

                SubProcessOperation subProcessOperation = new SubProcessOperationCSV(cacheProcessDefinition, csvOperationOuput, processAPI);
                ManagePurgeResult managePurgeResult = PurgeOperation.detectPurgeSubProcessOnly(listProcessResult, timeSearch, milkJobExecution.getTenantId(), milkJobExecution.getJobStopAfterMaxItems(), subProcessOperation);
                milkJobOutput.addEvents(managePurgeResult.listEvents);
                finalReport.append("Detect transient ProcessInstance:<br>" + managePurgeResult.sqlQuery);

                milkJobOutput.nbItemsProcessed += managePurgeResult.nbRecords;
                totalCasesDetected += managePurgeResult.nbRecords;
                milkJobOutput.addEvents(managePurgeResult.listEvents);
            }
            milkJobExecution.setAvancementStep(140);

            csvOperationOuput.closeAndWriteToParameter(milkJobOutput, MilkPurgeArchivedCases.cstParamListOfCasesDocument);

            milkJobOutput.addReportInHtml(finalReport.toString());

            milkJobOutput.addReportTableBegin(new String[] { "Label", "Value" });
            milkJobOutput.addReportTableLine(new Object[] { "Total cases detected", totalCasesDetected });
            milkJobOutput.addReportTableLine(new Object[] { "Total cases in CSV file", milkJobOutput.nbItemsProcessed });
            milkJobOutput.addReportTableEnd();

            milkJobOutput.addEvent(new BEvent(eventSynthesisReport, "Total cases in list:" + milkJobOutput.nbItemsProcessed + ", Detected:" + totalCasesDetected));

            if (milkJobOutput.nbItemsProcessed == 0) {
                milkJobOutput.setExecutionStatus(ExecutionStatus.SUCCESSNOTHING);
                return milkJobOutput;
            }

            milkJobOutput.setExecutionStatus(milkJobExecution.isStopRequired() ? ExecutionStatus.SUCCESSPARTIAL : ExecutionStatus.SUCCESS);
        }

        catch (SearchException e1) {
            milkJobOutput.addEvent(new BEvent(eventSearchFailed, e1, ""));
            milkJobOutput.setExecutionStatus(ExecutionStatus.ERROR);
        } catch (Exception e1) {
            milkJobOutput.addEvent(new BEvent(eventWriteReportError, e1, ""));
            milkJobOutput.setExecutionStatus(ExecutionStatus.ERROR);
        }

        return milkJobOutput;
    }
    
    
    
    
    
    
    
    /**
     * Write the result in a CSV Ouput
     */
    public class SubProcessOperationCSV implements SubProcessOperation {

        private Map<Long, ProcessDefinition> cacheProcessDefinition;
        private ProcessAPI processAPI;
        CSVOperation csvOperationOuput = new CSVOperation();

        SubProcessOperationCSV(Map<Long, ProcessDefinition> cacheProcessDefinition, CSVOperation csvOperationOuput, ProcessAPI processAPI) {
            this.cacheProcessDefinition = cacheProcessDefinition;
            this.csvOperationOuput = csvOperationOuput;
            this.processAPI = processAPI;
        }

        public List<BEvent> detectOneSubprocessInstance(Map<String, Object> recordSubProcess) {
            List<BEvent> listEvents = new ArrayList<>();
            Long processId = TypesCast.getLong(recordSubProcess.get("PROCESSDEFINITIONID"), null);

            if ((processId != null) && !cacheProcessDefinition.containsKey(processId)) {
                try {
                    ProcessDefinition processDefinition = processAPI.getProcessDefinition(processId);
                    cacheProcessDefinition.put(processId, processDefinition);
                } catch (Exception e) {
                    cacheProcessDefinition.put(processId, null);
                }
            }

            try {
                Map<String, String> recordCsv = new HashMap<String, String>();
                recordCsv.put(cstColCaseId, recordSubProcess.get("SOURCEOBJECTID").toString());
                recordCsv.put(cstColProcessName, cacheProcessDefinition.get(processId) == null ? "" : cacheProcessDefinition.get(processId).getName());
                recordCsv.put(cstColProcessVersion, cacheProcessDefinition.get(processId) == null ? "" : cacheProcessDefinition.get(processId).getVersion());
                Long archiveDate = TypesCast.getLong(recordSubProcess.get("ARCHIVEDATE"), null);
                if (archiveDate != null)
                    recordCsv.put(cstColArchivedDate, TypesCast.sdfCompleteDate.format(new Date(archiveDate)));
                recordSubProcess.put(cstColStatus, ""); // status
                csvOperationOuput.writeRecord(recordCsv);
            } catch (IOException e) {
                listEvents.add(new BEvent(eventWriteReportError, e, ""));

            }
            return listEvents;
        }

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * From List
     * /*
     */
    /* ******************************************************************************** */
    /**
     * @param jobExecution
     * @return
     */
    public MilkJobOutput fromList(MilkJobExecution milkJobExecution) {

        MilkJobOutput milkJobOutput = milkJobExecution.getMilkJobOutput();

        Chronometer purgeMarker = milkJobOutput.beginChronometer("PurgeFromList");

        ProcessAPI processAPI = milkJobExecution.getApiAccessor().getProcessAPI();
        // get Input 
        long beginManipulateCsv = System.currentTimeMillis();

        String separatorCSV = milkJobExecution.getInputStringParameter(MilkPurgeArchivedCases.cstParamSeparatorCSV);

        Statistic statistic = new Statistic();

        long endManipulateCsv = System.currentTimeMillis();
        statistic.sumTimeManipulateCsv = endManipulateCsv - beginManipulateCsv;

        statistic.pleaseStopAfterManagedItems = milkJobExecution.getJobStopAfterMaxItems();

        List<Long> sourceProcessInstanceIds = new ArrayList<>();
        long nbAnalyseAlreadyReported = 0;
        try {
            CSVOperation csvOperationInput = new CSVOperation();

            csvOperationInput.loadCsvDocument(milkJobExecution, MilkPurgeArchivedCases.cstParamListOfCasesDocument, separatorCSV);
            statistic.totalLineCsv = csvOperationInput.getCount();
            if (statistic.totalLineCsv == 0) {
                // no document uploaded
                milkJobOutput.setExecutionStatus(ExecutionStatus.SUCCESSNOTHING);
                return milkJobOutput;
            }

            // update the report : prepare the production
            CSVOperation csvOperationOuput = new CSVOperation();
            csvOperationOuput.writeCsvDocument(new String[] { cstColCaseId, cstColProcessName, cstColProcessVersion, cstColArchivedDate, cstColStatus }, separatorCSV);

            milkJobExecution.setAvancementTotalStep(statistic.totalLineCsv);

            long lineNumber = 1;
            StringBuilder analysis = new StringBuilder();
            Map<String, String> record;

            // track time per status
            Chronometer currentMarker = null;
            String currentStatus = null;

            while ((record = csvOperationInput.getNextRecord()) != null) {

                if (milkJobExecution.isStopRequired()) {
                    csvOperationOuput.writeRecord(record);
                    analysis.append("Stop asked;");
                    break;
                }

                milkJobExecution.setAvancementStep(lineNumber);
                lineNumber++;

                Long caseId = TypesCast.getLong(record.get(cstColCaseId), null);
                String status = TypesCast.getString(record.get(cstColStatus), "");
                if (currentStatus == null)
                    currentStatus = status;
                if (currentMarker == null)
                    currentMarker = milkJobOutput.beginChronometer("Manipulate " + currentStatus);

                if (caseId == null) {
                    if (analysis.length() < 300)
                        analysis.append("Line[" + lineNumber + "] " + cstColCaseId + " undefined;");
                    else
                        nbAnalyseAlreadyReported++;
                    statistic.countBadDefinition++;
                } else if (status == null) {
                    statistic.countIgnored++;
                } else if (cstColStatus_V_ALREADYDELETED.equalsIgnoreCase(status)) {
                    statistic.countAlreadyDone++;
                } else if (cstColStatus_V_DELETE.equalsIgnoreCase(status)) {

                    // delete it
                    sourceProcessInstanceIds.add(caseId);
                    // use the preparation item to stop at the exact requested number
                    milkJobExecution.addPreparationItems(1);
                    if (sourceProcessInstanceIds.size() > 50) {
                        // purge now
                        long nbCaseDeleted = purgeList(sourceProcessInstanceIds, statistic, milkJobExecution, milkJobOutput, processAPI);
                        milkJobOutput.nbItemsProcessed = statistic.countNbItems;
                        milkJobExecution.addManagedItems(nbCaseDeleted);
                        milkJobExecution.setPreparationItems(0);
                        sourceProcessInstanceIds.clear();
                    }
                    // actually, deletion is not made, but we can trust the Bonita API
                    record.put(cstColStatus, cstColStatus_V_ALREADYDELETED);
                } else
                    statistic.countIgnored++;

                csvOperationOuput.writeRecord(record);

                // update the accumulate marker
                if (currentStatus != null && !currentStatus.equals(status)) {
                    // we change the type, so accumulate in the current status
                    milkJobOutput.endChronometer(currentMarker);
                    currentStatus = status;
                    currentMarker = milkJobOutput.beginChronometer("Manipulate " + status);
                }

            } // end loop

            // last accumulation
            if (currentMarker != null)
                milkJobOutput.endChronometer(currentMarker);

            // the end, purge a last time 
            if (!sourceProcessInstanceIds.isEmpty()) {
                currentMarker = milkJobOutput.beginChronometer("Manipulate " + cstColStatus_V_DELETE);

                long nbCaseDeleted = purgeList(sourceProcessInstanceIds, statistic, milkJobExecution, milkJobOutput, processAPI);
                milkJobExecution.addManagedItems(nbCaseDeleted);
                sourceProcessInstanceIds.clear();

                milkJobOutput.endChronometer(currentMarker);
            }
            milkJobOutput.nbItemsProcessed = statistic.countNbItems;

            // update the report now
            // We have to rewrite the complete document, with no change
            currentMarker = milkJobOutput.beginChronometer("Manipulate TOANALYSE");
            while ((record = csvOperationInput.getNextRecord()) != null) {
                csvOperationOuput.writeRecord(record);
            }
            milkJobOutput.endChronometer(currentMarker);

            csvOperationOuput.closeAndWriteToParameter(milkJobOutput, MilkPurgeArchivedCases.cstParamListOfCasesDocument);

            milkJobOutput.endChronometer(purgeMarker);

            // -------------------------------------------------- reorting
            // calculated the last ignore            
            statistic.countStillToAnalyse = statistic.totalLineCsv - lineNumber;
            milkJobOutput.addReportTableBegin(new String[] { "Mesure", "Value" });

            StringBuilder reportEvent = new StringBuilder();
            milkJobOutput.addReportTableLine(new Object[] { "Already done", statistic.countAlreadyDone });

            // reportEvent.append("AlreadyDone: " + statistic.countAlreadyDone + "; ");
            if (statistic.countIgnored > 0) {
                milkJobOutput.addReportTableLine(new Object[] { "Ignored(no status DELETE):", statistic.countIgnored });
                // reportEvent.append("Ignored(no status DELETE):" + statistic.countIgnored + ";");
            }

            if (statistic.countStillToAnalyse > 0) {
                milkJobOutput.addReportTableLine(new Object[] { "StillToAnalyse", statistic.countStillToAnalyse });
                // reportEvent.append("StillToAnalyse:" + statistic.countStillToAnalyse+";");
            }

            if (statistic.countBadDefinition > 0) {
                milkJobOutput.addReportTableLine(new Object[] { "Bad Definition(noCaseid):", statistic.countBadDefinition });
                milkJobOutput.addReportTableLine(new Object[] { "Analysis:", analysis.toString() });

                // reportEvent.append("Bad Definition(noCaseid):" + statistic.countBadDefinition + " : " + analysis.toString()+";");
            }

            if (nbAnalyseAlreadyReported > 0) {
                milkJobOutput.addReportTableLine(new Object[] { "More errors:", nbAnalyseAlreadyReported, "" });
                // reportEvent.append("(" + nbAnalyseAlreadyReported + ") more errors;");
            }

            // add Statistics
            // reportEvent.append("Cases Deleted:" + statistic.countNbItems + " in " + statistic.sumTimeDeleted + " ms ");
            milkJobOutput.addReportTableLine(new Object[] { "Cases Deleted:", statistic.countNbItems });
            if (statistic.countNbItems > 0) {
                milkJobOutput.addReportTableLine(new Object[] { "Average/case (ms/case):", ((int) (statistic.sumTimeDeleted / statistic.countNbItems)), "" });
                // reportEvent.append("( " + ((int) (statistic.sumTimeDeleted / statistic.countNbItems)) + " ms/case)");
            }
            milkJobOutput.addReportTableLine(new Object[] { "SearchCase time:", statistic.sumTimeSearch });
            milkJobOutput.addReportTableLine(new Object[] { "Manipulate CSV time (ms):", statistic.sumTimeManipulateCsv });
            milkJobOutput.addReportTableLine(new Object[] { "Total Execution time (ms):", purgeMarker.getTimeExecution() });

            // reportEvent.append("SearchCase time:" + statistic.sumTimeSearch + " ms;");
            // reportEvent.append("Manipulate CSV time:" + statistic.sumTimeManipulateCsv + " ms;");
            // reportEvent.append("Total Execution time:" + (endExecution - beginExecution) + " ms;");

            if (milkJobExecution.isStopRequired())
                reportEvent.append("Stop asked;");
            if (statistic.countNbItems >= statistic.pleaseStopAfterManagedItems)
                reportEvent.append("Reach the NumberOfItem;");

            milkJobOutput.addReportTableEnd();

            milkJobOutput.addChronometersInReport(false, false);

            milkJobOutput.setMeasure(MilkPurgeArchivedCases.cstMesureCasePurged, statistic.countNbItems);

            BEvent eventFinal = (statistic.countBadDefinition == 0) ? new BEvent(eventDeletionSuccess, reportEvent.toString()) : new BEvent(eventReportError, reportEvent.toString());

            milkJobOutput.addEvent(eventFinal);

            milkJobOutput.setExecutionStatus((milkJobExecution.isStopRequired() || statistic.countStillToAnalyse > 0) ? ExecutionStatus.SUCCESSPARTIAL : ExecutionStatus.SUCCESS);
            if (statistic.countBadDefinition > 0) {
                milkJobOutput.setExecutionStatus(ExecutionStatus.ERROR);
            }

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + ".fromList: Error Delete Archived ProcessInstance=[" + sourceProcessInstanceIds + "] Error[" + e.getMessage() + "] at " + exceptionDetails);
            milkJobOutput.setExecutionStatus(ExecutionStatus.ERROR);
            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + sourceProcessInstanceIds));

        }

        return milkJobOutput;
    }
    
    /**
     * We get a list of processInstanceId to purge. Is they still exist? Let's search them
     * @param sourceProcessInstanceIds
     * @param statistic
     * @param milkJobExecution
     * @param milkJobOutput
     * @param processAPI
     * @return
     * @throws DeletionException
     * @throws SearchException
     */
   
    private int purgeList(List<Long> sourceProcessInstanceIds, Statistic statistic, MilkJobExecution milkJobExecution, MilkJobOutput milkJobOutput, ProcessAPI processAPI) throws DeletionException, SearchException {
        Chronometer searchArchivedMarker = milkJobOutput.beginChronometer("searchArchived");

        SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, sourceProcessInstanceIds.size());
        for (int i = 0; i < sourceProcessInstanceIds.size(); i++) {
            if (i > 0)
                searchActBuilder.or();
            searchActBuilder.filter(ArchivedProcessInstancesSearchDescriptor.SOURCE_OBJECT_ID, sourceProcessInstanceIds.get(i));
        }
        SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());
        statistic.sumTimeSearch += milkJobOutput.endChronometer(searchArchivedMarker);

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

        Chronometer deleteMarker = milkJobOutput.beginChronometer("deleteArchived");
        long nbCaseDeleted = 0;

        if (!listRealId.isEmpty()) {
            // we can't trust this information
            
            ManagePurgeResult managePurgeResult =  PurgeOperation.deleteArchivedProcessInstance( listRealId, milkJobExecution, milkJobOutput, processAPI);
            nbCaseDeleted+= managePurgeResult.nbRecords;
            milkJobOutput.addEvents( managePurgeResult.listEvents );
            if (BEventFactory.isError( managePurgeResult.listEvents ))
                milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR);
            
            statistic.nbCasesDeleted += listRealId.size();
        }
        milkJobOutput.endChronometer(deleteMarker);

        logger.info(LOGGER_LABEL + ".purgeList: search in " + searchArchivedMarker.getTimeExecution() + " ms , delete in " + deleteMarker.getTimeExecution() + " ms for " + listRealId.size() + " nbCaseDeleted=" + nbCaseDeleted);
        logger.fine(LOGGER_LABEL + ".purgeList: InternalCaseDeletion=" + statistic.nbCasesDeleted);
        statistic.countNbItems += listRealId.size();
        statistic.sumTimeDeleted += deleteMarker.getTimeExecution();
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
