package org.bonitasoft.truckmilk.plugin.cases;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.bonitasoft.casedetails.CaseDetails;
import org.bonitasoft.casedetails.CaseDetails.CaseDetailComment;
import org.bonitasoft.casedetails.CaseDetails.CaseDetailDocument;
import org.bonitasoft.casedetails.CaseDetails.CaseDetailFlowNode;
import org.bonitasoft.casedetails.CaseDetails.CaseDetailVariable;
import org.bonitasoft.casedetails.CaseDetails.ProcessInstanceDescription;
import org.bonitasoft.casedetails.CaseDetails.ScopeVariable;
import org.bonitasoft.casedetails.CaseDetailsAPI;
import org.bonitasoft.casedetails.CaseDetailsAPI.CaseHistoryParameter;
import org.bonitasoft.casedetails.CaseDetailsAPI.LOADCOMMENTS;
import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.document.DocumentNotFoundException;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessDefinitionNotFoundException;
import org.bonitasoft.engine.bpm.process.ProcessInstanceSearchDescriptor;
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.identity.UserNotFoundException;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;

import org.bonitasoft.explorer.external.DatabaseDefinition;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.properties.DatabaseConnection;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkJobOutput.Chronometer;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter.FilterProcess;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJobExecution.DelayResult;
import org.bonitasoft.truckmilk.job.MilkJobExecution.ListProcessesResult;
import org.bonitasoft.truckmilk.toolbox.DatabaseTables;
import org.bonitasoft.truckmilk.toolbox.DatabaseTables.COLTYPE;
import org.bonitasoft.truckmilk.toolbox.DatabaseTables.DataColumn;
import org.bonitasoft.truckmilk.toolbox.DatabaseTables.DataDefinition;
import org.bonitasoft.truckmilk.toolbox.DatabaseTables.DataForeignKey;

public class MilkMoveArchive extends MilkPlugIn {

    private final static String LOGGER_LABEL = "MilkMoveArchive";
    private final static Logger logger = Logger.getLogger(MilkMoveArchive.class.getName());

    /**
     * Events
     */
    private final static BEvent eventCantConnectDatasource = new BEvent(MilkMoveArchive.class.getName(), 1, Level.ERROR,
            "Can't connect to databasource", "Can't connect to the datasource", "No operation can be perform", "Check the configuration, check the database");

    private static BEvent eventDeletionFailed = new BEvent(MilkMoveArchive.class.getName(), 2, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion of archived cases", "Cases are not deleted", "Check the exception");

    private static BEvent eventOperationFailed = new BEvent(MilkMoveArchive.class.getName(), 3, Level.ERROR,
            "Operation error", "An error arrive during the operation", "Operation failed", "Check the exception");

    private static BEvent eventProcessDefinitionNotFound = new BEvent(MilkMoveArchive.class.getName(), 4, Level.ERROR,
            "ProcessDefinition not found", "The process definition is not found", "Case can't be moved", "Check process definition and the database");

    private static BEvent eventSaveRecordFailed = new BEvent(MilkMoveArchive.class.getName(), 5, Level.ERROR,
            "Insert record failed", "Insert in the external database failed", "Case is not moved", "Check the exception");

    private final static BEvent eventCantAccessDocument = new BEvent(MilkMoveArchive.class.getName(), 6, Level.ERROR,
            "Can't access Document", "Can't access a document, move will be incomplete", "Case is not moved", "Check the exception");
    
    private final static BEvent eventCantExecuteExistProcessInstance = new BEvent(MilkMoveArchive.class.getName(), 7, Level.ERROR,
            "Can't count number of process Instance", "A count is executed to verify if the process instance was already copied, and it failed", "ProcessInstance are considered as already copied", "Check the exception");

    private final static String CSTOPERATION_HISTORYARCHIVE = "Archive all History";
    private final static String CSTOPERATION_NOHISTORYARCHIVE = "No history (only last value)";
    private final static String CSTOPERATION_ARCHIVE = "Archive";

    
    private final static String CSTOPERATION_COPY = "Copy";
    private final static String CSTOPERATION_MOVE = "Move";
    
    private final static String CSTOPERATION_IGNORE = "Ignore";
    private final static String CSTOPERATION_ARCHIVELIGHT = "Archive light";

    public enum POLICY_DATA {
        ARCHIVE, IGNORE
    }

    /**
     * Parameters
     */
    private final static PlugInParameter cstParamDatasource = PlugInParameter.createInstance("datasource", "Datasource", TypeParameter.STRING, "", "Give the datasource name, where data will be moved");

    private final static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Filter on process", TypeParameter.ARRAYPROCESSNAME, null, "Job manage only process which mach the filter. If no filter is given, all processes are inspected")
            .withMandatory(false)
            .withFilterProcess(FilterProcess.ALL);

    private static PlugInParameter cstParamDelay = PlugInParameter.createInstanceDelay("Delay", "Delay to delete case, based on the Create date", DELAYSCOPE.MONTH, 6, "All cases STARTED before this delay will be purged, even if they have a task execution after");

    private static PlugInParameter cstParamMoveProcessData = PlugInParameter.createInstanceListValues("ArchiveProcessData",
            "Policy on Process Data",
            new String[] { CSTOPERATION_ARCHIVE, CSTOPERATION_IGNORE }, CSTOPERATION_ARCHIVE, "Process Data are copied to the Archive database, with the history or not");

    private static PlugInParameter cstParamMoveLocalData = PlugInParameter.createInstanceListValues("ArchiveLocalData",
            "Policy on Local Data",
            new String[] { CSTOPERATION_ARCHIVE, CSTOPERATION_IGNORE }, CSTOPERATION_ARCHIVE, "Local Data are copied to the Archive database, with the history or not");

    private static PlugInParameter cstParamHistoryProcessData = PlugInParameter.createInstanceListValues("HistoryProcessData",
            "History on process data",
            new String[] { CSTOPERATION_HISTORYARCHIVE, CSTOPERATION_NOHISTORYARCHIVE }, CSTOPERATION_NOHISTORYARCHIVE, "A process variable value may change in the process. Archive all value (or only the last value)");

    private static PlugInParameter cstParamMoveActivity = PlugInParameter.createInstanceListValues("ArchiveActivity",
            "Policy on Process Data",
            new String[] { CSTOPERATION_ARCHIVE, CSTOPERATION_ARCHIVELIGHT, CSTOPERATION_IGNORE }, CSTOPERATION_ARCHIVE, "Activities are copied to the Archive database. Light copied only one record per activity (else 3 to multiple records)");

    private static PlugInParameter cstParamMoveDocument = PlugInParameter.createInstanceListValues("ArchiveDocument",
            "Policy on Document",
            new String[] { CSTOPERATION_ARCHIVE, CSTOPERATION_IGNORE }, CSTOPERATION_ARCHIVE, "Documents are copied to the Archive database");

    private static PlugInParameter cstParamMoveComment = PlugInParameter.createInstanceListValues("ArchiveComment",
            "Policy on Comment",
            new String[] { CSTOPERATION_ARCHIVE, CSTOPERATION_IGNORE }, CSTOPERATION_ARCHIVE, "Comments are copied to the Archive database");

    private final static PlugInParameter cstParamOperationCase = PlugInParameter.createInstanceListValues("operationCase", "Operation on case", 
            new String[] { CSTOPERATION_COPY, CSTOPERATION_MOVE }, CSTOPERATION_MOVE, "Case are move (archived case will be detelete) or copy to the Archive database");


    public MilkMoveArchive() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * plug in can check its environment, to detect if you missed something. An external component may
     * be required and are not installed.
     * 
     * @return a list of Events.
     */
    @Override
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        List<BEvent> listEvents = new ArrayList<>();
        DatabaseTables databaseTable = new DatabaseTables();
        String datasource = milkJobExecution.getInputStringParameter(cstParamDatasource);
        DatabaseConnection.ConnectionResult connectionResult = null;
        try {
            connectionResult = DatabaseConnection.getConnection(Arrays.asList(datasource));
            if (connectionResult.con == null) {
                listEvents.add(new BEvent(eventCantConnectDatasource, "Datasource[" + datasource + "]"));
                return listEvents;
            }
            connectionResult.con.setAutoCommit(true);

            List<DataDefinition> listTables = getListTableArchives();
            for (DataDefinition table : listTables)
                listEvents.addAll(databaseTable.checkCreateDatase(table, connectionResult.con));
            /*
             * if (BEventFactory.isError(listEvents)) {
             * connectionResult.con.rollback();
             * } else {
             * connectionResult.con.commit();
             * }
             */

        } catch (Exception e) {

            listEvents.add(new BEvent(eventCantConnectDatasource, e, "Datasource[" + datasource + "], error " + e.getMessage()));
            /*
             * try {
             * connectionResult.con.rollback();
             * } catch (SQLException e1) {
             * }
             */
            return listEvents;

        } finally {
            if (connectionResult != null && connectionResult.con != null)
                try {
                    connectionResult.con.close();
                } catch (Exception e) {
                    // do not log anything here
                }
        }
        return listEvents;
    }

    /**
     * return the description of ping job
     */
    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();

        plugInDescription.setName("MoveCaseArchive");
        plugInDescription.setExplanation("Move cases to an archive database");
        plugInDescription.setLabel("Move Case Archive");
        plugInDescription.setWarning("This plugin move cases to an external database, and purge them. Use the page XXX to access this information after");
        plugInDescription.setCategory(CATEGORY.CASES);

        plugInDescription.setStopJob(JOBSTOPPER.BOTH);

        plugInDescription.addParameter(cstParamDatasource);
        plugInDescription.addParameter(cstParamProcessFilter);
        plugInDescription.addParameter(cstParamDelay);
        plugInDescription.addParameter(cstParamMoveProcessData);
        plugInDescription.addParameter(cstParamMoveLocalData);
        plugInDescription.addParameter(cstParamHistoryProcessData);

        plugInDescription.addParameter(cstParamMoveActivity);
        plugInDescription.addParameter(cstParamMoveDocument);
        plugInDescription.addParameter(cstParamMoveComment);

        plugInDescription.addParameter(cstParamOperationCase);
        return plugInDescription;
    }

    private class MoveParameters {

        public POLICY_DATA moveProcessData;
        public POLICY_DATA moveLocalData;
        public boolean saveHistoryProcessData;
        public boolean moveActivities;
        public boolean moveSynthesisActivity;
        public boolean moveDocuments;
        public LOADCOMMENTS moveComments;
    }

    /**
     * execution of the job. Just calculated the result according the parameters, and return it.
     */
    @Override
    public MilkJobOutput executeJob(MilkJobExecution milkJobExecution) {
        MilkJobOutput milkJobOutput = milkJobExecution.getMilkJobOutput();

        ProcessAPI processAPI = milkJobExecution.getApiAccessor().getProcessAPI();
        // get Input 
        SearchOptionsBuilder searchBuilderCase = new SearchOptionsBuilder(0, milkJobExecution.getJobStopAfterMaxItems() + 1);

        String datasource = milkJobExecution.getInputStringParameter(cstParamDatasource);

        MoveParameters moveParameters = new MoveParameters();
        moveParameters.moveProcessData = POLICY_DATA.IGNORE;
        if (CSTOPERATION_ARCHIVE.equals(milkJobExecution.getInputStringParameter(cstParamMoveProcessData)))
            moveParameters.moveProcessData = POLICY_DATA.ARCHIVE;

        moveParameters.moveLocalData = POLICY_DATA.IGNORE;
        if (CSTOPERATION_ARCHIVE.equals(milkJobExecution.getInputStringParameter(cstParamMoveLocalData)))
            moveParameters.moveLocalData = POLICY_DATA.ARCHIVE;

        moveParameters.saveHistoryProcessData = false;
        if (CSTOPERATION_HISTORYARCHIVE.equals(milkJobExecution.getInputStringParameter(cstParamHistoryProcessData)))
            moveParameters.saveHistoryProcessData = true;

        moveParameters.moveActivities = CSTOPERATION_ARCHIVE.equals(milkJobExecution.getInputStringParameter(cstParamMoveActivity)) || CSTOPERATION_ARCHIVELIGHT.equals(milkJobExecution.getInputStringParameter(cstParamMoveActivity));
        moveParameters.moveSynthesisActivity = CSTOPERATION_ARCHIVELIGHT.equals(milkJobExecution.getInputStringParameter(cstParamMoveActivity));
        moveParameters.moveDocuments = CSTOPERATION_ARCHIVE.equals(milkJobExecution.getInputStringParameter(cstParamMoveDocument));
        moveParameters.moveComments = CSTOPERATION_ARCHIVE.equals(milkJobExecution.getInputStringParameter(cstParamMoveComment))? LOADCOMMENTS.ONLYUSERS : LOADCOMMENTS.NONE;

        String operationCase = milkJobExecution.getInputStringParameter(cstParamOperationCase );
        
        
        DatabaseConnection.ConnectionResult connectionResult = null;
        try {
            connectionResult = DatabaseConnection.getConnection(Arrays.asList(datasource));

            connectionResult.con.setAutoCommit(false);

            // Parameters
            ListProcessesResult listProcessResult = milkJobExecution.getInputArrayProcess(cstParamProcessFilter, false, searchBuilderCase, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                milkJobOutput.addEvents(listProcessResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return milkJobOutput;
            }
            DelayResult delayResult = milkJobExecution.getInputDelayParameter(cstParamDelay, new Date(), false);
            if (BEventFactory.isError(delayResult.listEvents)) {
                milkJobOutput.addEvents(delayResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                return milkJobOutput;
            }


            // search
            StringBuffer buildTheSQLRequest = new StringBuffer();
            SearchOptionsBuilder searchActBuilder = getSearchOptionBuilder(milkJobExecution, listProcessResult, delayResult, buildTheSQLRequest);

            Chronometer startSearchProcessInstance = milkJobOutput.beginChronometer("searchProcessInstance");
            SearchResult<ArchivedProcessInstance> searchProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());
            buildTheSQLRequest.append("Nb result : " + searchProcessInstance.getCount() + "<br>");
            milkJobOutput.addReportInHtml(buildTheSQLRequest.toString());

            milkJobOutput.endChronometer(startSearchProcessInstance);
            milkJobExecution.setAvancement(10);

            //---------------- operation
            milkJobOutput.addReportTableBegin(new String[] { "CaseId", "SubProcess", "Activity", "Datas", "Documents", "Comments", "Status" });
            Map<Long, User> cacheUsers = new HashMap<>();

            List<Long> listArchivedProcessInstances = new ArrayList<>();
            for (ArchivedProcessInstance archivedProcessInstance : searchProcessInstance.getResult()) {
                if (milkJobExecution.isStopRequired())
                    break;
                
                // operation copy? Maybe the case is already copied
                if (CSTOPERATION_COPY.equals(operationCase ) )
                {
                    ResultMove resultMove = existProcessInstanceInArchive( archivedProcessInstance.getSourceObjectId(),milkJobExecution.getTenantId(),connectionResult.con);
                    milkJobOutput.addEvents(resultMove.listEvents);
                    // already exist
                    if (resultMove.nbProcessInstances>0)
                        continue;
           
                }
                
                // move the data to the another database
                List<BEvent> listEvents = moveDatabase(moveParameters, archivedProcessInstance, milkJobExecution.getApiAccessor(), milkJobExecution.getTenantId(), milkJobOutput, connectionResult.con, cacheUsers);
                milkJobOutput.addEvents(listEvents);

                if (CSTOPERATION_MOVE.equals(operationCase ) ) {
                    if (!BEventFactory.isError(listEvents)) {
                        // delete the case Id
                        listArchivedProcessInstances.add(archivedProcessInstance.getSourceObjectId());
                    }
                    if (listArchivedProcessInstances.size() >= 50) {
                        try {
                            Chronometer processInstanceMarker = milkJobOutput.beginChronometer("deleteProcessInstance");
                            processAPI.deleteArchivedProcessInstancesInAllStates(listArchivedProcessInstances);
                            listArchivedProcessInstances.clear();
                            milkJobOutput.endChronometer(processInstanceMarker, listArchivedProcessInstances.size());
                        } catch (Exception e) {
                            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "ListArchiveId: " + listArchivedProcessInstances.toString() + " " + e.getMessage()));
                            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                            listArchivedProcessInstances.clear(); // do not try to purge them twice at output
                            break;
                        }
                    }
                }
                milkJobOutput.nbItemsProcessed++;
            }
            milkJobOutput.addReportTableEnd();

            // purge the last list
            if (! listArchivedProcessInstances.isEmpty()) {
                try {
                    Chronometer processInstanceMarker = milkJobOutput.beginChronometer("deleteProcessInstance");
                    processAPI.deleteArchivedProcessInstancesInAllStates(listArchivedProcessInstances);
                    milkJobOutput.endChronometer(processInstanceMarker, listArchivedProcessInstances.size());
                } catch (Exception e) {
                    milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "ListArchiveId: " + listArchivedProcessInstances.toString() + " " + e.getMessage()));
                    milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                }
            }
            
            milkJobOutput.addChronometersInReport(true, true);

            if (BEventFactory.isError(milkJobOutput.getListEvents())) {
                milkJobOutput.executionStatus = ExecutionStatus.ERROR;
            } else {
                milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;
                if (milkJobExecution.isStopRequired())
                    milkJobOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;
            }

        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventOperationFailed, e, ""));
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;

        } finally {
            if (connectionResult != null && connectionResult.con != null)
                try {
                    connectionResult.con.close();
                } catch (Exception e) {
                }
        }
        return milkJobOutput;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Move to the database */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    private List<BEvent> moveDatabase(MoveParameters moveParameters, ArchivedProcessInstance archivedProcessInstance, APIAccessor apiAccessor, long tenantId, MilkJobOutput milkJobOutput, Connection con, Map<Long, User> cacheUsers) {

        CaseDetailsAPI caseDetailsAPI = new CaseDetailsAPI();
        CaseHistoryParameter caseHistoryParameter = new CaseHistoryParameter();
        caseHistoryParameter.caseId = archivedProcessInstance.getSourceObjectId();
        caseHistoryParameter.tenantId = tenantId;
        caseHistoryParameter.loadSubProcess = true;
        caseHistoryParameter.loadContract = false;

        caseHistoryParameter.loadProcessVariables = moveParameters.moveProcessData == POLICY_DATA.ARCHIVE;
        caseHistoryParameter.loadBdmVariables = moveParameters.moveProcessData == POLICY_DATA.ARCHIVE;
        caseHistoryParameter.loadContentBdmVariables = false;
        caseHistoryParameter.loadArchivedHistoryProcessVariable = moveParameters.saveHistoryProcessData;
        caseHistoryParameter.loadActivities = moveParameters.moveActivities;
        caseHistoryParameter.loadEvents = false;
        caseHistoryParameter.loadTimers = false;
        caseHistoryParameter.loadDocuments = moveParameters.moveDocuments;
        caseHistoryParameter.loadComments = moveParameters.moveComments;

        List<BEvent> listEvents = new ArrayList<>();
        CaseDetails caseDetails = null;

        try {
            // caseHistoryParameter.caseId = 3009L;
            // caseDetails = caseDetailsAPI.getCaseDetails(caseHistoryParameter, apiAccessor.getProcessAPI(), apiAccessor.getIdentityAPI(), apiAccessor.getBusinessDataAPI(), null);
            String statusCase = "";
            caseHistoryParameter.caseId = archivedProcessInstance.getSourceObjectId();
            Chronometer loadCaseChronometer = milkJobOutput.beginChronometer("loadcase");
            caseDetails = caseDetailsAPI.getCaseDetails(caseHistoryParameter, apiAccessor.getProcessAPI(), apiAccessor.getIdentityAPI(), apiAccessor.getBusinessDataAPI(), null);
            milkJobOutput.endChronometer(loadCaseChronometer, 1);
            if (BEventFactory.isError(caseDetails.listEvents))
            {
                listEvents.addAll(caseDetails.listEvents);
                statusCase = "Error" + BEventFactory.getSyntheticLog(listEvents);
                milkJobOutput.addReportTableLine(new Object[] { caseHistoryParameter.caseId, caseDetails.listProcessInstances.size(), 0, 0, 0, 0, statusCase });
                return listEvents;
            }
            int nbActivities = 0;
            int nbDatas = 0;
            int nbDocuments = 0;
            int nbComments = 0;

            for (ProcessInstanceDescription processInstance : caseDetails.listProcessInstances) {
                ResultMove resultMove = moveDatabaseProcessInstance(moveParameters, processInstance, caseDetails, apiAccessor, tenantId, con, cacheUsers);
                listEvents.addAll(resultMove.listEvents);
                nbActivities += resultMove.nbActivities;
                nbDatas += resultMove.nbDatas;
                nbDocuments += resultMove.nbDocuments;
                nbComments += resultMove.nbComments;
                if (BEventFactory.isError(listEvents))
                    break;
            }
            
            if (BEventFactory.isError(listEvents)) {
                statusCase = "Error";
                con.rollback();
            } else {
                statusCase = "Success";
                con.commit();
            }

            milkJobOutput.addReportTableLine(new Object[] { caseHistoryParameter.caseId, caseDetails.listProcessInstances.size(), nbActivities, nbDatas, nbDocuments, nbComments, statusCase });
        } catch (SQLException e) {
            listEvents.add(new BEvent(eventSaveRecordFailed, e, "CaseId[" + (caseDetails == null ? "null" : caseDetails.rootCaseId)));
            if (BEventFactory.isError(listEvents)) {
                try {
                    con.rollback();
                } catch (SQLException e1) {
                    // Do nothing here
                }
            }
        } catch (Exception e) {
            try {
                con.rollback();
            } catch (SQLException e1) {
                // do not match that one
            }
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();

            listEvents.add(new BEvent(eventSaveRecordFailed, e, "CaseId[" + (caseDetails == null ? "null" : caseDetails.rootCaseId) + "] Exception " + e.getMessage() + " at " + exceptionDetails));

        }
        return listEvents;
    }

    private class ResultMove {

        public List<BEvent> listEvents = new ArrayList<>();
        public int nbActivities = 0;
        public int nbDatas = 0;
        public int nbDocuments = 0;
        public int nbComments = 0;
        public int nbProcessInstances=0;

    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Private */
    /*                                                                      */
    /* -------------------------------------------------------------------- */


    private ResultMove existProcessInstanceInArchive(long processInstance, long tenantId, Connection con) {
        ResultMove resultMove = new ResultMove();
        String sqlRequest = "select count(*) as numberofcases from "
                + DatabaseDefinition.BDE_TABLE_PROCESSINSTANCE
                +" where "+DatabaseDefinition.BDE_PROCESSINSTANCE_TENANTID+"=?"
                +" and "+DatabaseDefinition.BDE_PROCESSINSTANCE_PROCESSINSTANCEID+" = ?";
        try (PreparedStatement pstmt = con.prepareStatement(sqlRequest.toString())) {
                pstmt.setObject(1, tenantId);
                pstmt.setObject(2, processInstance);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    resultMove.nbProcessInstances = rs.getInt("numberofcases");
                }
                return resultMove; // something strange : a count should have an answer
        } catch(Exception e) {
            logger.severe("Error when executing "+sqlRequest+" "+e.getMessage());
            resultMove.listEvents.add( new BEvent(eventCantExecuteExistProcessInstance, e, "SqlRequest ["+sqlRequest+"] "+e.getMessage()));
            return null;
        }
    }
    /**
     * @param processInstance
     * @param caseDetails
     * @param apiAccessor
     * @param tenantId
     * @param con
     * @return
     */
    private ResultMove moveDatabaseProcessInstance(MoveParameters moveParameters, ProcessInstanceDescription processInstance, CaseDetails caseDetails, APIAccessor apiAccessor, long tenantId, Connection con, Map<Long, User> cacheUsers) {
        ResultMove resultMove = new ResultMove();

        // move first the CaseId
        Map<String, Object> record = new HashMap<>();
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_TENANTID, caseDetails.tenantId);
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_PROCESSDEFINITIONID, processInstance.processDefinitionId);
        try {
            ProcessDefinition processDefinition = caseDetails.getCaseDetailsAPI().getProcessDefinition(processInstance.processDefinitionId, apiAccessor.getProcessAPI());
            if (processDefinition != null) {
                record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_PROCESSDEFINITIONNAME, processDefinition.getName());
                record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_PROCESSDEFINITIONVERSION, processDefinition.getVersion());
            }
        } catch (ProcessDefinitionNotFoundException e) {
            resultMove.listEvents.add(new BEvent(eventProcessDefinitionNotFound, "ProcessDefinition[" + processInstance.processDefinitionId + "] register in case[" + processInstance.processInstanceId + "] TenantId[" + tenantId + "]"));
            return resultMove;
        }
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_ROOTPROCESSINSTANCEID, processInstance.rootProcessInstanceId);
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_PARENTPROCESSINSTANCEID, processInstance.parentProcessInstanceId);
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_PROCESSINSTANCEID, processInstance.processInstanceId);
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_START_DATE, dateToLong(processInstance.startDate));
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_END_DATE, dateToLong(processInstance.endDate));
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_STARTEDBY, processInstance.archProcessInstance.getStartedBy());
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_STARTEDBYNAME, getUserName(processInstance.archProcessInstance.getStartedBy(), apiAccessor.getIdentityAPI(), cacheUsers));

        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_STARTEDBYSUBSTITUTE, processInstance.archProcessInstance.getStartedBySubstitute());
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_STARTEDBYSUBSTITUTENAME, getUserName(processInstance.archProcessInstance.getStartedBySubstitute(), apiAccessor.getIdentityAPI(), cacheUsers));

        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_ARCHIVEDATE, dateToLong(processInstance.archProcessInstance.getArchiveDate()));
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX1, processInstance.archProcessInstance.getStringIndexValue(1));
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX2, processInstance.archProcessInstance.getStringIndexValue(2));
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX3, processInstance.archProcessInstance.getStringIndexValue(3));
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX4, processInstance.archProcessInstance.getStringIndexValue(4));
        record.put(DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX5, processInstance.archProcessInstance.getStringIndexValue(5));
        String informationContext = "ProcessInstance[" + processInstance.processInstanceId + "]";
        DatabaseTables databaseTables = new DatabaseTables();
        resultMove.listEvents.addAll(databaseTables.insert(DatabaseDefinition.BDE_TABLE_PROCESSINSTANCE, record, informationContext, con));
        if (BEventFactory.isError(resultMove.listEvents))
            return resultMove;

        if (moveParameters.moveActivities)
            moveDatabaseFlownodeInstance(moveParameters, processInstance, caseDetails, resultMove, apiAccessor, tenantId, con, cacheUsers);

        if (moveParameters.moveProcessData.equals(POLICY_DATA.ARCHIVE) || moveParameters.moveLocalData.equals(POLICY_DATA.ARCHIVE))
            moveDatabaseDataInstance(moveParameters, processInstance, caseDetails, resultMove, apiAccessor, tenantId, con);

        if (moveParameters.moveDocuments)
            moveDatabaseDocumentInstance(moveParameters, processInstance, caseDetails, resultMove, apiAccessor, tenantId, con);
        if (moveParameters.moveComments != LOADCOMMENTS.NONE)
            moveDatabaseCommentsInstance(moveParameters, processInstance, caseDetails, resultMove, apiAccessor, tenantId, con, cacheUsers);

        return resultMove;
    }

    /**
     * @param moveParameters
     * @param processInstance
     * @param caseDetails
     * @param resultMove
     * @param apiAccessor
     * @param tenantId
     * @param con
     */
    private void moveDatabaseFlownodeInstance(MoveParameters moveParameters, ProcessInstanceDescription processInstance, 
            CaseDetails caseDetails, ResultMove resultMove, APIAccessor apiAccessor, long tenantId, Connection con, Map<Long, User> cacheUsers) {
        DatabaseTables databaseTables = new DatabaseTables();
        // StringBuilder logAnalysis= new StringBuilder();
        /**
         * if we want to get only one item per flownode, then we have to 1/ order it by the date, 2/ keep the last item for each sourceObjectid
         * Example
         * ParentId SourceId Type ArchiveDate Name State
         * 3001 60002 auto 1600975938212 ServiceTasks executing
         * 3001 60002 auto 1600975940900 ServiceTasks completed <====
         * 3001 60003 user 1600975940949 User task initializing
         * 3001 60003 user 1600977696921 User task ready <====
         * 60004 60008 auto 1600977697064 Instanciate 5 executing
         * 60004 60008 auto 1600977697067 Instanciate 5 completed <====
         */
        // logAnalysis.append("Nb line"+caseDetails.listCaseDetailFlowNodes+";");
        // INVERSE the order 
        Collections.sort(caseDetails.listCaseDetailFlowNodes, new Comparator<CaseDetailFlowNode>() {

            public int compare(CaseDetailFlowNode s1,
                    CaseDetailFlowNode s2) {
                return s2.getDate().compareTo(s1.getDate());
            }
        });

        // so now we keep only the FIRST one
        Set<Long> registerSourceObjectId = new HashSet<>();

        for (CaseDetailFlowNode flowNode : caseDetails.listCaseDetailFlowNodes) {
            if (processInstance.processInstanceId != flowNode.getProcessInstanceId()) {
                continue;
            }
            // logAnalysis.append("Source="+flowNode.getSourceObjectId()+";");
            // we works only with ARCHIVE case they have all a sourceObjectId()
            if (moveParameters.moveSynthesisActivity && registerSourceObjectId.contains(flowNode.getSourceObjectId()))
                continue;
            registerSourceObjectId.add(flowNode.getSourceObjectId());
            // logAnalysis.append("KEEP;PID="+flowNode.getProcessInstanceId()+"/Par="+flowNode.getParentContainerId()+"/Sour="+flowNode.getSourceObjectId()+";");;

            resultMove.nbActivities++;
            List<Map<String, Object>> listRecords = new ArrayList<>();
            Map<String, Object> record = new HashMap<>();
            listRecords.add(record);

            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_TENANTID, caseDetails.tenantId);
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_ID, flowNode.getId());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_FLOWNODEDEFINITIONID, flowNode.getFlownodeDefinitionId());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_KIND, flowNode.getType().toString());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_ARCHIVEDATE, dateToLong(flowNode.getDate()));
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_PROCESSINSTANCEID, flowNode.getProcessInstanceId());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_PARENTCONTAINERID, flowNode.getParentContainerId());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_SOURCEOBJECTID, flowNode.getSourceObjectId());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_NAME, flowNode.getName());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_DISPLAYNAME, flowNode.getDisplayName());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_DISPLAYDESCRIPTION, flowNode.getDisplayDescription());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_STATENAME, flowNode.getState());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_REACHEDSTATEDATE, dateToLong(flowNode.getReachedStateDate()));
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_GATEWAYTYPE, flowNode.getFlowNodeType().toString());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_LOOP_COUNTER, flowNode.getLoopCounter());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_NUMBEROFINSTANCES, flowNode.getNumberOfInstances());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_EXECUTEDBY, flowNode.getExecutedBy());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_EXECUTEDBYNAME, getUserName(flowNode.getExecutedBy(), apiAccessor.getIdentityAPI(), cacheUsers));
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_EXECUTEDBYSUBSTITUTE, flowNode.getExecutedBySubstitute());
            record.put(DatabaseDefinition.BDE_FLOWNODEINSTANCE_EXECUTEDBYSUBSTITUTENAME, getUserName(flowNode.getExecutedBySubstitute(), apiAccessor.getIdentityAPI(), cacheUsers));
            String informationContext = "ProcessInstance[" + processInstance.processInstanceId + "] ActivityId [" + flowNode.getId() + "]";

            resultMove.listEvents.addAll(databaseTables.insert(DatabaseDefinition.BDE_TABLE_FLOWNODEINSTANCE, record, informationContext, con));
            if (BEventFactory.isError(resultMove.listEvents))
                return;
        }
        // logger.info(logAnalysis.toString());
    }

    /**
     * Copy the Data information
     * 
     * @param processInstance
     * @param caseDetails
     * @param apiAccessor
     * @param tenantId
     * @param con
     * @return
     */
    private void moveDatabaseDataInstance(MoveParameters moveParameters, ProcessInstanceDescription processInstance, CaseDetails caseDetails, ResultMove resultMove, APIAccessor apiAccessor, long tenantId, Connection con) {
        DatabaseTables databaseTables = new DatabaseTables();

        for (CaseDetailVariable processVariable : caseDetails.listVariables) {
            if (processInstance.processInstanceId != processVariable.processInstanceId) {
                continue;
            }

            if (processVariable.scopeVariable == ScopeVariable.LOCAL && moveParameters.moveLocalData.equals(POLICY_DATA.IGNORE))
                continue;
            if (processVariable.scopeVariable == ScopeVariable.PROCESS && moveParameters.moveProcessData.equals(POLICY_DATA.IGNORE))
                continue;
            if (processVariable.scopeVariable == ScopeVariable.BDM && moveParameters.moveProcessData.equals(POLICY_DATA.IGNORE))
                continue;

            // save it now
            resultMove.nbDatas++;
            List<Map<String, Object>> listRecords = new ArrayList<>();
            Map<String, Object> record = new HashMap<>();
            listRecords.add(record);
            record.put(DatabaseDefinition.BDE_DATAINSTANCE_TENANTID, caseDetails.tenantId);
            record.put(DatabaseDefinition.BDE_DATAINSTANCE_ID, processVariable.id);
            record.put(DatabaseDefinition.BDE_DATAINSTANCE_NAME, processVariable.name);

            record.put(DatabaseDefinition.BDE_DATAINSTANCE_PROCESSINSTANCEID, processVariable.processInstanceId);
            if (processVariable.scopeVariable == ScopeVariable.LOCAL)
                record.put(DatabaseDefinition.BDE_DATAINSTANCE_ACTIVITYID, processVariable.activityId);

            record.put(DatabaseDefinition.BDE_DATAINSTANCE_CONTAINERTYPE, processVariable.scopeVariable.toString());
            // a BDM variable does not have a date archived
            record.put(DatabaseDefinition.BDE_DATAINSTANCE_ARCHIVEDATE, dateToLong(processVariable.dateArchived));

            // this is a process variable ?
            record.put(DatabaseDefinition.BDE_DATAINSTANCE_SCOPE, processVariable.scopeVariable.toString());
            if (processVariable.scopeVariable == ScopeVariable.LOCAL || processVariable.scopeVariable == ScopeVariable.PROCESS) {
                record.put(DatabaseDefinition.BDE_DATAINSTANCE_CLASSNAME, processVariable.value == null ? null : processVariable.value.getClass().getName());
                // according the type, use the correct field
                if (processVariable.value instanceof Float)
                    record.put(DatabaseDefinition.BDE_DATAINSTANCE_FLOATVALUE, processVariable.value);
                else if (processVariable.value instanceof Double)
                    record.put(DatabaseDefinition.BDE_DATAINSTANCE_DOUBLEVALUE, processVariable.value);
                else if (processVariable.value instanceof Boolean)
                    record.put(DatabaseDefinition.BDE_DATAINSTANCE_BOOLEANVALUE, processVariable.value);
                else if (processVariable.value instanceof Date)
                    record.put(DatabaseDefinition.BDE_DATAINSTANCE_DATEVALUE, dateToLong((Date) processVariable.value));
                else if (processVariable.value instanceof Long || processVariable.value instanceof Integer)
                    record.put(DatabaseDefinition.BDE_DATAINSTANCE_LONGVALUE, processVariable.value);
                else
                    record.put(DatabaseDefinition.BDE_DATAINSTANCE_VALUE, processVariable.value == null ? null : processVariable.value.toString());
            }

            else if (processVariable.scopeVariable == ScopeVariable.BDM) {
                record.put(DatabaseDefinition.BDE_DATAINSTANCE_BDMNAME, processVariable.bdmName);
                record.put(DatabaseDefinition.BDE_DATAINSTANCE_BDMISMULTIPLE, processVariable.bdmIsMultiple);
                record.put(DatabaseDefinition.BDE_DATAINSTANCE_BDMINDEX, 0);
                // value may be null
                if (!processVariable.listPersistenceId.isEmpty())
                    record.put(DatabaseDefinition.BDE_DATAINSTANCE_BDMPERSISTENCEID, processVariable.listPersistenceId.get(0));

                // then create one record per item in the list
                for (int i = 1; i < processVariable.listPersistenceId.size(); i++) {
                    Map<String, Object> duplicateRecord = new HashMap<>();
                    duplicateRecord.putAll(record);
                    duplicateRecord.put(DatabaseDefinition.BDE_DATAINSTANCE_BDMINDEX, i);
                    duplicateRecord.put(DatabaseDefinition.BDE_DATAINSTANCE_BDMPERSISTENCEID, processVariable.listPersistenceId.get(i));
                    listRecords.add(duplicateRecord);
                }

            }

            for (Map<String, Object> recordInList : listRecords) {
                String informationContext = "ProcessInstance[" + processInstance.processInstanceId + "] ";
                if (record.get(DatabaseDefinition.BDE_DATAINSTANCE_NAME) != null)
                    informationContext += "ProcessVariable[" + record.get(DatabaseDefinition.BDE_DATAINSTANCE_NAME) + "]";
                if (record.get(DatabaseDefinition.BDE_DOCUMENTINSTANCE_NAME) != null)
                    informationContext += "BDM[" + record.get(DatabaseDefinition.BDE_DOCUMENTINSTANCE_NAME) + "] index[" + DatabaseDefinition.BDE_DOCUMENTINSTANCE_INDEX + "]";

                resultMove.listEvents.addAll(databaseTables.insert(DatabaseDefinition.BDE_TABLE_DATAINSTANCE, recordInList, informationContext, con));
                if (BEventFactory.isError(resultMove.listEvents))
                    return;
            }

        }

    }

    /**
     * @param processInstance
     * @param caseDetails
     * @param resultMove
     * @param apiAccessor
     * @param tenantId
     * @param con
     */
    private void moveDatabaseDocumentInstance(MoveParameters moveParameters, ProcessInstanceDescription processInstance, CaseDetails caseDetails, ResultMove resultMove, APIAccessor apiAccessor, long tenantId, Connection con) {
        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        DatabaseTables databaseTables = new DatabaseTables();

        for (CaseDetailDocument documentVariable : caseDetails.listDocuments) {
            try {
                if (processInstance.processInstanceId != documentVariable.processInstanceId || documentVariable.document == null) {
                    continue;
                }
                resultMove.nbDocuments++;
                List<Map<String, Object>> listRecords = new ArrayList<>();
                Map<String, Object> record = new HashMap<>();
                listRecords.add(record);
                record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_TENANTID, caseDetails.tenantId);

                record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_ID, documentVariable.document.getId());

                record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_NAME, documentVariable.document.getName());
                record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_PROCESSINSTANCEID, documentVariable.processInstanceId);
                record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_VERSION, documentVariable.document.getVersion());
                // the archived date does not exist at this moment in Bonita
                record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_ARCHIVEDATE, null);

                record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_INDEX, documentVariable.document.getIndex());
                record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_ISMULTIPLE, documentVariable.document.getIndex() >= 0);
                record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_AUTHOR, documentVariable.document.getAuthor());
                record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_FILENAME, documentVariable.document.getContentFileName());
                record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_MIMETYPE, documentVariable.document.getContentMimeType());
                record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_URL, documentVariable.document.getUrl());
                record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_HASCONTENT, documentVariable.document.hasContent());
                byte[] contentDocument = processAPI.getDocumentContent(documentVariable.document.getContentStorageId());
                if (contentDocument != null)
                    record.put(DatabaseDefinition.BDE_DOCUMENTINSTANCE_CONTENT, new ByteArrayInputStream(contentDocument));

                for (Map<String, Object> recordInList : listRecords) {
                    String informationContext = "ProcessInstance[" + processInstance.processInstanceId + "], Document[" + record.get(DatabaseDefinition.BDE_DOCUMENTINSTANCE_NAME);

                    resultMove.listEvents.addAll(databaseTables.insert(DatabaseDefinition.BDE_TABLE_DOCUMENTINSTANCE, recordInList, informationContext, con));
                    if (BEventFactory.isError(resultMove.listEvents))
                        return;
                }
            } catch (DocumentNotFoundException e) {
                resultMove.listEvents.add(new BEvent(eventCantAccessDocument, e, "ProcessId[" + documentVariable.processInstanceId + "] DocumentName[" + documentVariable.document.getName() + "]"));
            }

        }

    }

    /**
     * @param moveParameters
     * @param processInstance
     * @param caseDetails
     * @param resultMove
     * @param apiAccessor
     * @param tenantId
     * @param con
     */
    private void moveDatabaseCommentsInstance(MoveParameters moveParameters, ProcessInstanceDescription processInstance, CaseDetails caseDetails, ResultMove resultMove, APIAccessor apiAccessor, long tenantId, Connection con,
            Map<Long, User> cacheUsers) {
        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        DatabaseTables databaseTables = new DatabaseTables();

        for (CaseDetailComment commentVariable : caseDetails.listComments) {
            try {
                if (processInstance.processInstanceId != commentVariable.processInstanceId) {
                    continue;
                }
                resultMove.nbComments++;
                List<Map<String, Object>> listRecords = new ArrayList<>();
                Map<String, Object> record = new HashMap<>();
                record.put(DatabaseDefinition.BDE_COMMENTINSTANCE_TENANTID, caseDetails.tenantId);
                if (commentVariable.comment != null) {
                    record.put(DatabaseDefinition.BDE_COMMENTINSTANCE_ID, commentVariable.comment.getId());
                    record.put(DatabaseDefinition.BDE_COMMENTINSTANCE_USERID, commentVariable.comment.getUserId());
                    record.put(DatabaseDefinition.BDE_COMMENTINSTANCE_USERNAME, getUserName(commentVariable.comment.getUserId(), apiAccessor.getIdentityAPI(), cacheUsers));
                    record.put(DatabaseDefinition.BDE_COMMENTINSTANCE_CONTENT, commentVariable.comment.getContent());
                    record.put(DatabaseDefinition.BDE_COMMENTINSTANCE_POSTDATE, commentVariable.comment.getPostDate());
                    record.put(DatabaseDefinition.BDE_COMMENTINSTANCE_PROCESSINSTANCEID, commentVariable.comment.getProcessInstanceId());
                } else {
                    record.put(DatabaseDefinition.BDE_COMMENTINSTANCE_ID, commentVariable.archivedComment.getId());
                    record.put(DatabaseDefinition.BDE_COMMENTINSTANCE_USERID, commentVariable.archivedComment.getUserId());
                    record.put(DatabaseDefinition.BDE_COMMENTINSTANCE_USERNAME, getUserName(commentVariable.archivedComment.getUserId(), apiAccessor.getIdentityAPI(), cacheUsers));
                    record.put(DatabaseDefinition.BDE_COMMENTINSTANCE_CONTENT, commentVariable.archivedComment.getContent());
                    record.put(DatabaseDefinition.BDE_COMMENTINSTANCE_POSTDATE, dateToLong(commentVariable.archivedComment.getPostDate() ));
                    record.put(DatabaseDefinition.BDE_COMMENTINSTANCE_PROCESSINSTANCEID, commentVariable.archivedComment.getProcessInstanceId());
                }
                String informationContext = "ProcessInstance[" + processInstance.processInstanceId + "], Comment[" + record.get(DatabaseDefinition.BDE_COMMENTINSTANCE_ID);

                resultMove.listEvents.addAll(databaseTables.insert(DatabaseDefinition.BDE_TABLE_COMMENTINSTANCE, record, informationContext, con));
                if (BEventFactory.isError(resultMove.listEvents))
                    return;

            } catch (Exception e) {
                resultMove.listEvents.add(new BEvent(eventCantAccessDocument, e, "ProcessId[" + commentVariable.processInstanceId + "] Comment[" + commentVariable.comment.getId() + "]"));
            }
        }
    }

    /**
     * @param jobExecution
     * @param listProcessResult
     * @param delayResult
     * @param buildTheSQLRequest
     * @return
     */

    private SearchOptionsBuilder getSearchOptionBuilder(MilkJobExecution jobExecution, ListProcessesResult listProcessResult,
            DelayResult delayResult,
            StringBuffer buildTheSQLRequest) {

        // ------------- Active
        Integer maximumArchiveDeletionPerRoundInt = jobExecution.getJobStopAfterMaxItems();
        // default value is 1 Million
        if (maximumArchiveDeletionPerRoundInt == null || maximumArchiveDeletionPerRoundInt.equals(MilkJob.CSTDEFAULT_STOPAFTER_MAXITEMS))
            maximumArchiveDeletionPerRoundInt = 1000000;
        int maximumArchiveDeletionPerRound = maximumArchiveDeletionPerRoundInt;

        SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, maximumArchiveDeletionPerRound + 1);
        buildTheSQLRequest.append("select * from process_instance where ");

        if (!listProcessResult.listProcessDeploymentInfo.isEmpty()) {
            searchActBuilder.leftParenthesis();
            buildTheSQLRequest.append(" (");

            for (int i = 0; i < listProcessResult.listProcessDeploymentInfo.size(); i++) {
                if (i > 0) {
                    searchActBuilder.or();
                    buildTheSQLRequest.append("  or ");
                }
                searchActBuilder.filter(ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, listProcessResult.listProcessDeploymentInfo.get(i).getProcessId());
                buildTheSQLRequest.append(" processdefinitionid=" + listProcessResult.listProcessDeploymentInfo.get(i).getProcessId());

            }
            searchActBuilder.rightParenthesis();
            searchActBuilder.and();
            buildTheSQLRequest.append(") and ");
        }

        searchActBuilder.lessOrEquals(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, delayResult.delayDate.getTime());
        buildTheSQLRequest.append(" startdate <= " + delayResult.delayDate.getTime());

        searchActBuilder.sort(ProcessInstanceSearchDescriptor.START_DATE, Order.ASC);
        buildTheSQLRequest.append(" order by startdate asc;<br>");
        return searchActBuilder;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* GetListTableArchives */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * @return
     */
    private List<DataDefinition> getListTableArchives() {
        List<DataDefinition> listTables = new ArrayList<>();

        // Case
        DataDefinition caseTable = new DataDefinition(DatabaseDefinition.BDE_TABLE_PROCESSINSTANCE);

        caseTable.listColumns = Arrays.asList(new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_TENANTID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_PROCESSDEFINITIONID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_PROCESSDEFINITIONNAME, COLTYPE.STRING, 150),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_PROCESSDEFINITIONVERSION, COLTYPE.STRING, 50),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_ROOTPROCESSINSTANCEID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_PARENTPROCESSINSTANCEID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_PROCESSINSTANCEID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_START_DATE, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_END_DATE, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_STARTEDBY, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_STARTEDBYNAME, COLTYPE.STRING, 255),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_STARTEDBYSUBSTITUTE, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_STARTEDBYSUBSTITUTENAME, COLTYPE.STRING, 255),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_ARCHIVEDATE, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX1, COLTYPE.STRING, 255),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX2, COLTYPE.STRING, 255),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX3, COLTYPE.STRING, 255),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX4, COLTYPE.STRING, 255),
                new DataColumn(DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX5, COLTYPE.STRING, 255));
        caseTable.mapIndexes.put("PI_PDEF_IDX", Arrays.asList(DatabaseDefinition.BDE_PROCESSINSTANCE_TENANTID, DatabaseDefinition.BDE_PROCESSINSTANCE_PROCESSDEFINITIONID));
        caseTable.mapIndexes.put("PI_ST1_IDX", Arrays.asList(DatabaseDefinition.BDE_PROCESSINSTANCE_TENANTID, DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX1));
        caseTable.mapIndexes.put("PI_ST2_IDX", Arrays.asList(DatabaseDefinition.BDE_PROCESSINSTANCE_TENANTID, DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX2));
        caseTable.mapIndexes.put("PI_ST3_IDX", Arrays.asList(DatabaseDefinition.BDE_PROCESSINSTANCE_TENANTID, DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX3));
        caseTable.mapIndexes.put("PI_ST4_IDX", Arrays.asList(DatabaseDefinition.BDE_PROCESSINSTANCE_TENANTID, DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX4));
        caseTable.mapIndexes.put("PI_ST5_IDX", Arrays.asList(DatabaseDefinition.BDE_PROCESSINSTANCE_TENANTID, DatabaseDefinition.BDE_PROCESSINSTANCE_STRINGINDEX5));

        caseTable.mapConstraints.put("PI_PID_CONST", Arrays.asList(DatabaseDefinition.BDE_PROCESSINSTANCE_TENANTID, DatabaseDefinition.BDE_PROCESSINSTANCE_PROCESSINSTANCEID));
        listTables.add(caseTable);

        // Data
        // Case
        DataDefinition dataTable = new DataDefinition(DatabaseDefinition.BDE_TABLE_DATAINSTANCE);

        dataTable.listColumns = Arrays.asList(new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_TENANTID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_ID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_NAME, COLTYPE.STRING, 50),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_SCOPE, COLTYPE.STRING, 10),

                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_DESCRIPTION, COLTYPE.STRING, 50),

                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_PROCESSINSTANCEID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_ACTIVITYID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_CONTAINERTYPE, COLTYPE.STRING, 60),

                /** Process Variable */
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_CLASSNAME, COLTYPE.STRING, 100),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_VALUE, COLTYPE.TEXT),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_LONGVALUE, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_DATEVALUE, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_BOOLEANVALUE, COLTYPE.BOOLEAN),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_DOUBLEVALUE, COLTYPE.DECIMAL),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_FLOATVALUE, COLTYPE.DECIMAL),
                // new DataColumn("CLOBVALUE", COLTYPE.BLOB),
                // new DataColumn("DISCRIMINANT", COLTYPE.DECIMAL),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_BDMNAME, COLTYPE.STRING, 255),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_BDMISMULTIPLE, COLTYPE.BOOLEAN),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_BDMINDEX, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_BDMPERSISTENCEID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_DATAINSTANCE_ARCHIVEDATE, COLTYPE.LONG));
        dataTable.mapConstraints.put("DI_DAT_CONST", Arrays.asList(DatabaseDefinition.BDE_DATAINSTANCE_TENANTID,
                DatabaseDefinition.BDE_DATAINSTANCE_PROCESSINSTANCEID,
                DatabaseDefinition.BDE_DATAINSTANCE_ACTIVITYID,
                DatabaseDefinition.BDE_DATAINSTANCE_NAME,
                DatabaseDefinition.BDE_DATAINSTANCE_BDMINDEX));
        // it's not possible to create a foreignkey  : the key is processinstance+tenantid and no way to create a foreignkey like this
        
        dataTable.mapIndexes.put("DI_DAT_IDX", Arrays.asList(DatabaseDefinition.BDE_DATAINSTANCE_TENANTID, DatabaseDefinition.BDE_DATAINSTANCE_PROCESSINSTANCEID, DatabaseDefinition.BDE_DATAINSTANCE_ACTIVITYID));
        listTables.add(dataTable);

        // Case
        DataDefinition docTable = new DataDefinition(DatabaseDefinition.BDE_TABLE_DOCUMENTINSTANCE);

        docTable.listColumns = Arrays.asList(new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_TENANTID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_ID, COLTYPE.LONG),

                new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_NAME, COLTYPE.STRING, 50),
                new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_PROCESSINSTANCEID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_VERSION, COLTYPE.STRING, 50),
                new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_ARCHIVEDATE, COLTYPE.LONG),

                new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_INDEX, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_ISMULTIPLE, COLTYPE.BOOLEAN),
                new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_AUTHOR, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_FILENAME, COLTYPE.STRING, 255),
                new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_MIMETYPE, COLTYPE.STRING, 255),
                new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_URL, COLTYPE.STRING, 1024),
                new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_HASCONTENT, COLTYPE.BOOLEAN),
                new DataColumn(DatabaseDefinition.BDE_DOCUMENTINSTANCE_CONTENT, COLTYPE.BLOB));
        docTable.mapIndexes.put("DO_PID_IDX", Arrays.asList(DatabaseDefinition.BDE_DOCUMENTINSTANCE_TENANTID, DatabaseDefinition.BDE_DOCUMENTINSTANCE_ID));
        listTables.add(docTable);

        // Activity
        DataDefinition flowNodeTable = new DataDefinition(DatabaseDefinition.BDE_TABLE_FLOWNODEINSTANCE);

        flowNodeTable.listColumns = Arrays.asList(new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_TENANTID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_ID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_FLOWNODEDEFINITIONID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_KIND, COLTYPE.STRING, 25),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_ARCHIVEDATE, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_PROCESSINSTANCEID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_PARENTCONTAINERID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_SOURCEOBJECTID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_NAME, COLTYPE.STRING, 255),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_DISPLAYNAME, COLTYPE.STRING, 255),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_DISPLAYDESCRIPTION, COLTYPE.STRING, 255),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_STATENAME, COLTYPE.STRING, 50),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_REACHEDSTATEDATE, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_GATEWAYTYPE, COLTYPE.STRING, 50),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_LOOP_COUNTER, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_NUMBEROFINSTANCES, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_EXECUTEDBY, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_EXECUTEDBYNAME, COLTYPE.STRING,255),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_EXECUTEDBYSUBSTITUTE, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_FLOWNODEINSTANCE_EXECUTEDBYSUBSTITUTENAME, COLTYPE.STRING,255));
        flowNodeTable.mapIndexes.put("FL_PID_IDX", Arrays.asList(DatabaseDefinition.BDE_FLOWNODEINSTANCE_TENANTID, DatabaseDefinition.BDE_FLOWNODEINSTANCE_ID, DatabaseDefinition.BDE_FLOWNODEINSTANCE_PROCESSINSTANCEID, DatabaseDefinition.BDE_FLOWNODEINSTANCE_PARENTCONTAINERID));
        listTables.add(flowNodeTable);

        // Activity
        DataDefinition commentsTable = new DataDefinition(DatabaseDefinition.BDE_TABLE_COMMENTINSTANCE);

        commentsTable.listColumns = Arrays.asList(new DataColumn(DatabaseDefinition.BDE_COMMENTINSTANCE_TENANTID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_COMMENTINSTANCE_ID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_COMMENTINSTANCE_USERID, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_COMMENTINSTANCE_USERNAME, COLTYPE.STRING, 255),
                new DataColumn(DatabaseDefinition.BDE_COMMENTINSTANCE_CONTENT, COLTYPE.STRING, 512),
                new DataColumn(DatabaseDefinition.BDE_COMMENTINSTANCE_POSTDATE, COLTYPE.LONG),
                new DataColumn(DatabaseDefinition.BDE_COMMENTINSTANCE_PROCESSINSTANCEID, COLTYPE.LONG));
        commentsTable.mapIndexes.put("CO_IDX", Arrays.asList(DatabaseDefinition.BDE_COMMENTINSTANCE_TENANTID, DatabaseDefinition.BDE_COMMENTINSTANCE_ID, DatabaseDefinition.BDE_COMMENTINSTANCE_PROCESSINSTANCEID));
        listTables.add(commentsTable);

        return listTables;
    }

    private Long dateToLong(Date date) {
        if (date == null)
            return null;
        return date.getTime();
    }

    private String getUserName(Long userId, IdentityAPI identityAPI, Map<Long, User> cacheUsers) {
        if (userId == null || userId < 0)
            return null;
        User user = cacheUsers.get(userId);
        if (user != null)
            return user.getUserName();
        try {
            user = identityAPI.getUser(userId);
        } catch (UserNotFoundException e) {
            // user does not exist? strange, because the user come from the ProcessInstance
            logger.severe(LOGGER_LABEL + " UserId not found[" + userId + "]");
            return null;
        }
        cacheUsers.put(userId, user);
        return user.getUserName();
    }
}
