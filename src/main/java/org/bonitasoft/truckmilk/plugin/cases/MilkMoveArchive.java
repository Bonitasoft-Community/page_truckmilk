package org.bonitasoft.truckmilk.plugin.cases;

import java.io.ByteArrayInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
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
import org.bonitasoft.casedetails.CaseDetails.CaseDetailDocument;
import org.bonitasoft.casedetails.CaseDetails.CaseDetailFlowNode;
import org.bonitasoft.casedetails.CaseDetails.CaseDetailVariable;
import org.bonitasoft.casedetails.CaseDetails.ProcessInstanceDescription;
import org.bonitasoft.casedetails.CaseDetails.ScopeVariable;
import org.bonitasoft.casedetails.CaseDetailsAPI;
import org.bonitasoft.casedetails.CaseDetailsAPI.CaseHistoryParameter;
import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.document.DocumentNotFoundException;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessDefinitionNotFoundException;
import org.bonitasoft.engine.bpm.process.ProcessInstanceSearchDescriptor;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkJobOutput.Chronometer;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter.FilterProcess;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJobExecution.DelayResult;
import org.bonitasoft.truckmilk.job.MilkJobExecution.ListProcessesResult;
import org.bonitasoft.truckmilk.toolbox.DatabaseTables;
import org.bonitasoft.truckmilk.toolbox.DatabaseTables.COLTYPE;
import org.bonitasoft.truckmilk.toolbox.DatabaseTables.ConnectionResult;
import org.bonitasoft.truckmilk.toolbox.DatabaseTables.DataColumn;
import org.bonitasoft.truckmilk.toolbox.DatabaseTables.DataDefinition;

public class MilkMoveArchive extends MilkPlugIn {

    /**
     * ProcessInstance
     */
    private static final String BDE_TABLE_PROCESSINSTANCE = "Bonita_Process";

    private static final String BDE_PROCESSINSTANCE_STRINGINDEX5 = "StringIndex5";
    private static final String BDE_PROCESSINSTANCE_STRINGINDEX4 = "StringIndex4";
    private static final String BDE_PROCESSINSTANCE_STRINGINDEX3 = "StringIndex3";
    private static final String BDE_PROCESSINSTANCE_STRINGINDEX2 = "StringIndex2";
    private static final String BDE_PROCESSINSTANCE_STRINGINDEX1 = "StringIndex1";
    private static final String BDE_PROCESSINSTANCE_ARCHIVEDATE = "ArchivedDate";
    private static final String BDE_PROCESSINSTANCE_STARTEDBY = "StartedBy";
    private static final String BDE_PROCESSINSTANCE_STARTEDBYSUBSTITUTE = "StartedBySubstitute";
    private static final String BDE_PROCESSINSTANCE_END_DATE = "EndDate";
    private static final String BDE_PROCESSINSTANCE_START_DATE = "StartDate";
    private static final String BDE_PROCESSINSTANCE_PROCESSINSTANCEID = "processinstanceid";
    private static final String BDE_PROCESSINSTANCE_ROOTPROCESSINSTANCEID = "rootprocessinstanceid";
    private static final String BDE_PROCESSINSTANCE_PARENTPROCESSINSTANCEID = "parentprocessinstanceid";
    private static final String BDE_PROCESSINSTANCE_PROCESSDEFINITIONVERSION = "processdefinitionversion";
    private static final String BDE_PROCESSINSTANCE_PROCESSDEFINITIONNAME = "processdefinitionname";
    private static final String BDE_PROCESSINSTANCE_PROCESSDEFINITIONID = "processdefinitionid";
    private static final String BDE_PROCESSINSTANCE_TENANTID = "tenantid";

    /**
     * Datainstance
     */
    private static final String BDE_TABLE_DATAINSTANCE = "Bonita_Data";

    private static final String BDE_DATAINSTANCE_TENANTID = "TENANTID";
    private static final String BDE_DATAINSTANCE_NAME = "NAME";
    private static final String BDE_DATAINSTANCE_SCOPE = "SCOPE";
    private static final String BDE_DATAINSTANCE_ID = "ID";
    private static final String BDE_DATAINSTANCE_DESCRIPTION = "DESCRIPTION";

    private static final String BDE_DATAINSTANCE_PROCESSINSTANCEID = "PROCESSINSTANCEID";
    // variable can be local : in that circunstance, the ACTIVITYID is set
    private static final String BDE_DATAINSTANCE_ACTIVITYID = "ACTIVITYID";

    private static final String BDE_DATAINSTANCE_CONTAINERTYPE = "CONTAINERTYPE";
    private static final String BDE_DATAINSTANCE_ARCHIVEDATE = "ARCHIVEDATE";

    private static final String BDE_DATAINSTANCE_CLASSNAME = "CLASSNAME";
    private static final String BDE_DATAINSTANCE_VALUE = "VALUE";
    private static final String BDE_DATAINSTANCE_FLOATVALUE = "FLOATVALUE";
    private static final String BDE_DATAINSTANCE_DOUBLEVALUE = "DOUBLEVALUE";
    private static final String BDE_DATAINSTANCE_BOOLEANVALUE = "BOOLEANVALUE";
    private static final String BDE_DATAINSTANCE_DATEVALUE = "DATEVALUE";
    private static final String BDE_DATAINSTANCE_LONGVALUE = "LONGVALUE";

    // For a BDM variable. BDM Name is the equivalent of the ClassName     
    private static final String BDE_DATAINSTANCE_BDMNAME = "BDMNAME";
    private static final String BDE_DATAINSTANCE_BDMISMULTIPLE = "BDMISMULTIPLE";
    private static final String BDE_DATAINSTANCE_BDMINDEX = "BDMINDEX";
    private static final String BDE_DATAINSTANCE_BDMPERSISTENCEID = "BDMPERSISTENCEID";

    /**
     * FlowNode
     */
    private static final String BDE_TABLE_FLOWNODEINSTANCE = "Bonita_Flownode";

    private static final String BDE_FLOWNODEINSTANCE_TENANTID = "TENANTID";
    private static final String BDE_FLOWNODEINSTANCE_ID = "ID";
    private static final String BDE_FLOWNODEINSTANCE_FLOWNODEDEFINITIONID = "FLOWNODEFINITIONID";
    private static final String BDE_FLOWNODEINSTANCE_KIND = "KIND";
    private static final String BDE_FLOWNODEINSTANCE_ARCHIVEDATE = "ARCHIVEDATE";
    private static final String BDE_FLOWNODEINSTANCE_PROCESSINSTANCEID = "PROCESSINSTANCEID";
    private static final String BDE_FLOWNODEINSTANCE_PARENTCONTAINERID = "PARENTCONTAINERID";
    private static final String BDE_FLOWNODEINSTANCE_SOURCEOBJECTID = "SOURCEOBJECTID";
    private static final String BDE_FLOWNODEINSTANCE_NAME = "NAME";
    private static final String BDE_FLOWNODEINSTANCE_DISPLAYNAME = "DISPLAYNAME";
    private static final String BDE_FLOWNODEINSTANCE_STATENAME = "STATENAME";
    private static final String BDE_FLOWNODEINSTANCE_REACHEDSTATEDATE = "REACHEDSTATEDATE";

    private static final String BDE_FLOWNODEINSTANCE_GATEWAYTYPE = "GATEWAYTYPE";
    private static final String BDE_FLOWNODEINSTANCE_LOOP_COUNTER = "LOOPCOUNTER";
    private static final String BDE_FLOWNODEINSTANCE_NUMBEROFINSTANCES = "NUMBEROFINSTANCES";
    // LOOP_MAX
    // LOOPCARDINALITY
    // LOOPDATAINPUTREF
    // LOOPDATAOUTPUTREF
    // DESCRIPTION
    // SEQUENTIAL
    //DATAINPUTITEMREF
    //DATAOUTPUTITEMREF
    // NBACTIVEINST
    // NBCOMPLETEDINST
    // NBTERMINATEDINST
    private static final String BDE_FLOWNODEINSTANCE_EXECUTEDBY = "EXECUTEDBY";
    private static final String BDE_FLOWNODEINSTANCE_EXECUTEDBYSUBSTITUTE = "EXECUTEDBYSUBSTITUTE";
    // private static final String BDE_FLOWNODEINSTANCE_ACTIVITYINSTANCEID
    // ABORTING
    // TRIGGEREDBYEVENT
    // INTERRUPTING

    /**
     * Document
     */
    private static final String BDE_TABLE_DOCUMENT = "Bonita_Document";

    private static final String BDE_DOCUMENTINSTANCE_TENANTID = "TENANTID";
    private static final String BDE_DOCUMENTINSTANCE_ID = "ID";
    private static final String BDE_DOCUMENTINSTANCE_NAME = "NAME";
    private static final String BDE_DOCUMENTINSTANCE_PROCESSINSTANCEID = "PROCESSINSTANCEID";
    private static final String BDE_DOCUMENTINSTANCE_VERSION = "VERSION";
    private static final String BDE_DOCUMENTINSTANCE_ARCHIVEDATE = "ARCHIVEDATE";

    private static final String BDE_DOCUMENTINSTANCE_INDEX = "DOCINDEX";
    private static final String BDE_DOCUMENTINSTANCE_AUTHOR = "AUTHOR";
    private static final String BDE_DOCUMENTINSTANCE_FILENAME = "FILENAME";
    private static final String BDE_DOCUMENTINSTANCE_MIMETYPE = "MIMETYPE";
    private static final String BDE_DOCUMENTINSTANCE_URL = "URL";
    private static final String BDE_DOCUMENTINSTANCE_HASCONTENT = "HASCONTENT";
    private static final String BDE_DOCUMENTINSTANCE_CONTENT = "CONTENT";

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

    private final static String CSTOPERATION_HISTORYARCHIVE = "Archive all History";
    private final static String CSTOPERATION_NOHISTORYARCHIVE = "No history (only last value)";
    private final static String CSTOPERATION_ARCHIVE = "Archive";

    private final static String CSTOPERATION_IGNORE = "Ignore";
    private final static String CSTOPERATION_ARCHIVELIGHT = "Archive light";
    
    public enum POLICY_DATA { ARCHIVE, IGNORE }; 
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
            new String[] {  CSTOPERATION_ARCHIVE, CSTOPERATION_IGNORE }, CSTOPERATION_ARCHIVE, "Local Data are copied to the Archive database, with the history or not");

    private static PlugInParameter cstParamHistoryProcessData = PlugInParameter.createInstanceListValues("HistoryProcessData",
            "History on process data",
            new String[] {  CSTOPERATION_HISTORYARCHIVE, CSTOPERATION_NOHISTORYARCHIVE }, CSTOPERATION_NOHISTORYARCHIVE, "A process variable value may change in the process. Archive all value (or only the last value)");

    
    private static PlugInParameter cstParamMoveActivity = PlugInParameter.createInstanceListValues("ArchiveActivity",
            "Policy on Process Data",
            new String[] { CSTOPERATION_ARCHIVE, CSTOPERATION_ARCHIVELIGHT, CSTOPERATION_IGNORE }, CSTOPERATION_ARCHIVE, "Activities are copied to the Archive database. Light copied only one record per activity (else 3 to multiple records)");

    private static PlugInParameter cstParamMoveDocument = PlugInParameter.createInstanceListValues("ArchiveDocument",
            "Policy on Document",
            new String[] { CSTOPERATION_ARCHIVE, CSTOPERATION_IGNORE }, CSTOPERATION_ARCHIVE, "Document are copied to the Archive database");

    private final static PlugInParameter cstParamDeleteCase = PlugInParameter.createInstance("deletecase", "Delera Archived Case", TypeParameter.BOOLEAN, Boolean.FALSE, "After move, delete the archived case");

    private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

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
        ConnectionResult connectionResult = null;
        try {
            connectionResult = databaseTable.getConnection(datasource);
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
        plugInDescription.addParameter(cstParamMoveActivity);
        plugInDescription.addParameter(cstParamMoveDocument);

        return plugInDescription;
    }

    private class MoveParameters {

        public POLICY_DATA moveProcessData;
        public POLICY_DATA moveLocalData;
        public boolean saveHistoryProcessData;
        public boolean moveActivity;
        public boolean moveSynthesisActivity;
        public boolean moveDocument;
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

        DatabaseTables databaseTable = new DatabaseTables();
        String datasource = milkJobExecution.getInputStringParameter(cstParamDatasource);

        MoveParameters moveParameters = new MoveParameters();
        moveParameters.moveProcessData =POLICY_DATA.IGNORE;
        if (CSTOPERATION_ARCHIVE.equals(milkJobExecution.getInputStringParameter(cstParamMoveProcessData)))
            moveParameters.moveProcessData =POLICY_DATA.ARCHIVE;
        
        moveParameters.moveLocalData = POLICY_DATA.IGNORE;
        if (CSTOPERATION_ARCHIVE.equals(milkJobExecution.getInputStringParameter(cstParamMoveLocalData)))
            moveParameters.moveLocalData =POLICY_DATA.ARCHIVE;

        moveParameters.saveHistoryProcessData = false;
        if (CSTOPERATION_HISTORYARCHIVE.equals(milkJobExecution.getInputStringParameter(cstParamHistoryProcessData)))
            moveParameters.saveHistoryProcessData =true;

        
        moveParameters.moveActivity = CSTOPERATION_ARCHIVE.equals(milkJobExecution.getInputStringParameter(cstParamMoveActivity)) || CSTOPERATION_ARCHIVELIGHT.equals(milkJobExecution.getInputStringParameter(cstParamMoveActivity));
        moveParameters.moveSynthesisActivity = CSTOPERATION_ARCHIVELIGHT.equals(milkJobExecution.getInputStringParameter(cstParamMoveActivity));
        moveParameters.moveDocument = CSTOPERATION_ARCHIVELIGHT.equals(milkJobExecution.getInputStringParameter(cstParamMoveDocument));

        ConnectionResult connectionResult = null;
        try {
            connectionResult = databaseTable.getConnection(datasource);

            connectionResult.con.setAutoCommit(false);

            // Parameters
            ListProcessesResult listProcessResult =  milkJobExecution.getInputArrayProcess( cstParamProcessFilter, false, searchBuilderCase, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                milkJobOutput.addEvents(listProcessResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return milkJobOutput;
            }
            DelayResult delayResult = milkJobExecution.getInputDelayParameter( cstParamDelay, new Date(), false);
            if (BEventFactory.isError(delayResult.listEvents)) {
                milkJobOutput.addEvents(delayResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                return milkJobOutput;
            }

            boolean deleteCase = milkJobExecution.getInputBooleanParameter(cstParamDeleteCase);

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
            milkJobOutput.addReportTableBegin(new String[] { "CaseId", "SubProcess", "Activity", "Datas", "Documents", "Status" });

            List<Long> listArchivedProcessInstances = new ArrayList<>();
            for (ArchivedProcessInstance archivedProcessInstance : searchProcessInstance.getResult()) {
                if (milkJobExecution.isStopRequired())
                    break;
                // move the data to the another database
                List<BEvent> listEvents = moveDatabase(moveParameters, archivedProcessInstance, milkJobExecution.getApiAccessor(), milkJobExecution.getTenantId(), milkJobOutput, connectionResult.con);
                milkJobOutput.addEvents(listEvents);

                if (deleteCase) {
                    if (!BEventFactory.isError(listEvents)) {
                        // delete the case Id
                        listArchivedProcessInstances.add(archivedProcessInstance.getSourceObjectId());
                    }
                    if (listArchivedProcessInstances.size() >= 50) {
                        try {
                            Chronometer processInstanceMarker = milkJobOutput.beginChronometer("deleteProcessInstance");
                            // Security : do not do that processAPI.deleteArchivedProcessInstancesInAllStates(listArchivedProcessInstances);
                            milkJobOutput.endChronometer(processInstanceMarker, listArchivedProcessInstances.size());
                        } catch (Exception e) {
                            milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "ListArchiveId: " + listArchivedProcessInstances.toString() + " " + e.getMessage()));
                            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
                            break;
                        }
                    }
                }
                milkJobOutput.nbItemsProcessed++;
            }
            milkJobOutput.addReportTableEnd();

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

    private List<BEvent> moveDatabase(MoveParameters moveParameters, ArchivedProcessInstance archivedProcessInstance, APIAccessor apiAccessor, long tenantId, MilkJobOutput milkJobOutput, Connection con) {

        CaseDetailsAPI caseDetailsAPI = new CaseDetailsAPI();
        CaseHistoryParameter caseHistoryParameter = new CaseHistoryParameter();
        caseHistoryParameter.caseId = archivedProcessInstance.getSourceObjectId();
        caseHistoryParameter.tenantId = tenantId;
        caseHistoryParameter.loadSubProcess = true;
        caseHistoryParameter.loadContract = false;
        
        caseHistoryParameter.loadProcessVariables = moveParameters.moveProcessData == POLICY_DATA.ARCHIVE ;
        caseHistoryParameter.loadBdmVariables = moveParameters.moveProcessData == POLICY_DATA.ARCHIVE;
        caseHistoryParameter.loadContentBdmVariables = false;

         
        
        caseHistoryParameter.loadArchivedHistoryProcessVariable = moveParameters.saveHistoryProcessData;
        
        
        caseHistoryParameter.loadActivities = moveParameters.moveActivity;
        caseHistoryParameter.loadEvents = false;
        caseHistoryParameter.loadTimers = false;
        
        
        caseHistoryParameter.loadDocuments = moveParameters.moveDocument;

        List<BEvent> listEvents = new ArrayList<>();
        CaseDetails caseDetails = null;

        try {
            caseHistoryParameter.caseId = 3009L;
            caseDetails = caseDetailsAPI.getCaseDetails(caseHistoryParameter, apiAccessor.getProcessAPI(), apiAccessor.getIdentityAPI(), apiAccessor.getBusinessDataAPI(), null);

            caseHistoryParameter.caseId = archivedProcessInstance.getSourceObjectId();
            caseDetails = caseDetailsAPI.getCaseDetails(caseHistoryParameter, apiAccessor.getProcessAPI(), apiAccessor.getIdentityAPI(), apiAccessor.getBusinessDataAPI(), null);

            int nbActivities = 0;
            int nbDatas = 0;
            int nbDocuments = 0;

            for (ProcessInstanceDescription processInstance : caseDetails.listProcessInstances) {
                ResultMove resultMove = moveDatabaseProcessInstance(moveParameters, processInstance, caseDetails, apiAccessor, tenantId, con);
                listEvents.addAll(resultMove.listEvents);
                nbActivities += resultMove.nbActivities;
                nbDatas += resultMove.nbDatas;
                nbDocuments += resultMove.nbDocuments;
                if (BEventFactory.isError(listEvents))
                    break;
            }
            String statusCase = "";
            if (BEventFactory.isError(listEvents)) {
                statusCase = "Error";
                con.rollback();
            } else {
                statusCase = "Success";
                con.commit();
            }

            milkJobOutput.addReportTableLine(new Object[] { caseHistoryParameter.caseId, caseDetails.listProcessInstances.size(), nbActivities, nbDatas, nbDocuments, statusCase });
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

    }

    /**
     * @param processInstance
     * @param caseDetails
     * @param apiAccessor
     * @param tenantId
     * @param con
     * @return
     */
    private ResultMove moveDatabaseProcessInstance(MoveParameters moveParameters, ProcessInstanceDescription processInstance, CaseDetails caseDetails, APIAccessor apiAccessor, long tenantId, Connection con) {
        ResultMove resultMove = new ResultMove();

        // move first the CaseId
        Map<String, Object> record = new HashMap<>();
        record.put(BDE_PROCESSINSTANCE_TENANTID, caseDetails.tenantId);
        record.put(BDE_PROCESSINSTANCE_PROCESSDEFINITIONID, processInstance.processDefinitionId);
        try {
            ProcessDefinition processDefinition = caseDetails.getCaseDetailsAPI().getProcessDefinition(processInstance.processDefinitionId, apiAccessor.getProcessAPI());
            if (processDefinition != null) {
                record.put(BDE_PROCESSINSTANCE_PROCESSDEFINITIONNAME, processDefinition.getName());
                record.put(BDE_PROCESSINSTANCE_PROCESSDEFINITIONVERSION, processDefinition.getVersion());
            }
        } catch (ProcessDefinitionNotFoundException e) {
            resultMove.listEvents.add(new BEvent(eventProcessDefinitionNotFound, "ProcessDefinition[" + processInstance.processDefinitionId + "] register in case[" + processInstance.processInstanceId + "] TenantId[" + tenantId + "]"));
            return resultMove;
        }
        record.put(BDE_PROCESSINSTANCE_ROOTPROCESSINSTANCEID, processInstance.rootProcessInstanceId);
        record.put(BDE_PROCESSINSTANCE_PARENTPROCESSINSTANCEID, processInstance.parentProcessInstanceId);
        record.put(BDE_PROCESSINSTANCE_PROCESSINSTANCEID, processInstance.processInstanceId);
        record.put(BDE_PROCESSINSTANCE_START_DATE, dateToLong(processInstance.startDate));
        record.put(BDE_PROCESSINSTANCE_END_DATE, dateToLong(processInstance.endDate));
        record.put(BDE_PROCESSINSTANCE_STARTEDBY, processInstance.archProcessInstance.getStartedBy());
        record.put(BDE_PROCESSINSTANCE_STARTEDBYSUBSTITUTE, processInstance.archProcessInstance.getStartedBySubstitute());

        record.put(BDE_PROCESSINSTANCE_ARCHIVEDATE, dateToLong(processInstance.archProcessInstance.getArchiveDate()));
        record.put(BDE_PROCESSINSTANCE_STRINGINDEX1, processInstance.archProcessInstance.getStringIndexValue(1));
        record.put(BDE_PROCESSINSTANCE_STRINGINDEX2, processInstance.archProcessInstance.getStringIndexValue(2));
        record.put(BDE_PROCESSINSTANCE_STRINGINDEX3, processInstance.archProcessInstance.getStringIndexValue(3));
        record.put(BDE_PROCESSINSTANCE_STRINGINDEX4, processInstance.archProcessInstance.getStringIndexValue(4));
        record.put(BDE_PROCESSINSTANCE_STRINGINDEX5, processInstance.archProcessInstance.getStringIndexValue(4));
        String informationContext = "ProcessInstance[" + processInstance.processInstanceId + "]";
        DatabaseTables databaseTables = new DatabaseTables();
        resultMove.listEvents.addAll(databaseTables.insert(BDE_TABLE_PROCESSINSTANCE, record, informationContext, con));
        if (BEventFactory.isError(resultMove.listEvents))
            return resultMove;

        if (moveParameters.moveActivity)
            moveDatabaseFlownodeInstance(moveParameters, processInstance, caseDetails, resultMove, apiAccessor, tenantId, con);

        if (moveParameters.moveProcessData.equals(POLICY_DATA.ARCHIVE) || moveParameters.moveLocalData.equals(POLICY_DATA.ARCHIVE))
            moveDatabaseDataInstance(moveParameters, processInstance, caseDetails, resultMove, apiAccessor, tenantId, con);

        if (moveParameters.moveDocument)
            moveDatabaseDocumentInstance(moveParameters, processInstance, caseDetails, resultMove, apiAccessor, tenantId, con);
        return resultMove;
    }

    private void moveDatabaseFlownodeInstance(MoveParameters moveParameters, ProcessInstanceDescription processInstance, CaseDetails caseDetails, ResultMove resultMove, APIAccessor apiAccessor, long tenantId, Connection con) {
        DatabaseTables databaseTables = new DatabaseTables();
        StringBuilder logAnalysis= new StringBuilder();
        /**
         * if we want to get only one item per flownode, then we have to 1/ order it by the date, 2/ keep the last item for each sourceObjectid
         * Example
         * ParentId  SourceId Type   ArchiveDate     Name             State
         * 3001    60002   auto    1600975938212   ServiceTasks    executing
         * 3001    60002   auto    1600975940900   ServiceTasks    completed        <==== 
         * 3001    60003   user    1600975940949   User task        initializing
         * 3001    60003   user    1600977696921   User task        ready           <====
         * 60004   60008   auto    1600977697064   Instanciate 5   executing
         * 60004   60008   auto    1600977697067   Instanciate 5   completed        <====
         */
        logAnalysis.append("Nb line"+caseDetails.listCaseDetailFlowNodes+";");
        // INVERSE the order 
        Collections.sort(caseDetails.listCaseDetailFlowNodes, new Comparator<CaseDetailFlowNode>()
           {
             public int compare(CaseDetailFlowNode s1,
                     CaseDetailFlowNode s2)
             {
               return s2.getDate().compareTo(s1.getDate());
             }
           });

        // so now we keep only the FIRST one
        Set<Long> registerSourceObjectId = new HashSet<>();
        
        for (CaseDetailFlowNode flowNode : caseDetails.listCaseDetailFlowNodes) {
            if (processInstance.processInstanceId != flowNode.getProcessInstanceId()) {
                continue;
            }
            logAnalysis.append("Source="+flowNode.getSourceObjectId()+";");
            // we works only with ARCHIVE case they have all a sourceObjectId()
            if (moveParameters.moveSynthesisActivity && registerSourceObjectId.contains( flowNode.getSourceObjectId()))
                continue;
            registerSourceObjectId.add( flowNode.getSourceObjectId() );
            logAnalysis.append("KEEP;PID="+flowNode.getProcessInstanceId()+"/Par="+flowNode.getParentContainerId()+"/Sour="+flowNode.getSourceObjectId()+";");;
            
            resultMove.nbActivities++;
            List<Map<String, Object>> listRecords = new ArrayList<>();
            Map<String, Object> record = new HashMap<>();
            listRecords.add(record);
            
            
            record.put(BDE_FLOWNODEINSTANCE_TENANTID, caseDetails.tenantId);
            record.put(BDE_FLOWNODEINSTANCE_ID, flowNode.getId());
            record.put(BDE_FLOWNODEINSTANCE_FLOWNODEDEFINITIONID, flowNode.getFlownodeDefinitionId());
            record.put(BDE_FLOWNODEINSTANCE_KIND, flowNode.getType().toString());
            record.put(BDE_FLOWNODEINSTANCE_ARCHIVEDATE, dateToLong( flowNode.getDate()));
            record.put(BDE_FLOWNODEINSTANCE_PROCESSINSTANCEID, flowNode.getProcessInstanceId());
            record.put(BDE_FLOWNODEINSTANCE_PARENTCONTAINERID, flowNode.getParentContainerId());
            record.put(BDE_FLOWNODEINSTANCE_SOURCEOBJECTID, flowNode.getSourceObjectId());
            record.put(BDE_FLOWNODEINSTANCE_NAME, flowNode.getName());
            record.put(BDE_FLOWNODEINSTANCE_DISPLAYNAME, flowNode.getDisplayName());
            record.put(BDE_FLOWNODEINSTANCE_STATENAME, flowNode.getState());
            record.put(BDE_FLOWNODEINSTANCE_REACHEDSTATEDATE, dateToLong( flowNode.getReachedStateDate()));
            record.put(BDE_FLOWNODEINSTANCE_GATEWAYTYPE, flowNode.getFlowNodeType().toString());
            record.put(BDE_FLOWNODEINSTANCE_LOOP_COUNTER, flowNode.getLoopCounter());
            record.put(BDE_FLOWNODEINSTANCE_NUMBEROFINSTANCES, flowNode.getNumberOfInstances());
            record.put(BDE_FLOWNODEINSTANCE_EXECUTEDBY, flowNode.getExecutedBy());
            record.put(BDE_FLOWNODEINSTANCE_EXECUTEDBYSUBSTITUTE, flowNode.getExecutedBySubstitute());
            String informationContext = "ProcessInstance[" + processInstance.processInstanceId + "] ActivityId ["+flowNode.getId()+"]";
            
            resultMove.listEvents.addAll(databaseTables.insert(BDE_TABLE_FLOWNODEINSTANCE, record, informationContext, con));
            if (BEventFactory.isError(resultMove.listEvents))
                return;
        }
        logger.info(logAnalysis.toString());
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
            record.put(BDE_DATAINSTANCE_TENANTID, caseDetails.tenantId);
            record.put(BDE_DATAINSTANCE_ID, processVariable.id);
            record.put(BDE_DATAINSTANCE_NAME, processVariable.name);

            record.put(BDE_DATAINSTANCE_PROCESSINSTANCEID, processVariable.processInstanceId);
            if (processVariable.scopeVariable == ScopeVariable.LOCAL)
                record.put(BDE_DATAINSTANCE_ACTIVITYID, processVariable.activityId);

            record.put(BDE_DATAINSTANCE_CONTAINERTYPE, processVariable.scopeVariable.toString());
            // a BDM variable does not have a date archived
            record.put(BDE_DATAINSTANCE_ARCHIVEDATE, dateToLong(processVariable.dateArchived));

            // this is a process variable ?
            record.put(BDE_DATAINSTANCE_SCOPE, processVariable.scopeVariable.toString());
            if (processVariable.scopeVariable == ScopeVariable.LOCAL || processVariable.scopeVariable == ScopeVariable.PROCESS) {
                record.put(BDE_DATAINSTANCE_CLASSNAME, processVariable.value == null ? null : processVariable.value.getClass().getName());
                // according the type, use the correct field
                if (processVariable.value instanceof Float)
                    record.put(BDE_DATAINSTANCE_FLOATVALUE, processVariable.value);
                else if (processVariable.value instanceof Double)
                    record.put(BDE_DATAINSTANCE_DOUBLEVALUE, processVariable.value);
                else if (processVariable.value instanceof Boolean)
                    record.put(BDE_DATAINSTANCE_BOOLEANVALUE, processVariable.value);
                else if (processVariable.value instanceof Date)
                    record.put(BDE_DATAINSTANCE_DATEVALUE, dateToLong((Date) processVariable.value));
                else if (processVariable.value instanceof Long || processVariable.value instanceof Integer)
                    record.put(BDE_DATAINSTANCE_LONGVALUE, processVariable.value);
                else
                    record.put(BDE_DATAINSTANCE_VALUE, processVariable.value == null ? null : processVariable.value.toString());
            }

            else if (processVariable.scopeVariable == ScopeVariable.BDM) {
                record.put(BDE_DATAINSTANCE_BDMNAME, processVariable.bdmName);
                record.put(BDE_DATAINSTANCE_BDMISMULTIPLE, processVariable.bdmIsMultiple);
                record.put(BDE_DATAINSTANCE_BDMINDEX, 0);
                // value may be null
                if (!processVariable.listPersistenceId.isEmpty())
                    record.put(BDE_DATAINSTANCE_BDMPERSISTENCEID, processVariable.listPersistenceId.get(0));

                // then create one record per item in the list
                for (int i = 1; i < processVariable.listPersistenceId.size(); i++) {
                    Map<String, Object> duplicateRecord = new HashMap<>();
                    duplicateRecord.putAll(record);
                    duplicateRecord.put(BDE_DATAINSTANCE_BDMINDEX, i);
                    duplicateRecord.put(BDE_DATAINSTANCE_BDMPERSISTENCEID, processVariable.listPersistenceId.get(i));
                    listRecords.add(duplicateRecord);
                }

            }

            for (Map<String, Object> recordInList : listRecords) {
                String informationContext = "ProcessInstance[" + processInstance.processInstanceId + "] ";
                if (record.get(BDE_DATAINSTANCE_NAME) != null)
                    informationContext += "ProcessVariable[" + record.get(BDE_DATAINSTANCE_NAME) + "]";
                if (record.get(BDE_DOCUMENTINSTANCE_NAME) != null)
                    informationContext += "BDM[" + record.get(BDE_DOCUMENTINSTANCE_NAME) + "] index[" + BDE_DOCUMENTINSTANCE_INDEX + "]";

                resultMove.listEvents.addAll(databaseTables.insert(BDE_TABLE_DATAINSTANCE, recordInList, informationContext, con));
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
                record.put(BDE_DOCUMENTINSTANCE_TENANTID, caseDetails.tenantId);

                record.put(BDE_DOCUMENTINSTANCE_ID, documentVariable.document.getId());

                record.put(BDE_DOCUMENTINSTANCE_NAME, documentVariable.document.getName());
                record.put(BDE_DOCUMENTINSTANCE_PROCESSINSTANCEID, documentVariable.processInstanceId);
                record.put(BDE_DOCUMENTINSTANCE_VERSION, documentVariable.document.getVersion());
                // the archived date does not exist at this moment in Bonita
                record.put(BDE_DOCUMENTINSTANCE_ARCHIVEDATE, null);

                record.put(BDE_DOCUMENTINSTANCE_INDEX, documentVariable.document.getIndex());
                record.put(BDE_DOCUMENTINSTANCE_AUTHOR, documentVariable.document.getAuthor());
                record.put(BDE_DOCUMENTINSTANCE_FILENAME, documentVariable.document.getContentFileName());
                record.put(BDE_DOCUMENTINSTANCE_MIMETYPE, documentVariable.document.getContentMimeType());
                record.put(BDE_DOCUMENTINSTANCE_URL, documentVariable.document.getUrl());
                record.put(BDE_DOCUMENTINSTANCE_HASCONTENT, documentVariable.document.hasContent());
                byte[] contentDocument = processAPI.getDocumentContent(documentVariable.document.getContentStorageId());
                if (contentDocument != null)
                    record.put(BDE_DOCUMENTINSTANCE_CONTENT, new ByteArrayInputStream(contentDocument));

                for (Map<String, Object> recordInList : listRecords) {
                    String informationContext = "ProcessInstance[" + processInstance.processInstanceId + "], Document[" + record.get(BDE_DOCUMENTINSTANCE_NAME);

                    resultMove.listEvents.addAll(databaseTables.insert(BDE_TABLE_DOCUMENT, recordInList, informationContext, con));
                    if (BEventFactory.isError(resultMove.listEvents))
                        return;
                }
            } catch (DocumentNotFoundException e) {
                resultMove.listEvents.add(new BEvent(eventCantAccessDocument, e, "ProcessId[" + documentVariable.processInstanceId + "] DocumentName[" + documentVariable.document.getName() + "]"));
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
        DataDefinition caseTable = new DataDefinition(BDE_TABLE_PROCESSINSTANCE);

        caseTable.listColumns = Arrays.asList(new DataColumn(BDE_PROCESSINSTANCE_TENANTID, COLTYPE.LONG),
                new DataColumn(BDE_PROCESSINSTANCE_PROCESSDEFINITIONID, COLTYPE.LONG),
                new DataColumn(BDE_PROCESSINSTANCE_PROCESSDEFINITIONNAME, COLTYPE.STRING, 150),
                new DataColumn(BDE_PROCESSINSTANCE_PROCESSDEFINITIONVERSION, COLTYPE.STRING, 50),
                new DataColumn(BDE_PROCESSINSTANCE_ROOTPROCESSINSTANCEID, COLTYPE.LONG),
                new DataColumn(BDE_PROCESSINSTANCE_PARENTPROCESSINSTANCEID, COLTYPE.LONG),
                new DataColumn(BDE_PROCESSINSTANCE_PROCESSINSTANCEID, COLTYPE.LONG),
                new DataColumn(BDE_PROCESSINSTANCE_START_DATE, COLTYPE.LONG),
                new DataColumn(BDE_PROCESSINSTANCE_END_DATE, COLTYPE.LONG),
                new DataColumn(BDE_PROCESSINSTANCE_STARTEDBY, COLTYPE.LONG),
                new DataColumn(BDE_PROCESSINSTANCE_STARTEDBYSUBSTITUTE, COLTYPE.LONG),
                new DataColumn(BDE_PROCESSINSTANCE_ARCHIVEDATE, COLTYPE.LONG),
                new DataColumn(BDE_PROCESSINSTANCE_STRINGINDEX1, COLTYPE.STRING, 255),
                new DataColumn(BDE_PROCESSINSTANCE_STRINGINDEX2, COLTYPE.STRING, 255),
                new DataColumn(BDE_PROCESSINSTANCE_STRINGINDEX3, COLTYPE.STRING, 255),
                new DataColumn(BDE_PROCESSINSTANCE_STRINGINDEX4, COLTYPE.STRING, 255),
                new DataColumn(BDE_PROCESSINSTANCE_STRINGINDEX5, COLTYPE.STRING, 255));
        caseTable.mapIndexes.put("PI_PDEF_IDX", Arrays.asList(BDE_PROCESSINSTANCE_TENANTID, BDE_PROCESSINSTANCE_PROCESSDEFINITIONID));
        caseTable.mapIndexes.put("PI_ST1_IDX", Arrays.asList(BDE_PROCESSINSTANCE_TENANTID, BDE_PROCESSINSTANCE_STRINGINDEX1));
        caseTable.mapIndexes.put("PI_ST2_IDX", Arrays.asList(BDE_PROCESSINSTANCE_TENANTID, BDE_PROCESSINSTANCE_STRINGINDEX2));
        caseTable.mapIndexes.put("PI_ST3_IDX", Arrays.asList(BDE_PROCESSINSTANCE_TENANTID, BDE_PROCESSINSTANCE_STRINGINDEX3));
        caseTable.mapIndexes.put("PI_ST4_IDX", Arrays.asList(BDE_PROCESSINSTANCE_TENANTID, BDE_PROCESSINSTANCE_STRINGINDEX4));
        caseTable.mapIndexes.put("PI_ST5_IDX", Arrays.asList(BDE_PROCESSINSTANCE_TENANTID, BDE_PROCESSINSTANCE_STRINGINDEX5));

        caseTable.mapConstraints.put("PI_PID_CONST", Arrays.asList(BDE_PROCESSINSTANCE_TENANTID, BDE_PROCESSINSTANCE_PROCESSINSTANCEID));
        listTables.add(caseTable);

        // Data
        // Case
        DataDefinition dataTable = new DataDefinition(BDE_TABLE_DATAINSTANCE);

        dataTable.listColumns = Arrays.asList(new DataColumn(BDE_DATAINSTANCE_TENANTID, COLTYPE.LONG),
                new DataColumn(BDE_DATAINSTANCE_ID, COLTYPE.LONG),
                new DataColumn(BDE_DATAINSTANCE_NAME, COLTYPE.STRING, 50),
                new DataColumn(BDE_DATAINSTANCE_SCOPE, COLTYPE.STRING, 10),

                new DataColumn(BDE_DATAINSTANCE_DESCRIPTION, COLTYPE.STRING, 50),

                new DataColumn(BDE_DATAINSTANCE_PROCESSINSTANCEID, COLTYPE.LONG),
                new DataColumn(BDE_DATAINSTANCE_ACTIVITYID, COLTYPE.LONG),
                new DataColumn(BDE_DATAINSTANCE_CONTAINERTYPE, COLTYPE.STRING, 60),

                /** Process Variable */
                new DataColumn(BDE_DATAINSTANCE_CLASSNAME, COLTYPE.STRING, 100),
                new DataColumn(BDE_DATAINSTANCE_VALUE, COLTYPE.TEXT),
                new DataColumn(BDE_DATAINSTANCE_LONGVALUE, COLTYPE.LONG),
                new DataColumn(BDE_DATAINSTANCE_DATEVALUE, COLTYPE.LONG),
                new DataColumn(BDE_DATAINSTANCE_BOOLEANVALUE, COLTYPE.BOOLEAN),
                new DataColumn(BDE_DATAINSTANCE_DOUBLEVALUE, COLTYPE.DECIMAL),
                new DataColumn(BDE_DATAINSTANCE_FLOATVALUE, COLTYPE.DECIMAL),
                // new DataColumn("CLOBVALUE", COLTYPE.BLOB),
                // new DataColumn("DISCRIMINANT", COLTYPE.DECIMAL),
                new DataColumn(BDE_DATAINSTANCE_BDMNAME, COLTYPE.STRING, 255),
                new DataColumn(BDE_DATAINSTANCE_BDMISMULTIPLE, COLTYPE.BOOLEAN),
                new DataColumn(BDE_DATAINSTANCE_BDMINDEX, COLTYPE.LONG),
                new DataColumn(BDE_DATAINSTANCE_BDMPERSISTENCEID, COLTYPE.LONG),
                new DataColumn(BDE_DATAINSTANCE_ARCHIVEDATE, COLTYPE.LONG));
        dataTable.mapIndexes.put("DI_PID_IDX", Arrays.asList(BDE_DATAINSTANCE_TENANTID, BDE_DATAINSTANCE_PROCESSINSTANCEID, BDE_DATAINSTANCE_ACTIVITYID));
        listTables.add(dataTable);

        // Case
        DataDefinition docTable = new DataDefinition(BDE_TABLE_DOCUMENT);

        docTable.listColumns = Arrays.asList(new DataColumn(BDE_DOCUMENTINSTANCE_TENANTID, COLTYPE.LONG),
                new DataColumn(BDE_DOCUMENTINSTANCE_ID, COLTYPE.LONG),

                new DataColumn(BDE_DOCUMENTINSTANCE_NAME, COLTYPE.STRING, 50),
                new DataColumn(BDE_DOCUMENTINSTANCE_PROCESSINSTANCEID, COLTYPE.LONG),
                new DataColumn(BDE_DOCUMENTINSTANCE_VERSION, COLTYPE.STRING, 50),
                new DataColumn(BDE_DOCUMENTINSTANCE_ARCHIVEDATE, COLTYPE.LONG),

                new DataColumn(BDE_DOCUMENTINSTANCE_INDEX, COLTYPE.LONG),
                new DataColumn(BDE_DOCUMENTINSTANCE_AUTHOR, COLTYPE.LONG),
                new DataColumn(BDE_DOCUMENTINSTANCE_FILENAME, COLTYPE.STRING, 255),
                new DataColumn(BDE_DOCUMENTINSTANCE_MIMETYPE, COLTYPE.STRING, 255),
                new DataColumn(BDE_DOCUMENTINSTANCE_URL, COLTYPE.STRING, 1024),
                new DataColumn(BDE_DOCUMENTINSTANCE_HASCONTENT, COLTYPE.BOOLEAN),
                new DataColumn(BDE_DOCUMENTINSTANCE_CONTENT, COLTYPE.BLOB));
        docTable.mapIndexes.put("DO_PID_IDX", Arrays.asList(BDE_DOCUMENTINSTANCE_TENANTID, BDE_DOCUMENTINSTANCE_ID));
        listTables.add(docTable);

        // Activity
        DataDefinition flowNodeTable = new DataDefinition(BDE_TABLE_FLOWNODEINSTANCE);

        flowNodeTable.listColumns = Arrays.asList(new DataColumn( BDE_FLOWNODEINSTANCE_TENANTID, COLTYPE.LONG),
                new DataColumn( BDE_FLOWNODEINSTANCE_ID, COLTYPE.LONG),
                new DataColumn( BDE_FLOWNODEINSTANCE_FLOWNODEDEFINITIONID, COLTYPE.LONG),
                new DataColumn( BDE_FLOWNODEINSTANCE_KIND, COLTYPE.STRING, 25),
                new DataColumn( BDE_FLOWNODEINSTANCE_ARCHIVEDATE, COLTYPE.LONG),
                new DataColumn( BDE_FLOWNODEINSTANCE_PROCESSINSTANCEID, COLTYPE.LONG),
                new DataColumn( BDE_FLOWNODEINSTANCE_PARENTCONTAINERID, COLTYPE.LONG),
                new DataColumn( BDE_FLOWNODEINSTANCE_SOURCEOBJECTID, COLTYPE.LONG),
                new DataColumn( BDE_FLOWNODEINSTANCE_NAME, COLTYPE.STRING, 255),
                new DataColumn( BDE_FLOWNODEINSTANCE_DISPLAYNAME, COLTYPE.STRING, 255),
                new DataColumn( BDE_FLOWNODEINSTANCE_STATENAME, COLTYPE.STRING, 50),
                new DataColumn( BDE_FLOWNODEINSTANCE_REACHEDSTATEDATE, COLTYPE.LONG),
                new DataColumn( BDE_FLOWNODEINSTANCE_GATEWAYTYPE, COLTYPE.STRING, 50),
                new DataColumn( BDE_FLOWNODEINSTANCE_LOOP_COUNTER, COLTYPE.LONG),
                new DataColumn( BDE_FLOWNODEINSTANCE_NUMBEROFINSTANCES, COLTYPE.LONG),
                new DataColumn( BDE_FLOWNODEINSTANCE_EXECUTEDBY, COLTYPE.LONG),
                new DataColumn( BDE_FLOWNODEINSTANCE_EXECUTEDBYSUBSTITUTE, COLTYPE.LONG));
        flowNodeTable.mapIndexes.put("FL_PID_IDX", Arrays.asList( BDE_FLOWNODEINSTANCE_TENANTID, BDE_FLOWNODEINSTANCE_ID, BDE_FLOWNODEINSTANCE_PROCESSINSTANCEID, BDE_FLOWNODEINSTANCE_PARENTCONTAINERID));
        listTables.add(flowNodeTable);

        return listTables;
    }

    private Long dateToLong(Date date) {
        if (date == null)
            return null;
        return date.getTime();
    }
}
