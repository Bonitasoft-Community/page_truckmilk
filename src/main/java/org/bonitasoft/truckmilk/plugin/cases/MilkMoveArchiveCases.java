package org.bonitasoft.truckmilk.plugin.cases;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.casedetails.CaseDetailsAPI.LOADCOMMENTS;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessInstanceSearchDescriptor;
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
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
import org.bonitasoft.truckmilk.plugin.cases.movearchive.MoveBonita;
import org.bonitasoft.truckmilk.plugin.cases.movearchive.MoveExternal;
import org.bonitasoft.truckmilk.plugin.cases.movearchive.MoveInt;
import org.bonitasoft.truckmilk.plugin.cases.movearchive.MoveInt.ResultMove;

public class MilkMoveArchiveCases extends MilkPlugIn {

    // private final static String LOGGER_LABEL = "MilkMoveArchive";
    // private final static Logger logger = Logger.getLogger(MilkMoveArchiveCases.class.getName());

    /**
     * Events
     */
    private final static BEvent eventCantConnectDatasource = new BEvent(MilkMoveArchiveCases.class.getName(), 1, Level.ERROR,
            "Can't connect to databasource", "Can't connect to the datasource", "No operation can be perform", "Check the configuration, check the database");

    private static BEvent eventDeletionFailed = new BEvent(MilkMoveArchiveCases.class.getName(), 2, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion of archived cases", "Cases are not deleted", "Check the exception");

    private static BEvent eventOperationFailed = new BEvent(MilkMoveArchiveCases.class.getName(), 3, Level.ERROR,
            "Operation error", "An error arrive during the operation", "Operation failed", "Check the exception");

    private final static String CSTOPERATION_HISTORYARCHIVE = "Archive all History";
    private final static String CSTOPERATION_NOHISTORYARCHIVE = "No history (only last value)";
    private final static String CSTOPERATION_ARCHIVE = "Archive";

    private final static String CSTOPERATION_COPY = "Copy";
    private final static String CSTOPERATION_MOVE = "Move";

    private final static String CSTOPERATION_IGNORE = "Ignore";
    private final static String CSTOPERATION_ARCHIVELIGHT = "Archive light";

    private final static String CSTEXTERNAL_DATABASE = "Database";
    private final static String CSTEXTERNAL_BONITA = "Bonita database";

    
    private final static String CSTMISSINGUSER_ERROR ="Do not create, return an error";
    private final static String CSTMISSINGUSER_ANONYMOUS ="Use an anonymous user";
    
    
    
    public enum POLICY_DATA {
        ARCHIVE, IGNORE
    }

    public enum POLICY_USER { ERROR, ANONYMOUS };
        
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

    private final static PlugInParameter cstParamTypeExternalBase = PlugInParameter.createInstanceListValues("ExternalBase", "External database",
            new String[] { CSTEXTERNAL_DATABASE, CSTEXTERNAL_BONITA }, CSTEXTERNAL_DATABASE, "External database is a Archive data, or a different Bonita Instance. Attention, in the external Bonita Database, the process (name+version), username must exist.");

    private final static PlugInParameter cstParamOperationCase = PlugInParameter.createInstanceListValues("operationCase", "Operation on case",
            new String[] { CSTOPERATION_COPY, CSTOPERATION_MOVE }, CSTOPERATION_MOVE, "Case are move (archived case will be detelete) or copy to the Archive database");

    private final static PlugInParameter cstParamMissingUser = PlugInParameter.createInstanceListValues("missingUser", "Policy for a missing user",
            new String[] { CSTMISSINGUSER_ERROR, CSTMISSINGUSER_ANONYMOUS }, CSTMISSINGUSER_ERROR, "When a user involved in a process, does not exist in the target base, choose the policy")
            .withVisibleConditionParameterValueEqual(cstParamTypeExternalBase, CSTEXTERNAL_BONITA);

    public MilkMoveArchiveCases() {
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
        String datasource = milkJobExecution.getInputStringParameter(cstParamDatasource);
        String externalDatabase = milkJobExecution.getInputStringParameter(cstParamTypeExternalBase);

        DatabaseConnection.ConnectionResult connectionResult = null;
        try {
            connectionResult = DatabaseConnection.getConnection(Arrays.asList(datasource));
            if (connectionResult.con == null) {
                listEvents.add(new BEvent(eventCantConnectDatasource, "Datasource[" + datasource + "]"));
                return listEvents;
            }

            MoveInt moveInt = null;
            if (CSTEXTERNAL_DATABASE.equals(externalDatabase))
                moveInt = new MoveExternal();
            else
                moveInt = new MoveBonita();

            listEvents.addAll(moveInt.checkJobEnvironment(milkJobExecution, connectionResult));

        } catch (Exception e) {

            listEvents.add(new BEvent(eventCantConnectDatasource, e, "Datasource[" + datasource + "], error " + e.getMessage()));

            try {
                connectionResult.con.rollback();
            } catch (SQLException e1) {
            }

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

        plugInDescription.setName("MoveArchiveCases");
        plugInDescription.setExplanation("Move cases to an archive database");
        plugInDescription.setLabel("Move Archive Case");
        plugInDescription.setWarning("This plugin move cases to an external database, and purge them. Use the page XXX to access this information after");
        plugInDescription.setCategory(CATEGORY.CASES);

        plugInDescription.setStopJob(JOBSTOPPER.BOTH);

        plugInDescription.addParameter(cstParamDatasource);
        plugInDescription.addParameter(cstParamTypeExternalBase);
        plugInDescription.addParameter(cstParamOperationCase);
        
        plugInDescription.addParameter(cstParamProcessFilter);
        plugInDescription.addParameter(cstParamDelay);
        plugInDescription.addParameter(cstParamMoveProcessData);
        plugInDescription.addParameter(cstParamMoveLocalData);
        plugInDescription.addParameter(cstParamHistoryProcessData);

        plugInDescription.addParameter(cstParamMoveActivity);
        plugInDescription.addParameter(cstParamMoveDocument);
        plugInDescription.addParameter(cstParamMoveComment);
        plugInDescription.addParameter(cstParamMissingUser);

        return plugInDescription;
    }

    public class MoveParameters {

        public POLICY_DATA moveProcessData;
        public POLICY_DATA moveLocalData;
        public boolean saveHistoryProcessData;
        public boolean moveActivities;
        public boolean moveSynthesisActivity;
        public boolean moveDocuments;
        public LOADCOMMENTS moveComments;
        public POLICY_USER missingUserPolicy;
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
        moveParameters.moveComments = CSTOPERATION_ARCHIVE.equals(milkJobExecution.getInputStringParameter(cstParamMoveComment)) ? LOADCOMMENTS.ONLYUSERS : LOADCOMMENTS.NONE;

        moveParameters.missingUserPolicy = POLICY_USER.ERROR;
        if (CSTMISSINGUSER_ANONYMOUS.equals(milkJobExecution.getInputStringParameter(cstParamMissingUser)))
            moveParameters.missingUserPolicy = POLICY_USER.ANONYMOUS;
                
                
        String operationCase = milkJobExecution.getInputStringParameter(cstParamOperationCase);

        String externalDatabase = milkJobExecution.getInputStringParameter(cstParamTypeExternalBase);
        MoveInt moveInt = null;
        if (CSTEXTERNAL_DATABASE.equals(externalDatabase))
            moveInt = new MoveExternal();
        else
            moveInt = new MoveBonita();

        DatabaseConnection.ConnectionResult connectionResult = null;
        try {
            connectionResult = DatabaseConnection.getConnection(Arrays.asList(datasource));

            connectionResult.con.setAutoCommit(false);

            // Parameters
            ListProcessesResult listProcessResult = milkJobExecution.getInputArrayProcess(cstParamProcessFilter, false, searchBuilderCase, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                milkJobOutput.addEvents(listProcessResult.listEvents);
                milkJobOutput.setExecutionStatus( ExecutionStatus.BADCONFIGURATION );
                return milkJobOutput;
            }
            DelayResult delayResult = milkJobExecution.getInputDelayParameter(cstParamDelay, new Date(), false);
            if (BEventFactory.isError(delayResult.listEvents)) {
                milkJobOutput.addEvents(delayResult.listEvents);
                milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );
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
            milkJobExecution.setAvancementTotalStep(searchProcessInstance.getCount() + 10);
            milkJobExecution.setAvancementStep(10);

            //---------------- operation
            milkJobOutput.addReportTableBegin(new String[] { "CaseId", "SubProcess", "Activity", "Datas", "Documents", "Comments", "Status" });
            Map<Long, User> cacheUsers = new HashMap<>();

            List<Long> listArchivedProcessInstances = new ArrayList<>();
            for (ArchivedProcessInstance archivedProcessInstance : searchProcessInstance.getResult()) {
                if (milkJobExecution.isStopRequired())
                    break;
                milkJobExecution.setAvancementStepPlusOne();

                // operation copy? Maybe the case is already copied - this is the same for MOVE, test it 
                ResultMove resultMove = moveInt.existProcessInstanceInArchive(archivedProcessInstance.getSourceObjectId(), milkJobExecution.getTenantId(), connectionResult.con);
                milkJobOutput.addEvents(resultMove.listEvents);
                // already exist
                if (resultMove.nbProcessInstances > 0)
                    continue;

                // move the data to the another database
                List<BEvent> listEvents = moveInt.copyToDatabase(moveParameters, archivedProcessInstance, milkJobExecution.getApiAccessor(), milkJobExecution.getTenantId(), milkJobOutput, connectionResult.con, cacheUsers);
                milkJobOutput.addEvents(listEvents);

                if (CSTOPERATION_MOVE.equals(operationCase)) {
                    if (!BEventFactory.isError(listEvents)) {
                        // delete the case Id
                        listArchivedProcessInstances.add(archivedProcessInstance.getSourceObjectId());
                    }
                  
                }
                                  
                if (! BEventFactory.isError(listEvents)) {
                    milkJobOutput.nbItemsProcessed++;
                }

                if (listArchivedProcessInstances.size() >= 50) {
                    try {
                        Chronometer processInstanceMarker = milkJobOutput.beginChronometer("deleteProcessInstance");
                        processAPI.deleteArchivedProcessInstancesInAllStates(listArchivedProcessInstances);
                        listArchivedProcessInstances.clear();
                        milkJobOutput.endChronometer(processInstanceMarker, listArchivedProcessInstances.size());
                    } catch (Exception e) {
                        milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "ListArchiveId: " + listArchivedProcessInstances.toString() + " " + e.getMessage()));
                        milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );
                        listArchivedProcessInstances.clear(); // do not try to purge them twice at output
                        break;
                    }
                }
            }
            milkJobOutput.addReportTableEnd();

            // purge the last list
            if (!listArchivedProcessInstances.isEmpty()) {
                try {
                    Chronometer processInstanceMarker = milkJobOutput.beginChronometer("deleteProcessInstance");
                    processAPI.deleteArchivedProcessInstancesInAllStates(listArchivedProcessInstances);
                    milkJobOutput.endChronometer(processInstanceMarker, listArchivedProcessInstances.size());
                } catch (Exception e) {
                    milkJobOutput.addEvent(new BEvent(eventDeletionFailed, e, "ListArchiveId: " + listArchivedProcessInstances.toString() + " " + e.getMessage()));
                    milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );
                }
            }

            milkJobOutput.addChronometersInReport(true, true);

            if (BEventFactory.isError(milkJobOutput.getListEvents())) {
                milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );
            } else {
                milkJobOutput.setExecutionStatus( ExecutionStatus.SUCCESS );
                if (milkJobExecution.isStopRequired())
                    milkJobOutput.setExecutionStatus( ExecutionStatus.SUCCESSPARTIAL );
            }

        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventOperationFailed, e, ""));
            milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );

        } finally {
            if (connectionResult != null && connectionResult.con != null)
                try {
                    connectionResult.con.close();
                } catch (Exception e) {
                }
        }
        return milkJobOutput;
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Private */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

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
        buildTheSQLRequest.append(" archivedate <= " + delayResult.delayDate.getTime());

        searchActBuilder.sort(ArchivedProcessInstancesSearchDescriptor.ARCHIVE_DATE, Order.ASC);
        buildTheSQLRequest.append(" order by archivedate asc;<br>");
        return searchActBuilder;
    }

}
