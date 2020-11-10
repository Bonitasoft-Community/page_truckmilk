package org.bonitasoft.truckmilk.plugin.cases.movearchive;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.properties.BonitaEngineConnection;
import org.bonitasoft.properties.DatabaseConnection.ConnectionResult;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.plugin.cases.MilkMoveArchiveCases.MoveParameters;
import org.bonitasoft.truckmilk.plugin.cases.MilkMoveArchiveCases.POLICY_USER;
import org.bonitasoft.truckmilk.toolbox.DatabaseTables;
import org.bonitasoft.truckmilk.toolbox.PlaceHolder;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

public class MoveBonita extends MoveInt {

    private final static String LOGGER_LABEL = "MilkMoveArchive.MoveBonita";
    private final static Logger logger = Logger.getLogger(MoveBonita.class.getName());

    protected final static BEvent eventCantMoveProcessInstance = new BEvent(MoveBonita.class.getName(), 1, Level.ERROR,
            "Can't move process Instance", "An error arrive during the moving of a process instance", "Case can't be moved", "Check error");
    protected final static BEvent eventProcessDoesNotExist = new BEvent(MoveBonita.class.getName(), 2, Level.APPLICATIONERROR,
            "Process does not exist", "To move a case, the process definition must be loaded in the target Bonita Database. One process (name+version) or subprocess is missing", "Case can't be moved", "Check error");
    
    protected final static BEvent eventUserDoesNotExist = new BEvent(MoveBonita.class.getName(), 3, Level.APPLICATIONERROR,
            "User does not exist", "To move a case, all users involved in a case must exist in the target Bonita Database.", "Case can't be moved", "Check error");
    
    private static final String CST_ARCHPROCESSINSTANCE = "ARCH_PROCESS_INSTANCE";
    private static final String CST_ARCHPROCESSINSTANCE_SOURCEOBJECTID = "sourceobjectid";
    private static final String CST_ARCHPROCESSINSTANCE_TENANTID = "tenantid";

    private static final String CST_ARCHFLOWNODEINSTANCE = "ARCH_FLOWNODE_INSTANCE";
    private static final String CST_ARCHPROCESSCOMMENT = "ARCH_PROCESS_COMMENT";

    private static final String CST_ARCHREFBIZDATAINST = "ARCH_REF_BIZ_DATA_INST";
    private static final String CST_ARCHMULTIBIZDATA = "ARCH_MULTI_BIZ_DATA";
    private static final String CST_DOCUMENT = "DOCUMENT";

    private static final String CST_ARCHDOCUMENTMAPPING = "ARCH_DOCUMENT_MAPPING";
    private static final String CST_ARCHDATAINSTANCE = "ARCH_DATA_INSTANCE";
    private static final String CST_ARCHCONTRACTDATA = "ARCH_CONTRACT_DATA";
    private static final String CST_ARCHCONNECTORINSTANCE = "ARCH_CONNECTOR_INSTANCE";

    private class TableDescription {

        public String tableName;
        public String sqlRequest;

        public TableDescription(String tableName, String sqlRequest) {
            this.tableName = tableName;
            this.sqlRequest = sqlRequest;
        }

        public String getSqlRequest() {
            return sqlRequest;
        }

    };

    private List<TableDescription> listTableDescription = Arrays.asList(
            new TableDescription(CST_ARCHPROCESSINSTANCE,
                    "select " + CST_ARCHPROCESSINSTANCE + ".*, "
                            + "  PROCESS_DEFINITION.NAME as PROCESSDEFINITION_PROCESSNAME_PROCESSDEFINITIONID, PROCESS_DEFINITION.VERSION as PROCESSDEFINITION_PROCESSVERSION, "
                            + "  userstartby.username as USER_USERNAME_STARTEDBY, "
                            + "  usersubstituteby.username as USER_USERNAME_STARTEDBYSUBSTITUTE"
                            + " from " + CST_ARCHPROCESSINSTANCE
                            + "  left join PROCESS_DEFINITION on ( PROCESS_DEFINITION.PROCESSID = " + CST_ARCHPROCESSINSTANCE + ".PROCESSDEFINITIONID)"
                            + "  left join USER_ userstartby on ( userstartby.ID = " + CST_ARCHPROCESSINSTANCE + ".STARTEDBY)"
                            + "  left join USER_ usersubstituteby on ( usersubstituteby.ID = " + CST_ARCHPROCESSINSTANCE + ".STARTEDBYSUBSTITUTE)"
                            + " where " + CST_ARCHPROCESSINSTANCE + ".ROOTPROCESSINSTANCEID = {{PROCESSINSTANCEID}} and " + CST_ARCHPROCESSINSTANCE + ".TENANTID= {{TENANTID}}"),
            new TableDescription(CST_ARCHFLOWNODEINSTANCE,
                    "select * from " + CST_ARCHFLOWNODEINSTANCE
                            + " where " + CST_ARCHFLOWNODEINSTANCE + ".ROOTCONTAINERID = {{PROCESSINSTANCEID}} "
                            + " and " + CST_ARCHFLOWNODEINSTANCE + ".TENANTID= {{TENANTID}}"),
            new TableDescription(CST_ARCHPROCESSCOMMENT,
                    "select " + CST_ARCHPROCESSCOMMENT + ".* "
                            + " from " + CST_ARCHPROCESSCOMMENT
                            + " where PROCESSINSTANCEID "
                            + "  in (select " + CST_ARCHPROCESSINSTANCE + ".SOURCEOBJECTID "
                            + "    from " + CST_ARCHPROCESSINSTANCE
                            + "    where " + CST_ARCHPROCESSINSTANCE + ".ROOTPROCESSINSTANCEID={{PROCESSINSTANCEID}} "
                            + "     and " + CST_ARCHPROCESSINSTANCE + ".TENANTID={{TENANTID}} )"
                            + " and " + CST_ARCHPROCESSCOMMENT + ".TENANTID={{TENANTID}} "),
            new TableDescription(CST_ARCHREFBIZDATAINST,
                    "select " + CST_ARCHREFBIZDATAINST + ".* "
                            + " from " + CST_ARCHREFBIZDATAINST
                            + " where ORIG_PROC_INST_ID "
                            + "  in (select " + CST_ARCHPROCESSINSTANCE + ".SOURCEOBJECTID "
                            + "    from " + CST_ARCHPROCESSINSTANCE
                            + "    where " + CST_ARCHPROCESSINSTANCE + ".ROOTPROCESSINSTANCEID={{PROCESSINSTANCEID}} "
                            + "     and " + CST_ARCHPROCESSINSTANCE + ".TENANTID={{TENANTID}} )"
                            + " and " + CST_ARCHREFBIZDATAINST + ".TENANTID={{TENANTID}} "),
            new TableDescription(CST_ARCHMULTIBIZDATA,
                    "select " + CST_ARCHMULTIBIZDATA + ".* "
                            + " from " + CST_ARCHMULTIBIZDATA
                            + " where ID "
                            + "   in (select " + CST_ARCHREFBIZDATAINST + ".ID "
                            + "      from " + CST_ARCHREFBIZDATAINST
                            + "     where " + CST_ARCHREFBIZDATAINST + ".ORIG_PROC_INST_ID "
                            + "       in (select " + CST_ARCHPROCESSINSTANCE + ".SOURCEOBJECTID "
                            + "           from " + CST_ARCHPROCESSINSTANCE
                            + "           where " + CST_ARCHPROCESSINSTANCE + ".ROOTPROCESSINSTANCEID={{PROCESSINSTANCEID}} "
                            + "             and " + CST_ARCHPROCESSINSTANCE + ".TENANTID={{TENANTID}} )"
                            + "       and " + CST_ARCHREFBIZDATAINST + ".TENANTID={{TENANTID}}) "
                            + "  and " + CST_ARCHMULTIBIZDATA + ".TENANTID={{TENANTID}} "),
            new TableDescription(CST_DOCUMENT,
                    "select " + CST_DOCUMENT + ".* "
                            + " from " + CST_DOCUMENT
                            + " where ID "
                            + "   in (select " + CST_ARCHDOCUMENTMAPPING + ".documentid "
                            + "       from " + CST_ARCHDOCUMENTMAPPING
                            + "       where PROCESSINSTANCEID "
                            + "          in (select " + CST_ARCHPROCESSINSTANCE + ".SOURCEOBJECTID "
                            + "              from " + CST_ARCHPROCESSINSTANCE
                            + "              where " + CST_ARCHPROCESSINSTANCE + ".ROOTPROCESSINSTANCEID={{PROCESSINSTANCEID}} "
                            + "                and " + CST_ARCHPROCESSINSTANCE + ".TENANTID={{TENANTID}} )"
                            + "          and " + CST_ARCHDOCUMENTMAPPING + ".TENANTID={{TENANTID}}) "
                            + "   and "+CST_DOCUMENT+".TENANTID = {{TENANTID}}"),
            new TableDescription(CST_ARCHDOCUMENTMAPPING,
                    "select " + CST_ARCHDOCUMENTMAPPING + ".* "
                            + " from " + CST_ARCHDOCUMENTMAPPING
                            + " where PROCESSINSTANCEID "
                            + "  in (select " + CST_ARCHPROCESSINSTANCE + ".SOURCEOBJECTID "
                            + "    from " + CST_ARCHPROCESSINSTANCE
                            + "    where " + CST_ARCHPROCESSINSTANCE + ".ROOTPROCESSINSTANCEID={{PROCESSINSTANCEID}} "
                            + "     and " + CST_ARCHPROCESSINSTANCE + ".TENANTID={{TENANTID}} )"
                            + " and " + CST_ARCHDOCUMENTMAPPING + ".TENANTID={{TENANTID}} "),
            new TableDescription(CST_ARCHDATAINSTANCE,
                    "select " + CST_ARCHDATAINSTANCE + ".* "
                            + " from " + CST_ARCHDATAINSTANCE
                            + " where CONTAINERID "
                            + "  in (select " + CST_ARCHPROCESSINSTANCE + ".SOURCEOBJECTID "
                            + "    from " + CST_ARCHPROCESSINSTANCE
                            + "    where " + CST_ARCHPROCESSINSTANCE + ".ROOTPROCESSINSTANCEID={{PROCESSINSTANCEID}} "
                            + "     and " + CST_ARCHPROCESSINSTANCE + ".TENANTID={{TENANTID}} )"
                            + " and " + CST_ARCHDATAINSTANCE + ".CONTAINERTYPE='PROCESS_INSTANCE' "
                            + " and " + CST_ARCHDATAINSTANCE + ".TENANTID={{TENANTID}} "
                            + " union "
                            + "select " + CST_ARCHDATAINSTANCE + ".* "
                            + " from " + CST_ARCHDATAINSTANCE
                            + " where CONTAINERID "
                            + "  in (select " + CST_ARCHFLOWNODEINSTANCE + ".ID "
                            + "    from " + CST_ARCHFLOWNODEINSTANCE
                            + "    where " + CST_ARCHFLOWNODEINSTANCE + ".ROOTCONTAINERID = {{PROCESSINSTANCEID}} "
                            + "      and " + CST_ARCHFLOWNODEINSTANCE + ".TENANTID= {{TENANTID}})"
                            + " and " + CST_ARCHDATAINSTANCE + ".CONTAINERTYPE='ACTIVITY_INSTANCE' "
                            + " and " + CST_ARCHDATAINSTANCE + ".TENANTID={{TENANTID}} "),
            new TableDescription(CST_ARCHCONTRACTDATA,
                    "select " + CST_ARCHCONTRACTDATA + ".* "
                            + " from " + CST_ARCHCONTRACTDATA
                            + " where  "
                            + CST_ARCHCONTRACTDATA + ".KIND='PROCESS' "
                            + "  and "+CST_ARCHCONTRACTDATA + ".SCOPEID "
                            + "   in (select " + CST_ARCHFLOWNODEINSTANCE + ".ID "
                            + "    from " + CST_ARCHFLOWNODEINSTANCE
                            + "    where " + CST_ARCHFLOWNODEINSTANCE + ".ROOTCONTAINERID = {{PROCESSINSTANCEID}} "
                            + "      and " + CST_ARCHFLOWNODEINSTANCE + ".TENANTID= {{TENANTID}})"
                            + " and " + CST_ARCHCONTRACTDATA + ".TENANTID={{TENANTID}} "
                            + "union "
                            + " select " + CST_ARCHCONTRACTDATA + ".* "
                            + " from " + CST_ARCHCONTRACTDATA
                            + " where "
                            + CST_ARCHCONTRACTDATA + ".KIND='TASK' "
                            + " and " + CST_ARCHCONTRACTDATA + ".SCOPEID "
                            + "  in (select " + CST_ARCHFLOWNODEINSTANCE + ".SOURCEOBJECTID "
                            + "    from " + CST_ARCHFLOWNODEINSTANCE
                            + "    where " + CST_ARCHFLOWNODEINSTANCE + ".ROOTCONTAINERID = {{PROCESSINSTANCEID}} "
                            + "      and " + CST_ARCHFLOWNODEINSTANCE + ".TENANTID= {{TENANTID}})"
                            + " and " + CST_ARCHCONTRACTDATA + ".TENANTID={{TENANTID}} "),
            new TableDescription(CST_ARCHCONNECTORINSTANCE, 
                    "select " + CST_ARCHCONNECTORINSTANCE + ".* "
                            + " from " + CST_ARCHCONNECTORINSTANCE
                            + " where  "
                            + CST_ARCHCONNECTORINSTANCE + ".CONTAINERTYPE='flowNode' "
                            + "  and "+CST_ARCHCONNECTORINSTANCE+".CONTAINERID in (select " + CST_ARCHFLOWNODEINSTANCE + ".ID "
                            + "    from " + CST_ARCHFLOWNODEINSTANCE
                            + "    where " + CST_ARCHFLOWNODEINSTANCE + ".ROOTCONTAINERID = {{PROCESSINSTANCEID}} "
                            + "      and " + CST_ARCHFLOWNODEINSTANCE + ".TENANTID= {{TENANTID}})"
                            + " and " + CST_ARCHCONNECTORINSTANCE + ".TENANTID={{TENANTID}} "
                            + "union "
                            + " select " + CST_ARCHCONNECTORINSTANCE + ".* "
                            + " from " + CST_ARCHCONNECTORINSTANCE
                            + " where "
                            + CST_ARCHCONNECTORINSTANCE + ".CONTAINERTYPE='process' "
                            + " and " + CST_ARCHCONNECTORINSTANCE + ".CONTAINERID "
                            + "  in (select " + CST_ARCHFLOWNODEINSTANCE + ".SOURCEOBJECTID "
                            + "    from " + CST_ARCHFLOWNODEINSTANCE
                            + "    where " + CST_ARCHFLOWNODEINSTANCE + ".ROOTCONTAINERID = {{PROCESSINSTANCEID}} "
                            + "      and " + CST_ARCHFLOWNODEINSTANCE + ".TENANTID= {{TENANTID}})"
                            + " and " + CST_ARCHCONNECTORINSTANCE + ".TENANTID={{TENANTID}} ")
                    );

    @Override
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution, ConnectionResult con) {
        return new ArrayList<>();
    }

    @Override
    public ResultMove existProcessInstanceInArchive(long processInstance, long tenantId, Connection con) {
        ResultMove resultMove = new ResultMove();
        String sqlRequest = "select count(*) as numberofcases from "
                + CST_ARCHPROCESSINSTANCE
                + " where " + CST_ARCHPROCESSINSTANCE_TENANTID + "=?"
                + " and " + CST_ARCHPROCESSINSTANCE_SOURCEOBJECTID + " = ?";
        try (PreparedStatement pstmt = con.prepareStatement(sqlRequest.toString())) {
            pstmt.setObject(1, tenantId);
            pstmt.setObject(2, processInstance);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                resultMove.nbProcessInstances = rs.getInt("numberofcases");
            }
            return resultMove; // something strange : a count should have an answer
        } catch (Exception e) {
            logger.severe(LOGGER_LABEL + "Error when executing " + sqlRequest + " " + e.getMessage());
            resultMove.listEvents.add(new BEvent(eventCantExecuteExistProcessInstance, e, "SqlRequest [" + sqlRequest + "] " + e.getMessage()));
            return resultMove;
        }

    }

    @Override
    public List<BEvent> copyToDatabase(MoveParameters moveParameters, ArchivedProcessInstance archivedProcessInstance, APIAccessor apiAccessor, long tenantId, MilkJobOutput milkJobOutput,
            Connection con, Map<Long, User> cacheUsers) {

        DatabaseTables databaseTables = new DatabaseTables();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("PROCESSINSTANCEID", archivedProcessInstance.getSourceObjectId());
        parameters.put("TENANTID", tenantId);
        List<BEvent> listEvents = new ArrayList<>();
        String statusCase="";
        Map<String,Integer> statsPerTable = new HashMap<>();
        for (int i = 0; i < listTableDescription.size(); i++) {
            TableDescription tableDescription = listTableDescription.get(i);
            statsPerTable.put(tableDescription.tableName, Integer.valueOf(0));
        }
        
        try {
            con.setAutoCommit(false);

            Connection bonitaConnection = BonitaEngineConnection.getConnection();
            
            
            // loop on each table description
            for (int i = 0; i < listTableDescription.size(); i++) {
                TableDescription tableDescription = listTableDescription.get(i);
                String sqlRequest = tableDescription.getSqlRequest();
                if (sqlRequest == null)
                    continue;
                String sqlRequestFinal = PlaceHolder.replacePlaceHolder(parameters, sqlRequest);
                List<Map<String, Object>> listRecords = databaseTables.executeSqlRequest(sqlRequestFinal, null, 10000, bonitaConnection);

                for (Map<String, Object> record : listRecords) {
                    // some attribute have to be replaced
                    Map<String, Object> recordToInsert = new HashMap<>();
                    for (String attribut : record.keySet()) {
                        if (attribut.startsWith("PROCESSDEFINITION_PROCESSNAME")) {
                            // search the process definition in the destination database
                            Long pid = searchProcessDefinition((String) record.get(attribut),
                                    (String) record.get("PROCESSDEFINITION_PROCESSVERSION"),
                                    (Long) record.get("TENANTID"), con);
                            if (pid==null) {
                                listEvents.add( new BEvent( eventProcessDoesNotExist, "Process["+record.get(attribut)+"] Version["+record.get("PROCESSDEFINITION_PROCESSVERSION")+"]"));
                                break;
                            }
                            
                            String attributToSave = attribut.substring("PROCESSDEFINITION_PROCESSNAME".length() + 1);
                            recordToInsert.put(attributToSave, pid);
                        } else if (attribut.equals("PROCESSDEFINITION_PROCESSVERSION")) {
                            continue;
                        } else if (attribut.startsWith("USER_USERNAME") ) {
                            Long userNameId =null;
                            String attributToSave = attribut.substring("USER_USERNAME".length() + 1);
                            // attention with user : a NULL user is maybe save with the value "O"..... so keep that value
                             
                            if (record.get(attribut) ==null) {
                                userNameId = TypesCast.getLong( record.get(attributToSave), null);
                            } else {
                                userNameId = searchUserName( (String) record.get(attribut),
                                        (Long) record.get("TENANTID"), con);
                                if (userNameId == null && moveParameters.missingUserPolicy == POLICY_USER.ANONYMOUS)
                                    userNameId= -1L; // this is the Anonymous Bonita User
                                
                                if (userNameId==null) {
                                    listEvents.add( new BEvent( eventUserDoesNotExist, "User["+record.get(attribut)+"]"));
                                    break;
                                }
                            }
                            recordToInsert.put(attributToSave, userNameId);
                        } else  {
                         // Attention, the attribute may be overridden by a previous mechanism, and show again
                            if (! recordToInsert.containsKey(attribut))
                                recordToInsert.put(attribut, record.get(attribut));
                        }
                    }
                    if (BEventFactory.isError(listEvents))
                        break;
                    List<BEvent> listEventsInsert = databaseTables.insert(tableDescription.tableName, recordToInsert, "Insert data RootProcessInstance["+ archivedProcessInstance.getSourceObjectId()+"]", con);
                    listEvents.addAll(listEventsInsert);
                    if (BEventFactory.isError(listEventsInsert))
                        break;
                }
                statsPerTable.put(tableDescription.tableName, listRecords.size());
                if (BEventFactory.isError(listEvents))
                    break;
            }

            if (BEventFactory.isError(listEvents))
                con.rollback();
            else 
                con.commit();
            StringBuffer logSynthetic = new StringBuffer();
            for (BEvent event : listEvents) {
                if (! event.isError())
                    continue;
                logSynthetic.append(event.getTitle()+" "+event.getParameters()+";");
            }
            statusCase= BEventFactory.isError(listEvents) ? "Error:"+logSynthetic.toString():"Moved";
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + " Exception " + e.getMessage() + " at " + exceptionDetails);
            listEvents.add(new BEvent(eventCantMoveProcessInstance, e, "Process Instance[" + archivedProcessInstance.getSourceObjectId() + "] at " + exceptionDetails));
            statusCase="Error";
            try {
                con.rollback();
            } catch (Exception e2) {
            }

        }
        milkJobOutput.addReportTableLine(new Object[] { archivedProcessInstance.getSourceObjectId(),
                statsPerTable.get(CST_ARCHPROCESSINSTANCE),
                statsPerTable.get(CST_ARCHFLOWNODEINSTANCE),
                statsPerTable.get(CST_ARCHDATAINSTANCE), 
                statsPerTable.get(CST_ARCHDOCUMENTMAPPING), 
                statsPerTable.get(CST_ARCHPROCESSCOMMENT), statusCase });

        return listEvents;
    }

    /**
     * get the processDefinition ID
     */
    private Map<String, Long> cacheProcessDefinition = new HashMap<>();

    private Long searchProcessDefinition(String processName, String processVersion, Long tenantId, Connection con) {
        String keyCache = processName + "##" + processVersion;
        if (cacheProcessDefinition.containsKey(keyCache))
            return cacheProcessDefinition.get(keyCache);

        // search then
        DatabaseTables databaseTables = new DatabaseTables();

        List<Object> parameters = new ArrayList<>();
        parameters.add(processName);
        parameters.add(processVersion);
        parameters.add(tenantId);
        try {
            List<Map<String, Object>> listProcessDefinition = databaseTables.executeSqlRequest("select PROCESSID from PROCESS_DEFINITION where NAME=? and VERSION=? and tenantid=?", parameters, 2000, con);
            Long processId = (! listProcessDefinition.isEmpty()) ? TypesCast.getLong(listProcessDefinition.get(0).get("PROCESSID"), null) : null;
            cacheProcessDefinition.put(keyCache, processId);
            return processId;
        } catch (Exception e) {
            logger.severe("Error retring process [" + processName + "] Version[" + processVersion + "] TenantId[" + tenantId + "] : " + e.getMessage());
            return null;
        }

    }

    /**
     * get the processDefinition ID
     */
    private Map<String, Long> cacheUserName = new HashMap<>();

    /**
     * If the user name does not exist, replace it by a -1,which is the "null" value for Bonita
     * @param userName
     * @param tenantId
     * @param con
     * @return
     */
    private Long searchUserName(String userName, Long tenantId, Connection con) {

        if (cacheUserName.containsKey(userName))
            return cacheUserName.get(userName);

        // search then
        DatabaseTables databaseTables = new DatabaseTables();

        List<Object> parameters = new ArrayList<>();
        parameters.add(userName);
        parameters.add(tenantId);
        try {
            List<Map<String, Object>> listUsers = databaseTables.executeSqlRequest("select ID from USER_ where USERNAME=? and tenantid=?", parameters, 2000, con);
            Long processId = (! listUsers.isEmpty()) ? TypesCast.getLong(listUsers.get(0).get("ID"), null) : null;
            cacheProcessDefinition.put(userName, processId);
            return processId;
        } catch (Exception e) {
            logger.severe("Error retring user Name[" + userName + "] TenantId[" + tenantId + "] : " + e.getMessage());
            return null;
        }

    }

}
