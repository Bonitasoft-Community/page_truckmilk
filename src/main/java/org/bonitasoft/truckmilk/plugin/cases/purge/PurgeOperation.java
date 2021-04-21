/* ******************************************************************************** */
/*                                                                                  */
/* PurgeOperation */
/* This class is the basic for all purges operation (list or direct) */
/*                                                                                  */
/*                                                                                  */
/* ******************************************************************************** */
package org.bonitasoft.truckmilk.plugin.cases.purge;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.properties.BonitaEngineConnection;
import org.bonitasoft.radar.archive.RadarCleanArchivedDross;
import org.bonitasoft.radar.archive.RadarCleanArchivedDross.DrossExecution;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJobExecution.ListProcessesResult;
import org.bonitasoft.truckmilk.plugin.cases.MilkPurgeArchivedCases;

import lombok.Data;

public class PurgeOperation {

    static Logger logger = Logger.getLogger(PurgeOperation.class.getName());
    private static final String LOGGER_LABEL = "MilkPurgeArchive.PurgeOperation ";

    private static final BEvent eventErrorExecutionQuery = new BEvent(PurgeOperation.class.getName(), 8,
            Level.ERROR,
            "Error during the SqlQuery", "The SQL Query to detect a stuck flow node failed", "No stick flow nodes can be detected",
            "Check exception");

    private static BEvent eventDeletionFailed = new BEvent(PurgeOperation.class.getName(), 5, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion of archived cases", "Cases are not deleted", "Check the exception");

    /**
     * methid processAPI.deleteArchivedProcessInstancesInAllStates(sourceProcessInstanceIds) is very long, even if there are nothing to purge
     * so, let's first search the real number to purge, and do the purge only on real case.
     * 
     * @return
     */
    public static @Data class Statistic {

        long pleaseStopAfterManagedItems = 0;
        long countIgnored = 0;
        long countAlreadyDone = 0;
        long countBadDefinition = 0;
        long countStillToAnalyse = 0;
        long countNbItems = 0;
        long totalLineCsv = 0;
        long sumTimeSearch = 0;
        long sumTimeDeleted = 0;
        long sumTimeManipulateCsv = 0;
        long nbCasesDeleted = 0;
    }

    public static class ManagePurgeResult {

        public String sqlQuery;
        public long nbRecords = 0;
        public List<BEvent> listEvents = new ArrayList<>();
    }
    /**
     * DeleteArchivedprocessinstance.
     * According the policy, a total or a partial purge can be executed.
     * - total : the API is called
     * - partial : delete only Contract, or DataInstance for example.
     * 
     * @param sourceProcessInstanceIds : list of ID, 50 max in this list
     * @param milkJobExecution
     * @param milkJobOutput
     * @param processAPI
     * @return
     */
    public static ManagePurgeResult deleteArchivedProcessInstance(List<Long> sourceProcessInstanceIds, MilkJobExecution milkJobExecution, MilkJobOutput milkJobOutput, ProcessAPI processAPI) {
        ManagePurgeResult managePurgeResult = new ManagePurgeResult();
        if (sourceProcessInstanceIds.isEmpty())
            return managePurgeResult;
        
        boolean allowPurgeActivities=true;
        String typePurge = milkJobExecution.getInputStringParameter(MilkPurgeArchivedCases.cstParamTypePurge);
        if (!MilkPurgeArchivedCases.CSTTYPEPURGE_PARTIALPURGE.equals(typePurge)) {
            try {
                managePurgeResult.nbRecords= processAPI.deleteArchivedProcessInstancesInAllStates(sourceProcessInstanceIds);
            } catch (Exception e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String exceptionDetails = sw.toString();
                logger.severe(LOGGER_LABEL + ".fromList: Error Delete Archived ProcessInstance=[" + sourceProcessInstanceIds + "] Error[" + e.getMessage() + "] at " + exceptionDetails);

                managePurgeResult.listEvents.add(new BEvent(eventDeletionFailed, e, "Purge:" + sourceProcessInstanceIds));
                return managePurgeResult;
            }
        }
        /**
         * build the IN clause
         */
        StringBuilder inClause = new StringBuilder();
        for (Long id : sourceProcessInstanceIds) {
            if (inClause.length() > 0) {
                inClause.append(",");
            }
            inClause.append("?");
        }
        List<Long> doubleSourceProcessInstanceIds = new ArrayList<>();
        doubleSourceProcessInstanceIds.addAll(sourceProcessInstanceIds);
        doubleSourceProcessInstanceIds.addAll(sourceProcessInstanceIds);

        // now purge item per item 
        if (MilkPurgeArchivedCases.CST_YES.equals(milkJobExecution.getInputStringParameter(MilkPurgeArchivedCases.cstParamPurgeContract))) {
            managePurgeResult.nbRecords+= executeSql("delete ARCH_CONTRACT_DATA acon "
                    + " where "
                    + " acon.tenantid=" + milkJobExecution.getTenantId()
                    + " and ( (acon.kind='PROCESS' "
                    + "        and acon.scopeid in (select ar.sourceobjectid from arch_process_instance ar where ar.rootprocessinstanceid in (" + inClause + ") and ar.tenantid=" + milkJobExecution.getTenantId() + ")"
                    + "       )"
                    + "    or (acon.kind='TASK' "
                    + "        and acon.scopeid in (select af.sourceobjectid from arch_flownode_instance af where af.rootcontainerid in (" + inClause + ") and af.tenantid=" + milkJobExecution.getTenantId() + ")"
                    + "       )"
                    + "     )",
                    doubleSourceProcessInstanceIds);
        } else
            allowPurgeActivities=false;
        
        // ArchDatainstance 
        String purgeArchData = "delete ARCH_DATA_INSTANCE ad where "
                + " ad.tenantid=" + milkJobExecution.getTenantId()
                + " and ( ( ad.containertype='PROCESS_INSTANCE' and ad.containerid in (select ar.sourceobjectid from arch_process_instance ar where ar.rootprocessinstanceid in (" + inClause + ")))"
                + "   or (ad.containertype='ACTIVITY_INSTANCE' and ad.containerid in (select af.sourceobjectid from arch_flownode_instance af where af.rootcontainerid in (" + inClause + " )))"
                + " )";
        if (MilkPurgeArchivedCases.CST_ALL.equals(milkJobExecution.getInputStringParameter(MilkPurgeArchivedCases.cstParamPurgeDataInstance))) {
            managePurgeResult.nbRecords+=executeSql(purgeArchData, doubleSourceProcessInstanceIds);
        } else if (MilkPurgeArchivedCases.CST_KEEPLASTARCHIVE.equals(milkJobExecution.getInputStringParameter(MilkPurgeArchivedCases.cstParamPurgeDataInstance))) {
            managePurgeResult.nbRecords+=executeSql(purgeArchData + " and ad.archivedate != (select max(allData.archivedate) from ARCH_DATA_INSTANCE allData where alldata.sourceobjectid = ad.sourceobjectid and alldata.tenantid=" + milkJobExecution.getTenantId() + ")",
                    doubleSourceProcessInstanceIds);
            allowPurgeActivities=false;
        } else
            allowPurgeActivities=false;
        

        // Documents
        if (MilkPurgeArchivedCases.CST_YES.equals(milkJobExecution.getInputStringParameter(MilkPurgeArchivedCases.cstParamPurgeDocument))) {
            managePurgeResult.nbRecords+= executeSql("delete from DOCUMENT "
                    + "  where id in (select adocmap.documentid from ARCH_Document_mapping adocmap "
                    + "     where adocmap.processinstanceid in (select ar.sourceobjectid from arch_process_instance ar where ar.rootprocessinstanceid in (" + inClause + ")"
                    + "        and ar.tenantid=" + milkJobExecution.getTenantId() + "))"
                    + " and tenantid=" + milkJobExecution.getTenantId(),
                    sourceProcessInstanceIds);

            managePurgeResult.nbRecords+=executeSql("delete from ARCH_DOCUMENT_MAPPING adoc "
                    + " where adoc.processinstanceid in (select ar.sourceobjectid from arch_process_instance ar where ar.rootprocessinstanceid in (" + inClause + ")"
                    + " and ar.tenantid=" + milkJobExecution.getTenantId() + ")"
                    + "  and adoc.tenantid=" + milkJobExecution.getTenantId(),
                    sourceProcessInstanceIds);
        }

        // Comments
        if (MilkPurgeArchivedCases.CST_YES.equals(milkJobExecution.getInputStringParameter(MilkPurgeArchivedCases.cstParamPurgeComment))) {
            managePurgeResult.nbRecords+=executeSql("delete from ARCH_PROCESS_COMMENT acom "
                    + " where acom.processinstanceid in (select ar.sourceobjectid from arch_process_instance ar where ar.rootprocessinstanceid in (" + inClause + ")"
                    + "and ar.tenantid=" + milkJobExecution.getTenantId() + ")"
                    + " and acom.tenantid=" + milkJobExecution.getTenantId(),
                    sourceProcessInstanceIds);
        }

        // Activities
        if (allowPurgeActivities && MilkPurgeArchivedCases.CST_YES.equals(milkJobExecution.getInputStringParameter(MilkPurgeArchivedCases.cstParamPurgeActivity))) {
            managePurgeResult.nbRecords+=executeSql("delete from ARCH_FLOWNODE_INSTANCE afl where afl.rootcontainerid in (" + inClause + ") and tenantid=" + milkJobExecution.getTenantId(), sourceProcessInstanceIds);
        }
        // Business Attachement
        if (MilkPurgeArchivedCases.CST_YES.equals(milkJobExecution.getInputStringParameter(MilkPurgeArchivedCases.cstParamPurgeBusinessAttachement))) {
            managePurgeResult.nbRecords+=executeSql("delete from ARCH_REF_BIZ_DATA_INST abus where "
                    + " abus.orig_proc_inst_id in (select ar.sourceobjectid from arch_process_instance ar where ar.rootprocessinstanceid in (" + inClause + ") and ar.tenantid="+milkJobExecution.getTenantId()+")"
                    + " and abus.tenantid=" + milkJobExecution.getTenantId(), sourceProcessInstanceIds);

        }

        if (MilkPurgeArchivedCases.CST_YES.equals(milkJobExecution.getInputStringParameter(MilkPurgeArchivedCases.cstParamPurgeSubProcess))) {
            managePurgeResult.nbRecords+=executeSql("delete ARCH_PROCESS_INSTANCE where callerid !=-1 and rootprocessinstanceid in (" + inClause + ")"
                    + " and tenantid=" + milkJobExecution.getTenantId(),
                    sourceProcessInstanceIds);
            DrossExecution drossExecution = RadarCleanArchivedDross.deleteDrossAll(milkJobExecution.getTenantId(), 10000);
            managePurgeResult.listEvents.addAll(drossExecution.getListEvents());
            
        }

        // let's say we did the job, so we work on the number of processId required
        return managePurgeResult;
    }

    /**
     * @param sqlRequest
     * @param sourceProcessInstanceIds
     * @return
     */
    private static int executeSql(String sqlRequest, List<Long> sourceProcessInstanceIds) {
        PreparedStatement pstmt = null;

        try (Connection con = BonitaEngineConnection.getConnection();) {
            pstmt = con.prepareStatement(sqlRequest);
            for (int i = 0; i < sourceProcessInstanceIds.size(); i++)
                pstmt.setObject(i + 1, sourceProcessInstanceIds.get(i));

            int nbDeleted = pstmt.executeUpdate();
            return nbDeleted;

        } catch (Exception e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            logger.severe(LOGGER_LABEL + "During getCountOfFlowNode : " + e.toString() + " SqlQuery[" + sqlRequest + "] at " + sw.toString());

            return -1;
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                    pstmt = null;
                } catch (final SQLException localSQLException) {
                    logger.severe(LOGGER_LABEL + "During close : " + localSQLException.toString());
                }
            }

        }

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* SubProcess operation */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

  

    public interface SubProcessOperation {

        public List<BEvent> detectOneSubprocessInstance(Map<String, Object> recordSubProcess);
    }

    /**
     * @param listProcessResult
     * @param delay
     * @param tenantId
     * @param maxCount
     * @param subProcessOperation
     * @return
     */
    public static ManagePurgeResult detectPurgeSubProcessOnly(ListProcessesResult listProcessResult, long delay, long tenantId, long maxCount, SubProcessOperation subProcessOperation) {
        ManagePurgeResult managePurgeResult = new ManagePurgeResult();
        // Search all subprocess informations
        StringBuilder sqlQuery = new StringBuilder();
        if (maxCount == 0)
            maxCount = 10000;

        buildSelectArchProcessInstance(sqlQuery, listProcessResult, delay, tenantId);
        sqlQuery.append(" and ROOTPROCESSINSTANCEID != SOURCEOBJECTID"); // only sub process
        managePurgeResult.sqlQuery = sqlQuery.toString();

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try (Connection con = BonitaEngineConnection.getConnection();) {
            pstmt = con.prepareStatement(sqlQuery.toString());

            rs = pstmt.executeQuery();
            ResultSetMetaData rmd = pstmt.getMetaData();
            managePurgeResult.nbRecords = 0;
            while (rs.next() && managePurgeResult.nbRecords < maxCount) {
                managePurgeResult.nbRecords++;
                Map<String, Object> recordSubProcess = new HashMap<>();

                for (int column = 1; column <= rmd.getColumnCount(); column++)
                    recordSubProcess.put(rmd.getColumnName(column).toUpperCase(), rs.getObject(column));
                subProcessOperation.detectOneSubprocessInstance(recordSubProcess);
            }
            return managePurgeResult;

        } catch (Exception e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            logger.severe(LOGGER_LABEL + "During getCountOfFlowNode : " + e.toString() + " SqlQuery[" + sqlQuery + "] at " + sw.toString());
            managePurgeResult.listEvents.add(new BEvent(eventErrorExecutionQuery, e, " SqlQuery[" + sqlQuery + "]"));
            return managePurgeResult;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                } catch (final SQLException localSQLException) {
                    logger.severe(LOGGER_LABEL + "During close : " + localSQLException.toString());
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                    pstmt = null;
                } catch (final SQLException localSQLException) {
                    logger.severe(LOGGER_LABEL + "During close : " + localSQLException.toString());
                }
            }

        }
    }

    /**
     * Build the SQL Request to search in the ARCH_PROCESS_INSTANCE
     * 
     * @param buildTheSQLRequest
     */
    public static void buildSelectArchProcessInstance(StringBuilder buildTheSQLRequest, ListProcessesResult listProcessResult, long timeSearch, long tenantId) {
        buildTheSQLRequest.append("select * from ARCH_PROCESS_INSTANCE where ");
        if (!listProcessResult.listProcessDeploymentInfo.isEmpty()) {
            buildTheSQLRequest.append(" (");
            for (int i = 0; i < listProcessResult.listProcessDeploymentInfo.size(); i++) {
                if (i > 0)
                    buildTheSQLRequest.append(" or ");
                buildTheSQLRequest.append("PROCESSDEFINITIONID = " + listProcessResult.listProcessDeploymentInfo.get(i).getProcessId());
            }
            buildTheSQLRequest.append(" ) and ");
        }
        buildTheSQLRequest.append(" TENANTID=" + tenantId);
        if (timeSearch > 0)
            buildTheSQLRequest.append(" and ARCHIVEDATE <= " + timeSearch);
        buildTheSQLRequest.append(" and STATEID=6"); // only archive case
    }

    /**
     * Purge a list of sourceProcessId, according they should be a subprocess.
     * Do that by:
     * - a direct sqldeletion
     * - a call to the drossdata librairy to delete all attached information
     * 
     * @param listArchSourceProcessId
     * @param tenantId
     * @return
     */
    public static ManagePurgeResult purgeSubProcess(List<Long> listArchSourceProcessId, long tenantId) {
        ManagePurgeResult managePurgeResult = new ManagePurgeResult();

        String sqlQuery = "delete ARCH_PROCESS_INSTANCE where SOURCEOBJECTID=? and tenantid=?";
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try (Connection con = BonitaEngineConnection.getConnection();) {

            for (long id : listArchSourceProcessId) {

                // First, delete the Arch Process instance
                pstmt = con.prepareStatement(sqlQuery);
                pstmt.setLong(1, id);
                pstmt.setLong(2, tenantId);
                pstmt.executeUpdate();

                con.commit();
                managePurgeResult.nbRecords++;
            }
        } catch (Exception e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            logger.severe(LOGGER_LABEL + "During getCountOfFlowNode : " + e.toString() + " SqlQuery[" + sqlQuery + "] at " + sw.toString());
            managePurgeResult.listEvents.add(new BEvent(eventErrorExecutionQuery, e, " SqlQuery[" + sqlQuery + "]"));
            return managePurgeResult;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                } catch (final SQLException localSQLException) {
                    logger.severe(LOGGER_LABEL + "During close : " + localSQLException.toString());
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                    pstmt = null;
                } catch (final SQLException localSQLException) {
                    logger.severe(LOGGER_LABEL + "During close : " + localSQLException.toString());
                }
            }

        }

        // we generate a lot of dross : manage them now
        DrossExecution drossExecution = RadarCleanArchivedDross.deleteDrossAll(tenantId, 10000);
        managePurgeResult.listEvents.addAll(drossExecution.getListEvents());
        return managePurgeResult;
    }

}
