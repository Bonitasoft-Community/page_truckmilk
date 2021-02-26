package org.bonitasoft.truckmilk.plugin.other;

import java.io.ByteArrayInputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.bonitasoft.engine.core.contract.data.SContractDataDeletionException;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.platform.setup.command.configure.DatabaseConfiguration;
import org.bonitasoft.properties.BonitaEngineConnection;
import org.bonitasoft.properties.DatabaseConnection;
import org.bonitasoft.properties.DatabaseConnection.ConnectionResult;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.DELAYSCOPE;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TYPE_PLUGIN;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;

import groovy.ui.Console;

import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;

/**
* This plug in in a Labs feature.
* it copy the Bonita Engine to a external datasource. Idea is to have this datasource in a different database and then translate the bonitaengine from a datasource (MySql) to an another (Postgres)
* 
*/

public class MilkDuplicateBase extends MilkPlugIn {

    Logger logger = Logger.getLogger(MilkDuplicateBase.class.getName());
    private static final String LoggerHeader = "TruckMilk.MilkDuplicateBase: ";

    private static BEvent eventOperationFailed = new BEvent(MilkDuplicateBase.class.getName(), 1, Level.ERROR,
            "Operation error", "An error arrive during the operation", "Operation failed", "Check the exception");

    
    private static BEvent eventTableDeletionError = new BEvent(MilkDuplicateBase.class.getName(), 2, Level.ERROR,
            "Table purge error", "An error arrive during the purge", "Operation failed", "Check the exception");

    
    private static final String CSTCOPY_RESET = "Purge the destination database";
    private static final String CSTOPERATION_COMPLETE = "Complete the destination database";

    private final static PlugInParameter cstParamDestinationDatasource = PlugInParameter.createInstance("destinationDatasource", "Destination Datasource", TypeParameter.STRING, "", "Give the Datasource where the Bonita Engine will be copied");

    private static PlugInParameter cstParamCopyOption = PlugInParameter.createInstanceListValues("CopyOption", "Copy option",
            new String[] { CSTCOPY_RESET, CSTOPERATION_COMPLETE }, CSTCOPY_RESET, "Copy option, reset the destination database, or complete it");

    private int baseAdvancement = 0;

    public MilkDuplicateBase() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    @Override
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {

        return new ArrayList<>();
    }

    @Override
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();

        plugInDescription.setName("Duplicate Database");
        plugInDescription.setExplanation("Duplicate the current Bonita Database engine to an external Bonita Database. The external database must be created (do a start bundle) before, to have all tables created. Bonita Version must be sames");
        plugInDescription.setLabel("Duplicate Database");
        plugInDescription.setCategory(CATEGORY.OTHER);

        plugInDescription.setStopJob(JOBSTOPPER.BOTH);

        plugInDescription.addParameter(cstParamDestinationDatasource);
        plugInDescription.addParameter(cstParamCopyOption);
        return plugInDescription;
    }

    /**
     * the list of tables is given, in the correct order.
     * The copy will follow this order
     */
    private static String[] listTables = {
            "tenant",
            // organization
            "role",
            "group_",
            "user_",
            "user_login",
            "user_membership",
            "custom_usr_inf_val",
            "custom_usr_inf_def",
            "user_contactinfo",

            // process
            "dependency",
            "process_content",
            "process_definition",
            "dependencymapping",
            "actor",
            "actormember",
            "process_comment",
            "category",
            "processcategorymapping",
            "processsupervisor",
            "proc_parameter",
            
            // process instance
            "process_instance",
            "flownode_instance",
            "contract_data",
            "document",
            "document_mapping",
            "multi_biz_data",
            "ref_biz_data_inst",
            "pending_mapping",
            "message_instance",
            "waiting_event",
            "event_trigger_instance",
            "connector_instance",
            "data_instance",
            
            // archive
            "arch_process_comment",
            "arch_document_mapping",
            
            "arch_contract_data",
            "arch_flownode_instance",
            "arch_process_instance",
            "arch_connector_instance",
            "arch_multi_biz_data",            
            "arch_ref_biz_data_inst",
            "arch_data_instance",

            // Portal
            "report",
            "page", 
            "page_mapping", 
            "form_mapping", // <<= page, pageMapping

            "theme",
            "business_app",
            "business_app_page",
            "business_app_menu",
            
            "profile",
            "profilemember",
            "profileentry",
            
            "external_identity_mapping",
            
            // Engine
            "queriablelog_p",
            "queriable_log",
            "command",
            "job_log",
            "job_desc", // reference job_param
            "job_param", 
            
            "platformCommand"
};
  
    @Override
    public MilkJobOutput executeJob(MilkJobExecution milkJobExecution) {
        MilkJobOutput milkJobOutput = milkJobExecution.getMilkJobOutput();
        String externalDatasource = milkJobExecution.getInputStringParameter(cstParamDestinationDatasource);
        String copyOption = milkJobExecution.getInputStringParameter(cstParamCopyOption);
        
        List<String> listExternalDataSource = new ArrayList<>();
        listExternalDataSource.add( externalDatasource );
        
        Connection conSource=null;
        Connection conDestination = null;
        
        try {
            ConnectionResult connectionResult =      DatabaseConnection.getConnection(listExternalDataSource);
            milkJobOutput.addEvents( connectionResult.listEvents);
            if (BEventFactory.isError( connectionResult.listEvents)) {
                milkJobOutput.setExecutionStatus( ExecutionStatus.BADCONFIGURATION ); 
                return milkJobOutput;
            }
            conDestination = connectionResult.con;
            conDestination.setAutoCommit(false);
            
            conSource = BonitaEngineConnection.getConnection();
            
            if (CSTCOPY_RESET.equals(copyOption)) {
                // purge all tables
                milkJobOutput.addReportInHtmlCR("Purge destination database");
                milkJobOutput.addReportTableBegin(new String[] {"TableName", "Number of item", "Status"}, 200);
                for (int i=listTables.length-1;i>=0;i--) {
                    if ("tenant".equals(listTables[ i ]))
                        continue;
                        
                    if (! purgeTable( listTables[ i ], conDestination, milkJobOutput, milkJobExecution)) {
                        milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR);
                    }
                }
                milkJobOutput.addReportTableEnd();
            }
            if (BEventFactory.isError( milkJobOutput.getListEvents())) {
                return milkJobOutput;
            }
            // copy now
            // after the purge, advancement is 10 %
            milkJobExecution.setAvancement( 10 );
            baseAdvancement = 10;
            milkJobOutput.addReportInHtmlCR("Copy database");
            milkJobOutput.addReportTableBegin(new String[] {"TableName", "Number of items", "Status"}, 200);

            for (int i=0;i<listTables.length;i++) {
                String tableName  = listTables[ i ];
                baseAdvancement = 10 + (i * 90)/listTables.length;
                if (! copyTable(tableName, conSource, conDestination, copyOption, milkJobOutput,milkJobExecution))
                    break;
            }
            milkJobOutput.addReportTableEnd();
            
        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventOperationFailed, e, ""));
            milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );
        }
        finally {
            if (conSource != null)
                try {
                    conSource.close();
                } catch (SQLException e) {
                }
            if (conDestination!=null)
                try {
                    conDestination.close();
                } catch (SQLException e) {
                }
        }
        
        return milkJobOutput;
    }

    /**
     * 
     * @param tableName
     * @param conDestination
     * @param milkJobOutput
     * @return
     */
    private boolean purgeTable(String tableName, Connection conDestination, MilkJobOutput milkJobOutput, MilkJobExecution milkJobExecution) {
        long countTable = getCount(tableName, conDestination);
        milkJobExecution.setAvancementInformation("Purge "+tableName+" ("+countTable+" records)");

        String selectSql = "truncate table " + tableName+" cascade";
        PreparedStatement pstmt = null;
        try {
            logger.info(LoggerHeader + "Purge with [" + selectSql + "]");

            pstmt = conDestination.prepareStatement(selectSql);
            pstmt.executeUpdate();
            conDestination.commit();
            milkJobOutput.addReportTableLine( new Object[] { tableName, countTable, "OK" });
            return true;
        } catch (SQLException e) {
            logger.severe(LoggerHeader + " Exception during truncate table [" + tableName + "] : " + e.getMessage());
            milkJobOutput.addEvent( new BEvent(eventTableDeletionError, e, e.getMessage()));
            milkJobOutput.addReportTableLine( new Object[] { tableName, countTable, "FAILED"+e.getMessage() });
            try {
                conDestination.rollback();
            } catch (SQLException e1) {
                logger.severe(LoggerHeader + " Rollback failed during truncate table [" + tableName + "] : " + e1.getMessage());
            }
            return false;
        } finally {
            if (pstmt != null)
                try {
                    pstmt.close();
                } catch (SQLException e) {
                }
        }
    }

    /**
     * @param tableName
     * @param conSource
     * @param conDestination
     * @param copyOption
     * @param milkJobOutput
     * @param milkJobExecution
     * @return
     */
    private boolean copyTable(String tableName, Connection conSource, Connection conDestination, String copyOption, MilkJobOutput milkJobOutput, MilkJobExecution milkJobExecution) {

        long countTable = getCount(tableName, conSource);
        milkJobExecution.setAvancementInformation("Copy "+tableName+" ("+countTable+" records)");

        // not possible to order by tenant : this field does not exist systematically
        String selectSql = "select * from " + tableName + " order by id";
        PreparedStatement pstmt = null;
        PreparedStatement pCopystmt = null;
        long numberOfRecords = 0;
        try {

            pstmt = conSource.prepareStatement(selectSql);
            ResultSet rs = pstmt.executeQuery();

            // get the value of query
            StringBuilder insertDeclaration = new StringBuilder();
            StringBuilder insertValue = new StringBuilder();

            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    insertDeclaration.append(", ");
                    insertValue.append(", ");
                }
                insertDeclaration.append(md.getColumnName(i));
                insertValue.append("?");
            }
            String sqlInsert = "insert into " + tableName + " ( " + insertDeclaration.toString() + ") values (" + insertValue.toString() + ")";

            while (rs.next()) {
                numberOfRecords++;
                int advancement = baseAdvancement + (int) ((100 * numberOfRecords) / countTable);
                milkJobExecution.setAvancement(advancement);
                if (milkJobExecution.isStopRequired())
                    return false;

                // get all objects
                pCopystmt = conDestination.prepareStatement(sqlInsert);
                Long currentId = null;
                Long currentTenantId = null;
                for (int i = 1; i <= columnCount; i++) {
                    
                    if ( md.getColumnType( i ) == Types.CLOB) {
                        Clob clob = rs.getClob( md.getColumnName(i) );
                        pCopystmt.setClob(i, clob);
                    }

                    else if (md.getColumnType( i ) == Types.BLOB || md.getColumnType( i ) == Types.BINARY) {
                        Blob blob = rs.getBlob( md.getColumnName(i));
                        pCopystmt.setBlob(i, blob);
                    }
                    else 
                    {
                        pCopystmt.setObject(i, rs.getObject(i));
                    }
                    if ("id".equalsIgnoreCase(md.getColumnName(i)))
                        currentId = rs.getLong(i);
                    if ("tenantid".equalsIgnoreCase(md.getColumnName(i)))
                        currentTenantId = rs.getLong(i);

                }
                int nbRecord = pCopystmt.executeUpdate();
                pCopystmt.close();
                pCopystmt = null;
                if (nbRecord < 1) {
                    logger.severe(LoggerHeader + " Error during copy record  ID=[" + currentId + "] TenantId[" + currentTenantId + "] in tablename[" + tableName + "]");

                }

                conDestination.commit();
            }
            milkJobOutput.addReportTableLine( new Object[] { tableName, numberOfRecords, "OK" });

            return true;
        } catch (SQLException e) {
            logger.severe(LoggerHeader + " Exception during copy table [" + tableName + "] : " + e.getMessage());
            milkJobOutput.addReportTableLine( new Object[] { tableName, numberOfRecords, "Failed "+e.getMessage() });

            return true;
        } finally {
            if (pstmt != null)
                try {
                    pstmt.close();
                } catch (SQLException e) {
                }
            if (pCopystmt != null)
                try {
                    pCopystmt.close();
                } catch (SQLException e) {
                 
                }
        }

    }

    private long getCount(String tableName, Connection con) {
        PreparedStatement pstmt = null;
        try {
            String selectSql = "select count(*) as COUNTNUMBER from " + tableName;

            pstmt = con.prepareStatement(selectSql);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next())
                return rs.getLong("COUNTNUMBER");
            return 0L;
        } catch (Exception e) {
            return -1L;
        } finally {
            if (pstmt != null)
                try {
                    pstmt.close();
                } catch (SQLException e) {
                }
        }
    }
}
