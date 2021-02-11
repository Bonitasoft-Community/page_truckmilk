package org.bonitasoft.truckmilk.plugin.other;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;


import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.properties.BonitaEngineConnection;
import org.bonitasoft.properties.BonitaProperties;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJobExecution.DelayResult;



public class MilkRGPD extends MilkPlugIn {

    private static final String CSTOPERATION_PURGE = "Clean data";
    private final static String CSTOPERATION_DELETE = "Delete Data";
    private final static String CSTOPERATION_DETECT = "only Detection";
    private static BEvent eventOperationFailed = new BEvent(MilkRGPD.class.getName(), 1, Level.ERROR,
            "Operation error", "An error arrive during the operation", "Operation failed", "Check the exception");

    private static BEvent eventBadConfigurationCleanMissing = new BEvent(MilkRGPD.class.getName(), 2, Level.APPLICATIONERROR,
            "Bad configuration / Clean", "With a CLEAN policy, attributes to clean must be provide", "Operation failed", "Add attributes to clean");

    private static BEvent eventIntrospectBusinessDatabase = new BEvent(MilkRGPD.class.getName(), 3, Level.ERROR,
            "Introspection Business database failed", "The Business Database is introspected to verify the configuration. This introspection failed", "Operation failed", "Check the exception");

    
    private static BEvent eventBDMObjectDoesNotExist = new BEvent(MilkRGPD.class.getName(), 4, Level.APPLICATIONERROR,
            "BDM Object does not exist", "The Business Object does not exists", "Bad configuration", "Give an existing BDM Object");
    
    
    private static BEvent eventBDMDateAttributIncorrectType = new BEvent(MilkRGPD.class.getName(), 5, Level.APPLICATIONERROR,
            "Date attribute is not the correct type", "Purge is based on an attribute, which must be a Date", "Bad configuration", "Give a date Attribut in the BDM Object");
    
    private static BEvent eventBDMDateAttributNotExist = new BEvent(MilkRGPD.class.getName(), 6, Level.APPLICATIONERROR,
            "Date attribute does not exist", "Purge is based on an attribute, it does not exists", "Bad configuration", "Give an existing Attribut in the BDM Object");

    
    private static BEvent eventBDMObjectMissingAttributes = new BEvent(MilkRGPD.class.getName(), 7, Level.APPLICATIONERROR,
            "Missing attributes to clean", "Some attributes is request to be clean does not exists", "Bad configuration", "Give an existing Attributes in the BDM Object");
    
    /**
     * define the different parameters.
     * A parameter has a type (a Date, a String), and may have condition to display it or not
     */
    private final static PlugInParameter cstParamBDMObject = PlugInParameter.createInstance("bdmObject", "Bdm Object to clean", TypeParameter.STRING, "", "Give the BDM Object you want to purge.");
    private final static PlugInParameter cstParamBDMDateAttribute = PlugInParameter.createInstance("bdmDateAttribut", "BDM Date attribute", TypeParameter.STRING, "", "Attribute in the BDM with a Date. All object older than the delay are in the scope");

    private static PlugInParameter cstParamDelay = PlugInParameter.createInstanceDelay("Delay", "Delay to operate the policy", DELAYSCOPE.MONTH, 6, "All BDM Object older than this delay are in the scope");

    private static PlugInParameter cstParamPolicyRGPD = PlugInParameter.createInstanceListValues("Policy",
            "RGPD Policy",
            new String[] { CSTOPERATION_PURGE, CSTOPERATION_DELETE, CSTOPERATION_DETECT }, CSTOPERATION_PURGE, "Purge some attribut, or delete the object");

    private final static PlugInParameter cstParamAttributesToClean = PlugInParameter.createInstance("bdmAttributesToPurge", "Attributes to clean", TypeParameter.STRING, "", "List all attributes to clean (separate by a comma, example FirstName,LastName,Age")
            .withVisibleConditionParameterValueEqual(cstParamPolicyRGPD, CSTOPERATION_PURGE);

    public MilkRGPD() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    @Override
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {

        return new ArrayList<>();
    }

    @Override
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        List<BEvent> listEvents =new ArrayList<>();
        String bdmObject = milkJobExecution.getInputStringParameter(cstParamBDMObject);
        String bdmDateAttribut = milkJobExecution.getInputStringParameter(cstParamBDMDateAttribute);
        String attributesToClean = milkJobExecution.getInputStringParameter(cstParamAttributesToClean);
        try (Connection con = BonitaEngineConnection.getBusinessConnection();) {
            
            listEvents.addAll(checkBdmObject(bdmObject, bdmDateAttribut, attributesToClean, con)) ;         
        } catch (Exception e) {
            listEvents.add( new BEvent(eventOperationFailed, e, ""));
        }
        return listEvents;
        
        
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();

        plugInDescription.setName("RGPD Policy");
        plugInDescription.setExplanation("Purge data in the BDM, after a delay, to respect the RGPD");
        plugInDescription.setLabel("RGPD Policy");
        plugInDescription.setCategory(CATEGORY.OTHER);

        plugInDescription.setStopJob(JOBSTOPPER.BOTH);

        plugInDescription.addParameter(cstParamBDMObject);
        plugInDescription.addParameter(cstParamBDMDateAttribute);
        plugInDescription.addParameter(cstParamDelay);
        plugInDescription.addParameter(cstParamPolicyRGPD);
        plugInDescription.addParameter(cstParamAttributesToClean);
        return plugInDescription;
    }

    
    
    @Override
    public MilkJobOutput executeJob(MilkJobExecution milkJobExecution) {
        MilkJobOutput milkJobOutput = milkJobExecution.getMilkJobOutput();
        String bdmObject = milkJobExecution.getInputStringParameter(cstParamBDMObject);
        String bdmDateAttribut = milkJobExecution.getInputStringParameter(cstParamBDMDateAttribute);
        String policyRGPD = milkJobExecution.getInputStringParameter(cstParamPolicyRGPD);
        String attributesToClean = milkJobExecution.getInputStringParameter(cstParamAttributesToClean);

        DelayResult delayResult = milkJobExecution.getInputDelayParameter(cstParamDelay, new Date(), false);
        if (BEventFactory.isError(delayResult.listEvents)) {
            milkJobOutput.addEvents(delayResult.listEvents);
            milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );
            return milkJobOutput;
        }

        StringBuilder sqlRequest = new StringBuilder();
        StringBuilder whereFilter = new StringBuilder();

        try (Connection con = BonitaEngineConnection.getBusinessConnection();) {
            // build the request
            List<Object> sqlParam = new ArrayList<>();
            boolean isATransaction = false;
            String labelOperation = "";
            if (CSTOPERATION_DELETE.equals(policyRGPD)) {
                sqlRequest.append("delete " + bdmObject);
                isATransaction = true;
                labelOperation = "Deleted";
            } else if (CSTOPERATION_PURGE.equals(policyRGPD)) {
                sqlRequest.append("update " + bdmObject + " set ");
                whereFilter.append(" ( ");
                StringTokenizer st = new StringTokenizer(attributesToClean, ",");
                int count = 0;
                while (st.hasMoreTokens()) {
                    if (count > 0) {
                        sqlRequest.append(", ");
                        whereFilter.append(" or ");
                    }
                    count++;
                    String colName = st.nextToken();
                    sqlRequest.append( colName + " = null");
                    whereFilter.append( colName+" is not null");
                    
                }
                whereFilter.append(" ) ");                
                if (count == 0) {
                    milkJobOutput.addEvent(eventBadConfigurationCleanMissing);
                    milkJobOutput.setExecutionStatus( ExecutionStatus.BADCONFIGURATION );
                    return milkJobOutput;
                }
                isATransaction = true;
                labelOperation = "cleaned";

                
            } else if (CSTOPERATION_DETECT.equals(policyRGPD)) {
                sqlRequest.append("select * from " + milkJobExecution);
                isATransaction = false;
            }

            
            sqlRequest.append(" where 1=1 ");
            if (whereFilter.length()>0) {
                sqlRequest.append(" and " + whereFilter+" ");
            }
            if (bdmDateAttribut != null && bdmDateAttribut.trim().length() > 0) {
                sqlRequest.append(" and " + bdmDateAttribut + " < ? ");
                // format of date is a string in the BDM
                // '2020-10-26T22:00:00'
                
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

                // Attention, the date has to be translated to UTC.
                LocalDateTime localDate = delayResult.delayDate.toInstant().atZone( ZoneId.of("UTC") ).toLocalDateTime();
                sqlParam.add(localDate.format( formatter));
                
            }
            

            // ok, now do the job
            if (isATransaction) {

                con.setAutoCommit(false);
                try (PreparedStatement pstmt = con.prepareStatement(sqlRequest.toString());) {
                    for (int i = 0; i < sqlParam.size(); i++) {
                        pstmt.setObject(i + 1, sqlParam.get(i));
                    }
                    int numberOfRecords = pstmt.executeUpdate();
                    con.commit();
                    milkJobOutput.addReportInHtml(numberOfRecords + " objects " + labelOperation);
                    milkJobOutput.setNbItemsProcessed(numberOfRecords);
                } catch (Exception e) {
                    con.rollback();
                    milkJobOutput.addEvent(new BEvent(eventOperationFailed, e, "SqlRequest[" + sqlRequest + "] "));
                    milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );
                }
            } else {
                List<Map<String, Object>> listResult = BonitaEngineConnection.executeSqlRequest(sqlRequest.toString(), sqlParam, 100);
                if (listResult.isEmpty())
                    milkJobOutput.addReportInHtml("No record detected");
                else {
                    List<String> listKeys = new ArrayList<>();
                    for (String key : listResult.get(0).keySet())
                        listKeys.add(key);

                    milkJobOutput.addReportTableBegin(listKeys.toArray(new String[0]));
                    for (Map<String, Object> record : listResult) {
                        Object[] values = new Object[listKeys.size()];
                        for (int i = 0; i < listKeys.size(); i++)
                            values[i] = record.get(listKeys.get(i));
                        milkJobOutput.addReportTableLine(values);
                    }
                    milkJobOutput.setNbItemsProcessed(listResult.size());

                }
            }

        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventOperationFailed, e, "SqlRequest[" + sqlRequest + "] "));
            milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );

        }

        return milkJobOutput;
    }
    
    private List<BEvent> checkBdmObject(String bdmObject,String bdmDateAttribut, String attributesToClean,  Connection con) {
        List<BEvent> listEvents = new ArrayList<>();
        try {
        final DatabaseMetaData dbm = con.getMetaData();

        // check if "employee" table is there
        // nota: don't use the patern, it not give a correct result with H2
        final ResultSet tables = dbm.getTables(null, null, null, null);

        boolean exist = false;
        StringBuffer allObjects = new StringBuffer();
        while (tables.next()) {
            final String tableName = tables.getString("TABLE_NAME");
            if (tableName.equalsIgnoreCase(bdmObject)) {
                exist = true;
                bdmObject= tableName; // get the correct casse
                break;
            }
            allObjects.append( tableName+",");
        }
        if (!exist) {
            listEvents.add( new BEvent(eventBDMObjectDoesNotExist, "AllObjects detected: "+allObjects.toString()));
            return listEvents;
        }
        
        // check all attributes
        final ResultSet rs = dbm.getColumns(null /* catalog */, null /* schema */, bdmObject /* cstSqlTableName */,
                null /* columnNamePattern */);

        boolean foundDateAttribut=false;
        Set<String> allColumns = new HashSet<>();
        while (rs.next()) {
            String tableNameCol = rs.getString("TABLE_NAME");
            final String colName = rs.getString("COLUMN_NAME");
            final int length = rs.getInt("COLUMN_SIZE");
            allColumns.add(colName.toUpperCase());
                
            
            if (bdmDateAttribut !=null && bdmDateAttribut.equalsIgnoreCase(colName)) {
                foundDateAttribut=true;
                // Value must a date => so a String
                int dataType = rs.getInt("DATA_TYPE");
                if (dataType != java.sql.Types.LONGVARCHAR &&
                        dataType != java.sql.Types.NCHAR &&
                        dataType != java.sql.Types.NVARCHAR &&
                        dataType != java.sql.Types.VARCHAR)
                    listEvents.add( new BEvent(eventBDMDateAttributIncorrectType, "The Date Attribut must be a Date (varchar in database) - "+dataType+" detected" ));
            }
        }
        if (bdmDateAttribut !=null && ! foundDateAttribut)
            listEvents.add( new BEvent(eventBDMDateAttributNotExist, "Date attribut ["+bdmDateAttribut+"]" ));
            
        
        StringBuffer missingFields = new StringBuffer();
        StringTokenizer st = new StringTokenizer(attributesToClean, ",");
        while( st.hasMoreTokens()) {
            String colName = st.nextToken();
            if (! allColumns.contains(colName.toUpperCase())) {
                missingFields.append("["+colName.toUpperCase()+"],");
            }
        }
        if (missingFields.length()>0) {
            listEvents.add( new BEvent(eventBDMObjectMissingAttributes, "Missing Column to clean: "+missingFields.toString()));
            
        }
            
        } catch (Exception e) {
            listEvents.add(new BEvent(eventIntrospectBusinessDatabase, e, ""));

        }
        return listEvents;
    }
}
