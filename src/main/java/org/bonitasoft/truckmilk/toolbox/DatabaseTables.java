package org.bonitasoft.truckmilk.toolbox;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;

/**
 * Create table on a database
 */
public class DatabaseTables {

    private static final String CST_DRIVER_H2 = "H2";
    private static final String CST_DRIVER_ORACLE = "oracle";
    private static final String CST_DRIVER_POSTGRESQL = "PostgreSQL";
    private static final String CST_DRIVER_MYSQL = "MySQL";
    private static final String CST_DRIVER_SQLSERVER = "Microsoft SQL Server";

    Logger logger = Logger.getLogger(DatabaseTables.class.getName());
    private final String loggerLabel = "TruckMilk.DatabaseTables";

    private static BEvent eventCreationDatabase = new BEvent(DatabaseTables.class.getName(), 2, Level.ERROR,
            "Error during creation the table in the database", "Check Exception ",
            "The properties will not work (no read, no save)", "Check Exception");
    private static BEvent eventInsertError = new BEvent(DatabaseTables.class.getName(), 3, Level.ERROR,
            "Insert error", "Insert error",
            "Error during insertion of an record", "Check Exception");
    private static BEvent eventConnectDatabase = new BEvent(DatabaseTables.class.getName(), 3, Level.ERROR,
            "Can't connect", "Can't connect to the database",
            "The connection can't be establish", "Check Exception");
    private static BEvent eventSqlError = new BEvent(DatabaseTables.class.getName(), 4, Level.ERROR,
            "Sql Error ", "Sql execution error",
            "Request will not work", "Check Exception");

    public enum COLTYPE {
        LONG, BLOB, STRING, BOOLEAN, DECIMAL, TEXT
    }

    public static class DataColumn {

        public String colName;
        public COLTYPE colType;
        public int length;

        public DataColumn(String colName, COLTYPE colType, int length) {
            this.colName = colName;
            this.colType = colType;
            this.length = length;
        }

        /*
         * for all field without a size (LONG, BLOG)
         */
        public DataColumn(String colName, COLTYPE colType) {
            this.colName = colName;
            this.colType = colType;
            this.length = 0;
        }

    }

    public static class DataForeignKey {

        public String columnName;
        public String foreignTable;
        public List<String> listForeignColumns;

        public String getForeignName() {
            return "FK_" + columnName;
        }

        public static DataForeignKey getInstance(String columnName, String foreignTable, List<String> listForeignColumns) {
            DataForeignKey dataForeignKey = new DataForeignKey();
            dataForeignKey.columnName = columnName;
            dataForeignKey.foreignTable = foreignTable;
            dataForeignKey.listForeignColumns = listForeignColumns;
            return dataForeignKey;
        }
    }

    public static class DataDefinition {

        public String tableName;
        public List<DataColumn> listColumns;
        public Map<String, List<String>> mapIndexes = new HashMap<>();

        public Map<String, List<String>> mapConstraints = new HashMap<>();

        public Map<String, DataForeignKey> mapForeignConstraints = new HashMap<>();

        public DataDefinition(String tableName) {
            this.tableName = tableName;
        }

        public String getSqlTableName() {
            return tableName.toLowerCase();
        }

        public DataColumn getColumn(String colName) {
            for (DataColumn col : listColumns) {
                if (colName.equalsIgnoreCase(col.colName))
                    return col;
            }
            return null;
        }
    }

    /**
     * @param table
     * @param con
     * @return
     */
    public List<BEvent> checkCreateDatase(DataDefinition table, final Connection con) {

        final List<BEvent> listEvents = new ArrayList<>();
        StringBuffer logAnalysis = new StringBuffer();
        logAnalysis.append("CheckData [" + table.getSqlTableName() + "] ");

        try {
            final DatabaseMetaData dbm = con.getMetaData();
            // check if "employee" table is there
            // nota: don't use the patern, it not give a correct result with H2
            final ResultSet tables = dbm.getTables(null, null, null, null);

            boolean exist = false;
            while (tables.next()) {
                final String tableName = tables.getString("TABLE_NAME");
                if (tableName.equalsIgnoreCase(table.getSqlTableName())) {
                    exist = true;
                    break;
                }
            }
            logAnalysis.append("Table [" + table.getSqlTableName() + "] exist? " + exist + ";");
            if (exist) {
                updateTable(table, logAnalysis, con);
            } else {
                createTable(table, logAnalysis, con);
            }
            Map<String, IndexDescription> mapIndexes = readIndexDescription(table, con);

            createIndexes(table, mapIndexes, logAnalysis, con);
            createConstraints(table, mapIndexes, logAnalysis, con);

        } catch (final SqlExceptionRequest e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();

            logAnalysis.append(" ERROR during checkCreateDatase table[" + table.getSqlTableName()
                    + "] sqlRequest[" + e.sqlRequest
                    + "] " + e.sqlException.toString() + " : " + exceptionDetails);
            listEvents.add(new BEvent(eventCreationDatabase, e, "table[" + table.tableName + "] sqlRequest[" + e.sqlRequest + "] " + e.sqlException.toString()));

        } catch (final SQLException e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();

            logAnalysis.append(" ERROR during checkCreateDatase table[" + table.getSqlTableName()
                    + "] " + e.toString() + " : " + exceptionDetails);
            listEvents.add(new BEvent(eventCreationDatabase, e, "table[" + table.tableName + "] " + e.toString()));

        }
        if (BEventFactory.isError(listEvents))
            logger.severe(loggerLabel + logAnalysis.toString());
        else
            logger.info(loggerLabel + logAnalysis.toString());
        return listEvents;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* operations on record */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    private class CLobData {
        public String textContent;
    }
    public List<Map<String, Object>> executeSqlRequest(String sqlRequest, List<Object> parameters, int maximumResult, Connection con) throws Exception {
        List<Map<String, Object>> listResult = new ArrayList<>();
        try {
            PreparedStatement pstmt = con.prepareStatement(sqlRequest);
            ResultSet rs = null;
            if (parameters!=null)
                for (int i = 0; i < parameters.size(); i++)
                    pstmt.setObject(i + 1, parameters.get(i));
            rs = pstmt.executeQuery();

            ResultSetMetaData rsMeta = pstmt.getMetaData();
            while (rs.next()) {
                Map<String, Object> record = new HashMap<>();
                for (int columnIndex = 1; columnIndex <= rsMeta.getColumnCount(); columnIndex++) {
                    Object databaseObject = rs.getObject(columnIndex);
                    int columnType = rsMeta.getColumnType(columnIndex);
                    if (databaseObject instanceof Clob) {
                        Clob clob = (Clob) databaseObject;
                        record.put(rsMeta.getColumnLabel(columnIndex).toUpperCase(), clob);
                        /*
                        CLobData cLobData = new CLobData();
                        
                        cLobData.textContent = new BufferedReader(new InputStreamReader(clob.getAsciiStream(), StandardCharsets.UTF_8) )
                                  .lines()
                                  .collect(Collectors.joining("\n"));
                        
                        record.put(rsMeta.getColumnLabel(columnIndex).toUpperCase(), cLobData);
                        */
                    }
                    // simple type, no question
                    else if (databaseObject instanceof Long 
                            || databaseObject instanceof String
                            || databaseObject instanceof Float
                            || databaseObject instanceof Double
                        || databaseObject instanceof Integer)
                        record.put(rsMeta.getColumnLabel(columnIndex).toUpperCase(), databaseObject);
                    // detect a Blog : get the InputStream then
                    else if (columnType == Types.BINARY || columnType == Types.BLOB) {
                        InputStream inputStream = rs.getBinaryStream(columnIndex);
                        record.put(rsMeta.getColumnLabel(columnIndex).toUpperCase(), inputStream);
                        /*
                        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                        byte[] buf = new byte[8192];
                        int length;
                        while ((length = inputStream.read(buf)) > 0) {
                            outputStream.write(buf, 0, length);
                        }
                        byte[] data = outputStream.toByteArray();

                        ByteArrayInputStream byteInputStream = new ByteArrayInputStream(data);
                        record.put(rsMeta.getColumnLabel(columnIndex).toUpperCase(), byteInputStream);
                        inputStream.close();
                        */
                    } else
                        record.put(rsMeta.getColumnLabel(columnIndex).toUpperCase(), databaseObject);
                }
                listResult.add(record);
                if (listResult.size() >= maximumResult)
                    return listResult;
            }
            return listResult;
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * 
     * @param tableName
     * @param record
     * @param informationContext
     * @param con
     * @return
     */
    public List<BEvent> insert(String tableName, Map<String, Object> record, String informationContext, Connection con) {
        List<BEvent> listEvents = new ArrayList<>();
        PreparedStatement pstmt = null;
        try {
            List<Object> pObject = new ArrayList<>();
            StringBuilder sqlPartCol = new StringBuilder();
            StringBuilder sqlPartVal = new StringBuilder();

            int count = 0;
            for (Entry<String, Object> entry : record.entrySet()) {
                if (count > 0) {
                    sqlPartCol.append(",");
                    sqlPartVal.append(",");
                }
                count++;
                sqlPartCol.append(entry.getKey());
                sqlPartVal.append("? ");
                if (entry.getValue() instanceof CLobData)
                {
                    Clob clob = con.createClob();
                    clob.setString(1, ((CLobData) entry.getValue()).textContent );
                    pObject.add(clob);
                } // Document is read as a InputStream 
                else if (entry.getValue() instanceof InputStream ) {
                    pObject.add(entry.getValue());
                    
                } else
                    pObject.add(entry.getValue());
            }

            StringBuilder sqlRequest = new StringBuilder();
            sqlRequest.append("insert into " + tableName);
            sqlRequest.append(" (" + sqlPartCol.toString() + ") ");
            sqlRequest.append(" values (" + sqlPartVal.toString() + ")");

            pstmt = con.prepareStatement(sqlRequest.toString());
            for (int i = 0; i < pObject.size(); i++) {
                if (pObject.get(i) instanceof InputStream) {
                    InputStream in = (InputStream) pObject.get(i);
                    // see the content now
                    /*
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    byte[] buf = new byte[8192];
                    int length;
                    while ((length = ((InputStream) pObject.get(i)).read(buf)) > 0) {
                        outputStream.write(buf, 0, length);
                    }
                    byte[] data = outputStream.toByteArray();
                    logger.info("Size input "+data.length);
                    
                    ByteArrayInputStream byteInputStream = new ByteArrayInputStream(data);
                    
                    pstmt.setBinaryStream(i + 1, byteInputStream);
                    
                    */
                    pstmt.setBinaryStream(i + 1, in);
                }
                else
                    pstmt.setObject(i + 1, pObject.get(i));
            }
            pstmt.execute();
        } catch (Exception e) {
            listEvents.add(new BEvent(eventInsertError, e, "Table [" + tableName + "] " + informationContext+ " "+e.getMessage()));
        } finally {
            if (pstmt != null)
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    // no need to trace anything here
                }
        }
        return listEvents;
    }
    
    /**
     * 
     * @param sqlRequest
     * @param con
     * @return
     */
    public List<BEvent> executeSql(String sqlRequest, Connection con) {
        List<BEvent> listEvents = new ArrayList<>();
        try {
            executeAlterSql(Arrays.asList(sqlRequest), con );
        } catch (Exception e ) {
            listEvents.add( new BEvent( eventSqlError, e, "SqlRequest["+sqlRequest+"]" ));
        }
        return listEvents;
    }
    /* ******************************************************************************** */
    /*                                                                                  */
    /* operations on table */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * Update the table
     * 
     * @param table
     * @param con
     * @throws SQLException
     */
    private void updateTable(DataDefinition table, StringBuffer logAnalysis, final Connection con) throws SqlExceptionRequest, SQLException {
        final DatabaseMetaData dbm = con.getMetaData();
        final String databaseProductName = dbm.getDatabaseProductName();

        final Map<String, DataColumn> listColsExpected = new HashMap<>();
        for (DataColumn col : table.listColumns)
            listColsExpected.put(col.colName.toUpperCase(), col);

        final Map<String, DataColumn> alterCols = new HashMap<>();

        // Table exists : is the fields are correct ?
        final ResultSet rs = dbm.getColumns(null /* catalog */, null /* schema */, null /* cstSqlTableName */,
                null /* columnNamePattern */);

        while (rs.next()) {
            String tableNameCol = rs.getString("TABLE_NAME");
            final String colName = rs.getString("COLUMN_NAME");
            final int length = rs.getInt("COLUMN_SIZE");

            tableNameCol = tableNameCol == null ? "" : tableNameCol.toLowerCase();

            if (!tableNameCol.equalsIgnoreCase(table.getSqlTableName())) {
                continue;
            }
            // final int dataType = rs.getInt("DATA_TYPE");
            DataColumn column = table.getColumn(colName);
            if (column == null) {
                logAnalysis.append("Colum[" + colName.toLowerCase() + "] is not expected, keep it;");

            } else if (column.length != 0 && length < column.length) {
                logAnalysis.append("Colum[" + colName.toLowerCase() + "] length[" + length + "] expected["
                        + column.length + "];");
                alterCols.put(colName.toLowerCase(), column);
                listColsExpected.remove(colName.toLowerCase());

            } else
                listColsExpected.remove(colName.toUpperCase());
            // logAnalysis+="Remove Colum[" + colName.toLowerCase() + "] : list is now [ " + listColsExpected + "];";
        }
        // OK, create all missing column
        for (final DataColumn col : listColsExpected.values()) {
            String sqlRequest = "alter table " + table.getSqlTableName() + " add  " + getSqlField(col, databaseProductName);
            logAnalysis.append(sqlRequest + ";");

            executeAlterSql(Arrays.asList(sqlRequest), con);
        }
        // all change operation
        for (final DataColumn col : alterCols.values()) {
            String sqlRequest = "alter table " + table.getSqlTableName() + " alter column "
                    + getSqlField(col, databaseProductName);
            logAnalysis.append(sqlRequest + ";");

            executeAlterSql(Arrays.asList(sqlRequest), con);
        }
        logAnalysis.append("CheckCreateTable [" + table.getSqlTableName() + "] : Correct ");
        // add the constraint
        /*
         * String constraints = "alter table "+ cstSqlTableName + " add constraint uniq_propkey unique ("+
         * cstSqlTenantId+","
         * + cstSqlResourceName+","
         * + cstSqldomainName+","
         * + cstSqlPropertiesKey+")";
         * executeAlterSql(con, constraints);
         */

    }

    /**
     * Create a table
     * 
     * @param table
     * @param logAnalysis
     * @param con
     * @throws SQLException
     */
    private void createTable(DataDefinition table, StringBuffer logAnalysis, final Connection con) throws SqlExceptionRequest, SQLException {
        // create the table
        final DatabaseMetaData dbm = con.getMetaData();
        final String databaseProductName = dbm.getDatabaseProductName();

        StringBuffer createTableString = new StringBuffer();
        createTableString.append("create table " + table.getSqlTableName() + " (");
        for (int i = 0; i < table.listColumns.size(); i++) {
            if (i > 0)
                createTableString.append(", ");
            createTableString.append(getSqlField(table.listColumns.get(i), databaseProductName));
        }
        createTableString.append(")");
        logAnalysis.append("NOT EXIST : create it with script[" + createTableString.toString() + "]");
        executeAlterSql(Arrays.asList(createTableString.toString()), con);

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* operations on Index and constraints */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    private class IndexDescription {

        public String tableName;
        public String indexName;
        public boolean isUnique;
        public Set<String> columns = new HashSet<>();
    }

    private Map<String, IndexDescription> readIndexDescription(DataDefinition table, Connection con) throws SQLException {
        Map<String, IndexDescription> mapIndexes = new HashMap<>();

        ResultSet rs = null;
        try {
            DatabaseMetaData dbm = con.getMetaData();
            /** unique=false to get all informations */
            rs = dbm.getIndexInfo(null, null, table.getSqlTableName(), false, false);

            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                if (indexName == null)
                    continue;
                indexName = indexName.toLowerCase();
                IndexDescription indexDescription = mapIndexes.get(indexName);
                if (indexDescription == null) {
                    indexDescription = new IndexDescription();
                    indexDescription.tableName = table.getSqlTableName();
                    indexDescription.indexName = indexName;
                    mapIndexes.put(indexName, indexDescription);
                }

                final String columnName = rs.getString("COLUMN_NAME");
                // String indexQualifier = rs.getString("INDEX_QUALIFIER");
                indexDescription.isUnique = !rs.getBoolean("NON_UNIQUE");

                // nonUnique = true => index (else constraints)
                indexDescription.columns.add(columnName.toLowerCase());
            }
            rs.close();
            rs = null;
            // same with unique
            rs = dbm.getIndexInfo(null, null, table.getSqlTableName(), true, false);

            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                if (indexName == null)
                    continue;

                indexName = indexName.toLowerCase();
                IndexDescription indexDescription = mapIndexes.get(indexName);
                if (indexDescription == null) {
                    indexDescription = new IndexDescription();
                    indexDescription.tableName = table.getSqlTableName();
                    indexDescription.indexName = indexName;
                    mapIndexes.put(indexName, indexDescription);
                }

                final String columnName = rs.getString("COLUMN_NAME");
                // String indexQualifier = rs.getString("INDEX_QUALIFIER");
                indexDescription.isUnique = !rs.getBoolean("NON_UNIQUE");

                // nonUnique = true => index (else constraints)
                indexDescription.columns.add(columnName.toLowerCase());
            }
        } catch (SQLException e) {
            // do nothing here
            logger.severe("Error during getIndexInfo " + e.getMessage());
            throw e;
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (SQLException e) {
                    // do nothing here
                }
        }
        return mapIndexes;

    }

    private enum STATUSINDEX {
        NOTEXIST, DIFFERENT, EXIST
    }

    /**
     * @param tableName
     * @param indexName
     * @param isUniq
     * @param columns
     * @param mapIndexes
     * @return
     */
    private STATUSINDEX checkExistIndexes(String tableName, String indexName, boolean isUniq, List<String> columns, Map<String, IndexDescription> mapIndexes) {
        for (IndexDescription indexDescription : mapIndexes.values()) {
            if (!indexDescription.tableName.equalsIgnoreCase(tableName) ||
                    !indexDescription.indexName.equalsIgnoreCase(indexName))
                continue;
            // one index exist !
            if (indexDescription.isUnique != isUniq)
                return STATUSINDEX.DIFFERENT;
            // compare columns
            if (indexDescription.columns.size() != columns.size())
                return STATUSINDEX.DIFFERENT;
            // each columns name must exist
            for (String col : columns) {
                if (!indexDescription.columns.contains(col.toLowerCase()))
                    return STATUSINDEX.DIFFERENT;
            }
            return STATUSINDEX.EXIST;
        }
        return STATUSINDEX.NOTEXIST;

    }

    /**
     * @param table
     * @param logAnalysis
     * @param con
     * @throws SQLException
     */
    private void createConstraints(DataDefinition table, Map<String, IndexDescription> mapIndexes, StringBuffer logAnalysis, Connection con) throws SqlExceptionRequest {
        for (Entry<String, List<String>> constraint : table.mapConstraints.entrySet()) {
            List<String> listSqlOrder = new ArrayList<>();
            // is this constraints exist?
            STATUSINDEX status = checkExistIndexes(table.getSqlTableName(), constraint.getKey(), true, constraint.getValue(), mapIndexes);
            if (status == STATUSINDEX.EXIST)
                continue;
            if (status == STATUSINDEX.DIFFERENT) {
                // drop it before
                listSqlOrder.add("DROP INDEX " + constraint.getKey() + " ON " + table.getSqlTableName());
            }

            StringBuffer listOfFields = new StringBuffer();
            for (int i = 0; i < constraint.getValue().size(); i++) {
                if (i > 0)
                    listOfFields.append(", ");
                listOfFields.append(constraint.getValue().get(i));
            }
            // ALTER TABLE Persons  ADD CONSTRAINT UC_Person UNIQUE (ID,LastName);
            //  String createIndex = "ALTER TABLE " + table.tableName + " ADD UNIQUE INDEX " + constraint.getKey() + " ON " + "(" + listOfFields.toString() + ")";
            listSqlOrder.add("CREATE UNIQUE INDEX " + constraint.getKey() + " ON " + table.getSqlTableName() + "(" + listOfFields.toString() + ")");

            executeAlterSql(listSqlOrder, con);
        }

        for (Entry<String, DataForeignKey> foreignConstraint : table.mapForeignConstraints.entrySet()) {
            List<String> listSqlOrder = new ArrayList<>();
            // is this constraints exist?
            STATUSINDEX status = checkExistIndexes(table.getSqlTableName(), foreignConstraint.getValue().getForeignName(), true, foreignConstraint.getValue().listForeignColumns, mapIndexes);
            if (status == STATUSINDEX.EXIST)
                continue;
            if (status == STATUSINDEX.DIFFERENT) {
                // drop it before
                listSqlOrder.add("DROP FOREIGN KEY " + foreignConstraint.getValue().getForeignName() + " ON " + table.getSqlTableName());
            }

            StringBuffer listOfFields = new StringBuffer();
            for (int i = 0; i < foreignConstraint.getValue().listForeignColumns.size(); i++) {
                if (i > 0)
                    listOfFields.append(", ");
                listOfFields.append(foreignConstraint.getValue().listForeignColumns.get(i));
            }
            //  ALTER TABLE Orders ADD CONSTRAINT FK_PersonOrder FOREIGN KEY (PersonID) REFERENCES Persons(PersonID);           
            //  String createIndex = "ALTER TABLE " + table.tableName + " ADD UNIQUE INDEX " + constraint.getKey() + " ON " + "(" + listOfFields.toString() + ")";
            listSqlOrder.add("ALTER TABLE " + table.tableName + " ADD CONSTRAINT " + foreignConstraint.getValue().getForeignName()
                    + " REFERENCES " + foreignConstraint.getValue().foreignTable + "(" + listOfFields.toString() + ")");

            executeAlterSql(listSqlOrder, con);
        }

    }

    /**
     * @param table
     * @param logAnalysis
     * @param con
     * @throws SQLException
     */
    private void createIndexes(DataDefinition table, Map<String, IndexDescription> mapIndexes, StringBuffer logAnalysis, Connection con) throws SqlExceptionRequest {

        for (Entry<String, List<String>> index : table.mapIndexes.entrySet()) {
            List<String> listSqlOrder = new ArrayList<>();
            // is this constraints exist?
            STATUSINDEX status = checkExistIndexes(table.getSqlTableName(), index.getKey(), false, index.getValue(), mapIndexes);
            if (status == STATUSINDEX.EXIST)
                continue;
            if (status == STATUSINDEX.DIFFERENT) {
                // drop it before
                listSqlOrder.add("DROP INDEX " + index.getKey() + " ON " + table.getSqlTableName());
            }

            StringBuffer listOfFields = new StringBuffer();
            for (int i = 0; i < index.getValue().size(); i++) {
                if (i > 0)
                    listOfFields.append(", ");
                listOfFields.append(index.getValue().get(i));
            }

            listSqlOrder.add("CREATE INDEX " + index.getKey() + " ON " + table.getSqlTableName() + "(" + listOfFields.toString() + ")");
            executeAlterSql(listSqlOrder, con);
        }
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Basic operation */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public static class SqlExceptionRequest extends Exception {

        private static final long serialVersionUID = 1L;
        public final SQLException sqlException;
        public final String sqlRequest;

        public SqlExceptionRequest(SQLException sqlException, String sqlRequest) {
            this.sqlException = sqlException;
            this.sqlRequest = sqlRequest;
        }
    }

    /**
     * @param con
     * @param sqlRequest
     * @throws SQLException
     */
    private void executeAlterSql(final List<String> listSqlRequests, final Connection con) throws SqlExceptionRequest {
        for (String sqlRequest : listSqlRequests) {
            logger.fine(loggerLabel + "executeAlterSql : Execute [" + sqlRequest + "]");

            try (Statement stmt = con.createStatement()) {
                stmt.executeUpdate(sqlRequest);

                if (!con.getAutoCommit()) {
                    con.commit();
                }
            } catch (SQLException e) {
                throw new SqlExceptionRequest(e, sqlRequest);
            }
        }

    }

    public class TypeTranslation {

        public COLTYPE colType;
        public Map<String, String> translationTable = new HashMap<>();

        public TypeTranslation(COLTYPE colType, String oracle, String postGres, String h2, String mySql, String sqlServer, String def) {
            this.colType = colType;
            translationTable.put(CST_DRIVER_ORACLE, oracle);
            translationTable.put(CST_DRIVER_POSTGRESQL, postGres);
            translationTable.put(CST_DRIVER_H2, h2);
            translationTable.put(CST_DRIVER_MYSQL, mySql);
            translationTable.put(CST_DRIVER_SQLSERVER, sqlServer);
            translationTable.put("def", def);
        }

        public String getValue(String databaseName) {
            if (translationTable.get(databaseName) != null)
                return translationTable.get(databaseName);
            return translationTable.get("def");
        }

    }

    /* String oracle, String postGres, String h2, String mySql, String sqlServer, String def */
    private List<TypeTranslation> allTransations = Arrays.asList(
            new TypeTranslation(COLTYPE.LONG, "NUMBER", "BIGINT", "BIGINT", "BIGINT", "NUMERIC(19, 0)", "NUMBER"),
            new TypeTranslation(COLTYPE.BOOLEAN, "NUMBER(1)", "BOOLEAN", "BOOLEAN", "BOOLEAN", "BIT", "BOOLEAN"),
            new TypeTranslation(COLTYPE.DECIMAL, "NUMERIC(19,5)", "NUMERIC(19,5)", "NUMERIC(19,5)", "NUMERIC(19,5)", "NUMERIC(19,5)", "NUMERIC(19,5)"),
            new TypeTranslation(COLTYPE.BLOB, "BLOB", "BYTEA", "MEDIUMBLOB", "MEDIUMBLOB", "VARBINARY(MAX)", "BLOB"),
            new TypeTranslation(COLTYPE.TEXT, "CLOB", "TEXT", "CLOB", "MEDIUMTEXT", "NVARCHAR(MAX)", "TEXT"),
            new TypeTranslation(COLTYPE.STRING, "VARCHAR2(%d CHAR)", "VARCHAR(%d)", "VARCHAR(%d)", "VARCHAR(%d)", "NVARCHAR(%d)", "VARCHAR(%d)"));

    /**
     * calculate the field according different database
     *
     * @param colName
     * @param colSize
     * @param databaseProductName
     * @return
     */

    private String getSqlField(final DataColumn col, final String databaseProductName) {
        for (TypeTranslation typeTranslation : allTransations) {
            if (typeTranslation.colType == col.colType) {
                String value = String.format(typeTranslation.getValue(databaseProductName), col.length);
                return col.colName + " " + value;
            }
        }
        return "";
        /*
         * if (col.colType == COLTYPE.LONG) {
         * // long
         * if (CST_DRIVER_ORACLE.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " NUMBER ";
         * } else if (CST_DRIVER_POSTGRESQL.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " BIGINT";
         * } else if (CST_DRIVER_H2.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " BIGINT";
         * }
         * return col.colName + " BIGINT";
         * }
         * if (col.colType == COLTYPE.BOOLEAN) {
         * if (CST_DRIVER_ORACLE.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " NUMBER(1) ";
         * } else if (CST_DRIVER_POSTGRESQL.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " BIGINT";
         * } else if (CST_DRIVER_H2.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " BIGINT";
         * }
         * return col.colName + " BIGINT";
         * }
         * if (col.colType == COLTYPE.DECIMAL) {
         * if (CST_DRIVER_ORACLE.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " NUMERIC(19,5) ";
         * } else if (CST_DRIVER_POSTGRESQL.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " NUMERIC(19,5) ";
         * } else if (CST_DRIVER_H2.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " NUMERIC(19,5) ";
         * }
         * return col.colName + " NUMERIC(19,5) ";
         * }
         * if (col.colType == COLTYPE.BLOB) {
         * if (CST_DRIVER_ORACLE.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " BLOB ";
         * } else if (CST_DRIVER_POSTGRESQL.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " bytea";
         * } else if (CST_DRIVER_H2.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " BLOB";
         * }
         * return col.colName + " BLOB";
         * }
         * if (col.colType == COLTYPE.TEXT) {
         * if (CST_DRIVER_ORACLE.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " CLOB ";
         * } else if (CST_DRIVER_POSTGRESQL.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " TEXT";
         * } else if (CST_DRIVER_H2.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " CLOB";
         * }
         * // MySql
         * return col.colName + " MEDIUMTEXT";
         * // SQL SERVEUR
         * // NVARCHAR(MAX)
         * }
         */
        /*
         * varchar
         * if (CST_DRIVER_ORACLE.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " VARCHAR2(" + col.length + ")";
         * }
         * if (CST_DRIVER_POSTGRESQL.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " varchar(" + col.length + ")"; // old varying
         * } else if (CST_DRIVER_H2.equalsIgnoreCase(databaseProductName)) {
         * return col.colName + " varchar(" + col.length + ")";
         * } else {
         * return col.colName + " varchar(" + col.length + ")";
         * }
         */
    }
}
