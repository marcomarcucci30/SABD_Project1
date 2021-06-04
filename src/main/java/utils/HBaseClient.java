package utils;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class HBaseClient {

    /**
	 * 
	 */
	/* Configuration Parameters */
    private static final String ZOOKEEPER_HOST = "hbase-docker";
    private static final String ZOOKEEPER_PORT = "2181";
    private static final String HBASE_MASTER  = "hbase-docker:16000";
    private static final int HBASE_MAX_VERSIONS = Integer.MAX_VALUE;


    private static final boolean DEBUG = false;

    public enum ALTER_COLUMN_FAMILY {
        ADD, DELETE
    }

    private Connection connection = null;

    private static byte[] b(String s){
        return Bytes.toBytes(s);
    }

    /**
     * Create a connection with HBase
     * @return Connection
     * @throws IOException
     * @throws ServiceException
     */
    public Connection getConnection() throws IOException, ServiceException {

        if (!(connection == null || connection.isClosed() || connection.isAborted()))
            return connection;

//        if (!DEBUG)
//            Logger.getRootLogger().setLevel(Level.ERROR);

        Configuration conf  = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_HOST);
        conf.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
        conf.set("hbase.master", HBASE_MASTER);

        /* Check configuration */
        HBaseAdmin.checkHBaseAvailable(conf);

        if (DEBUG)
            System.out.println("HBase is running!");

        this.connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    /* *******************************************************************************
    *  Database administration
    * ******************************************************************************* */

    /**
     * Create a new table named tableName, with the specified columnFamilies
     *
     * @param tableName
     * @param columnFamilies
     * @return
     */
    public boolean createTable(String tableName, String... columnFamilies) {

        try {

            Admin admin = getConnection().getAdmin();
            HTableDescriptor tableDescriptor = new HTableDescriptor(
                    TableName.valueOf(tableName));


            for (String columnFamily : columnFamilies) {
                HColumnDescriptor cd = new HColumnDescriptor(columnFamily);
                cd.setMaxVersions(HBASE_MAX_VERSIONS);
                tableDescriptor.addFamily(cd);
            }

            admin.createTable(tableDescriptor);
            return true;

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return false;

    }

    /**
     * Retrieve the list of tables on the HBase data store
     *
     * @return
     */
    public List<String> listTables(){

        List<String> tables = new ArrayList<>();

        try {
            Admin admin = getConnection().getAdmin();
            HTableDescriptor[] tableDescriptor = admin.listTables();

            /* Save the table name as a string */
            for (int i=0; i<tableDescriptor.length;i++ ){
                String tableName = tableDescriptor[i].getNameAsString();
                tables.add(tableName);
            }

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return tables;

    }

    /**
     * Describe the table.
     * This function returns a serialization of the columnFamilies of the table.
     *
     * @param table name of the table to describe
     * @return comma separated list of columns
     */
    public String describeTable(String table){

        try {
            Admin admin = getConnection().getAdmin();
            TableName tableName = TableName.valueOf(table);
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);

            String columnFamilies = "";
            HColumnDescriptor[] columnDescriptors = tableDescriptor.getColumnFamilies();

            for (int j = 0; j < columnDescriptors.length; j++){
                columnFamilies += columnDescriptors[j].getNameAsString();

                if (j < columnDescriptors.length - 1)
                    columnFamilies += ", ";
            }

            return tableName + ": " + columnFamilies;

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return null;

    }

    /**
     * Add or remove a column family from a table.
     *
     * @param operation add or remove
     * @param table     tablename
     * @param columnFamily  name of the column family to add or remove
     * @return true if the operation is completed successfully
     */
    public boolean alterColumnFamily(ALTER_COLUMN_FAMILY operation, String table, String columnFamily){

        try {
            Admin admin = getConnection().getAdmin();
            TableName tableName = TableName.valueOf(table);


            switch (operation){
                case ADD:
                    HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
                    admin.addColumn(tableName, columnDescriptor);
                    break;
                case DELETE:
                    admin.deleteColumn(tableName, Bytes.toBytes(columnFamily));
                    break;
                default:
                    return false;
            }

            return true;

        } catch (IOException | ServiceException e) {
//            e.printStackTrace();
        }

        return false;

    }

    /**
     * Check if a table exists
     * @param table table name
     * @return  true if a table exists
     */
    public boolean exists(String table){

        try {

            Admin admin = getConnection().getAdmin();
            TableName tableName = TableName.valueOf(table);
            return admin.tableExists(tableName);

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return false;

    }

    /**
     * Drop a table and all its content
     *
     * @param table table to be deleted
     * @return true if a table has been deleted
     */
    public boolean dropTable(String table) {

        try {
            Admin admin = getConnection().getAdmin();
            TableName tableName = TableName.valueOf(table);

            // To delete a table or change its settings, you need to first disable the table
            admin.disableTable(tableName);

            // Delete the table
            admin.deleteTable(tableName);

            return true;

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return false;
    }


    /**
     * Erase the content of the table.
     *
     * @param table     table name
     * @param preserveSplits    remove the split from region servers
     * @return          true if the table has been erased
     */
    public boolean truncateTable(String table, boolean preserveSplits) {

        try {
            Admin admin = getConnection().getAdmin();
            TableName tableName = TableName.valueOf(table);

            // To delete a table or change its settings, you need to first disable the table
            admin.disableTable(tableName);

            // Truncate the table
            admin.truncateTable(tableName, preserveSplits);

            return true;

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return false;
    }


    /* *******************************************************************************
    *  CRUD operations
    * ******************************************************************************* */

    /**
     * Insert a new row in a table.
     *
     * To put a record, this method should be executed by providing triples of
     *  columnFamily, column, value
     *
     * If a columnFamily:column already exists, the value is updated.
     * Values are stacked according to their timestamp.
     *
     * @param table     table name
     * @param rowKey    row name
     * @param columns   columnFamily, column, value
     * @return          true if the record is inserted
     */
    public boolean put(String table, String rowKey, String... columns){

        if (columns == null || (columns.length % 3 != 0)) {
            // Invalid usage of the function; columns should contain 3-ple in the
            // following format:
            // - columnFamily
            // - column
            // - value
            return false;
        }

        try {

            Table hTable = getConnection().getTable(TableName.valueOf(table));

            Put p = new Put(b(rowKey));

            for (int i = 0; i < (columns.length / 3); i++){

                String columnFamily = columns[i * 3];
                String column       = columns[i * 3 + 1];
                String value        = columns[i * 3 + 2];

                p.addColumn(b(columnFamily), b(column), b(value));

            }

            // Saving the put Instance to the HTable.
            hTable.put(p);

            // closing HTable
            hTable.close();

            return true;
        } catch (IOException | ServiceException e) {
//            e.printStackTrace();
        }

        return false;
    }


    /**
     *
     * To get everything for a row, instantiate a Get object with the row to get.
     *
     * To further narrow the scope of what to Get, use the methods below.
     * - To get all columns from specific families, execute addFamily for each family to retrieve.
     * - To get specific columns, execute addColumn for each column to retrieve.
     * - To only retrieve columns within a specific range of version timestamps, execute setTimeRange.
     * - To only retrieve columns with a specific timestamp, execute setTimestamp.
     * - To limit the number of versions of each column to be returned, execute setMaxVersions.
     *
     * @param table     table to query
     * @param rowKey    row key to retrieve
     * @param columnFamily  columnFamily to retrieve
     * @param column        column to retrieve
     * @return          the value stored in the rowKey:columnFamily:column
     */
    public String get(String table, String rowKey, String columnFamily, String column){

        try {

            // Instantiating HTable class
            Table hTable = getConnection().getTable(TableName.valueOf(table));

            Get g = new Get(b(rowKey));

            // Narrowing the scope
            // g.addFamily(b(columnFamily));
            // g.addColumn(b(columnFamily), b(column));

            // Reading the data
            Result result = hTable.get(g);

            byte [] value = result.getValue(b(columnFamily), b(column));
            return Bytes.toString(value);

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     *
     * To get everything for a row, instantiate a Get object with the row to get.
     *
     * To further narrow the scope of what to Get, use the methods below.
     * - To get all columns from specific families, execute addFamily for each family to retrieve.
     * - To get specific columns, execute addColumn for each column to retrieve.
     * - To only retrieve columns within a specific range of version timestamps, execute setTimeRange.
     * - To only retrieve columns with a specific timestamp, execute setTimestamp.
     * - To limit the number of versions of each column to be returned, execute setMaxVersions.
     *
     * @param table     table to query
     * @param rowKey    row key to retrieve
     * @param columnFamily  columnFamily to retrieve
     * @param column        column to retrieve
     * @return          the value stored in the rowKey:columnFamily:column
     */
    public Map<Long,String> getAllVersions(String table, String rowKey, String columnFamily, String column){

        try {

            // Instantiating HTable class
            Table hTable = getConnection().getTable(TableName.valueOf(table));

            Get g = new Get(b(rowKey));

            // Narrowing the scope
            if (columnFamily != null)
                g.addFamily(b(columnFamily));

            if (columnFamily != null && column != null)
                g.addColumn(b(columnFamily), b(column));

            g.setMaxVersions(HBASE_MAX_VERSIONS);

            Result result = hTable.get(g);
            List<Cell> cells = result.getColumnCells(b(columnFamily), b(column));

            // Reading the data
            Map<Long, String> retval = new HashMap<>();
            for (Cell c : cells) {
                retval.put(c.getTimestamp(), new String(CellUtil.cloneValue(c)));
            }
            return retval;
        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return null;
    }


    /**
     * Delete a row (or column, or columnFamily) from a table.
     *
     * To delete all column of a columnFamily, set column to null
     * To delete all columnFamilies a rowKey,  set columnFamily to null
     *
     * @param table         table of interest
     * @param rowKey        row key to alter
     * @param columnFamily  columnFamily to alter or delete
     * @param column        column to alter or delete
     * @return              true if the value has been deleted
     */
    public boolean delete(String table, String rowKey, String columnFamily, String column){

        try {

            // Instantiating HTable class
            Table hTable = getConnection().getTable(TableName.valueOf(table));

            // Instantiating Delete class
            Delete delete = new Delete(b(rowKey));

            if (columnFamily != null && column != null)
                delete.addColumn(b(columnFamily), b(column));

            else if (columnFamily != null)
                delete.addFamily(b(columnFamily));

            // deleting the data
            hTable.delete(delete);

            // closing the HTable object
            hTable.close();

            return true;

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return false;
    }


    /**
     * Scan the content of the table.
     *
     * @param table            table to scan
     * @param columnFamily     columnFamily to scan
     * @param column           column to scan
     * @throws IOException
     * @throws ServiceException
     */
    public void scanTable(String table, String columnFamily, String column) throws IOException, ServiceException {

        Table products = getConnection().getTable(TableName.valueOf(table));

        Scan scan = new Scan();

        if (columnFamily != null && column != null)
            scan.addColumn(b(columnFamily), b(column));

        else if (columnFamily != null)
            scan.addFamily(b(columnFamily));

        ResultScanner scanner = products.getScanner(scan);

        // Reading values from scan result
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            System.out.println("Found row : " + result);
        }

        scanner.close();

    }

}
