package org.alecuba16;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

public class Main {
    private static final String tableName = "test";
    private static final String columnFamily = "cf";
    static Connection conn = null;

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zoo");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.cluster.distributed", "true");

        Admin hBaseAdmin = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            hBaseAdmin  = conn.getAdmin();

            TableName tname = TableName.valueOf(tableName);
            TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tname);
            ColumnFamilyDescriptorBuilder columnDescBuilder = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(columnFamily)).setBlocksize(32 * 1024)
                    .setCompressionType(Compression.Algorithm.NONE).setDataBlockEncoding(DataBlockEncoding.NONE);
            tableDescBuilder.setColumnFamily(columnDescBuilder.build());
            TableDescriptor tableDescriptor = tableDescBuilder.build();

            if (hBaseAdmin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("Table " + tableName + " is available.");
                hBaseAdmin.disableTable(TableName.valueOf(tableName));
                hBaseAdmin.modifyTable(tableDescriptor);
                hBaseAdmin.enableTable(TableName.valueOf(tableName));
            } else {
                System.out.println("Table " + tableName + " is not available.");
                hBaseAdmin.createTable(tableDescriptor);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (hBaseAdmin != null) {
                hBaseAdmin.close();
            }
        }

        // read to HBase
        getAllRecord(tableName);

        // write to HBase
        addRecord(tableName, "rowkey2", columnFamily, columnFamily, "value2");

        // read to HBase
        getAllRecord(tableName);

        conn.close();

    }

    public static void getAllRecord(String tableName) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for (Result r : ss) {
                for (Cell cell : r.listCells()) {
                    String row = new String(CellUtil.cloneRow(cell));
                    String family = new String(CellUtil.cloneFamily(cell));
                    String column = new String(CellUtil.cloneQualifier(cell));
                    String value = new String(CellUtil.cloneValue(cell));
                    long timestamp = cell.getTimestamp();
                    System.out.printf("%-20s column=%s:%s, timestamp=%s, value=%s\n", row, family, column, timestamp, value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // put one row
    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    Bytes.toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}