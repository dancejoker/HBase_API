import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

/**
 * Created by Administrator on 2017/3/13 0013.
 */
public class HBase_API {
    private Configuration configuration;
    @Before
    public  void init(){
        configuration  = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum","192.168.93.3,192.168.93.2,192.168.93.4");
    }

    /**
     * 创建表
     * @throws Exception
     */
    @Test
    public void createTable(){
        Connection con = null;
        HBaseAdmin hBaseAdmin = null;
        try {
            con  = ConnectionFactory.createConnection(configuration);
            hBaseAdmin = (HBaseAdmin) con.getAdmin();
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("hbase_try"));
            //添加列簇
            desc.addFamily(new HColumnDescriptor("f1"));
            desc.addFamily(new HColumnDescriptor("f2"));
            desc.addFamily(new HColumnDescriptor("f3"));
            if(hBaseAdmin.tableExists("hbase_try")){
                System.out.println("table is exist");
                System.exit(0);
            }else {
                hBaseAdmin.createTable(desc);
                System.out.println("create table successful");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(null != con){
                try {
                    con.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(hBaseAdmin != null)
                try {
                    hBaseAdmin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    /**
     * 插入数据
     * @throws Exception
     */
    @Test
    public void insertData(){
        Connection con = null;
        HTable table = null;
        try {
            con = ConnectionFactory.createConnection (configuration);
            table = (HTable) con.getTable(TableName.valueOf("hbase_try"));


            //一个PUT代表一行，构造函数传入的是行健（ROWKEYS）
            Put put = new Put("Bob".getBytes());
            //第一个参数是列族，第二个参数是该列族底下列的列属性，第三个参数是列属性的值
            //第一个行健对应的f1列族下的列属性location所对应的值
            put.addColumn("f1".getBytes(),"location".getBytes(),"Xi`an".getBytes());
            //第一个行健对应的f1列族下的列属性phoneNumber所对应的值
            put.addColumn("f1".getBytes(),"phoneNumber".getBytes(),"110".getBytes());
            //第一个行健对应的f1列族下的列属性Sex所对应的值
            put.addColumn("f1".getBytes(),"Sex".getBytes(),"male".getBytes());

            //第一个行健对应的f2列族下的列属性work所对应的值
            put.addColumn("f2".getBytes(),"work".getBytes(),"nurse".getBytes());
            //第一个行健对应的f2列族下的列属性favorite所对应的值
            put.addColumn("f3".getBytes(),"favorite".getBytes(),"basketball".getBytes());
            //添加第二行
            Put put2 = new Put("jack".getBytes());
            //第一个行健对应的f1列族下的列属性location所对应的值
            put2.addColumn("f1".getBytes(),"location".getBytes(),"Xi`an".getBytes());
            //第一个行健对应的f1列族下的列属性phoneNumber所对应的值
            put2.addColumn("f1".getBytes(),"phoneNumber".getBytes(),"110".getBytes());
            //第一个行健对应的f1列族下的列属性Sex所对应的值
            put2.addColumn("f1".getBytes(),"Sex".getBytes(),"male".getBytes());
            //第一个行健对应的f2列族下的列属性work所对应的值
            put2.addColumn("f2".getBytes(),"work".getBytes(),"nurse".getBytes());
            //第一个行健对应的f2列族下的列属性favorite所对应的值
            put2.addColumn("f3".getBytes(),"favorite".getBytes(),"basketball".getBytes());

            List<Put> puts = new ArrayList<Put>();
            puts.add(put);
            puts.add(put2);
            //添加进表中
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(null != con) {
                try {
                    con.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 向一个列簇中插入多个值
     */
    @Test
    public void insertColumnsValue(){
        Connection con = null;
        HTable table = null;
        try {
            con = ConnectionFactory.createConnection(configuration);
            table = (HTable) con.getTable(TableName.valueOf("hbase_try"));
            Put put = new Put("Bob".getBytes());
            //1.如果没有指定列修饰符（列属性），而在这之下已经有内容，则覆盖原先内容
            //2.如果有指定列修饰符（列属性），而在该列修饰符下如果存在内容则覆盖
            put.addColumn("f1".getBytes(),"location".getBytes(),"Shan`xi".getBytes());

            table.put(put);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(con != null)
                try {
                    con.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if(table != null)
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    /**
     * 遍历hbase表
     */
    @Test
    public void query(){
        Connection con = null;
        HTable table = null;
        ResultScanner scanner = null;
        try {
            con = ConnectionFactory.createConnection(configuration);
            table = (HTable)con.getTable(TableName.valueOf("hbase_try"));
            scanner = table.getScanner(new Scan());
            //遍历RowKey行健
            for(Result rs : scanner){
                //获得Rowkey(行健)
                String rowKey = Bytes.toString(rs.getRow());
                //family(列簇)， qualifiers(列修饰符)，value(列修饰符对应的值)
                NavigableMap<byte[],NavigableMap<byte[],byte[]>> familyMap = rs.getNoVersionMap();
                //遍历列簇
                for(byte[] fByte : familyMap.keySet()){
                    NavigableMap<byte[],byte[]> queMap = familyMap.get(fByte);
                    String familyName = Bytes.toString(fByte);
                    //遍历列属性
                    for(byte[] quaByte : queMap.keySet()){
                        byte[] valueByte = queMap.get(quaByte);
                        String quaName = Bytes.toString(quaByte);
                        String value = Bytes.toString(valueByte);
                        String result = String.format("rowKey : %s  | family : %s  | qualifiers : %s  | value:  %s",rowKey,familyName,quaName,value);
                        System.out.println(result);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(con != null){
                try {
                    con.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(table != null)
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }
    /**
     * 根据rowKey查询
     * 1.如果存储数据时没有存储列修饰符，则cell代表整个列簇的内容，查询出的就是该行下整个列簇的内容
     * 2.如果存储数据时有存储列修饰符，则每个列簇下的列修饰符各有一个cell
     */
    @Test
    public void getData(){
        Connection con = null;
        HTable table = null;
        try {
            con = ConnectionFactory.createConnection(configuration);
            table = (HTable)con.getTable(TableName.valueOf("hbase_try"));
            Get get = new Get(Bytes.toBytes("Bob"));
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                System.out.println("RowName : " + new String(CellUtil.cloneRow(cell)) + " ");
                System.out.println("Timetamp : " + cell.getTimestamp() + " ");
                System.out.println("column Family : " + new String(CellUtil.cloneFamily(cell)));
                System.out.println("row Name : " + new String(CellUtil.cloneQualifier(cell)));
                System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(null != table){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if(null != con){
                 try {
                     con.close();
                 } catch (IOException e) {
                     e.printStackTrace();
                 }
             }
            }
        }
    }

    /**
     * 查询表中的RowKey某个列簇中某列属性的值
     */
    @Test
    public void getResultByColumn(){
        Connection con = null;
        HTable table = null;
        try {
            con = ConnectionFactory.createConnection(configuration);
            table = (HTable)con.getTable(TableName.valueOf("hbase_try"));
            Get get = new Get(Bytes.toBytes("Bob"));
            get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("location"));
            get.setMaxVersions(5);
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                System.out.println("RowName : " + new String(CellUtil.cloneRow(cell)) + " ");
                System.out.println("Timetamp : " + cell.getTimestamp() + " ");
                System.out.println("column Family : " + new String(CellUtil.cloneFamily(cell)));
                System.out.println("row Name : " + new String(CellUtil.cloneQualifier(cell)));
                System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量查询
     */
    @Test
    public void scanData(){
        Connection con = null;
        HTable table = null ;
        ResultScanner scanner = null;
        Scan scan = new Scan();
        try {
            con = ConnectionFactory.createConnection(configuration);
            table = (HTable)con.getTable(TableName.valueOf("hbase_try"));
            scan.setStartRow(Bytes.toBytes("Bob"));
            scan.setStopRow(Bytes.toBytes("jack"));
            scanner = table.getScanner(scan);
            for(Result result : scanner){
                Cell[] cells = result.rawCells();
                for(Cell cell : cells){
                    System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
                    System.out.println("Timetamp:"+cell.getTimestamp()+" ");
                    System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
                    System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
                    System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(con != null){
                try {
                    con.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(table != null)
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    /**
     * 删除表
     */
    @Test
    public void deleteTable(){
        Connection con = null;
        HBaseAdmin admin = null;
        try {
            con = ConnectionFactory.createConnection(configuration);
            admin = (HBaseAdmin)con.getAdmin();
            TableName table = TableName.valueOf("hbase_try");
            if(admin.tableExists(table)){
                admin.disableTable(table);
                admin.deleteTable(table);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(con != null){
                try {
                    con.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(admin != null)
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    @Test
    public void deleteRow(){
        Connection con = null;
        HTable table = null;
        try {
            con = ConnectionFactory.createConnection(configuration);
            table = (HTable)con.getTable(TableName.valueOf("hbase_try"));
            //指定rowkey
            Delete delete = new Delete(Bytes.toBytes("jack"));
            //指定列簇和列属性
            delete.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("location"));

            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(null != con)
                try {
                    con.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if(table != null)
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

}
