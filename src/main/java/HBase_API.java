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
     * ������
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
            //����д�
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
     * ��������
     * @throws Exception
     */
    @Test
    public void insertData(){
        Connection con = null;
        HTable table = null;
        try {
            con = ConnectionFactory.createConnection (configuration);
            table = (HTable) con.getTable(TableName.valueOf("hbase_try"));


            //һ��PUT����һ�У����캯����������н���ROWKEYS��
            Put put = new Put("Bob".getBytes());
            //��һ�����������壬�ڶ��������Ǹ���������е������ԣ������������������Ե�ֵ
            //��һ���н���Ӧ��f1�����µ�������location����Ӧ��ֵ
            put.addColumn("f1".getBytes(),"location".getBytes(),"Xi`an".getBytes());
            //��һ���н���Ӧ��f1�����µ�������phoneNumber����Ӧ��ֵ
            put.addColumn("f1".getBytes(),"phoneNumber".getBytes(),"110".getBytes());
            //��һ���н���Ӧ��f1�����µ�������Sex����Ӧ��ֵ
            put.addColumn("f1".getBytes(),"Sex".getBytes(),"male".getBytes());

            //��һ���н���Ӧ��f2�����µ�������work����Ӧ��ֵ
            put.addColumn("f2".getBytes(),"work".getBytes(),"nurse".getBytes());
            //��һ���н���Ӧ��f2�����µ�������favorite����Ӧ��ֵ
            put.addColumn("f3".getBytes(),"favorite".getBytes(),"basketball".getBytes());
            //��ӵڶ���
            Put put2 = new Put("jack".getBytes());
            //��һ���н���Ӧ��f1�����µ�������location����Ӧ��ֵ
            put2.addColumn("f1".getBytes(),"location".getBytes(),"Xi`an".getBytes());
            //��һ���н���Ӧ��f1�����µ�������phoneNumber����Ӧ��ֵ
            put2.addColumn("f1".getBytes(),"phoneNumber".getBytes(),"110".getBytes());
            //��һ���н���Ӧ��f1�����µ�������Sex����Ӧ��ֵ
            put2.addColumn("f1".getBytes(),"Sex".getBytes(),"male".getBytes());
            //��һ���н���Ӧ��f2�����µ�������work����Ӧ��ֵ
            put2.addColumn("f2".getBytes(),"work".getBytes(),"nurse".getBytes());
            //��һ���н���Ӧ��f2�����µ�������favorite����Ӧ��ֵ
            put2.addColumn("f3".getBytes(),"favorite".getBytes(),"basketball".getBytes());

            List<Put> puts = new ArrayList<Put>();
            puts.add(put);
            puts.add(put2);
            //��ӽ�����
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
     * ��һ���д��в�����ֵ
     */
    @Test
    public void insertColumnsValue(){
        Connection con = null;
        HTable table = null;
        try {
            con = ConnectionFactory.createConnection(configuration);
            table = (HTable) con.getTable(TableName.valueOf("hbase_try"));
            Put put = new Put("Bob".getBytes());
            //1.���û��ָ�������η��������ԣ���������֮���Ѿ������ݣ��򸲸�ԭ������
            //2.�����ָ�������η��������ԣ������ڸ������η���������������򸲸�
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
     * ����hbase��
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
            //����RowKey�н�
            for(Result rs : scanner){
                //���Rowkey(�н�)
                String rowKey = Bytes.toString(rs.getRow());
                //family(�д�)�� qualifiers(�����η�)��value(�����η���Ӧ��ֵ)
                NavigableMap<byte[],NavigableMap<byte[],byte[]>> familyMap = rs.getNoVersionMap();
                //�����д�
                for(byte[] fByte : familyMap.keySet()){
                    NavigableMap<byte[],byte[]> queMap = familyMap.get(fByte);
                    String familyName = Bytes.toString(fByte);
                    //����������
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
     * ����rowKey��ѯ
     * 1.����洢����ʱû�д洢�����η�����cell���������дص����ݣ���ѯ���ľ��Ǹ����������дص�����
     * 2.����洢����ʱ�д洢�����η�����ÿ���д��µ������η�����һ��cell
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
     * ��ѯ���е�RowKeyĳ���д���ĳ�����Ե�ֵ
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
     * ������ѯ
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
     * ɾ����
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
            //ָ��rowkey
            Delete delete = new Delete(Bytes.toBytes("jack"));
            //ָ���дغ�������
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
