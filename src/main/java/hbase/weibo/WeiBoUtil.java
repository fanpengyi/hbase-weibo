package hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class WeiBoUtil {
    //命名空间
    private static final String NAME_SPACE = "weibo";
    //用户表
    private static final String RELATION_TABLE = NAME_SPACE+":relation";
    //内容表
    private static final String CONTENT_TABLE = NAME_SPACE+":content";
    //收件箱表
    private static final String INBOX_TABLE = NAME_SPACE+":inbox";

    // 获取hbase配置信息
    private static Configuration configuration = HBaseConfiguration.create();

    static{
        configuration.set("hbase.zookeeper.quorum","192.168.58.102");
    }

    /**
     *
     * @param namespace namsespce name
     * @throws IOException
     */

    public static void createNamespace(String namespace) throws IOException {

        //获取hbase管理员对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        //构建命名空间描述器

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(namespaceDescriptor);

        admin.close();
        connection.close();
    }


    /**
     *  建表方法
     * @param tableName 表名
     * @param version 几个版本
     * @param cfs  列族
     */

    public static void createTable(String tableName,int version,String... cfs) throws IOException {

        // get habse admin
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        for (String cf : cfs) {
            //创建列描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //设置版本号
            hColumnDescriptor.setMaxVersions(version);
            //加入到表描述器
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        admin.createTable(hTableDescriptor);
        admin.close();

        connection.close();


    }


    /**
     * 向内容表中添加数据   发布微博
     * @param tableName 表名
     * @param uid 用户id
     * @param cf 列族
     * @param cn 列名
     * @param value 列值
     */
    public static void putData(String tableName,String uid,String cf,String cn ,String value) throws IOException {

        //根据连接获取表
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));

        //封装put
        long ts = System.currentTimeMillis();
        // 内容表rowKey
        String rowKey = uid+"_"+ts;
        // 构建put对象 用于向表中存数据  参数是rowKey
        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),ts,Bytes.toBytes(value));

        table.put(put);

        //添加微博内容后 需要更新 收件箱表 更新关注人的微博信息

        Table relationTable = connection.getTable(TableName.valueOf(RELATION_TABLE));
        Table inboxTable = connection.getTable(TableName.valueOf(INBOX_TABLE));



        // 1 先去relation表中找这个人对应fans
        Get get = new Get(Bytes.toBytes(uid));
        Result result = relationTable.get(get);

        ArrayList<Put> puts = new ArrayList<>();
        for (Cell cell : result.rawCells()) {
            //找到列族是fans的列
            if("fans".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                byte[] inboxRowKey = CellUtil.cloneQualifier(cell);
                Put inboxPut = new Put(inboxRowKey);

                inboxPut.addColumn(Bytes.toBytes("info"),Bytes.toBytes(uid),ts,Bytes.toBytes(rowKey));
                puts.add(inboxPut);
            }
        }

        // 2 在收件箱表将新发布的这条内容 加上
        inboxTable.put(puts);

        table.close();
        inboxTable.close();
        connection.close();

    }


    public static void addAttends(String uid,String... attends) throws IOException {
        //1 relation 表添加用户

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table relationTable = connection.getTable(TableName.valueOf(RELATION_TABLE));

        Put attendPut = new Put(Bytes.toBytes(uid));

        //存放被关注用户的添加对象

        ArrayList<Put> puts = new ArrayList<>();

        puts.add(attendPut);

        for (String attend : attends) {
            //1 添加关注人
            attendPut.addColumn(Bytes.toBytes("attends"),Bytes.toBytes(attend),Bytes.toBytes(""));
            //2 在用户关系表中添加fans
            Put put = new Put(Bytes.toBytes(attend));
            put.addColumn(Bytes.toBytes("fans"),Bytes.toBytes(uid),Bytes.toBytes(""));

            puts.add(put);
        }

        relationTable.put(puts);

        //3 在收件箱表中 给当前用户添加关注用户最近所发布的微博的rowkey
        Table inboxTable = connection.getTable(TableName.valueOf(INBOX_TABLE));
        Table contentTable = connection.getTable(TableName.valueOf(CONTENT_TABLE));

        Put inboxPut = new Put(Bytes.toBytes(uid));

        if(attends.length <= 0){
            return ;
        }

        //循环添加要增加的数据
        for (String attend : attends) {
            //通过startRow和stopRow构建扫描器
            Scan scan = new Scan();
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_"));
            scan.setFilter(rowFilter);

            //获取所有符合扫描规则的数据
            ResultScanner scanner = contentTable.getScanner(scan);

            //循环遍历取出每条数据的rowKey添加到inboxPut
            for (Result result : scanner) {
                // 获取内容 行信息
                byte[] row = result.getRow();
                // inbox rowKey -- uid ; 列族 -- info ; 列名 -- attend ,内容 -- row
                inboxPut.addColumn(Bytes.toBytes("info"),Bytes.toBytes(attend),row);
                inboxTable.put(inboxPut);
            }

        }

        //关闭资源
        inboxTable.close();
        contentTable.close();
        relationTable.close();
        connection.close();

    }


    /**
     * 取关用户
     *
     */

    public  static void deleteRelation(String uid,String... deletes) throws IOException {

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table relationTable = connection.getTable(TableName.valueOf(RELATION_TABLE));

        //存放关系表中所有要输出的对象的集合

        ArrayList<Delete> deleteLists = new ArrayList<>();
        //1 用户关系表中 删除当前用户的attends

        Delete userDelete = new Delete(Bytes.toBytes(uid));

        for (String delete : deletes) {
            //给当前用户添加要删除的列
            userDelete.addColumn(Bytes.toBytes("attends"),Bytes.toBytes(delete));

            //2 在用户关系表中 删除被取关用户的fans
            Delete fanDelete = new Delete(Bytes.toBytes(delete));
            fanDelete.addColumn(Bytes.toBytes("fans"),Bytes.toBytes(uid));
            deleteLists.add(fanDelete);
        }

        deleteLists.add(userDelete);

        relationTable.delete(deleteLists);

        // 用户关系表删除操作

        Table inboxTable = connection.getTable(TableName.valueOf(INBOX_TABLE));
        ArrayList<Delete> inboxArrayList = new ArrayList<>();


        Get get = new Get(Bytes.toBytes(uid));
        Result result = inboxTable.get(get);

        Delete inboxDelete = new Delete(Bytes.toBytes(uid));

        for (Cell cell : result.rawCells()) {
            for (String delete : deletes) {
                //找到列是1002的列
                if(delete.equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    inboxDelete.addColumn(Bytes.toBytes("info"),Bytes.toBytes(delete));
                    inboxArrayList.add(inboxDelete);
                }
            }
        }

        inboxTable.delete(inboxArrayList);
        //关闭资源
        relationTable.close();
        inboxTable.close();
        connection.close();
    }


    /**
     *
     * @param uid
     */
    public static void getWeiBo(String uid) throws IOException {

    //获取微博内容表和收件箱表
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table contentTable = connection.getTable(TableName.valueOf(CONTENT_TABLE));
        Table inboxTable = connection.getTable(TableName.valueOf(INBOX_TABLE));

        Get get = new Get(Bytes.toBytes(uid));
        get.setMaxVersions(3);

        Result result = inboxTable.get(get);
        // inbox 表数据  一个cell 包括 rowKey -- uid；column -- 关注人id ， value -- content表rowkey
        for (Cell cell : result.rawCells()) {
            byte[] contentRowKey = CellUtil.cloneValue(cell);
            Get contentGet = new Get(contentRowKey);
            Result contentResult = contentTable.get(contentGet);
            for (Cell contentCell : contentResult.rawCells()) {
                String uid_ts =  Bytes.toString(CellUtil.cloneRow(contentCell));

                String id = uid_ts.split("_")[0];
                String ts = uid_ts.split("_")[1];
                String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(Long.parseLong(ts)));
                System.out.println("用户：" + id + "，时间" + date + "，内容：" + Bytes.toString(CellUtil.cloneValue(contentCell)));
            }

        }

        inboxTable.close();
        contentTable.close();
        connection.close();


    }







}
