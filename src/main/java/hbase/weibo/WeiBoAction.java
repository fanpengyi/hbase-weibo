package hbase.weibo;

import java.io.IOException;

public class WeiBoAction {

    //命名空间
    private static final String NAME_SPACE = "weibo";
    //用户表
    private static final String RELATION_TABLE = NAME_SPACE+":relation";
    //内容表
    private static final String CONTENT_TABLE = NAME_SPACE+":content";
    //收件箱表
    private static final String INBOX_TABLE = NAME_SPACE+":inbox";

    public static void init() throws IOException {
        WeiBoUtil.createNamespace(NAME_SPACE);
        //创建用户关系表
        WeiBoUtil.createTable(RELATION_TABLE, 1, "attends", "fans");
        //创建微博内容表
        WeiBoUtil.createTable(CONTENT_TABLE, 1, "info");
        //创建收件箱表
        WeiBoUtil.createTable(INBOX_TABLE, 100, "info");
    }


    public static void main(String[] args) throws IOException {
//        init();

        //关注
//        WeiBoUtil.addAttends("1001", "1002");

        //被关注的人发微博（多个人发微博）
//        WeiBoUtil.putData(CONTENT_TABLE, "1002", "info", "content", "今天天气真晴朗！");
//        WeiBoUtil.putData(CONTENT_TABLE, "1002", "info", "content", "春困秋乏！");
//        WeiBoUtil.putData(CONTENT_TABLE, "1003", "info", "content", "夏打盹！");
//        WeiBoUtil.putData(CONTENT_TABLE, "1001", "info", "content", "冬眠睡不醒！");
        //获取关注人的微博
//       WeiBoUtil.getWeiBo("1001");

        //关注已经发过微博的人
//        WeiBoUtil.addAttends("1002", "1001");

        //获取关注人的微博
//        WeiBoUtil.getWeiBo("1002");

        //取消关注
//       WeiBoUtil.deleteRelation("1001","1002");

        //获取关注人的微博
        WeiBoUtil.getWeiBo("1001");

    }



}
