package com.barton;
/**
 * Binlog事件监听器模板
 *
 * @author <a href="mailto:573511675@qq.com">menergy</a>
 *         DateTime: 13-12-26  下午2:34
 */

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import com.xiaoleilu.hutool.log.Log;
import com.xiaoleilu.hutool.log.LogFactory;

/**
 * create by barton on 2018-8-1
 */
public class NotificationListener implements BinlogEventListener {

    private static final Log logger = LogFactory.get();
    private String eventDatabase;
    private String selfEventDatabase = "";
    public NotificationListener(String eventDatabase){
        this.eventDatabase = eventDatabase;
    }
    /**
     * 监听事件
     *
     * @param event
     */
    public void onEvents(BinlogEventV4 event) {
        Class<?> eventType = event.getClass();
        // 事务开始
        if (eventType == QueryEvent.class) {
            QueryEvent actualEvent = (QueryEvent) event;
            selfEventDatabase = actualEvent.getDatabaseName().toString();
            //TODO,这里可以获取事件数据库信息,可做其它逻辑处理
            logger.info("事件数据库名：{}", actualEvent.getSql());
            return;
        }

        // 只监控指定数据库
        if (eventDatabase.equals(selfEventDatabase.trim())) {
            if (eventType == TableMapEvent.class) {

                TableMapEvent actualEvent = (TableMapEvent) event;
                long tableId = actualEvent.getTableId();
                String tableName = actualEvent.getTableName().toString();
                String database = actualEvent.getDatabaseName().toString();

                //TODO,这里可以获取事件表信息,可做其它逻辑处理
                logger.info("事件数据表ID：{}，事件数据库名称：{} 事件数据库表名称：{}", tableId,database, tableName);

            } else if (eventType == WriteRowsEventV2.class) { // 插入事件
                WriteRowsEventV2 actualEvent = (WriteRowsEventV2) event;
                long tableId = actualEvent.getTableId();

                //TODO,这里可以获取写行事件信息,可做其它逻辑处理
                logger.info("写行事件ID：{}", actualEvent.getRows());


            } else if (eventType == UpdateRowsEventV2.class) { // 更新事件

                UpdateRowsEventV2 actualEvent = (UpdateRowsEventV2) event;
                long tableId = actualEvent.getTableId();

                //TODO,这里可以获取更新事件信息,可做其它逻辑处理
                logger.info("更新事件ID：{}", tableId);
                logger.info("&&&&&");

            } else if (eventType == DeleteRowsEventV2.class) {// 删除事件

                DeleteRowsEventV2 actualEvent = (DeleteRowsEventV2) event;
                long tableId = actualEvent.getTableId();

                //TODO,这里可以获取删除事件信息,可做其它逻辑处理
                logger.info("删除事件ID：{}", tableId);

            } else if (eventType == XidEvent.class) {// 结束事务
                XidEvent actualEvent = (XidEvent) event;
                long xId = actualEvent.getXid();

                //TODO,这里可以获取结束事件信息,可做其它逻辑处理
                logger.info("结束事件ID：{}", xId);

            }
        }
    }
}