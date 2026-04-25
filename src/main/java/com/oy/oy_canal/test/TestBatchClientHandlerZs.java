package com.oy.oy_canal.test;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.oy.oy_canal.annotation.ZsCanalTableListener;
import com.oy.oy_canal.handler.ZsCanalBatchEntryHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 示例代码
 *
 * @author oy
 * @createDate 2026/4/24 10:46
 */
@Slf4j
@ZsCanalTableListener(database = "db_name", table = "table_name",handleBatch = true)
public class TestBatchClientHandlerZs implements ZsCanalBatchEntryHandler {

    @Override
    public void handler(CanalEntry.Entry entry,CanalEntry.RowChange rowChange) {
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            CanalEntry.EventType eventType = rowChange.getEventType();
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            System.out.println(afterColumnsList);
            switch (eventType) {
                case INSERT:
                    break;
                case UPDATE:
                    break;
                case DELETE:
                    break;
                default:
                    // CREATE / ALTER 等 DDL 直接忽略
                    break;
            }
        }
    }

}
