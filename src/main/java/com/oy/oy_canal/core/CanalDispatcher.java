package com.oy.oy_canal.core;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.oy.oy_canal.handler.ZsCanalBatchEntryHandler;
import com.oy.oy_canal.handler.ZsCanalEntryHandler;
import com.oy.oy_canal.registry.CanalListenerRegistry;
import com.oy.oy_canal.util.ZsCanalUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * canal处理分发
 *
 * @author oy
 * @createDate 2026/4/24 10:05
 */
public class CanalDispatcher {

    private CanalListenerRegistry registry;

    public CanalDispatcher(CanalListenerRegistry registry) {
        this.registry = registry;
    }

    public void dispatch(List<CanalEntry.Entry> entries) {
        // 操作了多少个表的数据
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                continue;
            }

            // 一个sql影响到的数据
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            String db = entry.getHeader().getSchemaName();
            String table = entry.getHeader().getTableName();

            // 用户是多条数据一起 还是 单个数据的处理
            ZsCanalEntryHandler handler = registry.getHandler(db, table);
            if (handler != null) {
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    Map<String, Object> before = ZsCanalUtil.toMap(rowData.getBeforeColumnsList());
                    Map<String, Object> after = ZsCanalUtil.toMap(rowData.getAfterColumnsList());

                    switch (rowChange.getEventType()) {
                        case INSERT:
                            handler.insert(entry,after);
                            break;
                        case UPDATE:
                            handler.update(entry,before, after);
                            break;
                        case DELETE:
                            handler.delete(entry,before);
                            break;
                    }
                }
            }

            // 多批次处理
            ZsCanalBatchEntryHandler batchHandler = registry.getBatchHandler(db, table);
            if(batchHandler != null){
                batchHandler.handler(entry,rowChange);
            }
        }
    }

    /***
     * 根据不同的表分组
     *
     * @param entries entries
     * @return {@link Map< String, List< CanalEntry.Entry>>}
     * @author ouyang
     * @date 2026/4/24 10:42
     */
    public Map<String, List<CanalEntry.Entry>> groupByTable(List<CanalEntry.Entry> entries) {
        Map<String, List<CanalEntry.Entry>> map = new HashMap<>();
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA){
                continue;
            }
            String key = entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName();
            map.computeIfAbsent(key, k -> new ArrayList<>()).add(entry);
        }
        return map;
    }
}
