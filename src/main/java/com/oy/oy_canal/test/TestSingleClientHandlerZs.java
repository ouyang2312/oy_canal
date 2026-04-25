package com.oy.oy_canal.test;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.oy.oy_canal.annotation.ZsCanalTableListener;
import com.oy.oy_canal.handler.ZsCanalEntryHandler;

import java.util.Map;

/**
 * 示例代码
 *
 * @author oy
 * @createDate 2026/4/24 10:46
 */
@ZsCanalTableListener(database = "db_name", table = {"table_name1", "table_name2"})
public class TestSingleClientHandlerZs implements ZsCanalEntryHandler {

    @Override
    public void insert(CanalEntry.Entry entry,Map<String, Object> row) {
        row.forEach((k,v)->{
            System.out.println(k + ":" + v);
        });
    }

    @Override
    public void update(CanalEntry.Entry entry,Map<String, Object> before, Map<String, Object> after) {
        after.forEach((k,v)->{
            System.out.println(k + ":" + v);
        });
    }

    @Override
    public void delete(CanalEntry.Entry entry, Map<String, Object> row) {
        row.forEach((k,v)->{
            System.out.println(k + ":" + v);
        });
    }
}
