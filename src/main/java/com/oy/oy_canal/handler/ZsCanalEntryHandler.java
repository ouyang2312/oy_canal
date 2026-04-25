package com.oy.oy_canal.handler;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.Map;

/**
 * canal单数据处理接口
 *
 * @author oy
 * @createDate 2026/4/24 10:07
 */
public interface ZsCanalEntryHandler {

    default void insert(CanalEntry.Entry entry,Map<String, Object> row) {}

    default void update(CanalEntry.Entry entry,Map<String, Object> before, Map<String, Object> after) {}

    default void delete(CanalEntry.Entry entry, Map<String, Object> row) {}

}
