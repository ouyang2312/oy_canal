package com.oy.oy_canal.handler;


import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * canal多数据处理接口（同一个表）
 *
 * @author oy
 * @createDate 2026/4/24 14:06
 */
public interface ZsCanalBatchEntryHandler {

    /***
     * 处理多条数据
     *
     * @param rowChange rowChange
     * @author ouyang
     * @date 2026/4/24 14:25
     */
    void handler(CanalEntry.Entry entry,CanalEntry.RowChange rowChange);

}
