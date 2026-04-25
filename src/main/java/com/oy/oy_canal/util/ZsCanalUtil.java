package com.oy.oy_canal.util;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 工具类
 *
 * @author oy
 * @createDate 2026/4/24 14:24
 */
public class ZsCanalUtil {

    /***
     * 列转map
     *
     * @param columns columns
     * @return {@link Map< String, Object>}
     * @author ouyang
     * @date 2026/4/24 14:24
     */
    public static Map<String, Object> toMap(List<CanalEntry.Column> columns) {
        Map<String, Object> map = new HashMap<>();
        for (CanalEntry.Column col : columns) {
            map.put(col.getName(), col.getValue());
        }
        return map;
    }

}
