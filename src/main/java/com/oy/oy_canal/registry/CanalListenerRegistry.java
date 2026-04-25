package com.oy.oy_canal.registry;

import com.oy.oy_canal.annotation.ZsCanalTableListener;
import com.oy.oy_canal.config.CanalProperties;
import com.oy.oy_canal.handler.ZsCanalBatchEntryHandler;
import com.oy.oy_canal.handler.ZsCanalEntryHandler;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * canal注解监听注册
 *
 * @author oy
 * @createDate 2026/4/24 10:07
 */
public class CanalListenerRegistry implements ApplicationContextAware {

    private final Map<String, ZsCanalEntryHandler> handlerMap = new HashMap<>();
    private final Map<String, ZsCanalBatchEntryHandler> batchHandlerMap = new HashMap<>();

    private final CanalProperties properties;

    public CanalListenerRegistry(CanalProperties properties) {
        this.properties = properties;
    }

    @Override
    public void setApplicationContext(ApplicationContext ctx) {
        Map<String, Object> beans = ctx.getBeansWithAnnotation(ZsCanalTableListener.class);

        for (Object bean : beans.values()) {
            Class<?> targetClass = AopUtils.getTargetClass(bean);
            ZsCanalTableListener ann = AnnotatedElementUtils.findMergedAnnotation(targetClass,ZsCanalTableListener.class);
            if (ann == null) {
                continue;
            }

            String[] table = ann.table();
            String database = ann.database();
            if (database == null || database.isEmpty()) {
                database = properties.getDb();
            }

            boolean handleBatch = ann.handleBatch();
            for (String item : table) {
                String key = buildKey(database, item);
                if (handleBatch) {
                    if (!(bean instanceof ZsCanalBatchEntryHandler)) {
                        throw new IllegalStateException(
                                "Bean must implement ZsCanalBatchEntryHandler: " + bean.getClass()
                        );
                    }
                    batchHandlerMap.put(key, (ZsCanalBatchEntryHandler) bean);
                } else {
                    if (!(bean instanceof ZsCanalEntryHandler)) {
                        throw new IllegalStateException(
                                "Bean must implement ZsCanalEntryHandler: " + bean.getClass()
                        );
                    }
                    handlerMap.put(key, (ZsCanalEntryHandler) bean);
                }
            }
        }
    }

    public ZsCanalEntryHandler getHandler(String db, String table) {
        return handlerMap.get(buildKey(db, table));
    }
    public ZsCanalBatchEntryHandler getBatchHandler(String db, String table) {
        return batchHandlerMap.get(buildKey(db, table));
    }

    private String buildKey(String db, String table) {
        return db + "." + table;
    }
}
