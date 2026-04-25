package com.oy.oy_canal.core;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * canal-client
 *
 * @author oy
 * @createDate 2026/4/24 10:05
 */
@Slf4j
public class CanalClient {

    private final CanalConnector connector;
    private final String filter;

    private volatile boolean connected = false;

    public CanalClient(CanalConnector connector, String filter) {
        this.connector = connector;
        this.filter = filter;
    }

    /**
     * 建立连接
     */
    public synchronized void connect() {
        if (connected){
            return;
        }
        try {
            connector.connect();
            connector.subscribe(filter);
            connected = true;
            log.info("Canal connected, filter={}", filter);
        } catch (Exception e) {
            connected = false;
            log.error("Canal连接失败", e);
            e.printStackTrace();
            throw new RuntimeException("Canal连接失败", e);
        }
    }

    /**
     * 获取数据
     */
    public Message getWithoutAck(int batchSize) {
        return connector.getWithoutAck(batchSize);
    }

    /**
     * ack
     */
    public void ack(long batchId) {
        connector.ack(batchId);
    }

    /**
     * rollback
     */
    public void rollback(long batchId) {
        connector.rollback(batchId);
    }

    /**
     * 断开连接
     */
    public synchronized void disconnect() {
        try {
            connector.disconnect();
        } catch (Exception ignored) {}
        connected = false;
        log.info("Canal disconnected");
    }

    public boolean isConnected() {
        return connected;
    }
}