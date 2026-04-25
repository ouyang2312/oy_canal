package com.oy.oy_canal.core;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.oy.oy_canal.config.CanalProperties;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * canal消费者
 *
 * @author oy
 * @createDate 2026/4/24 10:27
 */
@Slf4j
public class CanalConsumer implements Runnable {

    private final CanalClient client;
    private final CanalDispatcher dispatcher;
    private final CanalProperties properties;

    private final ExecutorService canalExecutor;
    private final ExecutorService tableExecutor;

    private volatile boolean running = true;

    public CanalConsumer(CanalClient client,
                         CanalDispatcher dispatcher,
                         CanalProperties properties,
                         ExecutorService canalExecutor,
                         ExecutorService tableExecutor
    ) {
        this.client = client;
        this.dispatcher = dispatcher;
        this.properties = properties;
        this.canalExecutor = canalExecutor;
        this.tableExecutor = tableExecutor;
    }

    @Override
    public void run() {
        while (running) {
            Long batchId = -1L;
            try {
                // 自动重连
                if (!client.isConnected()) {
                    client.connect();
                }

                Message msg = client.getWithoutAck(properties.getBatchSize());
                batchId = msg.getId();

                if (batchId == -1 || msg.getEntries().isEmpty()) {
                    Thread.sleep(properties.getEmptySleep());
                    continue;
                }

                // ====== 多表并发处理 ======
                Map<String, List<CanalEntry.Entry>> grouped = dispatcher.groupByTable(msg.getEntries());

                List<Future<?>> futures = new ArrayList<>();
                for (List<CanalEntry.Entry> list : grouped.values()) {
                    futures.add(tableExecutor.submit(() -> dispatcher.dispatch(list)));
                }

                // 等待全部执行完成
                for (Future<?> future : futures) {
                    future.get();
                }

                // 成功才 ack
                client.ack(batchId);

            } catch (Exception e) {
                log.error("Canal消费异常，准备重连", e);
                try {
                    // 回滚当前批次
                    client.rollback(batchId);
                } catch (Exception ignored) {

                }
                client.disconnect();
                sleep(properties.getReconnectInterval());
            }
        }
    }

    public void start() {
        log.info("CanalConsumer starting...");
        canalExecutor.submit(this);
    }

    public void stop() {
        log.info("CanalConsumer stopping...");
        running = false;
        client.disconnect();
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {

        }
    }
}