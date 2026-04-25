package com.oy.oy_canal.config;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.oy.oy_canal.core.CanalClient;
import com.oy.oy_canal.core.CanalConsumer;
import com.oy.oy_canal.core.CanalDispatcher;
import com.oy.oy_canal.registry.CanalListenerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 自动配置
 *
 * @author oy
 * @createDate 2026/4/24 10:05
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(CanalProperties.class)
public class CanalAutoConfiguration {

    /***
     * canal连接
     *
     * @param properties properties
     * @return {@link CanalConnector}
     * @author ouyang
     * @date 2026/4/24 13:50
     */
    @Bean
    public CanalConnector canalConnector(CanalProperties properties) {
        // 集群模式
        if (properties.isCluster()) {
            return CanalConnectors.newClusterConnector(
                    properties.getZkServers(),
                    properties.getDestination(),
                    properties.getUsername(),
                    properties.getPassword()
            );
        }

        return CanalConnectors.newSingleConnector(
                new InetSocketAddress(properties.getHost(), properties.getPort()),
                properties.getDestination(),
                properties.getUsername(),
                properties.getPassword()
        );
    }

    /***
     * canal消费程序线程池（单个线程）
     *
     * @return {@link ExecutorService}
     * @author ouyang
     * @date 2026/4/24 13:51
     */
    @Bean(name = "canalConsumerExecutor", destroyMethod = "shutdown")
    public ExecutorService canalConsumerExecutor() {
        return Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("canal-consumer");
            t.setDaemon(false);
            return t;
        });
    }

    /***
     * canal处理表数据的线程池（多线程，并发处理多张表）
     *
     * @return {@link ExecutorService}
     * @author ouyang
     * @date 2026/4/24 13:52
     */
    @Bean(name = "canalTableExecutor", destroyMethod = "shutdown")
    public ExecutorService canalTableExecutor() {
        int poolSize = Runtime.getRuntime().availableProcessors() * 2;
        log.info("初始化 Canal 表处理线程池，核心线程数：{}", poolSize);

        return Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r);
            t.setName("canal-table-worker");
            t.setDaemon(false);
            return t;
        });
    }


    @Bean(initMethod = "start", destroyMethod = "stop")
    public CanalConsumer canalConsumer(CanalClient client,
                                       CanalDispatcher dispatcher,
                                       CanalProperties properties,
                                       @Qualifier("canalConsumerExecutor") ExecutorService canalExecutor,
                                       @Qualifier("canalTableExecutor") ExecutorService tableExecutor
    ) {
        return new CanalConsumer(client, dispatcher, properties, canalExecutor,tableExecutor);
    }

    @Bean
    public CanalDispatcher canalDispatcher(CanalListenerRegistry canalListenerRegistry) {
        return new CanalDispatcher(canalListenerRegistry);
    }

    @Bean
    public CanalListenerRegistry canalListenerRegistry(CanalProperties properties) {
        return new CanalListenerRegistry(properties);
    }

    @Bean
    public CanalClient canalClient(CanalConnector connector,CanalProperties properties) {
        // 推荐：让 server 控制过滤,如果到时候需要自己配置，那就从 properties 中取
//        String filter = ".*\\..*";
//        String filter = "jnd_dev_change_version\\\\.(bus_sell_client)";
        return new CanalClient(connector, null);
    }
}
