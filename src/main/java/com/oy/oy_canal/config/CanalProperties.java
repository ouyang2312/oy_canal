package com.oy.oy_canal.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * canal配置
 *
 * @author oy
 * @createDate 2026/4/24 10:05
 */
@Data
@ConfigurationProperties(prefix = "canal")
public class CanalProperties {

    private String host = "127.0.0.1";
    private int port = 11111;
    /** instance 名 */
    private String destination = "example";
    /** canal用户名 */
    private String username = "";
    /** canal密码 */
    private String password = "";
    /** 批大小 */
    private int batchSize = 2000;
    /** 空轮询等待ms */
    private long emptySleep = 100;
    /** 重连间隔 */
    private long reconnectInterval = 2000;

    /** 是否集群模式（zk） */
    private boolean cluster = false;
    private String zkServers;

    /** db默认是哪个库 */
    private String db = "";
}
