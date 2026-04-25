1.maven 配置
```maven
        <dependency>
            <groupId>com.zs</groupId>
            <artifactId>canal-util</artifactId>
            <version>0.0.1-SNAPSHOT</version>
        </dependency>

```

2. 宿主服务配置文件
```yml

canal:
  host: 127.0.0.1
  port: 11111
  destination: example

  batch-size: 1000
  empty-sleep: 100
  reconnect-interval: 2000

```
多集群的目前还没测试。。。


3. 宿主项目怎么使用？
   参考一下

批次处理：[TestBatchClientHandler.java](src%2Fmain%2Fjava%2Fcom%2Fzs%2Fcanalutil%2Ftest%2FTestBatchClientHandler.java)

单个处理：[TestSingleClientHandler.java](src%2Fmain%2Fjava%2Fcom%2Fzs%2Fcanalutil%2Ftest%2FTestSingleClientHandler.java)



    
