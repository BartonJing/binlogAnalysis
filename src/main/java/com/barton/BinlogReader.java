package com.barton;

import com.xiaoleilu.hutool.log.Log;
import com.xiaoleilu.hutool.log.LogFactory;



/**
 * create by barton on 2018-8-1
 */
public class BinlogReader {
    private static final Log log = LogFactory.get();
    private AutoOpenReplicator aor;

    public static void main(String args[]) {
        log.info("&&&&&&&&&&&&&&&&");
        // 配置从MySQL Master进行复制
        final AutoOpenReplicator aor = new AutoOpenReplicator();
        aor.setBinlogFileName("mysql-bin.000003");
        aor.setServerId(100001);
        aor.setHost("localhost");
        aor.setPort(3306);
        aor.setUser("root");
        aor.setPassword("root");
        aor.setBinlogPosition(4);

        aor.setAutoReconnect(false);
        aor.setDelayReconnect(5);
        aor.setBinlogEventListener(new NotificationListener("pytest"));
        aor.start1();
    }

}
