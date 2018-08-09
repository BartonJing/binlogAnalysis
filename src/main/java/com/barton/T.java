package com.barton;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.xiaoleilu.hutool.log.Log;
import com.xiaoleilu.hutool.log.LogFactory;

import java.io.BufferedReader;

/**
 * create by barton on 2018-8-1
 */
public class T {
    private static final Log log = LogFactory.get();
    public static  void main(String [] atgs ) throws  Exception{
        final OpenReplicator or = new OpenReplicator();
        or.setUser("root");
        or.setPassword("root");
        or.setHost("localhost");
        or.setPort(3306);
        or.setServerId(1);
        or.setBinlogPosition(4);
        or.setBinlogFileName("mysql-bin.000001");
        or.setBinlogEventListener(new BinlogEventListener() {
            public void onEvents(BinlogEventV4 event) {
                // your code goes here
                log.info(event.toString());
            }
        });
        or.start();

        /*System.out.println("press 'q' to stop");
        final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        for(String line = br.readLine(); line != null; line = br.readLine()) {
            if(line.equals("q")) {
                or.stop();
                break;
            }
        }*/
    }
}
