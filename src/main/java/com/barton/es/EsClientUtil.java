package com.barton.es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * create by barton on 2018-8-2
 */
public class EsClientUtil {
    public static TransportClient client = null;
    /**
     * 获取Client
     * @return
     */
    public static TransportClient getClient(){
        if(client != null){
            return client;
        }
        String ip = "localhost";
        int port = 9300;
        Settings esSettings = Settings.builder()

                .put("cluster.name", "my-application") //设置ES实例的名称

                .put("client.transport.sniff", true) //自动嗅探整个集群的状态，把集群中其他ES节点的ip添加到本地的客户端列表中

                .build();

        client = new PreBuiltTransportClient(esSettings);//初始化client较老版本发生了变化，此方法有几个重载方法，初始化插件等。

        //此步骤添加IP，至少一个，其实一个就够了，因为添加了自动嗅探配置

        try {
            client.addTransportAddress(new TransportAddress(InetAddress.getByName(ip), port));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

    /**
     * 关闭client
     */
    public static void closeClient(){
        client.close();
    }

}
