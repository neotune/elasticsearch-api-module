package com.github.elasticsearch.client;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.function.Function;


public class ElasticsearchTransportClient {
    public ElasticsearchTransportClient(Properties properties) {
        this.properties = properties;
        this.init();
    }

    public void init() {
        properties.validate();
        this.client = new PreBuiltTransportClient(Settings.builder().putProperties(properties.getSettings(), Function.identity()).build(), properties.getPlugins());
        properties.getNodes().forEach((node) -> {
            try {
                this.client.addTransportAddress(new TransportAddress(InetAddress.getByName(node.getHost()), node.getPort()));
            } catch (UnknownHostException var4) {
                throw new IllegalArgumentException("잘못된 호스트 정보입니다." + node, var4);
            }
        });

        logger.info("Connected to ElasticSearch : {}", this.client.listedNodes().toString());
    }

    public void close() {
        if (client == null)
            return;

        synchronized (lock) {
            if (client != null) {
                client.close();
                client = null;
            }
        }
    }

    public TransportClient client() {
        return client;
    }

    public Properties properties() {
        return properties;
    }

    private TransportClient client;

    private Properties properties;

    private final Object lock = new Object();

    private final Logger logger = LoggerFactory.getLogger(ElasticsearchTransportClient.class);
}
