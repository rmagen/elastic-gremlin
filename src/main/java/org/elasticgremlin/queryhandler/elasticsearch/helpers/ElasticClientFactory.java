package org.elasticgremlin.queryhandler.elasticsearch.helpers;

import org.apache.commons.configuration.Configuration;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.*;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * The elastic client factory.
 */
public class ElasticClientFactory {


    ////////////////////////////////////////////////////////////////////////////
    // Inner classes

    /**
     * Client type class.
     */
    public static class ClientType {
        /**
         * Transport client.
         */
        public static String TRANSPORT_CLIENT = "TRANSPORT_CLIENT";

        /**
         * Node client.
         */
        public static String NODE_CLIENT = "NODE_CLIENT";

        /**
         * Node.
         */
        public static String NODE = "NODE";
    }


    ////////////////////////////////////////////////////////////////////////////
    // Methods
    /**
     * Creates the client from the configuration.
     *
     * @param configuration the configuration.
     * @return the client.
     */
    public static Client create(Configuration configuration) throws UnknownHostException {
        String clientType = configuration.getString("elasticsearch.client", ClientType.NODE);
        String clusterName = configuration.getString("elasticsearch.cluster.name", "elasticsearch");

        if (clientType.equals(ClientType.TRANSPORT_CLIENT)) {
            String concatenatedAddresses = configuration.getString("elasticsearch.cluster.address", "127.0.0.1:9300");
            String[] addresses = concatenatedAddresses.split(",");
            InetSocketTransportAddress[] inetSocketTransportAddresses = new InetSocketTransportAddress[addresses.length];
            for(int i = 0; i < addresses.length; i++) {
                String address = addresses[i];
                String[] split = address.split(":");
                if(split.length != 2) throw new IllegalArgumentException("Address invalid:" + address +  ". Should contain ip and port, e.g. 127.0.0.1:9300");
                inetSocketTransportAddresses[i] = new InetSocketTransportAddress(InetAddress.getByName(split[0]), Integer.parseInt(split[1]));
            }
            return createTransportClient(clusterName, inetSocketTransportAddresses);
        }
        else{
            String port = configuration.getString("elasticsearch.cluster.port", "9300");
            return createNode(clusterName, clientType.equals(ClientType.NODE_CLIENT), Integer.parseInt(port)).client();
        }
    }

    /**
     * Creates transport client.
     *
     * @param clusterName the elasticsearch cluster name.
     * @param addresses the addresses of the clusters.
     * @return the transport client.
     */
    public static TransportClient createTransportClient(String clusterName, InetSocketTransportAddress... addresses) {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", true).build();
        TransportClient transportClient = TransportClient.builder().settings(settings).build().addTransportAddresses(addresses);
        return transportClient;
    }

    /**
     * Creates a elasticsearch node.
     *
     * @param clusterName the elasticsearch cluster name.
     * @param client the client.
     * @param port the port node listens to.
     * @return a new node.
     */
    public static Node createNode(String clusterName, boolean client, int port) {
        Settings settings = Settings.settingsBuilder()
                .put("script.groovy.sandbox.enabled", true)
                .put("script.inline", "on")
                .put("script.indexed", "on")
                .put("transport.tcp.port", port).build();
        Node node = NodeBuilder.nodeBuilder().client(client).data(!client).clusterName(clusterName).settings(settings).build();
        node.start();
        return node;
    }
}
