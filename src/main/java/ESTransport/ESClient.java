package ESTransport;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ESClient {
    private final static Logger logger = LogManager.getLogger(ESClient.class);

    public static final ESClient INSTANCE = new ESClient();

    private TransportClient client;
    public void initClient() {
        try {
            client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (UnknownHostException e) {
            logger.error("Could not connect to ES", e);
        }
    }

    public TransportClient getClient() {
        return client;
    }
}
