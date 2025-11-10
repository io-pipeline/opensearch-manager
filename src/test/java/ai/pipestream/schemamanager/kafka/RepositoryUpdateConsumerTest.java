package ai.pipestream.schemamanager.kafka;

import ai.pipestream.repository.filesystem.Drive;
import ai.pipestream.repository.filesystem.DriveUpdateNotification;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class RepositoryUpdateConsumerTest {

    @Inject
    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @Inject
    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.apicurio.registry.url")
    String apicurioRegistryUrl;

    @Inject
    OpenSearchClient openSearchClient;

    private KafkaProducer<String, Object> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtobufKafkaSerializer.class.getName());
        props.put("apicurio.registry.url", apicurioRegistryUrl);
        props.put("apicurio.registry.auto-register", "true");
        props.put("apicurio.registry.artifact-id", "drive-updates-value");
        return new KafkaProducer<>(props);
    }

    private boolean documentExists(String index, String id) {
        try {
            var resp = openSearchClient.get(g -> g.index(index).id(id), Map.class);
            return resp.found();
        } catch (IOException e) {
            return false;
        }
    }

    @Test
    public void testConsumer_onDriveCreated_indexesDrive() throws Exception {
        Drive drive = Drive.newBuilder().setName("test-drive-1").build();
        DriveUpdateNotification notification = DriveUpdateNotification.newBuilder()
                .setDrive(drive)
                .setUpdateType("CREATED")
                .build();

        try (KafkaProducer<String, Object> producer = createProducer()) {
            producer.send(new ProducerRecord<>("drive-updates", drive.getName(), notification)).get();
        }

        long deadline = System.currentTimeMillis() + 15000; // up to 15s for indexing
        boolean found = false;
        while (System.currentTimeMillis() < deadline) {
            if (documentExists("filesystem-drives", drive.getName())) {
                found = true;
                break;
            }
            Thread.sleep(500);
        }

        assertTrue(found, "Indexed drive document not found in OpenSearch within timeout");
    }
}