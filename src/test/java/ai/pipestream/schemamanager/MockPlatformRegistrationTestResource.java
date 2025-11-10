package ai.pipestream.schemamanager;

import ai.pipestream.grpc.wiremock.MockServiceFactory;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.util.Map;

/**
 * Starts the platform registration WireMock server for tests and routes
 * the OpenSearch manager's registration client to it.
 */
public class MockPlatformRegistrationTestResource implements QuarkusTestResourceLifecycleManager {

    @Override
    public Map<String, String> start() {
        MockServiceFactory.startMockPlatformRegistrationService();

        int mockPort = MockServiceFactory.getMockServer().getGrpcPort();

        return Map.ofEntries(
            Map.entry("pipeline.test.mock-services.enabled", "true"),
            Map.entry("pipeline.test.mock-services.platform-registration.enabled", "true"),

            // Disable real service registration to avoid hitting Consul during tests
            Map.entry("service.registration.enabled", "false"),
            Map.entry("service.registration.grpc.enabled", "false"),

            // Route any platform-registration client traffic to the mock
            Map.entry("quarkus.grpc.server.port", "0"),
            Map.entry("quarkus.grpc.clients.platform-registration.host", "localhost"),
            Map.entry("quarkus.grpc.clients.platform-registration.port", String.valueOf(mockPort)),
            Map.entry("quarkus.grpc.clients.platform-registration-service.host", "localhost"),
            Map.entry("quarkus.grpc.clients.platform-registration-service.port", String.valueOf(mockPort)),
            Map.entry("quarkus.stork.platform-registration-service.service-discovery.type", "static"),
            Map.entry("quarkus.stork.platform-registration-service.service-discovery.address-list", "localhost:" + mockPort),

            // Prevent Consul lookups during tests
            Map.entry("pipeline.consul.host", "localhost"),
            Map.entry("pipeline.consul.port", "0")
        );
    }

    @Override
    public void stop() {
        MockServiceFactory.stopMockPlatformRegistrationService();
    }
}
