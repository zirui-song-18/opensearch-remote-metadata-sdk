package org.opensearch.remote.metadata.client.impl;

import org.opensearch.OpenSearchException;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.remote.metadata.client.GetDataObjectRequest;
import org.opensearch.remote.metadata.client.GetDataObjectResponse;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.remote.metadata.common.CommonValue.AWS_OPENSEARCH_SERVICE;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_ENDPOINT_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_GLOBAL_TENANT_ID_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_REGION_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_SERVICE_NAME_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_TYPE_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_OPENSEARCH;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_ID_FIELD_KEY;
import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AOSOpenSearchClientTests {

    private AOSOpenSearchClient aosOpenSearchClient;
    private static final String TEST_THREAD_POOL = "test_pool";
    private static final String TEST_INDEX = "test_index";
    private static final String TEST_ID = "test_id";
    private static final String TEST_TENANT_ID = "test_tenant";
    private static final String GLOBAL_TENANT_ID = "global_tenant";

    @Mock
    private OpenSearchAsyncClient mockOpenSearchAsyncClient;

    private static TestThreadPool testThreadPool = new TestThreadPool(
        AOSOpenSearchClientTests.class.getName(),
        new ScalingExecutorBuilder(
            TEST_THREAD_POOL,
            1,
            Math.max(1, OpenSearchExecutors.allocatedProcessors(Settings.EMPTY) - 1),
            TimeValue.timeValueMinutes(1),
            TEST_THREAD_POOL
        )
    );

    @AfterAll
    static void cleanup() {
        ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
    }

    @BeforeEach
    void setUp() {
        AOSOpenSearchClient.GLOBAL_TENANT_ID = null;
        MockitoAnnotations.openMocks(this);
        aosOpenSearchClient = new AOSOpenSearchClient();
    }

    @Test
    void testSupportsMetadataType() {
        assertTrue(aosOpenSearchClient.supportsMetadataType(AWS_OPENSEARCH_SERVICE));
        assertFalse(aosOpenSearchClient.supportsMetadataType(REMOTE_OPENSEARCH));
        assertFalse(aosOpenSearchClient.supportsMetadataType("unsupported_type"));
    }

    @Test
    void testInitialize() {
        Map<String, String> metadataSettings = new HashMap<>();
        metadataSettings.put(REMOTE_METADATA_TYPE_KEY, AWS_OPENSEARCH_SERVICE);
        metadataSettings.put(REMOTE_METADATA_ENDPOINT_KEY, "https://example.amazonaws.com");
        metadataSettings.put(REMOTE_METADATA_REGION_KEY, "us-west-2");
        metadataSettings.put(REMOTE_METADATA_SERVICE_NAME_KEY, "es");

        assertDoesNotThrow(() -> aosOpenSearchClient.initialize(metadataSettings));
    }

    @Test
    void testInitializeWithInvalidSettings() {
        Map<String, String> metadataSettings = new HashMap<>();
        metadataSettings.put(REMOTE_METADATA_TYPE_KEY, AWS_OPENSEARCH_SERVICE);
        // Missing endpoint and region

        assertThrows(OpenSearchException.class, () -> aosOpenSearchClient.initialize(metadataSettings));
    }

    @Test
    void testCreateOpenSearchClient() throws Exception {
        Map<String, String> metadataSettings = new HashMap<>();
        metadataSettings.put(REMOTE_METADATA_TYPE_KEY, AWS_OPENSEARCH_SERVICE);
        metadataSettings.put(REMOTE_METADATA_ENDPOINT_KEY, "https://example.amazonaws.com");
        metadataSettings.put(REMOTE_METADATA_REGION_KEY, "us-west-2");
        metadataSettings.put(REMOTE_METADATA_SERVICE_NAME_KEY, "es");

        aosOpenSearchClient.initialize(metadataSettings);

        OpenSearchAsyncClient client = aosOpenSearchClient.createOpenSearchAsyncClient();
        assertNotNull(client);
        assertTrue(client._transport() instanceof AwsSdk2Transport);
    }

    @Test
    void testClose() throws Exception {
        aosOpenSearchClient.openSearchAsyncClient = mockOpenSearchAsyncClient;
        when(mockOpenSearchAsyncClient._transport()).thenReturn(mock(AwsSdk2Transport.class));

        aosOpenSearchClient.close();

        verify(mockOpenSearchAsyncClient._transport(), times(1)).close();
    }

    @Test
    void testInitializeWithGlobalTenantId() {
        aosOpenSearchClient.setThreadPool(testThreadPool);

        Map<String, String> metadataSettings = new HashMap<>();
        metadataSettings.put(REMOTE_METADATA_TYPE_KEY, AWS_OPENSEARCH_SERVICE);
        metadataSettings.put(REMOTE_METADATA_ENDPOINT_KEY, "https://example.amazonaws.com");
        metadataSettings.put(REMOTE_METADATA_REGION_KEY, "us-west-2");
        metadataSettings.put(REMOTE_METADATA_SERVICE_NAME_KEY, "es");
        metadataSettings.put(TENANT_ID_FIELD_KEY, "tenant_id");
        metadataSettings.put(REMOTE_METADATA_GLOBAL_TENANT_ID_KEY, GLOBAL_TENANT_ID);
        aosOpenSearchClient.initialize(metadataSettings);
        assertEquals(GLOBAL_TENANT_ID, AOSOpenSearchClient.GLOBAL_TENANT_ID);
    }

    @Test
    void testBuildGlobalCacheKey() {
        // Test buildGlobalCacheKey indirectly through isGlobalResource
        assertEquals(
            String.format(Locale.ROOT, "%s:%s", TEST_INDEX, TEST_ID),
            aosOpenSearchClient.buildGlobalCacheKey(TEST_INDEX, TEST_ID)
        );
        assertNotEquals(String.format(Locale.ROOT, "%s:%s", "foo", "far"), aosOpenSearchClient.buildGlobalCacheKey(TEST_INDEX, TEST_ID));
    }

    @Test
    void testGetDataObjectWithoutGlobalTenant() throws IOException {
        aosOpenSearchClient.openSearchAsyncClient = mockOpenSearchAsyncClient;

        GetDataObjectRequest request = GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build();

        GetDataObjectResponse mockResponse = GetDataObjectResponse.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .source(Map.of("test", "data", "tenant_id", TEST_TENANT_ID))
            .build();
        when(mockOpenSearchAsyncClient.get(any(org.opensearch.client.opensearch.core.GetRequest.class), any(Class.class))).thenReturn(
            CompletableFuture.completedFuture(mockResponse)
        );

        CompletableFuture<GetDataObjectResponse> result = aosOpenSearchClient.getDataObjectAsync(
            request,
            testThreadPool.executor(TEST_THREAD_POOL),
            true
        ).toCompletableFuture();

        assertNotNull(result);
        // Should only call get once since no global tenant fallback logic
        verify(mockOpenSearchAsyncClient, times(1)).get(any(org.opensearch.client.opensearch.core.GetRequest.class), any(Class.class));
    }

    @Test
    void testCacheGlobalResourcesWithNullClient() {
        // Should not throw exception when client is null
        assertDoesNotThrow(() -> {
            AOSOpenSearchClient client = new AOSOpenSearchClient();
            // Trigger caching through initialize without global tenant
            Map<String, String> settings = new HashMap<>();
            settings.put(REMOTE_METADATA_TYPE_KEY, AWS_OPENSEARCH_SERVICE);
            settings.put(REMOTE_METADATA_ENDPOINT_KEY, "https://example.amazonaws.com");
            settings.put(REMOTE_METADATA_REGION_KEY, "us-west-2");
            settings.put(REMOTE_METADATA_SERVICE_NAME_KEY, "es");
            client.initialize(settings);
        });
    }

    @Test
    void testGetDataObjectWithGlobalTenantUserDataFound() throws IOException {
        AOSOpenSearchClient.GLOBAL_TENANT_ID = GLOBAL_TENANT_ID;
        aosOpenSearchClient.openSearchAsyncClient = mockOpenSearchAsyncClient;

        GetDataObjectRequest request = GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build();

        GetDataObjectResponse userResponse = GetDataObjectResponse.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .source(Map.of("user", "data"))
            .build();
        when(mockOpenSearchAsyncClient.get(any(org.opensearch.client.opensearch.core.GetRequest.class), any(Class.class))).thenReturn(
            CompletableFuture.completedFuture(userResponse)
        );

        CompletableFuture<GetDataObjectResponse> result = aosOpenSearchClient.getDataObjectAsync(
            request,
            testThreadPool.executor(TEST_THREAD_POOL),
            true
        ).toCompletableFuture();

        assertNotNull(result);
        verify(mockOpenSearchAsyncClient, times(1)).get(any(org.opensearch.client.opensearch.core.GetRequest.class), any(Class.class));
    }

    @Test
    void testStartAndStopGlobalResourcesCacheScheduler() {
        AOSOpenSearchClient client = new AOSOpenSearchClient();
        client.setThreadPool(testThreadPool);

        assertDoesNotThrow(() -> client.close());
    }
}
