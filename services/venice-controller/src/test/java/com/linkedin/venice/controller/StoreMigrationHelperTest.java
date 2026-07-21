package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;


public class StoreMigrationHelperTest {
  private static final String STORE_NAME = "test-store";
  private static final String SRC_CLUSTER = "src-cluster";
  private static final String DEST_CLUSTER = "dest-cluster";
  private static final String LOCAL_REGION = "dc-0";
  private static final String KEY_SCHEMA = "\"string\"";
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();

  @Test
  public void testCannotMigrateOutOfEncryptionCluster() {
    VeniceException exception =
        expectThrows(VeniceException.class, () -> validateMigration(emptyStore(), true, false, Collections.emptySet()));
    assertTrue(exception.getMessage().contains("Cannot migrate store"));
  }

  @Test
  public void testCannotMigrateStoreWithVersionIntoEncryptionCluster() {
    StoreInfo store = emptyStore();
    store.setVersions(Collections.singletonList(new VersionImpl(STORE_NAME, 1, "push-id")));

    VeniceException exception =
        expectThrows(VeniceException.class, () -> validateMigration(store, false, true, Collections.emptySet()));
    assertTrue(exception.getMessage().contains("The store must have no versions or topics"));
  }

  @Test
  public void testCannotMigrateStoreWithTopicIntoEncryptionCluster() {
    Set<PubSubTopic> topics = Collections.singleton(TOPIC_REPOSITORY.getTopic(STORE_NAME + "_v1"));

    VeniceException exception =
        expectThrows(VeniceException.class, () -> validateMigration(emptyStore(), false, true, topics));
    assertTrue(exception.getMessage().contains("The store must have no versions or topics"));
  }

  @Test
  public void testCanMigrateEmptyStoreIntoEncryptionCluster() {
    validateMigration(emptyStore(), false, true, Collections.emptySet());
  }

  @Test
  public void testCanMigrateNonEmptyStoreIntoNonEncryptionCluster() {
    StoreInfo store = emptyStore();
    store.setVersions(Collections.singletonList(new VersionImpl(STORE_NAME, 1, "push-id")));
    validateMigration(store, false, false, Collections.emptySet());
  }

  @Test
  public void testUnrelatedTopicDoesNotBlockMigrationIntoEncryptionCluster() {
    Set<PubSubTopic> topics = Collections.singleton(TOPIC_REPOSITORY.getTopic("other-store_v1"));
    validateMigration(emptyStore(), false, true, topics);
  }

  @Test
  public void testEncryptionEnabledForMigrationIntoEncryptionCluster() {
    UpdateStoreQueryParams capturedParams = runCloneAndCaptureUpdateParams(true);
    assertEquals(capturedParams.getEncryptionEnabled(), Optional.of(true));
  }

  @Test
  public void testEncryptionPreservedForMigrationIntoNonEncryptionCluster() {
    UpdateStoreQueryParams capturedParams = runCloneAndCaptureUpdateParams(false);
    assertEquals(capturedParams.getEncryptionEnabled(), Optional.of(false));
  }

  private void validateMigration(
      StoreInfo store,
      boolean srcEncryptionCluster,
      boolean destEncryptionCluster,
      Set<PubSubTopic> topics) {
    StoreMigrationHelper.validateEncryptionClusterMigration(
        store,
        srcEncryptionCluster,
        destEncryptionCluster,
        topics,
        SRC_CLUSTER,
        DEST_CLUSTER,
        STORE_NAME);
  }

  private StoreInfo emptyStore() {
    Store store = TestUtils.createTestStore(STORE_NAME, "owner", System.currentTimeMillis());
    return StoreInfo.fromStore(store);
  }

  private UpdateStoreQueryParams runCloneAndCaptureUpdateParams(boolean destEncryptionCluster) {
    Store srcStore = TestUtils.createTestStore(STORE_NAME, "owner", System.currentTimeMillis());
    srcStore.setEncryptionEnabled(false);
    StoreInfo srcStoreInfo = StoreInfo.fromStore(srcStore);

    ControllerClient destControllerClient = mock(ControllerClient.class);
    NewStoreResponse newStoreResponse = mock(NewStoreResponse.class);
    doReturn(false).when(newStoreResponse).isError();
    doReturn(newStoreResponse).when(destControllerClient)
        .createNewStore(anyString(), anyString(), anyString(), anyString());

    SchemaResponse schemaResponse = mock(SchemaResponse.class);
    doReturn(false).when(schemaResponse).isError();
    doReturn(schemaResponse).when(destControllerClient).addValueSchema(anyString(), anyString());

    ControllerResponse updateStoreResponse = mock(ControllerResponse.class);
    doReturn(false).when(updateStoreResponse).isError();
    doReturn(updateStoreResponse).when(destControllerClient).updateStore(anyString(), any());

    Map<String, Map<String, StoreInfo>> srcStoresInChildColos =
        Collections.singletonMap(STORE_NAME, Collections.emptyMap());

    StoreMigrationHelper.cloneDestinationStoreAndSyncConfigs(
        destControllerClient,
        srcStoreInfo,
        KEY_SCHEMA,
        Collections.singletonList(new SchemaEntry(1, "\"int\"")),
        srcStoresInChildColos,
        DEST_CLUSTER,
        STORE_NAME,
        LOCAL_REGION,
        destEncryptionCluster,
        mock(Logger.class));

    ArgumentCaptor<UpdateStoreQueryParams> paramsCaptor = ArgumentCaptor.forClass(UpdateStoreQueryParams.class);
    verify(destControllerClient).updateStore(eq(STORE_NAME), paramsCaptor.capture());
    return paramsCaptor.getValue();
  }
}
