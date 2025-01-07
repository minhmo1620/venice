package com.linkedin.venice.serialization.avro;

import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE;
import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.SERVER_ADMIN_RESPONSE;

import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStore;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroProtocolDefinitionTest {
  @Test
  public void testGetSerializer() {
    Assert.assertNotNull(KAFKA_MESSAGE_ENVELOPE.getSerializer());
    Assert.assertNotNull(SERVER_ADMIN_RESPONSE.getSerializer());
  }

  @Test
  public void testSerializeWithDifferentSchemaVersions() {
    AdminOperationSerializer serializer = new AdminOperationSerializer();
    AdminOperation message = createDummyAdminOperation();

    int latestSchemaId = AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;

    for (int schemaId = latestSchemaId; schemaId > 7; schemaId--) {
      // Serialize the object of latest version with specific schema version
      System.out.println("Serializing with schema version: " + schemaId);
      byte[] serializedBytes = serializer.serialize(message, schemaId);

      // Assert that the serialized bytes are not null or empty
      Assert.assertNotNull(serializedBytes);
      Assert.assertTrue(serializedBytes.length > 0);

      // Deserialize the bytes back to an object with the same schema version and verify the content
      AdminOperation deserializedObject = serializer.deserialize(ByteBuffer.wrap(serializedBytes), schemaId);
      Assert.assertNotNull(deserializedObject);
    }
  }

  @Test
  public void testDeserializeWithLatestSchemaVersion() {
    AdminOperationSerializer serializer = new AdminOperationSerializer();
    AdminOperation message = createDummyAdminOperation();
    int latestSchemaId = AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;

    for (int schemaId = latestSchemaId; schemaId > 7; schemaId--) {
      // Serialize the object with the current schema version
      System.out.println("Serializing with schema version: " + schemaId);
      byte[] serializedBytes = serializer.serialize(message, schemaId);

      // Assert that the serialized bytes are not null or empty
      Assert.assertNotNull(serializedBytes);
      Assert.assertTrue(serializedBytes.length > 0);

      // Deserialize the bytes back to an object by the latest schema version and verify the content
      AdminOperation deserializedObject = serializer.deserialize(ByteBuffer.wrap(serializedBytes), latestSchemaId);
      Assert.assertNotNull(deserializedObject);
    }
  }

  private AdminOperation createDummyAdminOperation() {
    String clusterName = "testCluster";
    String storeName = "testStore";
    DeleteStore deleteStore = (DeleteStore) AdminMessageType.DELETE_STORE.getNewInstance();
    deleteStore.clusterName = clusterName;
    deleteStore.storeName = storeName;

    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.DELETE_STORE.getValue();
    message.payloadUnion = deleteStore;
    return message;
  }
}
