package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_MIN_IN_SYNC_REPLICAS;
import static com.linkedin.venice.ConfigKeys.KAFKA_OVER_SSL;
import static com.linkedin.venice.ConfigKeys.KAFKA_REPLICATION_FACTOR;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.common.PushStatusStoreUtils.CONTROLLER_HEARTBEAT_INSTANCE_NAME;
import static com.linkedin.venice.controller.UserSystemStoreLifeCycleHelper.AUTO_META_SYSTEM_STORE_PUSH_ID_PREFIX;
import static com.linkedin.venice.exceptions.VeniceNoStoreException.DOES_NOT_EXISTS;
import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_HYBRID_OFFSET_LAG_THRESHOLD;
import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD;
import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_REAL_TIME_TOPIC_NAME;
import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_REWIND_TIME_IN_SECONDS;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static com.linkedin.venice.meta.Version.DEFAULT_RT_VERSION_NUMBER;
import static com.linkedin.venice.meta.Version.PushType;
import static com.linkedin.venice.meta.Version.REAL_TIME_TOPIC_SUFFIX;
import static com.linkedin.venice.meta.VersionStatus.ERROR;
import static com.linkedin.venice.meta.VersionStatus.KILLED;
import static com.linkedin.venice.meta.VersionStatus.NOT_CREATED;
import static com.linkedin.venice.meta.VersionStatus.ONLINE;
import static com.linkedin.venice.meta.VersionStatus.PUSHED;
import static com.linkedin.venice.meta.VersionStatus.STARTED;
import static com.linkedin.venice.pushmonitor.OfflinePushStatus.HELIX_ASSIGNMENT_COMPLETED;
import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.PARTICIPANT_MESSAGE_SYSTEM_STORE_VALUE;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static com.linkedin.venice.utils.AvroSchemaUtils.isValidAvroSchema;
import static com.linkedin.venice.utils.RegionUtils.isRegionPartOfRegionsFilterList;
import static com.linkedin.venice.utils.RegionUtils.parseRegionsFilterList;
import static com.linkedin.venice.views.ViewUtils.ETERNAL_TOPIC_RETENTION_ENABLED;
import static com.linkedin.venice.views.ViewUtils.LOG_COMPACTION_ENABLED;
import static com.linkedin.venice.views.ViewUtils.PARTITION_COUNT;
import static com.linkedin.venice.views.ViewUtils.USE_FAST_KAFKA_OPERATION_TIMEOUT;

import com.linkedin.avroutil1.compatibility.AvroIncompatibleSchemaException;
import com.linkedin.avroutil1.compatibility.RandomRecordGenerator;
import com.linkedin.avroutil1.compatibility.RecordGenerationConfig;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.ZstdWithDictCompressor;
import com.linkedin.venice.controller.datarecovery.DataRecoveryManager;
import com.linkedin.venice.controller.exception.HelixClusterMaintenanceModeException;
import com.linkedin.venice.controller.helix.SharedHelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.controller.helix.SharedHelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationManager;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.controller.init.PerClusterInternalRTStoreInitializationRoutine;
import com.linkedin.venice.controller.init.SystemSchemaInitializationRoutine;
import com.linkedin.venice.controller.kafka.StoreStatusDecider;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.protocol.admin.HybridStoreConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreViewConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.logcompaction.CompactionManager;
import com.linkedin.venice.controller.logcompaction.LogCompactionService;
import com.linkedin.venice.controller.multitaskscheduler.MultiTaskSchedulerService;
import com.linkedin.venice.controller.multitaskscheduler.StoreMigrationManager;
import com.linkedin.venice.controller.repush.RepushJobRequest;
import com.linkedin.venice.controller.repush.RepushOrchestrator;
import com.linkedin.venice.controller.stats.AddVersionLatencyStats;
import com.linkedin.venice.controller.stats.DeadStoreStats;
import com.linkedin.venice.controller.stats.DisabledPartitionStats;
import com.linkedin.venice.controller.stats.LogCompactionStats;
import com.linkedin.venice.controller.stats.PushJobStatusStats;
import com.linkedin.venice.controllerapi.AdminOperationProtocolVersionControllerResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.NodeReplicasReadinessState;
import com.linkedin.venice.controllerapi.RepushInfo;
import com.linkedin.venice.controllerapi.RepushJobResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreComparisonInfo;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateClusterConfigQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoragePersonaQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.ResourceStillExistsException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.exceptions.VeniceStoreAlreadyExistsException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixLiveInstanceMonitor;
import com.linkedin.venice.helix.HelixPartitionState;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.helix.HelixReadWriteLiveClusterConfigRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.HelixStoreGraveyard;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.StoragePersonaRepository;
import com.linkedin.venice.helix.VeniceOfflinePushMonitorAccessor;
import com.linkedin.venice.helix.ZkAllowlistAccessor;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.ETLStoreConfigImpl;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.InstanceStatus;
import com.linkedin.venice.meta.LiveClusterConfig;
import com.linkedin.venice.meta.LiveInstanceChangedListener;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.RoutersClusterConfig;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.StoreDataAudit;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.StoreName;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.meta.SystemStoreAttributes;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.participant.protocol.KillPushJob;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerContext;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatusWithDetails;
import com.linkedin.venice.pushmonitor.KillOfflinePushMessage;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PushMonitor;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import com.linkedin.venice.pushmonitor.PushMonitorUtils;
import com.linkedin.venice.pushmonitor.PushStatusDecider;
import com.linkedin.venice.pushmonitor.StatusSnapshot;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.StatusMessageChannel;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.system.store.MetaStoreReader;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.EncodingUtils;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.StoreUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.ConcurrencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.views.ViewUtils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManagerProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.utils.Assert;


/**
 * Helix Admin based on 0.8.4.215 APIs.
 *
 * <p>
 * After using controller as service mode. There are two levels of cluster and controllers. Each venice controller will
 * hold a level1 helix controller which will keep connecting to Helix, there is a cluster only used for all of these
 * level1 controllers(controller's cluster). The second level is our venice clusters. Like prod cluster, dev cluster
 * etc. Each of cluster will be Helix resource in the controller's cluster. Helix will choose one of level1 controller
 * becoming the leader of our venice cluster. In our distributed controllers state transition handler, a level2 controller
 * will be initialized to manage this venice cluster only. If this level1 controller is chosen as the leader controller
 * of multiple Venice clusters, multiple level2 controller will be created based on cluster specific config.
 * <p>
 * Admin is shared by multiple cluster's controllers running in one physical Venice controller instance.
 */
public class VeniceHelixAdmin implements Admin, StoreCleaner {
  static final List<ExecutionStatus> STATUS_PRIORITIES = Arrays.asList(
      ExecutionStatus.PROGRESS,
      ExecutionStatus.STARTED,
      ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED,
      ExecutionStatus.UNKNOWN,
      ExecutionStatus.NEW,
      ExecutionStatus.NOT_CREATED,
      ExecutionStatus.END_OF_PUSH_RECEIVED,
      ExecutionStatus.DVC_INGESTION_ERROR_OTHER,
      ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL,
      ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED,
      ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES,
      ExecutionStatus.ERROR,
      ExecutionStatus.WARNING,
      ExecutionStatus.COMPLETED,
      ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED,
      ExecutionStatus.ARCHIVED);

  private static final RedundantExceptionFilter EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  private static final Logger LOGGER = LogManager.getLogger(VeniceHelixAdmin.class);
  private static final int RECORD_COUNT = 10;
  public static final List<Class<? extends Throwable>> RETRY_FAILURE_TYPES = Collections.singletonList(Exception.class);

  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final String controllerClusterName;
  private final int controllerClusterReplica;
  private final String controllerName;
  private final String kafkaBootstrapServers;
  private final String kafkaSSLBootstrapServers;
  private final Map<String, AdminConsumerService> adminConsumerServices = new ConcurrentHashMap<>();

  private static final int CONTROLLER_CLUSTER_NUMBER_OF_PARTITION = 1;
  private static final long CONTROLLER_CLUSTER_RESOURCE_EV_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);
  private static final long CONTROLLER_CLUSTER_RESOURCE_EV_CHECK_DELAY_MS = 500;
  private static final long HELIX_RESOURCE_ASSIGNMENT_RETRY_INTERVAL_MS = 500;
  private static final long HELIX_RESOURCE_ASSIGNMENT_LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
  private static final long HELIX_MANAGER_DISCONNECT_TIMEOUT_MS = 1 * Time.MS_PER_MINUTE;

  protected static final int INTERNAL_STORE_GET_RRT_TOPIC_ATTEMPTS = 3;
  protected static final long INTERNAL_STORE_RTT_RETRY_BACKOFF_MS = TimeUnit.SECONDS.toMillis(5);
  private static final int PARTICIPANT_MESSAGE_STORE_SCHEMA_ID = 1;
  private static final long PUSH_STATUS_STORE_WRITER_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
  private final InternalAvroSpecificSerializer<PushJobDetails> pushJobDetailsSerializer =
      AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();

  static final int VERSION_ID_UNSET = -1;

  // TODO remove this field and all invocations once we are fully on HaaS. Use the helixAdminClient instead.
  private final HelixAdmin admin;
  /**
   * Client/wrapper used for performing Helix operations in Venice.
   */
  private final HelixAdminClient helixAdminClient;
  private TopicManagerRepository topicManagerRepository;
  private final ZkClient zkClient;
  private final HelixAdapterSerializer adapterSerializer;
  private final ZkAllowlistAccessor allowlistAccessor;
  private final ExecutionIdAccessor executionIdAccessor;
  private final RealTimeTopicSwitcher realTimeTopicSwitcher;
  private final long deprecatedJobTopicRetentionMs;

  private final long fatalDataValidationFailureRetentionMs;
  private final long deprecatedJobTopicMaxRetentionMs;
  private final HelixReadOnlyStoreConfigRepository storeConfigRepo;
  private final VeniceWriterFactory veniceWriterFactory;
  private final int minNumberOfStoreVersionsToPreserve;
  private final StoreGraveyard storeGraveyard;
  private final Map<String, String> participantMessageStoreRTTMap;
  private final boolean isControllerClusterHAAS;
  private final String coloLeaderClusterName;
  private final Optional<SSLFactory> sslFactory;
  private final boolean sslEnabled;
  private final String pushJobStatusStoreClusterName;
  private final PushStatusStoreReader pushStatusStoreReader;
  private final Lazy<PushStatusStoreWriter> pushStatusStoreWriter;
  private final SharedHelixReadOnlyZKSharedSystemStoreRepository zkSharedSystemStoreRepository;
  private final SharedHelixReadOnlyZKSharedSchemaRepository zkSharedSchemaRepository;
  private final MetaStoreWriter metaStoreWriter;
  private final MetaStoreReader metaStoreReader;
  private final D2Client d2Client;
  private final Map<String, D2Client> d2Clients;
  private final Map<String, HelixReadWriteLiveClusterConfigRepository> clusterToLiveClusterConfigRepo;
  private static final String ZK_INSTANCES_SUB_PATH = "INSTANCES";
  private static final String ZK_CUSTOMIZEDSTATES_SUB_PATH = "CUSTOMIZEDSTATES/" + HelixPartitionState.OFFLINE_PUSH;

  /**
   * Level-1 controller, it always being connected to Helix. And will create sub-controller for specific cluster when
   * getting notification from Helix.
   */
  private SafeHelixManager helixManager;
  /**
   * Builder used to build the data path to access Helix internal data of level-1 cluster.
   */
  private PropertyKey.Builder controllerClusterKeyBuilder;

  private PubSubTopic pushJobDetailsRTTopic;

  // Those variables will be initialized lazily.
  private int pushJobDetailsSchemaId = -1;

  private final Map<String, DisabledPartitionStats> disabledPartitionStatMap = new HashMap<>();
  private final Map<String, PushJobStatusStats> pushJobStatusStatsMap = new HashMap<>();
  private final Map<String, AddVersionLatencyStats> addVersionLatencyStatsMap = new HashMap<>();

  private static final String PUSH_JOB_DETAILS_WRITER = "PUSH_JOB_DETAILS_WRITER";
  private final Map<String, VeniceWriter> jobTrackingVeniceWriterMap = new VeniceConcurrentHashMap<>();

  // This map stores the time when topics were created. It only contains topics whose information has not yet been
  // persisted to Zk.
  private final Map<String, Long> topicToCreationTime = new VeniceConcurrentHashMap<>();

  /**
   * Controller Client Map per cluster per colo
   */
  private final Map<String, Map<String, ControllerClient>> clusterControllerClientPerColoMap =
      new VeniceConcurrentHashMap<>();
  private final Map<String, HelixLiveInstanceMonitor> liveInstanceMonitorMap = new HashMap<>();

  private final VeniceDistClusterControllerStateModelFactory controllerStateModelFactory;

  private final long backupVersionDefaultRetentionMs;

  private final int defaultMaxRecordSizeBytes;

  private final DataRecoveryManager dataRecoveryManager;
  private CompactionManager compactionManager;
  private final ParticipantStoreClientsManager participantStoreClientsManager;
  protected final PubSubTopicRepository pubSubTopicRepository;

  private final Object PUSH_JOB_DETAILS_CLIENT_LOCK = new Object();
  private AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> pushJobDetailsStoreClient = null;

  private final Object LIVENESS_HEARTBEAT_CLIENT_LOCK = new Object();
  private AvroSpecificStoreClient<BatchJobHeartbeatKey, BatchJobHeartbeatValue> livenessHeartbeatStoreClient = null;

  private final Lazy<ByteBuffer> emptyPushZSTDDictionary;

  private final Set<PushJobCheckpoints> pushJobUserErrorCheckpoints;
  private final LogContext logContext;

  final Map<String, DeadStoreStats> deadStoreStatsMap = new VeniceConcurrentHashMap<>();
  final Map<String, LogCompactionStats> logCompactionStatsMap = new VeniceConcurrentHashMap<>();

  // Test only.
  public VeniceHelixAdmin(
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      MetricsRepository metricsRepository,
      D2Client d2Client,
      PubSubTopicRepository pubSubTopicRepository,
      PubSubClientsFactory pubSubClientsFactory,
      PubSubPositionTypeRegistry pubSubPositionTypeRegistry) {
    this(
        multiClusterConfigs,
        metricsRepository,
        false,
        Collections.singletonMap(multiClusterConfigs.getRegionName(), d2Client),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        pubSubTopicRepository,
        pubSubClientsFactory,
        pubSubPositionTypeRegistry,
        Collections.EMPTY_LIST);
  }

  // TODO Use different configs for different clusters when creating helix admin.
  public VeniceHelixAdmin(
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      MetricsRepository metricsRepository,
      boolean sslEnabled,
      Map<String, D2Client> d2Clients,
      Optional<SSLConfig> sslConfig,
      Optional<DynamicAccessController> accessController,
      Optional<ICProvider> icProvider,
      PubSubTopicRepository pubSubTopicRepository,
      PubSubClientsFactory pubSubClientsFactory,
      PubSubPositionTypeRegistry pubSubPositionTypeRegistry,
      List<ClusterLeaderInitializationRoutine> additionalInitRoutines) {
    Validate.notNull(d2Clients);
    Validate.notEmpty(d2Clients);
    this.multiClusterConfigs = multiClusterConfigs;
    this.logContext = multiClusterConfigs.getLogContext();
    VeniceControllerClusterConfig commonConfig = multiClusterConfigs.getCommonConfig();
    this.controllerName =
        Utils.getHelixNodeIdentifier(multiClusterConfigs.getAdminHostname(), multiClusterConfigs.getAdminPort());
    this.controllerClusterName = multiClusterConfigs.getControllerClusterName();
    this.controllerClusterReplica = multiClusterConfigs.getControllerClusterReplica();
    this.kafkaBootstrapServers = multiClusterConfigs.getKafkaBootstrapServers();
    this.kafkaSSLBootstrapServers = multiClusterConfigs.getSslKafkaBootstrapServers();
    this.deprecatedJobTopicRetentionMs = multiClusterConfigs.getDeprecatedJobTopicRetentionMs();
    this.fatalDataValidationFailureRetentionMs = multiClusterConfigs.getFatalDataValidationFailureRetentionMs();
    this.deprecatedJobTopicMaxRetentionMs = multiClusterConfigs.getDeprecatedJobTopicMaxRetentionMs();
    this.backupVersionDefaultRetentionMs = multiClusterConfigs.getBackupVersionDefaultRetentionMs();
    this.defaultMaxRecordSizeBytes = multiClusterConfigs.getDefaultMaxRecordSizeBytes();

    this.minNumberOfStoreVersionsToPreserve = multiClusterConfigs.getMinNumberOfStoreVersionsToPreserve();

    LOGGER.info(
        "D2 client map: {}, getRegionName: {}, {}",
        d2Clients,
        multiClusterConfigs.getParentFabrics(),
        getRegionName());
    this.d2Client = d2Clients.get(getRegionName());
    Validate.notNull(this.d2Client);
    this.d2Clients = d2Clients;
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.sslEnabled = sslEnabled;

    if (sslEnabled) {
      try {
        String sslFactoryClassName = multiClusterConfigs.getSslFactoryClassName();
        Properties sslProperties = sslConfig.get().getSslProperties();
        sslFactory = Optional.of(SslUtils.getSSLFactory(sslProperties, sslFactoryClassName));
      } catch (Exception e) {
        LOGGER.error("Failed to create SSL engine", e);
        throw new VeniceException(e);
      }
    } else {
      sslFactory = Optional.empty();
    }

    // TODO: Consider re-using the same zkClient for the ZKHelixAdmin and TopicManager.
    ZkClient zkClientForHelixAdmin = ZkClientFactory.newZkClient(multiClusterConfigs.getZkAddress());
    zkClientForHelixAdmin
        .subscribeStateChanges(new ZkClientStatusStats(metricsRepository, "controller-zk-client-for-helix-admin"));
    /**
     * N.B.: The following setup steps are necessary when using the {@link ZKHelixAdmin} constructor which takes
     * in an external {@link ZkClient}.
     *
     * {@link ZkClient#setZkSerializer(ZkSerializer)} is necessary, otherwise Helix will throw:
     *
     * org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError: java.io.NotSerializableException: org.apache.helix.ZNRecord
     */
    zkClientForHelixAdmin.setZkSerializer(new ZNRecordSerializer());
    if (!zkClientForHelixAdmin.waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)) {
      throw new VeniceException("Failed to connect to ZK within " + ZkClient.DEFAULT_CONNECTION_TIMEOUT + " ms!");
    }
    this.admin = new ZKHelixAdmin(zkClientForHelixAdmin);
    this.helixAdminClient = new ZkHelixAdminClient(multiClusterConfigs, metricsRepository);
    // There is no way to get the internal zkClient from HelixManager or HelixAdmin. So create a new one here.
    this.zkClient = ZkClientFactory.newZkClient(multiClusterConfigs.getZkAddress());
    this.zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, "controller-zk-client"));
    this.adapterSerializer = new HelixAdapterSerializer();

    TopicManagerContext topicManagerContext =
        new TopicManagerContext.Builder().setPubSubTopicRepository(pubSubTopicRepository)
            .setPubSubPositionTypeRegistry(commonConfig.getPubSubPositionTypeRegistry())
            .setMetricsRepository(metricsRepository)
            .setPubSubPropertiesSupplier(this::getPubSubSSLPropertiesFromControllerConfig)
            .setPubSubAdminAdapterFactory(pubSubClientsFactory.getAdminAdapterFactory())
            .setPubSubConsumerAdapterFactory(pubSubClientsFactory.getConsumerAdapterFactory())
            .setTopicMetadataFetcherConsumerPoolSize(commonConfig.getTopicManagerMetadataFetcherConsumerPoolSize())
            .setTopicMetadataFetcherThreadPoolSize(commonConfig.getTopicManagerMetadataFetcherThreadPoolSize())
            .setVeniceComponent(VeniceComponent.CONTROLLER)
            .build();
    this.topicManagerRepository =
        new TopicManagerRepository(topicManagerContext, getKafkaBootstrapServers(isSslToKafka()));

    this.allowlistAccessor = new ZkAllowlistAccessor(zkClient, adapterSerializer);
    this.executionIdAccessor = new ZkExecutionIdAccessor(zkClient, adapterSerializer);
    this.storeConfigRepo = new HelixReadOnlyStoreConfigRepository(zkClient, adapterSerializer);
    storeConfigRepo.refresh();
    this.storeGraveyard = new HelixStoreGraveyard(zkClient, adapterSerializer, multiClusterConfigs.getClusters());
    this.veniceWriterFactory = new VeniceWriterFactory(
        commonConfig.getProps().toProperties(),
        pubSubClientsFactory.getProducerAdapterFactory(),
        metricsRepository,
        pubSubPositionTypeRegistry);
    this.realTimeTopicSwitcher = new RealTimeTopicSwitcher(
        topicManagerRepository.getLocalTopicManager(),
        veniceWriterFactory,
        commonConfig.getProps(),
        pubSubTopicRepository);
    this.participantMessageStoreRTTMap = new VeniceConcurrentHashMap<>();
    isControllerClusterHAAS = commonConfig.isControllerClusterLeaderHAAS();
    coloLeaderClusterName = commonConfig.getClusterName();
    pushJobStatusStoreClusterName = commonConfig.getPushJobStatusStoreClusterName();

    zkSharedSystemStoreRepository = new SharedHelixReadOnlyZKSharedSystemStoreRepository(
        zkClient,
        adapterSerializer,
        commonConfig.getSystemSchemaClusterName());
    zkSharedSchemaRepository = new SharedHelixReadOnlyZKSharedSchemaRepository(
        zkSharedSystemStoreRepository,
        zkClient,
        adapterSerializer,
        commonConfig.getSystemSchemaClusterName(),
        commonConfig.getRefreshAttemptsForZkReconnect(),
        commonConfig.getRefreshIntervalForZkReconnectInMs());
    metaStoreWriter = new MetaStoreWriter(
        topicManagerRepository.getLocalTopicManager(),
        veniceWriterFactory,
        zkSharedSchemaRepository,
        pubSubTopicRepository,
        commonConfig.getMetaStoreWriterCloseTimeoutInMS(),
        commonConfig.getMetaStoreWriterCloseConcurrency());
    metaStoreReader = new MetaStoreReader(d2Client, commonConfig.getClusterDiscoveryD2ServiceName());
    pushStatusStoreReader = new PushStatusStoreReader(
        d2Client,
        commonConfig.getClusterDiscoveryD2ServiceName(),
        commonConfig.getPushStatusStoreHeartbeatExpirationTimeInSeconds());
    pushStatusStoreWriter = Lazy.of(() -> {
      String pushStatusStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getZkSharedStoreName();
      SchemaEntry valueSchemaEntry = zkSharedSchemaRepository.getSupersetOrLatestValueSchema(pushStatusStoreName);
      DerivedSchemaEntry updateSchemaEntry =
          zkSharedSchemaRepository.getLatestDerivedSchema(pushStatusStoreName, valueSchemaEntry.getId());
      return new PushStatusStoreWriter(
          veniceWriterFactory,
          CONTROLLER_HEARTBEAT_INSTANCE_NAME,
          valueSchemaEntry,
          updateSchemaEntry);
    });

    clusterToLiveClusterConfigRepo = new VeniceConcurrentHashMap<>();
    participantStoreClientsManager = new ParticipantStoreClientsManager(
        d2Client,
        commonConfig.getClusterDiscoveryD2ServiceName(),
        topicManagerRepository,
        veniceWriterFactory,
        pubSubTopicRepository);
    dataRecoveryManager =
        new DataRecoveryManager(this, icProvider, pubSubTopicRepository, participantStoreClientsManager);

    List<ClusterLeaderInitializationRoutine> initRoutines = new ArrayList<>();
    initRoutines.add(
        new SystemSchemaInitializationRoutine(
            AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE,
            multiClusterConfigs,
            this));
    initRoutines
        .add(new SystemSchemaInitializationRoutine(AvroProtocolDefinition.PARTITION_STATE, multiClusterConfigs, this));
    initRoutines.add(
        new SystemSchemaInitializationRoutine(AvroProtocolDefinition.STORE_VERSION_STATE, multiClusterConfigs, this));
    initRoutines.add(
        new SystemSchemaInitializationRoutine(
            AvroProtocolDefinition.SERVER_METADATA_RESPONSE,
            multiClusterConfigs,
            this));
    initRoutines.add(
        new SystemSchemaInitializationRoutine(
            AvroProtocolDefinition.SERVER_STORE_PROPERTIES_PAYLOAD,
            multiClusterConfigs,
            this));

    if (multiClusterConfigs.isZkSharedMetaSystemSchemaStoreAutoCreationEnabled()) {
      // Add routine to create zk shared metadata system store
      initRoutines.add(
          new SystemSchemaInitializationRoutine(
              AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
              multiClusterConfigs,
              this,
              Optional.of(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE_KEY.getCurrentProtocolVersionSchema()),
              Optional.of(VeniceSystemStoreUtils.DEFAULT_USER_SYSTEM_STORE_UPDATE_QUERY_PARAMS),
              true));
    }
    if (multiClusterConfigs.isZkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled()) {
      // Add routine to create zk shared da vinci push status system store
      initRoutines.add(
          new SystemSchemaInitializationRoutine(
              AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE,
              multiClusterConfigs,
              this,
              Optional.of(AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE_KEY.getCurrentProtocolVersionSchema()),
              Optional.of(VeniceSystemStoreUtils.DEFAULT_USER_SYSTEM_STORE_UPDATE_QUERY_PARAMS),
              true));
    }
    initRoutines.addAll(additionalInitRoutines);

    // Participant stores are not read or written in parent colo. Parent controller skips participant store
    // initialization.
    if (!isParent() && multiClusterConfigs.isParticipantMessageStoreEnabled()) {
      LOGGER.info("Adding PerClusterInternalRTStoreInitializationRoutine for ParticipantMessageStore");
      initRoutines.add(
          new PerClusterInternalRTStoreInitializationRoutine(
              PARTICIPANT_MESSAGE_SYSTEM_STORE_VALUE,
              VeniceSystemStoreUtils::getParticipantStoreNameForCluster,
              multiClusterConfigs,
              this,
              ParticipantMessageKey.getClassSchema()));
    }

    for (String clusterName: multiClusterConfigs.getClusters()) {
      if (multiClusterConfigs.getControllerConfig(clusterName).isDeadStoreEndpointEnabled()) {
        Class<? extends DeadStoreStats> deadStoreStatsClass =
            ReflectUtils.loadClass(multiClusterConfigs.getControllerConfig(clusterName).getDeadStoreStatsClassName());
        try {
          DeadStoreStats deadStoreStats = ReflectUtils.callConstructor(
              deadStoreStatsClass,
              new Class[] { VeniceProperties.class },
              new Object[] { multiClusterConfigs.getControllerConfig(clusterName).getDeadStoreStatsConfigs() });

          deadStoreStatsMap.put(clusterName, deadStoreStats);
        } catch (Exception e) {
          LOGGER.error("Failed to enable " + DeadStoreStats.class.getSimpleName(), e);
          throw new VeniceException(e);
        }
      }
    }

    ClusterLeaderInitializationManager clusterLeaderInitializationManager = new ClusterLeaderInitializationManager(
        initRoutines,
        commonConfig.isConcurrentInitRoutinesEnabled(),
        commonConfig.getLogContext());

    // Create the controller cluster if required.
    if (isControllerClusterHAAS) {
      checkAndCreateVeniceControllerCluster();
    } else {
      createControllerClusterIfRequired();
    }

    controllerStateModelFactory = new VeniceDistClusterControllerStateModelFactory(
        zkClient,
        adapterSerializer,
        this,
        multiClusterConfigs,
        metricsRepository,
        clusterLeaderInitializationManager,
        realTimeTopicSwitcher,
        accessController,
        helixAdminClient);

    for (String clusterName: multiClusterConfigs.getClusters()) {
      if (multiClusterConfigs.getControllerConfig(clusterName).isLogCompactionEnabled()) {
        logCompactionStatsMap.putIfAbsent(clusterName, new LogCompactionStats(metricsRepository, clusterName));
      }

      if (!multiClusterConfigs.getControllerConfig(clusterName).isErrorLeaderReplicaFailOverEnabled()) {
        continue;
      }

      HelixLiveInstanceMonitor liveInstanceMonitor = new HelixLiveInstanceMonitor(this.zkClient, clusterName);
      DisabledPartitionStats disabledPartitionStats = new DisabledPartitionStats(metricsRepository, clusterName);
      PushJobStatusStats pushJobStatusStats = new PushJobStatusStats(metricsRepository, clusterName);
      disabledPartitionStatMap.put(clusterName, disabledPartitionStats);
      liveInstanceMonitorMap.put(clusterName, liveInstanceMonitor);
      pushJobStatusStatsMap.put(clusterName, pushJobStatusStats);
      addVersionLatencyStatsMap.put(clusterName, new AddVersionLatencyStats(metricsRepository, clusterName));
      // Register new instance callback
      liveInstanceMonitor.registerLiveInstanceChangedListener(new LiveInstanceChangedListener() {
        @Override
        public void handleNewInstances(Set<Instance> newInstances) {
          long startTime = System.currentTimeMillis();
          for (Instance instance: newInstances) {
            Map<String, List<String>> disabledPartitions =
                helixAdminClient.getDisabledPartitionsMap(clusterName, instance.getNodeId());
            for (Map.Entry<String, List<String>> entry: disabledPartitions.entrySet()) {
              helixAdminClient
                  .enablePartition(true, clusterName, instance.getNodeId(), entry.getKey(), entry.getValue());
              disabledPartitionStats.recordClearDisabledPartition(entry.getValue().size());
              LOGGER.info("Enabled disabled replica of resource {}, partitions {}", entry.getKey(), entry.getValue());
            }
          }
          LOGGER.info(
              "Enabling disabled replicas for instances {} took {} ms",
              newInstances.stream().map(Instance::getNodeId).collect(Collectors.joining(",")),
              LatencyUtils.getElapsedTimeFromMsToMs(startTime));
        }

        @Override
        public void handleDeletedInstances(Set<Instance> deletedInstances) {
        }
      });
    }

    if (multiClusterConfigs.isLogCompactionEnabled()) {
      Class<? extends RepushOrchestrator> repushOrchestratorClass =
          ReflectUtils.loadClass(multiClusterConfigs.getRepushOrchestratorClassName());
      try {
        RepushOrchestrator repushOrchestrator = ReflectUtils.callConstructor(
            repushOrchestratorClass,
            new Class[] { VeniceProperties.class },
            new Object[] { multiClusterConfigs.getRepushOrchestratorConfigs() });
        this.compactionManager = new CompactionManager(
            repushOrchestrator,
            multiClusterConfigs.getTimeSinceLastLogCompactionThresholdMS(),
            logCompactionStatsMap);
      } catch (Exception e) {
        LOGGER.error(
            "[log-compaction] Failed to enable repush for log compaction " + CompactionManager.class.getSimpleName(),
            e);
        throw new VeniceException(e);
      }
    }

    emptyPushZSTDDictionary =
        Lazy.of(() -> ByteBuffer.wrap(ZstdWithDictCompressor.buildDictionaryOnSyntheticAvroData()));

    pushJobUserErrorCheckpoints = commonConfig.getPushJobUserErrorCheckpoints();
  }

  private VeniceProperties getPubSubSSLPropertiesFromControllerConfig(String pubSubBootstrapServers) {
    VeniceControllerClusterConfig controllerConfig = multiClusterConfigs.getCommonConfig();

    VeniceProperties originalPros = controllerConfig.getProps();
    Properties clonedProperties = originalPros.toProperties();
    if (originalPros.getBooleanWithAlternative(KAFKA_OVER_SSL, SSL_TO_KAFKA_LEGACY, false)) {
      clonedProperties.setProperty(SSL_KAFKA_BOOTSTRAP_SERVERS, pubSubBootstrapServers);
    } else {
      clonedProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, pubSubBootstrapServers);
    }
    controllerConfig = new VeniceControllerClusterConfig(new VeniceProperties(clonedProperties));
    Properties properties = multiClusterConfigs.getCommonConfig().getProps().getPropertiesCopy();
    if (PubSubUtil.isPubSubSslProtocol(controllerConfig.getPubSubSecurityProtocol())) {
      Optional<SSLConfig> sslConfig = controllerConfig.getSslConfig();
      if (!sslConfig.isPresent()) {
        throw new VeniceException("SSLConfig should be present when Kafka SSL is enabled");
      }
      properties.putAll(sslConfig.get().getKafkaSSLConfig());
      properties.setProperty(PUBSUB_SECURITY_PROTOCOL, controllerConfig.getPubSubSecurityProtocol());
      properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, controllerConfig.getSslKafkaBootstrapServers());
    } else {
      properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, controllerConfig.getKafkaBootstrapServers());
    }
    return new VeniceProperties(properties);
  }

  public void startInstanceMonitor(String clusterName) {
    if (!multiClusterConfigs.getControllerConfig(clusterName).isErrorLeaderReplicaFailOverEnabled()) {
      return;
    }
    HelixLiveInstanceMonitor liveInstanceMonitor = liveInstanceMonitorMap.get(clusterName);
    if (liveInstanceMonitor == null) {
      LOGGER.warn("Could not find live instance monitor for cluster {}", clusterName);
      return;
    }
    liveInstanceMonitor.refresh();
  }

  public LiveInstanceMonitor getLiveInstanceMonitor(String clusterName) {
    return liveInstanceMonitorMap.get(clusterName);
  }

  public void clearInstanceMonitor(String clusterName) {
    if (!multiClusterConfigs.getControllerConfig(clusterName).isErrorLeaderReplicaFailOverEnabled()) {
      return;
    }
    HelixLiveInstanceMonitor liveInstanceMonitor = liveInstanceMonitorMap.get(clusterName);
    if (liveInstanceMonitor == null) {
      LOGGER.warn("Could not find live instance monitor for cluster {}", clusterName);
      return;
    }
    liveInstanceMonitor.clear();
  }

  private void checkAndCreateVeniceControllerCluster() {
    if (!helixAdminClient.isVeniceControllerClusterCreated()) {
      helixAdminClient.createVeniceControllerCluster();
    }
    if (!helixAdminClient.isClusterInGrandCluster(controllerClusterName)) {
      helixAdminClient.addClusterToGrandCluster(controllerClusterName);
    }
  }

  private synchronized void connectToControllerCluster() {
    if (helixManager != null) {
      LOGGER.warn("Controller {} is already connected to the controller cluster", controllerName);
      return;
    }
    InstanceType instanceType =
        isControllerClusterHAAS ? InstanceType.PARTICIPANT : InstanceType.CONTROLLER_PARTICIPANT;
    String zkAddress = multiClusterConfigs.getControllerClusterZkAddress();

    InstanceConfig.Builder defaultInstanceConfigBuilder =
        new InstanceConfig.Builder().setPort(Integer.toString(multiClusterConfigs.getAdminPort()));

    HelixManagerProperty helixManagerProperty =
        new HelixManagerProperty.Builder().setDefaultInstanceConfigBuilder(defaultInstanceConfigBuilder).build();

    SafeHelixManager tempManager = new SafeHelixManager(
        new ZKHelixManager(controllerClusterName, controllerName, instanceType, zkAddress, null, helixManagerProperty));

    // for refreshing the instance config list if the controller is already a participant in venice-controller cluster
    // but assigned to another venice cluster
    tempManager.addPreConnectCallback(
        new ControllerInstanceTagRefresher(tempManager, multiClusterConfigs.getControllerInstanceTagList()));
    StateMachineEngine stateMachine = tempManager.getStateMachineEngine();
    stateMachine.registerStateModelFactory(LeaderStandbySMD.name, controllerStateModelFactory);
    try {
      tempManager.connect();
    } catch (Exception ex) {
      String errorMessage = "Error connecting to Helix controller cluster " + controllerClusterName
          + " with controller " + controllerName;
      LOGGER.error(errorMessage, ex);
      throw new VeniceException(errorMessage, ex);
    }
    controllerClusterKeyBuilder = new PropertyKey.Builder(tempManager.getClusterName());
    helixManager = tempManager;
    LOGGER.info("Connected to controller cluster {} with controller {}", controllerClusterName, controllerName);
  }

  public ZkClient getZkClient() {
    return zkClient;
  }

  public ExecutionIdAccessor getExecutionIdAccessor() {
    return executionIdAccessor;
  }

  public HelixAdapterSerializer getAdapterSerializer() {
    return adapterSerializer;
  }

  // For testing purpose.
  void setTopicManagerRepository(TopicManagerRepository topicManagerRepository) {
    this.topicManagerRepository = topicManagerRepository;
  }

  /**
   * Create and configure the Venice storage cluster with required properties in Helix and waits
   * the resource's (partial) partition to appear in the external view.
   * @param clusterName Venice cluster name.
   */
  @Override
  public synchronized void initStorageCluster(String clusterName) {
    if (helixManager == null) {
      connectToControllerCluster();
    }
    // Simply validate cluster name here.
    clusterName = clusterName.trim();
    if (clusterName.startsWith("/") || clusterName.endsWith("/") || clusterName.indexOf(' ') >= 0) {
      throw new IllegalArgumentException("Invalid cluster name:" + clusterName);
    }
    if (multiClusterConfigs.getControllerConfig(clusterName).isVeniceClusterLeaderHAAS()) {
      setupStorageClusterAsNeeded(clusterName);
    } else {
      createClusterIfRequired(clusterName);
    }
    // The customized state config may get wiped or have never been written to ZK cluster config before, we need to
    // enable at first.
    HelixUtils.setupCustomizedStateConfig(admin, clusterName);
    // The resource and partition may be disabled for this controller before, we need to enable again at first. Then the
    // state transition will be triggered.
    List<String> partitionNames =
        Collections.singletonList(VeniceControllerStateModel.getPartitionNameFromVeniceClusterName(clusterName));
    helixAdminClient.enablePartition(true, controllerClusterName, controllerName, clusterName, partitionNames);
    if (multiClusterConfigs.getControllerConfig(clusterName).isParticipantMessageStoreEnabled()) {
      participantMessageStoreRTTMap.put(
          clusterName,
          Utils.composeRealTimeTopic(VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName)));
    }
    waitUntilClusterResourceIsVisibleInEV(clusterName);
  }

  private void waitUntilClusterResourceIsVisibleInEV(String clusterName) {
    long startTime = System.currentTimeMillis();
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(controllerClusterName);
    while (System.currentTimeMillis() - startTime < CONTROLLER_CLUSTER_RESOURCE_EV_TIMEOUT_MS) {
      ExternalView externalView = helixManager.getHelixDataAccessor().getProperty(keyBuilder.externalView(clusterName));
      String partitionName = HelixUtils.getPartitionName(clusterName, 0);
      if (externalView != null && externalView.getStateMap(partitionName) != null
          && !externalView.getStateMap(partitionName).isEmpty()) {
        LOGGER.info("External view is available for cluster resource: {}", clusterName);
        return;
      }
      try {
        // Wait
        Thread.sleep(CONTROLLER_CLUSTER_RESOURCE_EV_CHECK_DELAY_MS);
      } catch (InterruptedException e) {
        throw new VeniceException("Failed to verify the external view of cluster resource: " + clusterName, e);
      }
    }
    throw new VeniceException("Timed out when waiting for the external view of cluster resource: " + clusterName);
  }

  private boolean isResourceStillAlive(String clusterName, String resourceName) {
    String externalViewPath = "/" + clusterName + "/EXTERNALVIEW/" + resourceName;
    return zkClient.exists(externalViewPath);
  }

  List<String> getAllLiveHelixResources(String clusterName) {
    return zkClient.getChildren("/" + clusterName + "/EXTERNALVIEW");
  }

  /**
   * Test if a given helix resource is still alive (existent in ZK).
   * @param resourceName Helix resource name.
   * @return <code>true</code> if resource exists;
   *         <code>false</code> otherwise.
   */
  @Override
  public boolean isResourceStillAlive(String resourceName) {
    if (!Version.isATopicThatIsVersioned(resourceName)) {
      throw new VeniceException("Resource name: " + resourceName + " is invalid");
    }
    String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    // Find out the cluster first
    Optional<StoreConfig> storeConfig = storeConfigRepo.getStoreConfig(storeName);
    if (!storeConfig.isPresent()) {
      LOGGER.info(
          "StoreConfig doesn't exist for store: {}, will treat resource: {} as deprecated",
          storeName,
          resourceName);
      return false;
    }
    String clusterName = storeConfig.get().getCluster();
    return isResourceStillAlive(clusterName, resourceName);
  }

  /**
   * Test if a cluster is valid (in Helix cluster list).
   * @param clusterName Venice cluster name.
   * @return <code>true</code> if input cluster is in Helix cluster list;
   *         <code>false</code> otherwise.
   */
  @Override
  public boolean isClusterValid(String clusterName) {
    return admin.getClusters().contains(clusterName);
  }

  protected HelixAdmin getHelixAdmin() {
    return this.admin;
  }

  /**
   * Create a new ZK store and its configuration in the store repository and create schemas in the schema repository.
   * @param clusterName Venice cluster where the store locates.
   * @param storeName name of the store.
   * @param owner owner of the store.
   * @param keySchema key schema of the store.
   * @param valueSchema value schema of the store.
   * @param isSystemStore if the store is a system store.
   * @param accessPermissions json string representing the access-permissions.
   */
  @Override
  public void createStore(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema,
      boolean isSystemStore,
      Optional<String> accessPermissions) {
    HelixVeniceClusterResources clusterResources = getHelixVeniceClusterResources(clusterName);
    LOGGER.info("Start creating store {} in cluster {} with owner {}", storeName, clusterName, owner);
    try (AutoCloseableLock ignore = clusterResources.getClusterLockManager().createStoreWriteLock(storeName)) {
      checkPreConditionForCreateStore(clusterName, storeName, keySchema, valueSchema, isSystemStore, true);
      VeniceControllerClusterConfig config = getHelixVeniceClusterResources(clusterName).getConfig();
      Store newStore = new ZKStore(
          storeName,
          owner,
          System.currentTimeMillis(),
          config.getPersistenceType(),
          config.getRoutingStrategy(),
          config.getReadStrategy(),
          config.getOfflinePushStrategy(),
          config.getReplicationFactor());

      ReadWriteStoreRepository storeRepo = clusterResources.getStoreMetadataRepository();
      Store existingStore = storeRepo.getStore(storeName);
      if (existingStore != null) {
        /*
         * We already check the pre-condition before, so if we could find a store with the same name,
         * it means the store is a reprocessing store which is left by a failed deletion. So we should delete it.
         */
        deleteStore(clusterName, storeName, existingStore.getLargestUsedVersionNumber(), true);
      }
      /*
       * Now there is no store exists in the store repository, we will try to retrieve the info from the graveyard.
       * Get the largestUsedVersionNumber from graveyard to avoid resource conflict.
       */
      int largestUsedStoreVersion = storeGraveyard.getLargestUsedVersionNumber(storeName);
      if (largestUsedStoreVersion == NON_EXISTING_VERSION) {
        LOGGER.info(
            "Store: {} does NOT exist in the store graveyard. Will initialize the new store at version: {}.",
            storeName,
            NON_EXISTING_VERSION);
      } else {
        LOGGER.info(
            "Found store: {} in the store graveyard. Will initialize the new store at version: {}.",
            storeName,
            largestUsedStoreVersion);
      }

      int largestUsedRTStoreVersion = storeGraveyard.getLargestUsedRTVersionNumber(storeName);
      if (largestUsedRTStoreVersion == NON_EXISTING_VERSION) {
        LOGGER.info(
            "Store: {} does NOT exist in the store graveyard. Will initialize the RT version to {}.",
            storeName,
            NON_EXISTING_VERSION);
      } else {
        LOGGER.info(
            "Found store: {} in the store graveyard. Will initialize the RT version to {}.",
            storeName,
            largestUsedRTStoreVersion);
      }

      configureNewStore(newStore, config, largestUsedStoreVersion, largestUsedRTStoreVersion);

      storeRepo.addStore(newStore);
      // Create global config for that store.
      ZkStoreConfigAccessor storeConfigAccessor = getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor();
      if (!storeConfigAccessor.containsConfig(storeName)) {
        storeConfigAccessor.createConfig(storeName, clusterName);
      }

      // Add schemas
      ReadWriteSchemaRepository schemaRepo = clusterResources.getSchemaRepository();
      schemaRepo.initKeySchema(storeName, keySchema);
      schemaRepo.addValueSchema(storeName, valueSchema, HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID);
      LOGGER.info(
          "Completed creating store: {} in cluster: {} with owner: {} and largestUsedVersionNumber: {} and partitionCount: {}",
          storeName,
          clusterName,
          owner,
          newStore.getLargestUsedVersionNumber(),
          newStore.getPartitionCount());
    }
  }

  private void configureNewStore(
      Store newStore,
      VeniceControllerClusterConfig config,
      int largestUsedVersionNumber,
      int largestUsedRTVersionNumber) {
    newStore.setNativeReplicationEnabled(config.isMultiRegion());

    /**
     * Initialize default NR source fabric base on default config for different store types.
     */
    if (newStore.isHybrid()) {
      newStore.setNativeReplicationSourceFabric(config.getNativeReplicationSourceFabricAsDefaultForHybrid());
    } else {
      newStore.setNativeReplicationSourceFabric(config.getNativeReplicationSourceFabricAsDefaultForBatchOnly());
    }
    newStore.setLargestUsedVersionNumber(largestUsedVersionNumber);
    /* If this store existed previously, we do not want to use the same RT topic name that was used by the previous
    store. To ensure this, increase largestUsedRTVersionNumber and new RT name will be different */
    if (config.isRealTimeTopicVersioningEnabled()) {
      newStore.setLargestUsedRTVersionNumber(largestUsedRTVersionNumber + 1);
    }
  }

  /**
   * This method will delete store data, metadata, version and rt topics
   * One exception is for stores with isMigrating flag set. In that case, the corresponding kafka topics and storeConfig
   * will not be deleted so that they are still available for the cloned store.
   */
  @Override
  public void deleteStore(
      String clusterName,
      String storeName,
      boolean isAbortMigrationCleanup,
      int largestUsedVersionNumber,
      boolean waitOnRTTopicDeletion) {
    deleteStore(
        clusterName,
        storeName,
        largestUsedVersionNumber,
        waitOnRTTopicDeletion,
        false,
        isAbortMigrationCleanup);
  }

  public void deleteStore(
      String clusterName,
      String storeName,
      int largestUsedVersionNumber,
      boolean waitOnRTTopicDeletion) {
    deleteStore(clusterName, storeName, largestUsedVersionNumber, waitOnRTTopicDeletion, false, false);
  }

  private void deleteStore(
      String clusterName,
      String storeName,
      int largestUsedVersionNumber,
      boolean waitOnRTTopicDeletion,
      boolean isForcedDelete,
      boolean isAbortMigrationCleanup) {
    checkControllerLeadershipFor(clusterName);
    LOGGER.info("Start deleting store: {} in cluster {}", storeName, clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      ReadWriteStoreRepository storeRepository = resources.getStoreMetadataRepository();
      ZkStoreConfigAccessor storeConfigAccessor = getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor();
      StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
      Store store = storeRepository.getStore(storeName);
      try {
        checkPreConditionForDeletion(clusterName, storeName, store);
        setLargestUsedVersionForStoreDeletion(store, largestUsedVersionNumber);
        storeRepository.updateStore(store);
      } catch (VeniceNoStoreException e) {
        // It's possible for a store to partially exist due to partial delete/creation failures.
        LOGGER.warn("Store object is missing for store: {} will proceed with the rest of store deletion", storeName);
      }
      if (isAbortMigrationCleanup && store != null && !store.isMigrating()) {
        LOGGER.warn(
            "Deletion of store: {} in cluster: {} was issued as part of abort migration resource cleanup, but the store's "
                + "migrating flag is false. Please ensure the store's migrating flag is set to true in the destination "
                + "cluster before issuing the delete command to prevent accidental deletion of shared resources.",
            storeName,
            clusterName);
        return;
      }
      if (storeConfig != null) {
        setStoreConfigDeletingFlag(storeConfig, clusterName, storeName, store);
        if (storeConfig.isDeleting()) {
          boolean isMetaSystemStoreEnabled = store != null && store.isStoreMetaSystemStoreEnabled();
          storeConfigAccessor.updateConfig(storeConfig, isMetaSystemStoreEnabled);
        }
      }
      if (store != null) {
        List<PubSubTopic> rtTopics = Utils.getAllRealTimeTopicNames(store)
            .stream()
            .map(pubSubTopicRepository::getTopic)
            .collect(Collectors.toList());
        // Delete All versions and push statues
        deleteAllVersionsInStore(clusterName, storeName);
        resources.getPushMonitor().cleanupStoreStatus(storeName);
        // Truncate all the version topics, this is a prerequisite to delete the RT topic
        truncateOldTopics(clusterName, store, true);

        if (!store.isMigrating() && !isAbortMigrationCleanup) {
          // Some implementations of PubSubAdminAdapter may retry multiple times to get/update topic's config in order
          // to truncate it. Because of this, `truncateKafkaTopic` may take long enough time to make `delete store`
          // operation time out. Therefore, we check for the existence of RT topic before trying to truncate it.
          // No known implementation of PubSubAdminAdapter does retries for `containsTopic`.
          for (PubSubTopic rtTopic: rtTopics) {
            if (getTopicManager().containsTopic(rtTopic)) {
              // for RT topic block on deletion so that next create store does not see the lingering RT topic which
              // could have different partition count
              truncateKafkaTopic(rtTopic.getName());
              if (waitOnRTTopicDeletion && getTopicManager().containsTopic(rtTopic)) {
                throw new VeniceRetriableException("Waiting for RT topic deletion for store: " + storeName);
              }
            }
          }
        }

        // Cleanup system stores if applicable
        UserSystemStoreLifeCycleHelper.maybeDeleteSystemStoresForUserStore(
            this,
            storeRepository,
            resources.getPushMonitor(),
            clusterName,
            store,
            metaStoreWriter,
            LOGGER);

        if (isForcedDelete) {
          storeGraveyard.removeStoreFromGraveyard(clusterName, storeName);
        } else {
          // Move the store to graveyard. It will only re-create the znode for store's metadata excluding key and
          // value schemas.
          LOGGER.info("Putting store: {} into graveyard", storeName);
          storeGraveyard.putStoreIntoGraveyard(clusterName, storeRepository.getStore(storeName));
        }
        // Helix will remove all data under this store's znode including key and value schemas.
        resources.getStoreMetadataRepository().deleteStore(storeName);
      }
      storeConfig = storeConfigAccessor.getStoreConfig(storeName);
      // Delete the config for this store after deleting the store.
      if (storeConfig != null && storeConfig.isDeleting()) {
        storeConfigAccessor.deleteConfig(storeName);
      }
      LOGGER.info("Store {} in cluster {} has been deleted.", storeName, clusterName);
    }
  }

  private void setLargestUsedVersionForStoreDeletion(Store store, int providedLargestUsedVersion) {
    if (providedLargestUsedVersion == Store.IGNORE_VERSION) {
      // ignore and use the local largest used version number.
      LOGGER.info(
          "Provided largest used version number is: {} will use local largest used version for store graveyard.",
          providedLargestUsedVersion);
    } else if (providedLargestUsedVersion < store.getLargestUsedVersionNumber()) {
      throw new VeniceException(
          "Provided largest used version number: " + providedLargestUsedVersion
              + " is smaller than the local largest used version number: " + store.getLargestUsedVersionNumber()
              + " for store: " + store.getName());
    } else {
      store.setLargestUsedVersionNumber(providedLargestUsedVersion);
    }
  }

  private void setStoreConfigDeletingFlag(StoreConfig storeConfig, String clusterName, String storeName, Store store) {
    String currentlyDiscoveredClusterName = storeConfig.getCluster();
    if (!currentlyDiscoveredClusterName.equals(clusterName)) {
      // This is most likely the deletion after a store migration operation. In this case the storeConfig should
      // not be deleted because it's still being used to discover the cloned store
      LOGGER.warn(
          "storeConfig for this store {} in cluster {} will not be deleted because it is currently pointing to "
              + "another cluster: {}",
          storeName,
          clusterName,
          currentlyDiscoveredClusterName);
    } else if (store != null && store.isMigrating()) {
      // Cluster discovery is correct but store migration flag has been set.
      // This is most likely a direct deletion command from admin-tool sent to the wrong cluster.
      // i.e. instead of using the proper --end-migration command, a --delete-store command was issued AND sent to the
      // wrong cluster
      LOGGER.warn(
          "Skipping storeConfig deletion for store {} in cluster {} because this is either the cloned store "
              + "after a successful migration or the original store after a failed migration.",
          storeName,
          clusterName);
    } else {
      // Update deletion flag in Store to start deleting. In case of any failures during the deletion, the store
      // could be deleted later after controller is recovered.
      // A worse case is deletion succeeded in parent controller but failed in production controller. After skip
      // the admin message offset, a store was left in some prod colos. While user re-create the store, we will
      // delete this reprocessing store if isDeleting is true for this store.
      storeConfig.setDeleting(true);
    }
  }

  private Integer fetchSystemStoreSchemaId(String clusterName, String storeName, String valueSchemaStr) {
    if (isLeaderControllerFor(clusterName)) {
      // Can be fetched from local repository
      int valueSchemaId = getValueSchemaId(clusterName, storeName, valueSchemaStr);
      if (SchemaData.INVALID_VALUE_SCHEMA_ID == valueSchemaId) {
        throw new InvalidVeniceSchemaException(
            "Can not find any registered value schema for the store " + storeName + " that matches the requested schema"
                + valueSchemaStr);
      }
      return valueSchemaId;
    }

    ControllerClient controllerClient = ControllerClient
        .constructClusterControllerClient(clusterName, getLeaderController(clusterName).getUrl(sslEnabled), sslFactory);
    SchemaResponse response = controllerClient.getValueSchemaID(storeName, valueSchemaStr);
    if (response.isError()) {
      throw new VeniceException(
          "Failed to fetch schema id for store: " + storeName + ", error: " + response.getError());
    }
    return response.getId();
  }

  static boolean isPushJobFailedDueToUserError(
      PushJobDetailsStatus status,
      PushJobDetails pushJobDetails,
      Set<PushJobCheckpoints> pushJobUserErrorCheckpoints) {
    if (PushJobDetailsStatus.isFailed(status)) {
      PushJobCheckpoints checkpoint = PushJobCheckpoints.valueOf(pushJobDetails.getPushJobLatestCheckpoint());
      return (checkpoint != null && pushJobUserErrorCheckpoints.contains(checkpoint));
    }
    return false;
  }

  static void emitPushJobStatusMetrics(
      Map<String, PushJobStatusStats> pushJobStatusStatsMap,
      Map<String, LogCompactionStats> logCompactionStatsMap,
      PushJobStatusRecordKey pushJobDetailsKey,
      PushJobDetails pushJobDetailsValue,
      Set<PushJobCheckpoints> pushJobUserErrorCheckpoints) {
    List<PushJobDetailsStatusTuple> overallStatuses = pushJobDetailsValue.getOverallStatus();
    if (overallStatuses.isEmpty()) {
      return;
    }
    try {
      // Push job status detail metrics
      PushJobDetailsStatus overallStatus =
          PushJobDetailsStatus.valueOf(overallStatuses.get(overallStatuses.size() - 1).getStatus());
      if (PushJobDetailsStatus.isTerminal(overallStatus.getValue())) {
        String cluster = pushJobDetailsValue.getClusterName().toString();
        PushJobStatusStats pushJobStatusStats = pushJobStatusStatsMap.get(cluster);
        Utf8 incPushKey = new Utf8("incremental.push");
        boolean isIncrementalPush = false;
        if (pushJobDetailsValue.getPushJobConfigs().containsKey(incPushKey)) {
          isIncrementalPush = Boolean.parseBoolean(pushJobDetailsValue.getPushJobConfigs().get(incPushKey).toString());
        }
        StringBuilder logMessage = new StringBuilder();
        CharSequence storeName = pushJobDetailsKey.getStoreName();
        logMessage.append("Push job status for store name: ")
            .append(storeName)
            .append(", version: ")
            .append(pushJobDetailsKey.getVersionNumber())
            .append(", cluster: ")
            .append(cluster);
        if (PushJobDetailsStatus.isFailed(overallStatus)) {
          logMessage.append(" failed with status: ").append(overallStatus);
          if (isPushJobFailedDueToUserError(overallStatus, pushJobDetailsValue, pushJobUserErrorCheckpoints)) {
            logMessage.append(" due to user error");
            if (isIncrementalPush) {
              pushJobStatusStats.recordIncrementalPushFailureDueToUserErrorSensor();
            } else {
              pushJobStatusStats.recordBatchPushFailureDueToUserErrorSensor();
            }
          } else {
            logMessage.append(" due to non-user error");
            if (isIncrementalPush) {
              pushJobStatusStats.recordIncrementalPushFailureNotDueToUserErrorSensor();
            } else {
              pushJobStatusStats.recordBatchPushFailureNotDueToUserErrorSensor();
            }
          }
        } else if (PushJobDetailsStatus.isSucceeded(overallStatus)) {
          logMessage.append(" succeeded with status: ").append(overallStatus);
          // Emit metrics for successful push jobs
          if (isIncrementalPush) {
            pushJobStatusStats.recordIncrementalPushSuccessSensor();
          } else {
            pushJobStatusStats.recordBatchPushSuccessSensor();
          }

          // Scheduled log compaction metric
          if (Version.isPushIdRePush(pushJobDetailsValue.getPushId().toString())) {
            logCompactionStatsMap.get(cluster).setCompactionComplete(storeName.toString());
          }
        }
        // Append job duration in minutes to log message
        double jobDurationInMinutes = pushJobDetailsValue.getJobDurationInMs() / 60000.0;
        logMessage.append(", duration: ").append(String.format("%.1f mins", jobDurationInMinutes));
        LOGGER.info(
            "{}. Incremental push: {}, push job id: {}, checkpoint: {}",
            logMessage.toString(),
            isIncrementalPush,
            pushJobDetailsValue.getPushId(),
            PushJobCheckpoints.valueOf(pushJobDetailsValue.getPushJobLatestCheckpoint()));
      }
    } catch (Exception e) {
      LOGGER.error(
          "Failed to emit push job status metrics. pushJobDetails key: {}, value: {}",
          pushJobDetailsKey.toString(),
          pushJobDetailsValue.toString(),
          e);
    }
  }

  /**
   * Lazy initialize a Venice writer for an internal real time topic store of push job details records.
   * Use this writer to put a pair of push job detail record (<code>key</code> and <code>value</code>).
   * @param key key with which the specified value is to be associated.
   * @param value value to be associated with the specified key.
   */
  @Override
  public void sendPushJobDetails(PushJobStatusRecordKey key, PushJobDetails value) {
    if (isParent()) {
      String lastDualWriteError = "";
      for (Map.Entry<String, ControllerClient> entry: getControllerClientMap(getPushJobStatusStoreClusterName())
          .entrySet()) {
        LOGGER.info(
            "Sending controller request to send push job details: {} to region: {} for: {}",
            value,
            entry.getKey(),
            key);
        ControllerResponse response = entry.getValue()
            .sendPushJobDetails(
                key.storeName.toString(),
                key.versionNumber,
                getPushJobDetailsSerializer().serialize(null, value));
        if (response.isError()) {
          LOGGER.error(
              "Unable to send push job details to region: {}, for key: {}, value: {} for error: {}",
              entry.getKey(),
              key,
              value,
              response.getError());
          lastDualWriteError = response.getError();
        } else {
          // When A/A is enabled, we only need to write to one child region and it will be replicated to all regions.
          return;
        }
      }
      throw new VeniceException(
          "Unable to write push job details to any child region with last error:" + lastDualWriteError);
    } else {
      LOGGER.info("Sending push job details: {} for: {}", value, key);
      sendPushJobDetailsToLocalRT(key, value);
    }
  }

  void sendPushJobDetailsToLocalRT(PushJobStatusRecordKey key, PushJobDetails value) {
    // Emit push job status metrics
    emitPushJobStatusMetrics(pushJobStatusStatsMap, logCompactionStatsMap, key, value, pushJobUserErrorCheckpoints);
    // Send push job details to the push job status system store
    if (getPushJobStatusStoreClusterName().isEmpty()) {
      throw new VeniceException(
          ("Unable to send the push job details because " + ConfigKeys.PUSH_JOB_STATUS_STORE_CLUSTER_NAME)
              + " is not configured");
    }
    String pushJobDetailsStoreName = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
    if (pushJobDetailsRTTopic == null) {
      // Verify the RT topic exists and give some time in case it's getting created.
      PubSubTopic expectedRTTopic = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(pushJobDetailsStoreName));
      for (int attempt = 0; attempt < INTERNAL_STORE_GET_RRT_TOPIC_ATTEMPTS; attempt++) {
        if (attempt > 0)
          Utils.sleep(INTERNAL_STORE_RTT_RETRY_BACKOFF_MS);
        if (getTopicManager().containsTopicAndAllPartitionsAreOnline(expectedRTTopic)) {
          pushJobDetailsRTTopic = expectedRTTopic;
          LOGGER.info("Topic {} exists and is configured to receive push job details events", expectedRTTopic);
          break;
        }
      }
      if (pushJobDetailsRTTopic == null) {
        throw new VeniceException(
            "Expected RT topic " + expectedRTTopic + " to receive push job details events"
                + " not found. The topic either hasn't been created yet or it's mis-configured");
      }
    }

    VeniceWriter pushJobDetailsWriter = jobTrackingVeniceWriterMap.computeIfAbsent(PUSH_JOB_DETAILS_WRITER, k -> {
      pushJobDetailsSchemaId = fetchSystemStoreSchemaId(
          pushJobStatusStoreClusterName,
          VeniceSystemStoreUtils.getPushJobDetailsStoreName(),
          value.getSchema().toString());
      return getVeniceWriterFactory().createVeniceWriter(
          new VeniceWriterOptions.Builder(pushJobDetailsRTTopic.getName())
              .setKeyPayloadSerializer(new VeniceAvroKafkaSerializer(key.getSchema().toString()))
              .setValuePayloadSerializer(new VeniceAvroKafkaSerializer(value.getSchema().toString()))
              .build());
    });

    pushJobDetailsWriter.put(key, value, pushJobDetailsSchemaId, null);
  }

  /**
   * @return the value to which the specified key is mapped from the Venice internal real time topic store.
   */
  @Override
  public PushJobDetails getPushJobDetails(@Nonnull PushJobStatusRecordKey key) {
    Validate.notNull(key);
    ConcurrencyUtils.executeUnderConditionalLock(() -> {
      String storeName = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
      String d2Service = discoverCluster(storeName).getSecond();
      pushJobDetailsStoreClient = ClientFactory.getAndStartSpecificAvroClient(
          ClientConfig.defaultSpecificClientConfig(storeName, PushJobDetails.class)
              .setD2ServiceName(d2Service)
              .setD2Client(this.d2Client));
    }, () -> pushJobDetailsStoreClient == null, PUSH_JOB_DETAILS_CLIENT_LOCK);
    try {
      return pushJobDetailsStoreClient.get(key).get();
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  /**
   * @return the value to which the specified key is mapped from the Venice internal <code>BATCH_JOB_HEARTBEAT_STORE</code> topic store.
   */
  @Override
  public BatchJobHeartbeatValue getBatchJobHeartbeatValue(@Nonnull BatchJobHeartbeatKey batchJobHeartbeatKey) {
    Validate.notNull(batchJobHeartbeatKey);
    ConcurrencyUtils.executeUnderConditionalLock(() -> {
      String storeName = VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix();
      String d2Service = discoverCluster(storeName).getSecond();
      livenessHeartbeatStoreClient = ClientFactory.getAndStartSpecificAvroClient(
          ClientConfig.defaultSpecificClientConfig(storeName, BatchJobHeartbeatValue.class)
              .setD2ServiceName(d2Service)
              .setD2Client(this.d2Client));
    }, () -> livenessHeartbeatStoreClient == null, LIVENESS_HEARTBEAT_CLIENT_LOCK);
    try {
      return livenessHeartbeatStoreClient.get(batchJobHeartbeatKey).get();
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  /**
   * Create a local Venice writer based on store version info and, for each partition, use the writer to send
   * {@linkplain ControlMessageType#END_OF_PUSH END_OF_PUSH} and {@linkplain ControlMessageType#END_OF_SEGMENT END_OF_SEGMENT} control messages to Kafka.
   * @param clusterName name of the Venice cluster.
   * @param storeName name of the store.
   * @param versionNumber store version number.
   * @param alsoWriteStartOfPush if Venice writer sends a {@linkplain ControlMessageType#START_OF_PUSH START_OF_PUSH} control message first.
   */
  @Override
  public void writeEndOfPush(String clusterName, String storeName, int versionNumber, boolean alsoWriteStartOfPush) {
    // validate store and version exist
    Store store = getStore(clusterName, storeName);

    if (store == null) {
      throw new VeniceNoStoreException(storeName);
    }

    if (store.getCurrentVersion() == versionNumber) {
      if (!VeniceSystemStoreUtils.isSystemStore(storeName)) {
        // This check should only apply to non Zk shared stores.
        throw new VeniceHttpException(
            HttpStatus.SC_CONFLICT,
            "Cannot end push for version " + versionNumber + " that is currently being served");
      }
    }

    Version version = store.getVersion(versionNumber);
    if (version == null) {
      throw new VeniceHttpException(
          HttpStatus.SC_NOT_FOUND,
          "Version " + versionNumber + " was not found for Store " + storeName
              + ".  Cannot end push for version that does not exist");
    }

    String topicToReceiveEndOfPush = version.getPushType().isStreamReprocessing()
        ? Version.composeStreamReprocessingTopic(storeName, versionNumber)
        : Version.composeKafkaTopic(storeName, versionNumber);

    // write EOP message
    VeniceWriterFactory factory = getVeniceWriterFactory();
    int partitionCount = version.getPartitionCount() * version.getPartitionerConfig().getAmplificationFactor();
    VeniceWriterOptions.Builder vwOptionsBuilder =
        new VeniceWriterOptions.Builder(topicToReceiveEndOfPush).setUseKafkaKeySerializer(true)
            .setPartitionCount(partitionCount);
    if (multiClusterConfigs.isParent() && version.isNativeReplicationEnabled()) {
      vwOptionsBuilder.setBrokerAddress(version.getPushStreamSourceAddress());
    }
    try (VeniceWriter veniceWriter = factory.createVeniceWriter(vwOptionsBuilder.build())) {
      if (alsoWriteStartOfPush) {
        veniceWriter.broadcastStartOfPush(
            false,
            version.isChunkingEnabled(),
            version.getCompressionStrategy(),
            new HashMap<>());
      }
      veniceWriter.broadcastEndOfPush(new HashMap<>());
      veniceWriter.flush();
    }
  }

  /**
   * Test if a store is allowed for a batch push.
   * @param storeName name of a store.
   * @return {@code true} is the store is a participant system store or if Venice is running in single-region mode
   */
  @Override
  public boolean whetherEnableBatchPushFromAdmin(String storeName) {
    /*
     * Allow (empty) push to participant system store from child controller directly since participant stores are
     * independent in different fabrics (different data).
     */
    return VeniceSystemStoreUtils.isParticipantStore(storeName) || !multiClusterConfigs.isMultiRegion();
  }

  /**
   * Test if the store migration is allowed for a cluster. It reads the value "allow.store.migration" from
   * the <code>"/clusterName/ClusterConfig"</code> znode.
   * @param clusterName name of Venice cluster.
   * @return <code>true</code> if store migration is allowed for the input cluster;
   *         <code>false</code> otherwise.
   */
  @Override
  public boolean isStoreMigrationAllowed(String clusterName) {
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createClusterReadLock()) {
      HelixReadWriteLiveClusterConfigRepository clusterConfigRepository =
          getReadWriteLiveClusterConfigRepository(clusterName);
      return clusterConfigRepository.getConfigs().isStoreMigrationAllowed();
    }
  }

  /**
   * Main implementation for migrating a store from its source cluster to a new destination cluster.
   * A new store (with same properties, e.g. name, owner, key schema, value schema) is created
   * at the destination cluster and its StoreInfo is also cloned. For a store with enabled meta system store
   * or enabled davinci push status, those system stores are also migrated. Different store versions
   * are evaluated for the migration. For those versions to be migrated, it triggers the
   * {@linkplain ControllerRoute#ADD_VERSION ADD_VERSION} and starts ingestion at the destination cluster.
   * @param srcClusterName name of the source cluster.
   * @param destClusterName name of the destination cluster.
   * @param storeName name of the target store.
   */
  @Override
  public void migrateStore(String srcClusterName, String destClusterName, String storeName) {
    if (srcClusterName.equals(destClusterName)) {
      throw new VeniceException("Source cluster and destination cluster cannot be the same!");
    }

    // Get original store properties
    StoreInfo srcStore = StoreInfo.fromStore(this.getStore(srcClusterName, storeName));
    if (srcStore == null) {
      // Store not found in the source cluster... let's figure out if it's anywhere else...

      StoreConfig storeConfig = storeConfigRepo.getStoreConfigOrThrow(storeName);
      if (storeConfig == null || StringUtils.isEmpty(storeConfig.getCluster())) {
        // The store does not exist in any cluster, so we throw.
        throw new VeniceNoStoreException(storeName);
      }

      if (storeConfig.getCluster().equals(destClusterName)) {
        LOGGER.info(
            "Received command to migrate store '{}' from {} to {}, but that store is already at the destination. Will short-circuit the operation.",
            storeName,
            srcClusterName,
            destClusterName);
        return;
      } else {
        throw new VeniceNoStoreException(
            storeName,
            srcClusterName,
            "Migrate store failed because the store is not in the specified source cluster, but instead in '"
                + storeConfig.getCluster() + "'.");
      }
    }

    if (!isParent()) {
      // Update store and storeConfig to support single datacenter store migration
      this.updateStore(srcClusterName, storeName, new UpdateStoreQueryParams().setStoreMigration(true));
      this.setStoreConfigForMigration(storeName, srcClusterName, destClusterName);
    }

    String destControllerUrl = this.getLeaderController(destClusterName).getUrl(sslEnabled);
    ControllerClient destControllerClient =
        ControllerClient.constructClusterControllerClient(destClusterName, destControllerUrl, sslFactory);

    // Same as StoresRoutes#getStore, set up backup version retention time. Otherwise, parent and child src store
    // info will always be different for stores with negative BackupVersionRetentionMs.
    if (srcStore.getBackupVersionRetentionMs() < 0) {
      srcStore.setBackupVersionRetentionMs(getBackupVersionDefaultRetentionMs());
    }
    String keySchema = this.getKeySchema(srcClusterName, storeName).getSchema().toString();
    List<SchemaEntry> valueSchemaEntries = this.getValueSchemas(srcClusterName, storeName)
        .stream()
        .sorted(Comparator.comparingInt(SchemaEntry::getId))
        .collect(Collectors.toList());
    Map<String, Map<String, StoreInfo>> srcStoresInChildColos = new HashMap<>();
    Consumer<String> srcStoresInChildColosConsumer = migratingStoreName -> srcStoresInChildColos
        .put(migratingStoreName, getStoreInfoInChildColos(srcClusterName, migratingStoreName));
    srcStoresInChildColosConsumer.accept(storeName);
    if (srcStore.isStoreMetaSystemStoreEnabled()) {
      srcStoresInChildColosConsumer.accept(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName));
    }
    if (srcStore.isDaVinciPushStatusStoreEnabled()) {
      srcStoresInChildColosConsumer
          .accept(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName));
    }

    // Create a new store in destination cluster
    NewStoreResponse newStoreResponse = destControllerClient
        .createNewStore(storeName, srcStore.getOwner(), keySchema, valueSchemaEntries.get(0).getSchema().toString());
    if (newStoreResponse.isError()) {
      throw new VeniceException(
          "Failed to create store " + storeName + " in dest cluster " + destClusterName + ". Error "
              + newStoreResponse.getError());
    }
    // Add other value schemas
    for (SchemaEntry schemaEntry: valueSchemaEntries) {
      SchemaResponse schemaResponse =
          destControllerClient.addValueSchema(storeName, schemaEntry.getSchema().toString());
      if (schemaResponse.isError()) {
        throw new VeniceException(
            "Failed to add value schema " + schemaEntry.getId() + " into store " + storeName + " in dest cluster "
                + destClusterName + ". Error " + schemaResponse.getError());
      }
    }

    // Copy remaining properties that will make the cloned store almost identical to the original
    UpdateStoreQueryParams params = new UpdateStoreQueryParams(srcStore, true);
    Set<String> remainingRegions = new HashSet<>();
    remainingRegions.add(multiClusterConfigs.getRegionName());
    for (Map.Entry<String, StoreInfo> entry: srcStoresInChildColos.get(storeName).entrySet()) {
      UpdateStoreQueryParams paramsInChildColo = new UpdateStoreQueryParams(entry.getValue(), true);
      if (params.isDifferent(paramsInChildColo)) {
        // Src parent controller calls dest parent controller to update store with store configs in child colo.
        paramsInChildColo.setRegionsFilter(entry.getKey());
        LOGGER.info("Sending update-store request {} to store {} in {}", paramsInChildColo, storeName, entry.getKey());
        ControllerResponse updateStoreResponse = destControllerClient.updateStore(storeName, paramsInChildColo);
        if (updateStoreResponse.isError()) {
          throw new VeniceException(
              "Failed to update store " + storeName + " in dest cluster " + destClusterName + " in region "
                  + paramsInChildColo + ". Error " + updateStoreResponse.getError());
        }
      } else {
        remainingRegions.add(entry.getKey());
      }
    }
    params.setRegionsFilter(String.join(",", remainingRegions));
    LOGGER.info("Sending update-store request {} to store {} in {}", params, storeName, remainingRegions);
    ControllerResponse updateStoreResponse = destControllerClient.updateStore(storeName, params);
    if (updateStoreResponse.isError()) {
      throw new VeniceException(
          "Failed to update store " + storeName + " in dest cluster " + destClusterName + " in regions "
              + remainingRegions + ". Error " + updateStoreResponse.getError());
    }

    Consumer<String> versionMigrationConsumer = migratingStoreName -> {
      Store migratingStore = this.getStore(srcClusterName, migratingStoreName);
      List<Version> versionsToMigrate = getVersionsToMigrate(srcStoresInChildColos, migratingStore);
      LOGGER.info(
          "Adding versions {} to store {} in dest cluster {}",
          versionsToMigrate.stream().map(Version::getNumber).map(String::valueOf).collect(Collectors.joining(",")),
          migratingStoreName,
          destClusterName);
      for (Version version: versionsToMigrate) {
        VersionResponse versionResponse = destControllerClient.addVersionAndStartIngestion(
            migratingStoreName,
            version.getPushJobId(),
            version.getNumber(),
            // Version topic might be deleted in parent. Get partition count from version instead of topic manager.
            version.getPartitionCount(),
            version.getPushType(),
            // Pass through source addresses as they point to child Kafka in which version topics are not deleted.
            // Before migration ends, dest stores all leaders and followers consume version topics in local fabric.
            version.getPushStreamSourceAddress(),
            version.getHybridStoreConfig() == null ? -1 : version.getHybridStoreConfig().getRewindTimeInSeconds(),
            version.getRmdVersionId());
        if (versionResponse.isError()) {
          throw new VeniceException(
              "Failed to add version and start ingestion for store " + migratingStoreName + " and version "
                  + version.getNumber() + " in dest cluster " + destClusterName + ". Error "
                  + versionResponse.getError());
        }
      }
    };

    if (srcStore.isStoreMetaSystemStoreEnabled()) {
      // Migrate all the meta system store versions
      versionMigrationConsumer.accept(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName));
    }
    if (srcStore.isDaVinciPushStatusStoreEnabled()) {
      // enable da vinci push status system store in dest cluster
      versionMigrationConsumer.accept(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName));
    }
    // Migrate all the versions belonging to the regular Venice store
    versionMigrationConsumer.accept(storeName);
  }

  /**
   * Clear KILL messages from a participant system store.
   */
  public void clearIngestionKillMessageAndVerify(String clusterName, String versionTopicName) {
    long startTs = System.currentTimeMillis();
    ParticipantMessageKey key = new ParticipantMessageKey();
    key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
    key.resourceName = versionTopicName;
    ParticipantMessageValue value = RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
        () -> participantStoreClientsManager.getReader(clusterName).get(key).get(),
        5,
        Duration.ofMillis(100),
        Duration.ofMillis(200),
        Duration.ofSeconds(10),
        RETRY_FAILURE_TYPES);
    if (value == null) {
      return;
    }

    KillPushJob killPushJobMessage = (KillPushJob) value.messageUnion;
    LOGGER.info(
        "Deleting kill message for store-version: {} in cluster: {}. Message age: {}",
        versionTopicName,
        clusterName,
        System.currentTimeMillis() - killPushJobMessage.getTimestamp());

    // Use RetryUtils to add timed-wait
    RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
        () -> deleteParticipantStoreKillMessage(clusterName, versionTopicName),
        1,
        Duration.ofMillis(100),
        Duration.ofMillis(200),
        Duration.ofSeconds(10),
        RETRY_FAILURE_TYPES);

    // wait for kill message to be removed
    RetryUtils.executeWithMaxAttemptAndExponentialBackoff(() -> {
      if (participantStoreClientsManager.getReader(clusterName).get(key).get() != null) {
        throw new VeniceException(
            "Kill message still exists in participant store for store-version: " + versionTopicName + " in cluster: "
                + clusterName);
      }
    }, 5, Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofSeconds(10), RETRY_FAILURE_TYPES);
    LOGGER.info(
        "Spent: {}ms for kill message removal from participant store for store-version: {} in cluster: {}",
        System.currentTimeMillis() - startTs,
        versionTopicName,
        clusterName);
  }

  private Map<String, StoreInfo> getStoreInfoInChildColos(String srcClusterName, String storeName) {
    Map<String, StoreInfo> storeInfoMap = new HashMap<>();
    if (isParent()) {
      Map<String, ControllerClient> controllerClients = this.getControllerClientMap(srcClusterName);
      for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
        StoreResponse storeResponse = entry.getValue().getStore(storeName);
        if (storeResponse.isError()) {
          throw new VeniceException(
              "Could not query store from region " + entry.getKey() + " and cluster " + srcClusterName + ". "
                  + storeResponse.getError());
        }
        storeInfoMap.put(entry.getKey(), storeResponse.getStore());
      }
    }
    return storeInfoMap;
  }

  private List<Version> getVersionsToMigrate(
      Map<String, Map<String, StoreInfo>> srcStoresInChildColos,
      Store srcStore) {
    // For single datacenter store migration, sort, filter and return the version list
    if (!isParent()) {
      return srcStore.getVersions()
          .stream()
          .sorted(Comparator.comparingInt(Version::getNumber))
          .filter(version -> Arrays.asList(STARTED, PUSHED, ONLINE).contains(version.getStatus()))
          .collect(Collectors.toList());
    }

    // For multi data center store migration, all store versions in parent controller might be error versions.
    // Therefore, we have to gather live versions from child fabrics.
    Map<Integer, Version> versionNumberToVersionMap = new HashMap<>();
    for (StoreInfo storeInfo: srcStoresInChildColos.get(srcStore.getName()).values()) {
      storeInfo.getVersions()
          .stream()
          .filter(version -> Arrays.asList(STARTED, PUSHED, ONLINE).contains(version.getStatus()))
          .forEach(version -> versionNumberToVersionMap.putIfAbsent(version.getNumber(), version));
    }
    List<Version> versionsToMigrate = versionNumberToVersionMap.values()
        .stream()
        .sorted(Comparator.comparingInt(Version::getNumber))
        .collect(Collectors.toList());
    int largestChildVersionNumber =
        versionsToMigrate.size() > 0 ? versionsToMigrate.get(versionsToMigrate.size() - 1).getNumber() : 0;

    // Then append new versions from the source store in parent controller.
    // Because a new version might not appear in child controllers yet.
    srcStore.getVersions()
        .stream()
        .sorted(Comparator.comparingInt(Version::getNumber))
        .filter(version -> version.getNumber() > largestChildVersionNumber)
        .forEach(versionsToMigrate::add);

    return versionsToMigrate;
  }

  public Map<String, ControllerClient> getControllerClientMap(String clusterName) {
    return clusterControllerClientPerColoMap.computeIfAbsent(clusterName, cn -> {
      Map<String, ControllerClient> controllerClients = new HashMap<>();
      VeniceControllerClusterConfig controllerConfig = multiClusterConfigs.getControllerConfig(clusterName);
      controllerConfig.getChildDataCenterControllerUrlMap()
          .entrySet()
          .forEach(
              entry -> controllerClients.put(
                  entry.getKey(),
                  ControllerClient.constructClusterControllerClient(clusterName, entry.getValue(), sslFactory)));

      controllerConfig.getChildDataCenterControllerD2Map()
          .entrySet()
          .forEach(
              entry -> controllerClients.put(
                  entry.getKey(),
                  new D2ControllerClient(
                      controllerConfig.getD2ServiceName(),
                      clusterName,
                      entry.getValue(),
                      sslFactory)));

      // Respect d2Clients from controller constructor
      if (d2Clients != null) {
        controllerConfig.getChildDataCenterControllerD2Map()
            .entrySet()
            .forEach(
                entry -> controllerClients.put(
                    entry.getKey(),
                    new D2ControllerClient(
                        controllerConfig.getD2ServiceName(),
                        clusterName,
                        d2Clients.get(entry.getKey()),
                        sslFactory)));
      }

      return controllerClients;
    });
  }

  private StoreInfo getStoreInfo(String storeName, String clusterName) {
    ControllerClient controllerClient = getControllerClientMap(clusterName).get(getRegionName());
    if (controllerClient == null) {
      throw new VeniceException(
          "Failed to constructed controller client for cluster " + clusterName + " and region " + getRegionName());
    }
    StoreResponse storeResponse = controllerClient.getStore(storeName);
    if (storeResponse.isError()) {
      throw new VeniceException(
          "Failed to get store " + storeName + " from cluster " + clusterName + " and region " + getRegionName()
              + " with error " + storeResponse.getError());
    }
    return storeResponse.getStore();
  }

  /**
   * @see #updateClusterDiscovery(String, String, String, String)
   */
  @Override
  public void completeMigration(String srcClusterName, String destClusterName, String storeName) {
    this.updateClusterDiscovery(storeName, srcClusterName, destClusterName, srcClusterName);
  }

  /**
   * Abort store migration by resetting migration flag at the source cluster, resetting storeConfig, and updating
   * "cluster" in "/storeConfigs" znode back to the source cluster.
   * @param srcClusterName name of the source cluster.
   * @param destClusterName name of the destination cluster.
   * @param storeName name of the store in migration.
   */
  @Override
  public void abortMigration(String srcClusterName, String destClusterName, String storeName) {
    if (srcClusterName.equals(destClusterName)) {
      throw new VeniceException("Source cluster and destination cluster cannot be the same!");
    }

    /**
     * Reset migration flag.
     * Assume this is the source controller.
     * As a result, this store will be removed from the migration watchlist in {@link VeniceHelixAdmin}.
     */
    this.updateStore(srcClusterName, storeName, new UpdateStoreQueryParams().setStoreMigration(false));

    /**
     * Reset storeConfig.
     * Change destination to the source cluster name so that they are the same.
     * This indicates an aborted migration.
     * As a result, this store will be removed from the migration watchlist in {@link VeniceParentHelixAdmin}.
     */
    this.setStoreConfigForMigration(storeName, srcClusterName, srcClusterName);

    /**
     * Force update cluster discovery so that it will always point to the source cluster.
     * Whichever cluster it currently belongs to do not matter.
     */
    String clusterDiscovered = this.discoverCluster(storeName).getFirst();
    this.updateClusterDiscovery(storeName, clusterDiscovered, srcClusterName, srcClusterName);
  }

  /**
   * @see Admin#updateClusterDiscovery(String, String, String, String)
   */
  @Override
  public void updateClusterDiscovery(String storeName, String oldCluster, String newCluster, String initiatingCluster) {
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(initiatingCluster);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      ZkStoreConfigAccessor storeConfigAccessor = resources.getStoreConfigAccessor();
      StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
      if (storeConfig == null) {
        throw new VeniceException("Store config is empty!");
      } else if (!storeConfig.getCluster().equals(oldCluster)) {
        throw new VeniceException(
            "Store " + storeName + " is expected to be in " + oldCluster + " cluster, but is actually in "
                + storeConfig.getCluster());
      }
      Store store = getStore(initiatingCluster, storeName);
      storeConfig.setCluster(newCluster);
      storeConfigAccessor.updateConfig(storeConfig, store.isStoreMetaSystemStoreEnabled());
      LOGGER.info("Store {} now belongs to cluster {} instead of {}", storeName, newCluster, oldCluster);
    }
  }

  /**
   * Validates that a store has been completely deleted from the Venice cluster.
   * This method performs comprehensive checks across multiple subsystems to ensure
   * no lingering resources remain that would prevent safe store recreation.
   *
   * Resources checked:
   * 1. Store configuration in ZooKeeper
   * 2. Store metadata in store repository
   * 3. System stores (only those that were enabled for the original store)
   * 4. Kafka topics (version, RT, and system store topics)
   * 5. Helix resources
   *
   * @param clusterName the name of the cluster to check (must not be null or empty)
   * @param storeName the name of the store to validate deletion for (must not be null or empty)
   * @return StoreDeletedResult indicating whether the store is fully deleted or what resources remain
   * @throws IllegalArgumentException if clusterName or storeName is null or empty
   */
  public StoreDeletedValidation validateStoreDeleted(String clusterName, String storeName) {
    return StoreDeletionValidationUtils.validateStoreDeleted(this, clusterName, storeName);
  }

  /**
   * Check whether Controller should block the incoming store creation.
   * Inside this function, there is a logic to check whether there are any lingering resources since the requested
   * store could be just deleted recently.
   * This check should be skipped in Child Controller, but only enabled in Parent Controller because of the following
   * reasons:
   * 1. Parent Controller has the strict order that the system store must be created before the host Venice store.
   * 2. Child Controller doesn't have this strict order since the admin messages of Child Controller could be executed
   * in parallel since they are different store names. So when such kind of race condition happens, it will cause a
   * dead loop:
   *   a. The version creation of system store will create a RT topic in Parent Cluster.
   *   b. The RT topic will be mirrored by KMM to the Child Cluster.
   *   c. The version creation admin message of system store will be blocked in Child Controller since the host Venice
   *   store doesn't exist.
   *   d. The store creation admin message of the host Venice store will be blocked in Child Controller because of
   *   lingering resource check (RT topic of its system store already exists, which is created by KMM).
   *
   * TODO: Evaluate if this code needs to change now that KMM has been deprecated.
   *
   * In the future, once Venice gets rid of KMM, the topic won't be automatically created by KMM, and this race condition
   * will be addressed.
   * So far, Child Controller will skip lingering resource check when handling store creation admin message.
   */
  protected void checkPreConditionForCreateStore(
      String clusterName,
      String storeName,
      String keySchema,
      String valueSchema,
      boolean allowSystemStore,
      boolean skipLingeringResourceCheck) {
    if (!StoreName.isValidStoreName(storeName)) {
      throw new VeniceException("Invalid store name " + storeName + ". Only letters, numbers, underscore or dash");
    }
    AvroSchemaUtils.validateAvroSchemaStr(keySchema);
    AvroSchemaUtils.validateAvroSchemaStr(valueSchema);
    AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchema);
    checkControllerLeadershipFor(clusterName);
    checkStoreNameConflict(storeName, allowSystemStore);
    // Before creating store, check the global stores configs at first.
    // TODO As some store had already been created before we introduced global store config
    // TODO so we need a way to sync up the data. For example, while we loading all stores from ZK for a cluster,
    // TODO put them into global store configs.
    boolean isLegacyStore = false;
    /**
     * In the following situation, we will skip the lingering resource check for the new store request:
     * 1. The store is migrating, and the cluster name is equal to the migrating destination cluster.
     * 2. The legacy store.
     */
    ZkStoreConfigAccessor storeConfigAccessor = getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor();
    if (storeConfigAccessor.containsConfig(storeName)) {
      StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
      if (!storeConfig.isDeleting()) {
        if (!clusterName.equals(storeConfig.getMigrationDestCluster())) {
          throw new VeniceStoreAlreadyExistsException(storeName);
        } else {
          skipLingeringResourceCheck = true;
        }
      } else {
        /**
         * Store config could exist with deleting flag in the following situations:
         * 1. Controller of the same cluster tried to delete the store but failed. In this case, store will be
         *    deleted again in {@link VeniceHelixAdmin#createStore}.
         * 2. Controller of another cluster is deleting the store but has not removed storeConfig yet. In this
         *    case, since there is no lock between controllers of the two clusters, the other controller might
         *    delete the storeConfig when current controller is creating the store. To avoid the race condition,
         *    throw an exception to wait for storeConfig deletion.
         */
        if (!clusterName.equals(storeConfig.getCluster())) {
          throw new VeniceStoreAlreadyExistsException(
              "StoreConfig points to a different cluster " + storeConfig.getCluster()
                  + ". Please wait for storeConfig deletion or use delete-store command to remove storeConfig.");
        }
        isLegacyStore = true;
        skipLingeringResourceCheck = true;
      }
    }

    ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    Store store = repository.getStore(storeName);
    // If the store exists in store repository and it's still active(not being deleted), we don't allow to re-create it.
    if (store != null && !isLegacyStore) {
      throwStoreAlreadyExists(clusterName, storeName);
    }

    if (!skipLingeringResourceCheck) {
      checkResourceCleanupBeforeStoreCreation(clusterName, storeName, !multiClusterConfigs.isParent());
    }

    // Check whether the schema is valid or not
    new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, keySchema);
    new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchema);
  }

  private void checkStoreNameConflict(String storeName, boolean allowSystemStore) {
    if (storeName.endsWith(REAL_TIME_TOPIC_SUFFIX)) {
      throw new VeniceException(
          "Store name: " + storeName + " ends with a keyword - `" + REAL_TIME_TOPIC_SUFFIX + "`"
              + " reserved for internal usage, please change it.");
    }

    if (storeName.equals(AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT)) {
      throw new VeniceException("Store name: " + storeName + " clashes with the internal usage, please change it");
    }

    if (VeniceView.isViewStore(storeName)) {
      throw new VeniceException(
          "Store name: " + storeName + " clashes with the internal usage, please remove the prefix: "
              + VeniceView.VIEW_STORE_PREFIX);
    }

    if (!allowSystemStore && VeniceSystemStoreUtils.isSystemStore(storeName)) {
      throw new VeniceException(
          "Store name: " + storeName + " clashes with the Venice system store usage, please change it");
    }
  }

  Store checkPreConditionForDeletion(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    Store store = repository.getStore(storeName);
    checkPreConditionForDeletion(clusterName, storeName, store);
    return store;
  }

  private void checkPreConditionForDeletion(String clusterName, String storeName, Store store) {
    if (store == null) {
      throwStoreDoesNotExist(clusterName, storeName);
    }
    if (VeniceSystemStoreUtils.isUserSystemStore(storeName)) {
      /**
       * For system stores, such as meta system store/DaVinci push status system store, they
       * can be managed separately from the corresponding user stores.
       */
      return;
    }
    if (store.isEnableReads() || store.isEnableWrites()) {
      String errorMsg = "Unable to delete the entire store or versions for store: " + storeName
          + ". Store has not been disabled. Both read and write need to be disabled before deleting.";
      LOGGER.error(errorMsg);
      throw new VeniceException(errorMsg);
    }
  }

  Store checkPreConditionForSingleVersionDeletion(String clusterName, String storeName, int versionNum) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    Store store = repository.getStore(storeName);
    checkPreConditionForSingleVersionDeletion(clusterName, storeName, store, versionNum);
    return store;
  }

  private void checkPreConditionForSingleVersionDeletion(
      String clusterName,
      String storeName,
      Store store,
      int versionNum) {
    if (store == null) {
      throwStoreDoesNotExist(clusterName, storeName);
    }
    // The cannot delete current version restriction is only applied to non-system stores.
    if (!store.isSystemStore() && store.getCurrentVersion() == versionNum) {
      String errorMsg = "Unable to delete the version: " + versionNum
          + ". The current version could not be deleted from store: " + storeName;
      LOGGER.error(errorMsg);
      throw new VeniceUnsupportedOperationException("delete single version", errorMsg);
    }
  }

  /**
   * This function is only being used by store migration parent controllers, which write add version admin message.
   * We use {@link Version#DEFAULT_RT_VERSION_NUMBER} here that does not change anything in the version and the migrated
   * version copies whatever is there in the source version.
   */
  @Override
  public void addVersionAndStartIngestion(
      String clusterName,
      String storeName,
      String pushJobId,
      int versionNumber,
      int numberOfPartitions,
      PushType pushType,
      String remoteKafkaBootstrapServers,
      long rewindTimeInSecondsOverride,
      int replicationMetadataVersionId,
      boolean versionSwapDeferred,
      int repushSourceVersion) {
    addVersionAndStartIngestion(
        clusterName,
        storeName,
        pushJobId,
        versionNumber,
        numberOfPartitions,
        pushType,
        remoteKafkaBootstrapServers,
        rewindTimeInSecondsOverride,
        replicationMetadataVersionId,
        versionSwapDeferred,
        null,
        repushSourceVersion,
        DEFAULT_RT_VERSION_NUMBER);
  }

  /**
   * This is a wrapper for VeniceHelixAdmin#addVersion but performs additional operations needed for add version invoked
   * from the admin channel. Therefore, this method is mainly invoked from the admin task upon processing an add
   * version message.
   */
  public void addVersionAndStartIngestion(
      String clusterName,
      String storeName,
      String pushJobId,
      int versionNumber,
      int numberOfPartitions,
      PushType pushType,
      String remoteKafkaBootstrapServers,
      long rewindTimeInSecondsOverride,
      int replicationMetadataVersionId,
      boolean versionSwapDeferred,
      String targetedRegions,
      int repushSourceVersion,
      int currentRTVersionNumber) {
    Store store = getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    if (versionNumber <= store.getLargestUsedVersionNumber()) {
      LOGGER.info(
          "Ignoring the add version message since version {} is less than the largestUsedVersionNumber of {} for "
              + " store: {} in cluster: {}",
          versionNumber,
          store.getLargestUsedVersionNumber(),
          storeName,
          clusterName);
      return;
    }
    if (!isParent() && store.isMigrating()) {
      String versionTopicName = Version.composeKafkaTopic(storeName, versionNumber);
      LOGGER.info(
          "Deleting any old ingestion kill messages for version: {} in cluster: {} since store: {} is being migrated",
          versionTopicName,
          clusterName,
          storeName);
      try {
        clearIngestionKillMessageAndVerify(clusterName, versionTopicName);
      } catch (Exception e) {
        throw new VeniceRetriableException(
            "Failed to clear ingestion kill message for store: " + storeName + " and version: " + versionNumber
                + " in cluster: " + clusterName,
            e);
      }
    }
    addVersion(
        clusterName,
        storeName,
        pushJobId,
        versionNumber,
        numberOfPartitions,
        getReplicationFactor(clusterName, storeName),
        true,
        false,
        false,
        true,
        pushType,
        null,
        remoteKafkaBootstrapServers,
        Optional.empty(),
        rewindTimeInSecondsOverride,
        replicationMetadataVersionId,
        Optional.empty(),
        versionSwapDeferred,
        targetedRegions,
        repushSourceVersion,
        currentRTVersionNumber);
  }

  /**
   * This method is invoked in parent controllers to replicate new version signals for migrating store.
   */
  public void replicateAddVersionAndStartIngestion(
      String clusterName,
      String storeName,
      String pushJobId,
      int versionNumber,
      int numberOfPartitions,
      PushType pushType,
      String remoteKafkaBootstrapServers,
      long rewindTimeInSecondsOverride,
      int replicationMetadataVersionId) {
    checkControllerLeadershipFor(clusterName);
    try {
      StoreConfig storeConfig = storeConfigRepo.getStoreConfigOrThrow(storeName);
      String destinationCluster = storeConfig.getMigrationDestCluster();
      String sourceCluster = storeConfig.getMigrationSrcCluster();
      if (storeConfig.getCluster().equals(destinationCluster)) {
        // Migration has completed in this colo but the overall migration is still in progress.
        if (clusterName.equals(destinationCluster)) {
          // Mirror new pushes back to the source cluster in case we abort migration after completion.
          ControllerClient sourceClusterControllerClient = ControllerClient.constructClusterControllerClient(
              sourceCluster,
              getLeaderController(sourceCluster).getUrl(sslEnabled),
              sslFactory);
          VersionResponse response = sourceClusterControllerClient.addVersionAndStartIngestion(
              storeName,
              pushJobId,
              versionNumber,
              numberOfPartitions,
              pushType,
              remoteKafkaBootstrapServers,
              rewindTimeInSecondsOverride,
              replicationMetadataVersionId);
          if (response.isError()) {
            // Throw exceptions here to utilize admin channel's retry property to overcome transient errors.
            throw new VeniceException(
                "Replicate add version endpoint call back to source cluster: " + sourceCluster + " failed for store: "
                    + storeName + " with version: " + versionNumber + ". Error: " + response.getError());
          }
        }
      } else if (clusterName.equals(sourceCluster)) {
        // Migration is still in progress and we need to mirror new version signal from source to dest.
        ControllerClient destClusterControllerClient = ControllerClient.constructClusterControllerClient(
            destinationCluster,
            getLeaderController(destinationCluster).getUrl(sslEnabled),
            sslFactory);
        VersionResponse response = destClusterControllerClient.addVersionAndStartIngestion(
            storeName,
            pushJobId,
            versionNumber,
            numberOfPartitions,
            pushType,
            remoteKafkaBootstrapServers,
            rewindTimeInSecondsOverride,
            replicationMetadataVersionId);
        if (response.isError()) {
          throw new VeniceException(
              "Replicate add version endpoint call to destination cluster: " + destinationCluster
                  + " failed for store: " + storeName + " with version: " + versionNumber + ". Error: "
                  + response.getError());
        }
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Exception thrown when replicating add version for store: {} and version: {} as part of store migration",
          storeName,
          versionNumber,
          e);
    }
  }

  /**
   * A wrapper to invoke VeniceHelixAdmin#addVersion to only increment the version and create the topic(s) needed
   * without starting ingestion.
   */
  public Pair<Boolean, Version> addVersionAndTopicOnly(
      String clusterName,
      String storeName,
      String pushJobId,
      int versionNumber,
      int numberOfPartitions,
      int replicationFactor,
      boolean sendStartOfPush,
      boolean sorted,
      PushType pushType,
      String compressionDictionary,
      String remoteKafkaBootstrapServers,
      Optional<String> sourceGridFabric,
      long rewindTimeInSecondsOverride,
      int replicationMetadataVersionId,
      Optional<String> emergencySourceRegion,
      boolean versionSwapDeferred,
      String targetedRegions,
      int repushSourceVersion,
      int largestUsedRTVersionNumber) {
    return addVersion(
        clusterName,
        storeName,
        pushJobId,
        versionNumber,
        numberOfPartitions,
        replicationFactor,
        false,
        sendStartOfPush,
        sorted,
        false,
        pushType,
        compressionDictionary,
        remoteKafkaBootstrapServers,
        sourceGridFabric,
        rewindTimeInSecondsOverride,
        replicationMetadataVersionId,
        emergencySourceRegion,
        versionSwapDeferred,
        targetedRegions,
        repushSourceVersion,
        largestUsedRTVersionNumber);
  }

  /**
   * Only add version to the store without creating the topic or start ingestion. Used to sync version metadata in the
   * parent fabric during store migration.
   */
  public Version addVersionOnly(
      String clusterName,
      String storeName,
      String pushJobId,
      int versionNumber,
      int numberOfPartitions,
      PushType pushType,
      String remoteKafkaBootstrapServers,
      long rewindTimeInSecondsOverride,
      int replicationMetadataVersionId,
      int largestUsedRTVersionNumber) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
    Store store = repository.getStore(storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    Version version = new VersionImpl(storeName, versionNumber, pushJobId, numberOfPartitions);
    if (versionNumber < store.getLargestUsedVersionNumber() || store.containsVersion(versionNumber)) {
      LOGGER.info(
          "Ignoring the add version message since version {} is less than the largestUsedVersionNumber of {} for "
              + "store: {} in cluster: {}",
          versionNumber,
          store.getLargestUsedVersionNumber(),
          storeName,
          clusterName);
    } else {
      try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
        VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
        if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.META_STORE)) {
          setUpMetaStoreAndMayProduceSnapshot(clusterName, systemStoreType.extractRegularStoreName(storeName));
        }
        if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE)) {
          setUpDaVinciPushStatusStore(clusterName, systemStoreType.extractRegularStoreName(storeName));
        }

        // Update the store object to avoid potential system store flags reversion during repository.updateStore(store).
        store = repository.getStore(storeName);
        version.setPushType(pushType);
        store.addVersion(version, false, largestUsedRTVersionNumber);

        VeniceControllerClusterConfig clusterConfig = resources.getConfig();
        version.setNativeReplicationEnabled(clusterConfig.isMultiRegion());

        if (version.isNativeReplicationEnabled()) {
          if (remoteKafkaBootstrapServers != null) {
            version.setPushStreamSourceAddress(remoteKafkaBootstrapServers);
          } else {
            version.setPushStreamSourceAddress(getKafkaBootstrapServers(isSslToKafka()));
          }
        }
        /**
         * Version-level rewind time override.
         */
        handleRewindTimeOverride(store, version, rewindTimeInSecondsOverride);

        version.setRmdVersionId(replicationMetadataVersionId);

        repository.updateStore(store);
        LOGGER.info("Add version: {} for store: {}", version.getNumber(), storeName);
      }
    }
    return version;
  }

  private void handleRewindTimeOverride(Store store, Version version, long rewindTimeInSecondsOverride) {
    if (store.isHybrid() && rewindTimeInSecondsOverride >= 0
        && rewindTimeInSecondsOverride != version.getHybridStoreConfig().getRewindTimeInSeconds()) {
      HybridStoreConfig hybridStoreConfig = version.getHybridStoreConfig();
      LOGGER.info(
          "Overriding rewind time in seconds: {} for store: {} and version: {} the original rewind time config: {}",
          rewindTimeInSecondsOverride,
          store.getName(),
          version.getNumber(),
          hybridStoreConfig.getRewindTimeInSeconds());
      hybridStoreConfig.setRewindTimeInSeconds(rewindTimeInSecondsOverride);
      version.setHybridStoreConfig(hybridStoreConfig);
    }
  }

  /**
   * TODO refactor addVersion to these broken down methods instead of doing everything in one giant method.
   * Perform add version to a given store with the provided {@link Version}
   */
  public boolean addSpecificVersion(String clusterName, String storeName, Version version) {
    if (version == null) {
      throw new VeniceException("Version cannot be null");
    }
    int versionNumber = version.getNumber();
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    ReadWriteStoreRepository storeRepository = resources.getStoreMetadataRepository();
    try {
      try (AutoCloseableLock ignore = resources.getClusterLockManager().createClusterReadLock()) {
        try (AutoCloseableLock ignored = resources.getClusterLockManager().createStoreWriteLockOnly(storeName)) {
          Optional<Version> existingVersionWithSamePushId =
              getVersionWithPushId(clusterName, storeName, version.getPushJobId());
          if (existingVersionWithSamePushId.isPresent()) {
            return false;
          }
          Store store = storeRepository.getStore(storeName);
          if (store == null) {
            throwStoreDoesNotExist(clusterName, storeName);
          } else {
            if (store.containsVersion(versionNumber)) {
              throwVersionAlreadyExists(storeName, versionNumber);
            } else {
              store.addVersion(version, false, DEFAULT_RT_VERSION_NUMBER);
            }
            storeRepository.updateStore(store);
          }
        }
      }
      LOGGER.info("Added version: {} to store: {}", versionNumber, storeName);
      return true;
    } catch (Throwable e) {
      throw new VeniceException(
          "Failed to add provided version with version number: " + versionNumber + " to store: " + storeName
              + ". Detailed stack trace: " + ExceptionUtils.stackTraceToString(e),
          e);
    }
  }

  /**
   * Create the corresponding version topic based on the provided {@link Version}
   */
  public void createSpecificVersionTopic(String clusterName, String storeName, Version version) {
    if (version == null) {
      throw new VeniceException("Version cannot be null");
    }
    checkControllerLeadershipFor(clusterName);
    try {
      VeniceControllerClusterConfig controllerConfig = getHelixVeniceClusterResources(clusterName).getConfig();
      int amplificationFactor = version.getPartitionerConfig().getAmplificationFactor();
      topicToCreationTime.computeIfAbsent(version.kafkaTopicName(), topic -> System.currentTimeMillis());
      createBatchTopics(
          version,
          version.getPushType(),
          getTopicManager(),
          version.getPartitionCount() * amplificationFactor,
          controllerConfig,
          false);
    } finally {
      topicToCreationTime.remove(version.kafkaTopicName());
    }
  }

  /**
   * Create Helix-resources for a given storage node cluster and starts monitoring a new push.
   */
  public void createHelixResourceAndStartMonitoring(String clusterName, String storeName, Version version) {
    if (version == null) {
      throw new VeniceException("Version cannot be null");
    }
    checkControllerLeadershipFor(clusterName);
    Store store = getStore(clusterName, storeName);
    if (store == null) {
      throwStoreDoesNotExist(clusterName, storeName);
    } else {
      startMonitorOfflinePush(
          clusterName,
          version.kafkaTopicName(),
          version.getPartitionCount(),
          store.getReplicationFactor(),
          store.getOffLinePushStrategy());
      helixAdminClient.createVeniceStorageClusterResources(
          clusterName,
          version.kafkaTopicName(),
          version.getPartitionCount(),
          store.getReplicationFactor());
      try {
        retireOldStoreVersions(clusterName, storeName, true, store.getCurrentVersion());
      } catch (Throwable t) {
        LOGGER.error(
            "Failed to delete previous backup version while data recovery to store {} in cluster {}",
            storeName,
            clusterName,
            t);
      }
    }
  }

  /**
   * Create view resources.
   *
   * TODO: Today, this only creates Kafka topics for each view associated for the store version. We today only have kafka
   * based views.  But eventually as views get more complex, we would need to augment this function.
   *
   * @param params default parameters for the resources to be created that may be overridden depending on the behavior
   *               of each specific view associated to the store
   * @param store the store to create these resources for
   * @param version the store version to create these resources for
   */
  private void constructViewResources(Properties params, Store store, Version version, String compressionDictionary) {
    Map<String, ViewConfig> viewConfigs = version.getViewConfigs();
    if (viewConfigs == null || viewConfigs.isEmpty()) {
      return;
    }

    // Construct Kafka topics
    // TODO: Today we only have support for creating Kafka topics as a resource for a given view, but later we would
    // like to add support for potentially other resource types (maybe helix RG's as an example?)
    Map<String, VeniceProperties> viewTopicNamesAndConfigs =
        ViewUtils.getViewTopicsAndConfigs(viewConfigs.values(), params, store.getName(), version.getNumber());
    TopicManager topicManager = getTopicManager();
    for (Map.Entry<String, VeniceProperties> topicNameAndConfigs: viewTopicNamesAndConfigs.entrySet()) {
      String materializedViewTopicName = topicNameAndConfigs.getKey();
      PubSubTopic kafkaTopic = pubSubTopicRepository.getTopic(materializedViewTopicName);
      VeniceProperties kafkaTopicConfigs = topicNameAndConfigs.getValue();
      topicManager.createTopic(
          kafkaTopic,
          kafkaTopicConfigs.getInt(PARTITION_COUNT),
          kafkaTopicConfigs.getInt(KAFKA_REPLICATION_FACTOR),
          kafkaTopicConfigs.getBoolean(ETERNAL_TOPIC_RETENTION_ENABLED),
          kafkaTopicConfigs.getBoolean(LOG_COMPACTION_ENABLED),
          kafkaTopicConfigs.getOptionalInt(KAFKA_MIN_IN_SYNC_REPLICAS),
          kafkaTopicConfigs.getBoolean(USE_FAST_KAFKA_OPERATION_TIMEOUT));
      if (topicNameAndConfigs.getKey().endsWith(MaterializedView.MATERIALIZED_VIEW_TOPIC_SUFFIX)) {
        // Send SOP CM to materialized view topic with sorted flag equal to false due to reshuffle
        VeniceWriterOptions.Builder vwOptionsBuilder =
            new VeniceWriterOptions.Builder(materializedViewTopicName).setUseKafkaKeySerializer(true)
                .setPartitionCount(kafkaTopicConfigs.getInt(PARTITION_COUNT));
        ByteBuffer compressionDictBuffer = null;
        if (compressionDictionary != null) {
          compressionDictBuffer = ByteBuffer.wrap(EncodingUtils.base64DecodeFromString(compressionDictionary));
        } else if (version.getCompressionStrategy().equals(CompressionStrategy.ZSTD_WITH_DICT)) {
          compressionDictBuffer = emptyPushZSTDDictionary.get();
        }
        try (VeniceWriter veniceWriter = getVeniceWriterFactory().createVeniceWriter(vwOptionsBuilder.build())) {
          veniceWriter.broadcastStartOfPush(
              false,
              version.isChunkingEnabled(),
              version.getCompressionStrategy(),
              Optional.ofNullable(compressionDictBuffer),
              Collections.emptyMap());
        }
      }
    }
  }

  private void cleanUpViewResources(Properties params, Store store, Version version) {
    Map<String, ViewConfig> viewConfigs = version.getViewConfigs();
    if (viewConfigs == null || viewConfigs.isEmpty()) {
      return;
    }

    // Deconstruct Kafka topics
    // TODO: Today we only have support for Kafka topics as a resource for a given view, but later we would like
    // to add support for potentially other resource types (maybe helix RG's as an example?)
    Map<String, VeniceProperties> topicNamesAndConfigs = new HashMap<>();
    for (ViewConfig rawView: viewConfigs.values()) {
      VeniceView adminView =
          ViewUtils.getVeniceView(rawView.getViewClassName(), params, store.getName(), rawView.getViewParameters());
      topicNamesAndConfigs.putAll(adminView.getTopicNamesAndConfigsForVersion(version.getNumber()));
    }
    Set<String> versionTopicsToDelete = topicNamesAndConfigs.keySet()
        .stream()
        .filter(t -> VeniceView.parseVersionFromViewTopic(t) == version.getNumber())
        .collect(Collectors.toSet());
    for (String topic: versionTopicsToDelete) {
      truncateKafkaTopic(topic);
    }
  }

  private void createBatchTopics(
      Version version,
      PushType pushType,
      TopicManager topicManager,
      int partitionCount,
      VeniceControllerClusterConfig clusterConfig,
      boolean useFastKafkaOperationTimeout) {
    List<PubSubTopic> topicNamesToCreate = new ArrayList<>(2);
    topicNamesToCreate.add(pubSubTopicRepository.getTopic(version.kafkaTopicName()));
    if (pushType.isStreamReprocessing()) {
      PubSubTopic streamReprocessingTopic = pubSubTopicRepository
          .getTopic(Version.composeStreamReprocessingTopic(version.getStoreName(), version.getNumber()));
      topicNamesToCreate.add(streamReprocessingTopic);
    }
    topicNamesToCreate.forEach(
        topicNameToCreate -> topicManager.createTopic(
            topicNameToCreate,
            partitionCount,
            clusterConfig.getKafkaReplicationFactor(),
            true,
            false,
            clusterConfig.getMinInSyncReplicas(),
            useFastKafkaOperationTimeout));
  }

  private Pair<Boolean, Version> addVersion(
      String clusterName,
      String storeName,
      String pushJobId,
      int versionNumber,
      int numberOfPartitions,
      int replicationFactor,
      boolean startIngestion,
      boolean sendStartOfPush,
      boolean sorted,
      boolean useFastKafkaOperationTimeout,
      PushType pushType,
      String compressionDictionary,
      String remoteKafkaBootstrapServers,
      Optional<String> sourceGridFabric,
      long rewindTimeInSecondsOverride,
      int replicationMetadataVersionId,
      Optional<String> emergencySourceRegion,
      boolean versionSwapDeferred,
      int repushSourceVersion) {
    return addVersion(
        clusterName,
        storeName,
        pushJobId,
        versionNumber,
        numberOfPartitions,
        replicationFactor,
        startIngestion,
        sendStartOfPush,
        sorted,
        useFastKafkaOperationTimeout,
        pushType,
        compressionDictionary,
        remoteKafkaBootstrapServers,
        sourceGridFabric,
        rewindTimeInSecondsOverride,
        replicationMetadataVersionId,
        emergencySourceRegion,
        versionSwapDeferred,
        null,
        repushSourceVersion,
        getStore(clusterName, storeName).getLargestUsedRTVersionNumber());
  }

  private Optional<Version> getVersionFromSourceCluster(
      ReadWriteStoreRepository repository,
      String destinationCluster,
      String storeName,
      int versionNumber) {
    Store store = repository.getStore(storeName);
    if (store == null) {
      throwStoreDoesNotExist(destinationCluster, storeName);
    }
    if (!store.isMigrating()) {
      return Optional.empty();
    }
    ZkStoreConfigAccessor storeConfigAccessor = getStoreConfigAccessor(destinationCluster);
    StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
    if (!destinationCluster.equals(storeConfig.getMigrationDestCluster())) {
      // destinationCluster is passed from the add version task, so if the task is not in the destination cluster but in
      // the source cluster, do not replicate anything from the "source" since this is the source cluster
      return Optional.empty();
    }
    String migrationSourceCluster = storeConfig.getMigrationSrcCluster();
    ControllerClient srcControllerClient = getControllerClientMap(migrationSourceCluster).get(getRegionName());
    if (srcControllerClient == null) {
      throw new VeniceException(
          "Failed to constructed controller client for cluster " + migrationSourceCluster + " and region "
              + getRegionName());
    }
    StoreResponse srcStoreResponse = srcControllerClient.getStore(storeName);
    if (srcStoreResponse.isError()) {
      throw new VeniceException(
          "Failed to get store " + storeName + " from cluster " + migrationSourceCluster + " and region "
              + getRegionName() + " with error " + srcStoreResponse.getError());
    }
    StoreInfo srcStoreInfo = srcStoreResponse.getStore();
    // For ongoing new pushes, destination cluster does not need to replicate the exact version configs from the source
    // cluster; destination cluster can apply its own store configs to the new version.
    if (versionNumber > srcStoreInfo.getCurrentVersion()) {
      LOGGER.info(
          "Version {} is an ongoing new push for store {}, use the store configs to populate new version configs",
          versionNumber,
          storeName);
      return Optional.empty();
    }
    LOGGER.info(
        "During store migration, version configs from source cluster {}: {}",
        migrationSourceCluster,
        srcStoreInfo.getVersion(versionNumber).get());
    return srcStoreResponse.getStore().getVersion(versionNumber);
  }

  /**
   * Note, versionNumber may be VERSION_ID_UNSET, which must be accounted for.
   * Add version is a multi step process that can be broken down to three main steps:
   * 1. topic creation or verification
   * 2. version creation or addition
   * 3. start ingestion. The execution for some of these steps are different depending on how it's invoked.
   *
   * @return Boolean - whether the version is newly created. Version - existing or new version, or null if the version
   *         is skipped during store migration.
   */
  private Pair<Boolean, Version> addVersion(
      String clusterName,
      String storeName,
      String pushJobId,
      int versionNumber,
      int numberOfPartitions,
      int replicationFactor,
      boolean startIngestion,
      boolean sendStartOfPush,
      boolean sorted,
      boolean useFastKafkaOperationTimeout,
      PushType pushType,
      String compressionDictionary,
      String remoteKafkaBootstrapServers,
      Optional<String> sourceGridFabric,
      long rewindTimeInSecondsOverride,
      int replicationMetadataVersionId,
      Optional<String> emergencySourceRegion,
      boolean versionSwapDeferred,
      String targetedRegions,
      int repushSourceVersion,
      int currentRTVersionNumber) {
    AddVersionLatencyStats addVersionLatencyStats = addVersionLatencyStatsMap.get(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    MaintenanceSignal maintenanceSignal =
        HelixUtils.getClusterMaintenanceSignal(clusterName, resources.getHelixManager());

    if (maintenanceSignal != null) {
      throw new HelixClusterMaintenanceModeException(clusterName);
    }

    checkControllerLeadershipFor(clusterName);
    ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
    Version version = null;
    OfflinePushStrategy offlinePushStrategy;
    int currentVersionBeforePush = -1;
    boolean isRepush = Version.isPushIdRePush(pushJobId);
    VeniceControllerClusterConfig clusterConfig = resources.getConfig();
    BackupStrategy backupStrategy;

    try {
      try (AutoCloseableLock ignore = resources.getClusterLockManager().createClusterReadLock()) {
        try (AutoCloseableLock ignored = resources.getClusterLockManager().createStoreWriteLockOnly(storeName)) {
          Optional<Version> versionWithPushId = getVersionWithPushId(clusterName, storeName, pushJobId);
          if (versionWithPushId.isPresent()) {
            return new Pair<>(false, versionWithPushId.get());
          }

          /**
           * For meta system store, Controller will produce a snapshot to RT topic before creating a new version, so that
           * it will guarantee the rewind will recover the state for both store properties and replica statuses.
           */
          VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
          if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.META_STORE)) {
            setUpMetaStoreAndMayProduceSnapshot(clusterName, systemStoreType.extractRegularStoreName(storeName));
          }
          if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE)) {
            setUpDaVinciPushStatusStore(clusterName, systemStoreType.extractRegularStoreName(storeName));
          }

          Store store = repository.getStore(storeName);
          if (store == null) {
            throwStoreDoesNotExist(clusterName, storeName);
          }
          currentVersionBeforePush = store.getCurrentVersion();

          // Dest child controllers skip the version whose kafka topic is truncated
          if (store.isMigrating() && skipMigratingVersion(clusterName, storeName, versionNumber)) {

            if (versionNumber > store.getLargestUsedVersionNumber()) {
              store.setLargestUsedVersionNumber(versionNumber);
              repository.updateStore(store);
            }
            LOGGER.warn(
                "Skip adding version: {} for store: {} in cluster: {} because the version topic is truncated/deleted",
                versionNumber,
                storeName,
                clusterName);
            return new Pair<>(false, null);
          }
          backupStrategy = store.getBackupStrategy();
          offlinePushStrategy = store.getOffLinePushStrategy();

          // Check whether the store is migrating and whether the version is smaller or equal to the current version
          // in the source cluster. If so, replicate the version configs from the child controllers of the source
          // cluster;
          // it's safest to replicate the version configs from the child controller in the same region, because parent
          // region or other regions could have their own configs.
          Optional<Version> sourceVersion = (store.isMigrating() && versionNumber != VERSION_ID_UNSET)
              ? getVersionFromSourceCluster(repository, clusterName, storeName, versionNumber)
              : Optional.empty();
          if (sourceVersion.isPresent()) {
            long cloneSourceVersionTimestamp = System.currentTimeMillis();
            // Adding an existing version to the destination cluster whose version level resources are already created,
            // including Kafka topics with data ready, so skip the steps of recreating these resources.
            version = sourceVersion.get().cloneVersion();
            // Reset version statue; do not make any other config update, version configs are immutable and the version
            // Configs from the source clusters are source of truth
            version.setStatus(STARTED);

            // Do not do a target region push w/ deferred swap for store migration. Setting the following two configs
            // will enable target region push w/ deferred swap and this is not desirable as the version will never be
            // swapped in the non target regions for store migration as the DeferredVersionSwapService relies on parent
            // version status to coordinate the version swap
            if (version.isVersionSwapDeferred() && !StringUtils.isEmpty(version.getTargetSwapRegion())) {
              version.setVersionSwapDeferred(false);
              version.setTargetSwapRegion("");
            }

            if (store.containsVersion(version.getNumber())) {
              throwVersionAlreadyExists(storeName, version.getNumber());
            }
            // Update ZK with the new version
            store.addVersion(version, true, currentRTVersionNumber);
            repository.updateStore(store);

            addVersionLatencyStats.recordExistingSourceVersionHandlingLatency(
                LatencyUtils.getElapsedTimeFromMsToMs(cloneSourceVersionTimestamp));
          } else {
            if (versionNumber == VERSION_ID_UNSET) {
              // No version supplied, generate a new version. This could happen either in the parent
              // controller or local Samza jobs.
              version = new VersionImpl(storeName, store.peekNextVersionNumber(), pushJobId, numberOfPartitions);
            } else {
              if (store.containsVersion(versionNumber)) {
                throwVersionAlreadyExists(storeName, versionNumber);
              }
              version = new VersionImpl(storeName, versionNumber, pushJobId, numberOfPartitions);
            }
            long createBatchTopicStartTime = System.currentTimeMillis();
            topicToCreationTime.computeIfAbsent(version.kafkaTopicName(), topic -> System.currentTimeMillis());
            createBatchTopics(
                version,
                pushType,
                getTopicManager(),
                numberOfPartitions,
                clusterConfig,
                useFastKafkaOperationTimeout);
            addVersionLatencyStats
                .recordBatchTopicCreationLatency(LatencyUtils.getElapsedTimeFromMsToMs(createBatchTopicStartTime));

            String sourceKafkaBootstrapServers = null;

            store = repository.getStore(storeName);
            if (!store.containsVersion(version.getNumber())) {
              version.setPushType(pushType);
              store.addVersion(version, false, currentRTVersionNumber);
            }

            version.setNativeReplicationEnabled(store.isNativeReplicationEnabled());

            // Check whether native replication is enabled
            if (version.isNativeReplicationEnabled()) {
              if (remoteKafkaBootstrapServers != null) {
                /**
                 * AddVersion is invoked by {@link AdminExecutionTask}
                 * which is processing an AddVersion message that contains remote Kafka bootstrap servers url.
                 */
                version.setPushStreamSourceAddress(remoteKafkaBootstrapServers);
              } else {
                /**
                 * AddVersion is invoked by directly querying controllers
                 */
                String sourceFabric = getNativeReplicationSourceFabric(
                    clusterName,
                    store,
                    sourceGridFabric,
                    emergencySourceRegion,
                    targetedRegions);
                sourceKafkaBootstrapServers = getNativeReplicationKafkaBootstrapServerAddress(sourceFabric);
                if (sourceKafkaBootstrapServers == null) {
                  sourceKafkaBootstrapServers = getKafkaBootstrapServers(isSslToKafka());
                }
                version.setPushStreamSourceAddress(sourceKafkaBootstrapServers);
                version.setNativeReplicationSourceFabric(sourceFabric);
              }
            }

            /**
             * Version-level rewind time override.
             */
            handleRewindTimeOverride(store, version, rewindTimeInSecondsOverride);
            store.setPersistenceType(PersistenceType.ROCKS_DB);

            version.setRmdVersionId(replicationMetadataVersionId);

            version.setVersionSwapDeferred(versionSwapDeferred);

            version.setViewConfigs(store.getViewConfigs());

            if (isRepush && repushSourceVersion > NON_EXISTING_VERSION) {
              version.setRepushSourceVersion(repushSourceVersion);
            }

            if (versionSwapDeferred && StringUtils.isNotEmpty(targetedRegions)) {
              version.setTargetSwapRegion(targetedRegions);
            }

            repository.updateStore(store);
            if (isRealTimeTopicRequired(store, version)) {
              createOrUpdateRealTimeTopics(clusterName, store, version);
            }
            LOGGER.info("Add version: {} for store: {}", version.getNumber(), storeName);

            /**
             *  When native replication is enabled and it's in parent controller, directly create the topic in
             *  the specified source fabric if the source fabric is not the local fabric; the above topic creation
             *  is still required since child controllers need to create a topic locally, and parent controller uses
             *  local VT to determine whether there is any ongoing offline push.
             */
            if (multiClusterConfigs.isParent() && version.isNativeReplicationEnabled()
                && !version.getPushStreamSourceAddress().equals(getKafkaBootstrapServers(isSslToKafka()))) {
              if (sourceKafkaBootstrapServers == null) {
                throw new VeniceException(
                    "Parent controller should know the source Kafka bootstrap server url for store: " + storeName
                        + " and version: " + version.getNumber() + " in cluster: " + clusterName);
              }
              long createBatchTopicInParentTimestamp = System.currentTimeMillis();
              createBatchTopics(
                  version,
                  pushType,
                  getTopicManager(sourceKafkaBootstrapServers),
                  numberOfPartitions,
                  clusterConfig,
                  useFastKafkaOperationTimeout);
              addVersionLatencyStats.recordBatchTopicCreationLatency(
                  LatencyUtils.getElapsedTimeFromMsToMs(createBatchTopicInParentTimestamp));
            }

            // We shouldn't need to create view resources in parent fabric
            if (!multiClusterConfigs.isParent()) {
              Properties veniceViewProperties = new Properties();
              veniceViewProperties.put(PARTITION_COUNT, numberOfPartitions);
              veniceViewProperties.put(USE_FAST_KAFKA_OPERATION_TIMEOUT, useFastKafkaOperationTimeout);
              veniceViewProperties.putAll(clusterConfig.getProps().toProperties());
              veniceViewProperties.put(LOG_COMPACTION_ENABLED, false);
              veniceViewProperties.put(KAFKA_REPLICATION_FACTOR, clusterConfig.getKafkaReplicationFactor());
              veniceViewProperties.put(ETERNAL_TOPIC_RETENTION_ENABLED, true);

              constructViewResources(veniceViewProperties, store, version, compressionDictionary);
            }

            if (sendStartOfPush) {
              long sendStartOfPushTimestamp = System.currentTimeMillis();
              ByteBuffer compressionDictionaryBuffer = null;
              if (compressionDictionary != null) {
                compressionDictionaryBuffer =
                    ByteBuffer.wrap(EncodingUtils.base64DecodeFromString(compressionDictionary));
              } else if (version.getCompressionStrategy().equals(CompressionStrategy.ZSTD_WITH_DICT)) {
                // This compression strategy needs a dictionary even if there is no input data,
                // so we generate a dictionary based on synthetic data. This is done in vpj driver
                // as well, but this code will be triggered in cases like Samza batch push job
                // which is independent of the vpj flow.
                compressionDictionaryBuffer = emptyPushZSTDDictionary.get();
              }

              VeniceWriter veniceWriter = null;
              try {
                VeniceWriterOptions.Builder vwOptionsBuilder =
                    new VeniceWriterOptions.Builder(version.kafkaTopicName()).setUseKafkaKeySerializer(true)
                        .setPartitionCount(numberOfPartitions);
                if (multiClusterConfigs.isParent() && version.isNativeReplicationEnabled()) {
                  // Produce directly into one of the child fabric
                  vwOptionsBuilder.setBrokerAddress(version.getPushStreamSourceAddress());
                } else {
                  vwOptionsBuilder.setBrokerAddress(getKafkaBootstrapServers(isSslToKafka()));
                }
                veniceWriter = getVeniceWriterFactory().createVeniceWriter(vwOptionsBuilder.build());
                veniceWriter.broadcastStartOfPush(
                    sorted,
                    version.isChunkingEnabled(),
                    version.getCompressionStrategy(),
                    Optional.ofNullable(compressionDictionaryBuffer),
                    Collections.emptyMap());
                if (pushType.isStreamReprocessing()) {
                  // Send TS message to version topic to inform leader to switch to the stream reprocessing topic
                  veniceWriter.broadcastTopicSwitch(
                      Collections.singletonList(getKafkaBootstrapServers(isSslToKafka())),
                      Version.composeStreamReprocessingTopic(version.getStoreName(), version.getNumber()),
                      -1L, // -1 indicates rewinding from the beginning of the source topic
                      new HashMap<>());
                }
                addVersionLatencyStats
                    .recordStartOfPushLatency(LatencyUtils.getElapsedTimeFromMsToMs(sendStartOfPushTimestamp));
              } finally {
                if (veniceWriter != null) {
                  veniceWriter.close();
                }
              }
            }
          }
          if (startIngestion) {
            // We need to prepare to monitor before creating helix resource.
            startMonitorOfflinePush(
                clusterName,
                version.kafkaTopicName(),
                numberOfPartitions,
                replicationFactor,
                offlinePushStrategy);
            long helixResourceCreationStartTime = System.currentTimeMillis();
            helixAdminClient.createVeniceStorageClusterResources(
                clusterName,
                version.kafkaTopicName(),
                numberOfPartitions,
                replicationFactor);
            addVersionLatencyStats.recordHelixResourceCreationLatency(
                LatencyUtils.getElapsedTimeFromMsToMs(helixResourceCreationStartTime));
          }
        }

        if (startIngestion) {
          // Early delete backup version on start of a push, controlled by store config earlyDeleteBackupEnabled
          if (backupStrategy == BackupStrategy.DELETE_ON_NEW_PUSH_START
              && multiClusterConfigs.getControllerConfig(clusterName).isEarlyDeleteBackUpEnabled() && !isRepush) {
            try {
              long retireOldStoreStartTimestamp = System.currentTimeMillis();
              retireOldStoreVersions(clusterName, storeName, true, currentVersionBeforePush);
              addVersionLatencyStats
                  .recordRetireOldVersionsLatency(LatencyUtils.getElapsedTimeFromMsToMs(retireOldStoreStartTimestamp));
            } catch (Throwable t) {
              LOGGER.error(
                  "Failed to delete previous backup version while pushing {} to store {} in cluster {}",
                  versionNumber,
                  storeName,
                  clusterName,
                  t);
            }
          }
        }
      }

      if (startIngestion) {
        try {
          // Store write lock is released before polling status
          long startWaitingTimestamp = System.currentTimeMillis();
          waitUntilNodesAreAssignedForResource(
              clusterName,
              version.kafkaTopicName(),
              offlinePushStrategy,
              clusterConfig.getOffLineJobWaitTimeInMilliseconds(),
              replicationFactor);
          addVersionLatencyStats
              .recordResourceAssignmentWaitLatency(LatencyUtils.getElapsedTimeFromMsToMs(startWaitingTimestamp));
        } catch (VeniceNoClusterException e) {
          if (!isLeaderControllerFor(clusterName)) {
            int versionNumberInProgress = version == null ? versionNumber : version.getNumber();
            LOGGER.warn(
                "No longer the leader controller of cluster {}; do not fail the AddVersion command, since the new leader will monitor the push for store version {}_v{}",
                clusterName,
                storeName,
                versionNumberInProgress);
            return new Pair<>(true, version);
          }
        }
      }
      return new Pair<>(true, version);

    } catch (Throwable e) {
      if (useFastKafkaOperationTimeout && e instanceof PubSubOpTimeoutException) {
        // Expected and retriable exception skip error handling within VeniceHelixAdmin and let the caller to
        // handle the exception.
        throw e;
      }
      int failedVersionNumber = version == null ? versionNumber : version.getNumber();
      String errorMessage =
          "Failed to add version: " + failedVersionNumber + " to store: " + storeName + " in cluster: " + clusterName;
      LOGGER.error(errorMessage, e);
      try {
        if (version != null) {
          failedVersionNumber = version.getNumber();
          String statusDetails = "Version creation failure, caught:\n" + ExceptionUtils.stackTraceToString(e);
          long createVersionStartTime = System.currentTimeMillis();
          handleVersionCreationFailure(clusterName, storeName, failedVersionNumber, statusDetails);
          addVersionLatencyStats
              .recordVersionCreationFailureLatency(LatencyUtils.getElapsedTimeFromMsToMs(createVersionStartTime));
        }
      } catch (Throwable e1) {
        String handlingErrorMsg = "Exception occurred while handling version " + versionNumber
            + " creation failure for store " + storeName + " in cluster " + clusterName;
        LOGGER.error(handlingErrorMsg, e1);
        errorMessage += "\n" + handlingErrorMsg + ". Error message: " + e1.getMessage();
      }
      throw new VeniceException(errorMessage + ". Detailed stack trace: " + ExceptionUtils.stackTraceToString(e), e);
    } finally {
      // If topic creation succeeds and version info is persisted to Zk, we should remove the creation time for a topic
      // If topic creation fails or if version persistence fails, we should remove the creation time for a topic
      if (version != null) {
        topicToCreationTime.remove(version.kafkaTopicName());
      }
    }
  }

  /**
   * Determines whether real-time topics should be created for the given store and version.
   *
   * <p>Real-time topics are created based on the following conditions:
   * <ul>
   *   <li>The store and version must both be hybrid ({@code store.isHybrid()} and {@code version.isHybrid()}).</li>
   *   <li>If the controller is a child, real-time topics are always created.</li>
   *   <li>If the controller is a parent, real-time topics are created only if:
   *     <ul>
   *       <li>Active-active replication is disabled for the store, and</li>
   *       <li>Either the store's data replication policy is {@code DataReplicationPolicy.AGGREGATE}, or</li>
   *       <li>Incremental push is enabled for the store.</li>
   *     </ul>
   *   </li>
   * </ul>
   *
   * @param store the store being evaluated
   * @param version the version being evaluated
   * @return {@code true} if real-time topics should be created; {@code false} otherwise
   */
  boolean isRealTimeTopicRequired(Store store, Version version) {
    if (!store.isHybrid() || !version.isHybrid()) {
      return false;
    }

    // Only child regions need realtime topic.
    return !isParent();
  }

  /**
   * Creates or updates real-time topics for the specified store (using reference hybrid version) in the given cluster.
   *
   * <p>This method ensures that real-time topics (primary and separate, if applicable) are configured
   * correctly for a hybrid store. It creates the topics if they do not exist and updates their retention
   * time if necessary. For stores with separate real-time topics enabled, the method handles the creation
   * or update of those topics as well.
   *
   * @param clusterName the name of the cluster where the topics are managed
   * @param store the {@link Store} associated with the topics
   * @param version the {@link Version} containing the configuration for the topics, including partition count
   *                and hybrid store settings
   */
  void createOrUpdateRealTimeTopics(String clusterName, Store store, Version version) {
    LOGGER.info(
        "Setting up real-time topics for store: {} with reference hybrid version: {} in cluster: {}",
        store.getName(),
        version.getNumber(),
        clusterName);
    // Create real-time topic if it doesn't exist; otherwise, update the retention time if necessary
    PubSubTopic realTimeTopic = getPubSubTopicRepository().getTopic(Utils.getRealTimeTopicName(version));
    createOrUpdateRealTimeTopic(clusterName, store, version, realTimeTopic);

    // Create separate real-time topic if it doesn't exist; otherwise, update the retention time if necessary
    if (version.isSeparateRealTimeTopicEnabled()) {
      // TODO: Add support for repartitioning separate real-time topics, primarily needed for incremental push jobs.
      createOrUpdateRealTimeTopic(
          clusterName,
          store,
          version,
          getPubSubTopicRepository().getTopic(Utils.getSeparateRealTimeTopicName(version)));
    }
  }

  /**
   * Creates or updates a real-time topic for a given store in the specified cluster.
   *
   * <p>This method ensures that the real-time topic matches the expected configuration based on
   * the store's hybrid settings and the associated version. If the topic already exists:
   * <ul>
   *   <li>It validates the partition count against the expected partition count for the version.</li>
   *   <li>It updates the retention time if necessary.</li>
   * </ul>
   * If the topic does not exist, it creates the topic with the required configuration.
   *
   * @param clusterName the name of the cluster where the topic resides
   * @param store the {@link Store} store to which the topic belongs
   * @param version the reference hybrid {@link Version} containing
   * @param realTimeTopic the {@link PubSubTopic} representing the real-time topic
   * @throws VeniceException if the partition count of an existing topic does not match the expected value
   */
  void createOrUpdateRealTimeTopic(String clusterName, Store store, Version version, PubSubTopic realTimeTopic) {
    int expectedNumOfPartitions = version.getPartitionCount();
    TopicManager topicManager = getTopicManager();
    if (topicManager.containsTopic(realTimeTopic)) {
      validateAndUpdateTopic(realTimeTopic, store, version, expectedNumOfPartitions, topicManager);
    } else {
      VeniceControllerClusterConfig clusterConfig = getControllerConfig(clusterName);
      topicManager.createTopic(
          realTimeTopic,
          expectedNumOfPartitions,
          clusterConfig.getKafkaReplicationFactorRTTopics(),
          StoreUtils.getExpectedRetentionTimeInMs(store, store.getHybridStoreConfig()),
          false,
          // Note: do not enable RT compaction! Might make jobs in Online/Offline model stuck
          clusterConfig.getMinInSyncReplicasRealTimeTopics(),
          false);
    }
    LOGGER.info(
        "Completed setup for real-time topic: {} for store: {} with reference hybrid version: {} and partition count: {}",
        realTimeTopic.getName(),
        store.getName(),
        version.getNumber(),
        expectedNumOfPartitions);
  }

  /**
   * Validates the real-time topic's configuration and updates its retention time if necessary.
   *
   * <p>This method checks if the partition count of the real-time topic matches the expected partition count
   * for the specified version. If the counts do not match, an exception is thrown. Additionally, it validates
   * the topic's retention time against the expected retention time and updates it if required.
   *
   * @param realTimeTopic the {@link PubSubTopic} representing the real-time topic to validate
   * @param store the {@link Store} store to which the topic belongs
   * @param version the reference hybrid {@link Version}
   * @param expectedNumOfPartitions the expected number of partitions for the real-time topic
   * @param topicManager the {@link TopicManager} used for topic management operations
   * @throws VeniceException if the partition count of the topic does not match the expected partition count
   */
  void validateAndUpdateTopic(
      PubSubTopic realTimeTopic,
      Store store,
      Version version,
      int expectedNumOfPartitions,
      TopicManager topicManager) {
    int actualNumOfPartitions = topicManager.getPartitionCount(realTimeTopic);
    // Validate partition count
    if (actualNumOfPartitions != expectedNumOfPartitions) {
      LOGGER.error(
          "Real-time topic: {} for store: {} has different partition count: {} from version partition count: {} version: {} store: {}",
          realTimeTopic.getName(),
          store.getName(),
          actualNumOfPartitions,
          expectedNumOfPartitions,
          version,
          store);
      String errorMessage = String.format(
          "Real-time topic: %s for store: %s has different partition count: %d from version partition count: %d",
          realTimeTopic.getName(),
          store.getName(),
          actualNumOfPartitions,
          expectedNumOfPartitions);
      throw new VeniceException(errorMessage);
    }

    // Validate and update retention time if necessary
    HybridStoreConfig hybridStoreConfig = store.getHybridStoreConfig();
    long expectedRetentionTimeMs = StoreUtils.getExpectedRetentionTimeInMs(store, hybridStoreConfig);
    boolean isUpdated = topicManager.updateTopicRetentionWithRetries(realTimeTopic, expectedRetentionTimeMs);
    LOGGER.info(
        "{} retention time for real-time topic: {} for store: {} (hybrid version: {}, partition count: {})",
        isUpdated ? "Updated" : "Validated",
        realTimeTopic.getName(),
        store.getName(),
        version.getNumber(),
        expectedNumOfPartitions);
  }

  /**
   * During store migration, skip a version if:
   * This is the child controller of the destination cluster
   * And the kafka topic of related store and version is truncated
   * And kafka topic doesn't exist and the version is smaller than the largest used version number which means the version is already deleted in the source cluster.
   *
   * @param clusterName the name of the cluster
   * @param storeName the name of the store
   * @param versionNumber version number
   * @return whether to skip adding the version
   */
  private boolean skipMigratingVersion(String clusterName, String storeName, int versionNumber) {
    if (multiClusterConfigs.isParent()) {
      return false;
    }
    StoreConfig storeConfig = storeConfigRepo.getStoreConfigOrThrow(storeName);
    String destCluster = storeConfig.getMigrationDestCluster();
    if (clusterName.equals(destCluster)) {
      PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, versionNumber));
      String migrationSourceCluster = storeConfig.getMigrationSrcCluster();
      // If the topic doesn't exist, we don't know whether it's not created or already deleted, so we don't skip
      return getTopicManager().containsTopic(versionTopic) && isTopicTruncated(versionTopic.getName()) ||
      // The topic doesn't exist and less/equal than the largest used version number in the source, it's deleted
          !getTopicManager().containsTopic(versionTopic)
              && (versionNumber <= getStoreInfo(storeName, migrationSourceCluster).getLargestUsedVersionNumber());
    }
    return false;
  }

  void handleVersionCreationFailure(String clusterName, String storeName, int versionNumber, String statusDetails) {
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      checkPreConditionForSingleVersionDeletion(clusterName, storeName, versionNumber);
      // Mark offline push job as Error and clean up resources because add version failed.
      PushMonitor offlinePushMonitor = resources.getPushMonitor();
      offlinePushMonitor.markOfflinePushAsError(Version.composeKafkaTopic(storeName, versionNumber), statusDetails);
      deleteOneStoreVersion(clusterName, storeName, versionNumber);
    }
  }

  /**
   * Note: this currently use the pushID to guarantee idempotence, unexpected behavior may result if multiple
   * batch jobs push to the same store at the same time.
   */
  @Override
  public Version incrementVersionIdempotent(
      String clusterName,
      String storeName,
      String pushJobId,
      int numberOfPartitions,
      int replicationFactor,
      PushType pushType,
      boolean sendStartOfPush,
      boolean sorted,
      String compressionDictionary,
      Optional<String> sourceGridFabric,
      Optional<X509Certificate> requesterCert,
      long rewindTimeInSecondsOverride,
      Optional<String> emergencySourceRegion,
      boolean versionSwapDeferred,
      String targetedRegions,
      int repushSourceVersion) {
    if (StringUtils.isNotEmpty(targetedRegions)) {
      /**
       * only parent controller should handle this request. See {@link VeniceParentHelixAdmin#incrementVersionIdempotent}
       */
      throw new VeniceException(
          "Request of creating versions/topics for targeted region push should only be sent to parent controller");
    }
    checkControllerLeadershipFor(clusterName);
    VeniceControllerClusterConfig clusterConfig = getHelixVeniceClusterResources(clusterName).getConfig();
    int replicationMetadataVersionId = clusterConfig.getReplicationMetadataVersion();
    return pushType.isIncremental()
        ? getIncrementalPushVersion(clusterName, storeName, pushJobId)
        : addVersion(
            clusterName,
            storeName,
            pushJobId,
            VERSION_ID_UNSET,
            numberOfPartitions,
            replicationFactor,
            true,
            sendStartOfPush,
            sorted,
            false,
            pushType,
            compressionDictionary,
            null,
            sourceGridFabric,
            rewindTimeInSecondsOverride,
            replicationMetadataVersionId,
            emergencySourceRegion,
            versionSwapDeferred,
            repushSourceVersion).getSecond();
  }

  /**
   * The intended semantic is to use this method to find the version that something is currently pushing to.  It looks
   * at all versions greater than the current version and identifies the version with a status of STARTED.  If there
   * is no STARTED version, it creates a new one for the push to use.  This means we cannot use this method to support
   * multiple concurrent pushes.
   *
   * @param store
   * @return the started version if there is only one, throws an exception if there is an error version with
   * a greater number than the current version.  Otherwise returns Optional.empty()
   */
  protected static Optional<Version> getStartedVersion(Store store) {
    List<Version> startedVersions = new ArrayList<>();
    for (Version version: store.getVersions()) {
      if (version.getNumber() <= store.getCurrentVersion()) {
        continue;
      }
      switch (version.getStatus()) {
        case ONLINE:
        case PUSHED:
          break; // These we can ignore
        case STARTED:
          startedVersions.add(version);
          break;
        case ERROR:
        case NOT_CREATED:
        default:
          throw new VeniceException(
              "Version " + version.getNumber() + " for store " + store.getName() + " has status "
                  + version.getStatus().toString() + ".  Cannot create a new version until this store is cleaned up.");
      }
    }
    if (startedVersions.size() == 1) {
      return Optional.of(startedVersions.get(0));
    } else if (startedVersions.size() > 1) {
      String startedVersionsString = startedVersions.stream()
          .map(Version::getNumber)
          .map(n -> Integer.toString(n))
          .collect(Collectors.joining(","));
      throw new VeniceException(
          "Store " + store.getName() + " has versions " + startedVersionsString + " that are all STARTED.  "
              + "Cannot create a new version while there are multiple STARTED versions");
    }
    return Optional.empty();
  }

  private Optional<Version> getVersionWithPushId(String clusterName, String storeName, String pushId) {
    Store store = getStore(clusterName, storeName);
    if (store == null) {
      throwStoreDoesNotExist(clusterName, storeName);
    }
    for (Version version: store.getVersions()) {
      if (version.getPushJobId().equals(pushId)) {
        LOGGER.info(
            "Version request for pushId {} and store {}. pushId already exists, so returning existing version {}",
            pushId,
            store.getName(),
            version.getNumber());
        return Optional.of(version); // Early exit
      }
    }
    return Optional.empty();
  }

  /**
   * Ensures that a real-time topic exists for the given user system store.
   * If the topic does not already exist in PubSub, it creates the real-time topic
   * and returns the topic name. This method is specific to user system stores,
   * where real-time topics are eagerly created by the controller to prevent
   * blocking of threads that produce data to these stores.
   *
   * @param clusterName the name of the Venice cluster.
   * @param storeName the name of the store.
   * @return the name of the store's real-time topic.
   * @throws VeniceNoStoreException if the store does not exist in the specified cluster.
   * @throws VeniceException if the store is not a user system store or if the partition count is invalid.
   */
  void ensureRealTimeTopicExistsForUserSystemStores(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    Store store = getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (VeniceSystemStoreType.META_STORE != systemStoreType
        && VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE != systemStoreType) {
      LOGGER.error("Failed to create real time topic for store: {} because it is not a user system store.", storeName);
      throw new VeniceException(
          "Failed to create real time topic for store: " + storeName + " because it is not a user system store.");
    }
    PubSubTopic realTimeTopic = getPubSubTopicRepository().getTopic(Utils.getRealTimeTopicName(store));
    TopicManager topicManager = getTopicManager();
    if (topicManager.containsTopic(realTimeTopic)) {
      return;
    }

    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      // check again that the real-time topic does not exist
      if (topicManager.containsTopic(realTimeTopic)) {
        return;
      }
      Version version = store.getVersion(store.getLargestUsedVersionNumber());
      int partitionCount = version != null ? version.getPartitionCount() : store.getPartitionCount();
      if (partitionCount == 0) {
        partitionCount = VeniceSystemStoreUtils.DEFAULT_USER_SYSTEM_STORE_PARTITION_COUNT;
        LOGGER.warn(
            "UNEXPECTED! The partition count is set to 0 both in the store config and version config for user system store: {}. Will use the default partition count ({}) to create the RT topic.",
            storeName,
            partitionCount);
      }
      VeniceControllerClusterConfig clusterConfig = getControllerConfig(clusterName);
      LOGGER.info(
          "Creating real time topic for user system store: {} with partition count: {}",
          storeName,
          partitionCount);
      getTopicManager().createTopic(
          realTimeTopic,
          partitionCount,
          clusterConfig.getKafkaReplicationFactorRTTopics(),
          store.getRetentionTime(),
          false,
          // Note: do not enable RT compaction! Might make jobs in Online/Offline model stuck
          clusterConfig.getMinInSyncReplicasRealTimeTopics(),
          false);
    }
  }

  /**
   * @return replication metadata schema for a store in a cluster with specified schema ID and RMD protocol version ID.
   */
  @Override
  public Optional<Schema> getReplicationMetadataSchema(
      String clusterName,
      String storeName,
      int valueSchemaID,
      int rmdVersionID) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    SchemaEntry schemaEntry = schemaRepo.getReplicationMetadataSchema(storeName, valueSchemaID, rmdVersionID);
    if (schemaEntry == null) {
      return Optional.empty();
    } else {
      return Optional.of(schemaEntry.getSchema());
    }
  }

  @Override
  public Version getReferenceVersionForStreamingWrites(String clusterName, String storeName, String pushJobId) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreReadLock(storeName)) {
      validateStoreSetupForRTWrites(clusterName, storeName, pushJobId, PushType.STREAM);
      Store store = resources.getStoreMetadataRepository().getStore(storeName);
      Version hybridVersion = getReferenceHybridVersionForRealTimeWrites(clusterName, store, pushJobId);
      if (!isParent()) {
        PubSubTopic rtTopic = getPubSubTopicRepository().getTopic(Utils.getRealTimeTopicName(hybridVersion));
        int partitionCount = hybridVersion.getPartitionCount();
        validateTopicPresenceAndState(clusterName, storeName, pushJobId, PushType.STREAM, rtTopic, partitionCount);
      }
      return hybridVersion;
    }
  }

  @Override
  public Version getIncrementalPushVersion(String clusterName, String storeName, String pushJobId) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreReadLock(storeName)) {
      validateStoreSetupForRTWrites(clusterName, storeName, pushJobId, PushType.INCREMENTAL);
      Store store = resources.getStoreMetadataRepository().getStore(storeName);
      Version hybridVersion = getReferenceHybridVersionForRealTimeWrites(clusterName, store, pushJobId);
      // If real-time topic is required, validate that it exists and is in a good state
      if (isRealTimeTopicRequired(store, hybridVersion)) {
        validateTopicForIncrementalPush(clusterName, store, hybridVersion, pushJobId);
      }
      return hybridVersion;
    }
  }

  void validateStoreSetupForRTWrites(String clusterName, String storeName, String pushJobId, PushType pushType) {
    Store store = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository().getStore(storeName);
    if (store == null) {
      throwStoreDoesNotExist(clusterName, storeName);
    }
    if (!store.isHybrid()) {
      LOGGER.error(
          "{} push writes with pushJobId: {} on store: {} in cluster: {} are not allowed because it is not a hybrid store",
          pushType,
          pushJobId,
          storeName,
          clusterName);
      throw new VeniceException(
          "Store: " + storeName + " is not a hybrid store and cannot be used for " + pushType + " writes");
    }
    if (pushType == PushType.INCREMENTAL && !store.isIncrementalPushEnabled()) {
      LOGGER.error(
          "Incremental push with pushJobId: {} on store: {} in cluster: {} is not allowed because incremental push is not enabled",
          pushJobId,
          storeName,
          clusterName);
      throw new VeniceException("Store: " + storeName + " is not an incremental push store");
    }
  }

  void validateTopicForIncrementalPush(
      String clusterName,
      Store store,
      Version referenceHybridVersion,
      String pushJobId) {
    PubSubTopicRepository topicRepository = getPubSubTopicRepository();
    if (referenceHybridVersion.isSeparateRealTimeTopicEnabled()) {
      PubSubTopic separateRtTopic =
          topicRepository.getTopic(Utils.getSeparateRealTimeTopicName(referenceHybridVersion));
      validateTopicPresenceAndState(
          clusterName,
          store.getName(),
          pushJobId,
          PushType.INCREMENTAL,
          separateRtTopic,
          referenceHybridVersion.getPartitionCount());
      // We can consider short-circuiting here if the separate real-time topic is enabled and
      // the topic is in a good state
    }

    PubSubTopic rtTopic = topicRepository.getTopic(Utils.getRealTimeTopicName(referenceHybridVersion));
    validateTopicPresenceAndState(
        clusterName,
        store.getName(),
        pushJobId,
        PushType.INCREMENTAL,
        rtTopic,
        referenceHybridVersion.getPartitionCount());
  }

  void validateTopicPresenceAndState(
      String clusterName,
      String storeName,
      String pushJobId,
      PushType pushType,
      PubSubTopic topic,
      int partitionCount) {
    if (getTopicManager().containsTopicAndAllPartitionsAreOnline(topic, partitionCount)
        && !isTopicTruncated(topic.getName())) {
      return;
    }
    LOGGER.error(
        "{} push writes from pushJobId: {} cannot be accepted on store: {} in cluster: {} because the topic: {} is either absent or being truncated",
        pushType,
        pushJobId,
        storeName,
        clusterName,
        topic);
    getHelixVeniceClusterResources(clusterName).getVeniceAdminStats().recordUnexpectedTopicAbsenceCount();
    throw new VeniceException(
        pushType + " push writes cannot be accepted on store: " + storeName + " in cluster: " + clusterName
            + " because the topic: " + topic + " is either absent or being truncated");
  }

  Version getReferenceHybridVersionForRealTimeWrites(String clusterName, Store store, String pushJobId) {
    List<Version> versions = new ArrayList<>(store.getVersions());
    if (versions.isEmpty()) {
      LOGGER.error(
          "Store: {} in cluster: {} is not initialized with a version yet. Rejecting request for writes with pushJobId: {}",
          store.getName(),
          clusterName,
          pushJobId);
      throw new VeniceException("Store: " + store.getName() + " is not initialized with a version yet.");
    }

    versions.sort(Comparator.comparingInt(Version::getNumber).reversed());
    for (Version version: versions) {
      if (version.getHybridStoreConfig() != null && version.getStatus() != ERROR && version.getStatus() != KILLED) {
        LOGGER.info(
            "Found hybrid version: {} for store: {} in cluster: {}. Will use it as a reference for pushJobId: {}",
            version.getNumber(),
            version,
            clusterName,
            pushJobId);
        return version;
      }
    }

    String logMessage = String.format(
        "No valid hybrid store version (non-errored) found in store: %s in cluster: %s for pushJobId: %s.",
        store.getName(),
        clusterName,
        pushJobId);
    LOGGER.error(logMessage);
    throw new VeniceException(logMessage);
  }

  /**
   * @return The current version number of an input store in the specified Venice cluster or {@linkplain Store#NON_EXISTING_VERSION} if none exists.
   */
  @Override
  public int getCurrentVersion(String clusterName, String storeName) {
    Store store = getStoreForReadOnly(clusterName, storeName);
    if (store.isEnableReads()) {
      return store.getCurrentVersion();
    } else {
      return NON_EXISTING_VERSION;
    }
  }

  /**
   * @return Returns the online (completed, but not yet swapped) or future version with ongoing ingestion
   * else if none exists returns {@linkplain Store#NON_EXISTING_VERSION}
   */
  @Override
  public int getFutureVersion(String clusterName, String storeName) {
    // Find all ongoing offline pushes at first.
    PushMonitor monitor = getHelixVeniceClusterResources(clusterName).getPushMonitor();
    List<OfflinePushStatus> offlinePushStatuses = monitor.getOfflinePushStatusForStore(storeName);
    if (offlinePushStatuses.isEmpty()) {
      return NON_EXISTING_VERSION;
    }
    Optional<String> offlinePush = offlinePushStatuses.stream()
        .filter(status -> !status.getCurrentStatus().isTerminal())
        .map(OfflinePushStatus::getKafkaTopic)
        .findFirst();
    if (offlinePush.isPresent()) {
      return Version.parseVersionFromKafkaTopicName(offlinePush.get());
    }
    // If the push status is finished, then return the largest online version greater than current version
    int onlineFutureVersion = getFutureVersionWithStatus(clusterName, storeName, VersionStatus.ONLINE);
    int pushedFutureVersion = getFutureVersionWithStatus(clusterName, storeName, PUSHED);
    return pushedFutureVersion != NON_EXISTING_VERSION ? pushedFutureVersion : onlineFutureVersion;
  }

  @Override
  public int getBackupVersion(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreReadLock(storeName)) {
      Store store = resources.getStoreMetadataRepository().getStore(storeName);
      return getBackupVersionNumber(store.getVersions(), store.getCurrentVersion());
    }
  }

  public int getFutureVersionWithStatus(String clusterName, String storeName, VersionStatus status) {
    Store store = getStoreForReadOnly(clusterName, storeName);
    if (store.getVersions().isEmpty()) {
      return NON_EXISTING_VERSION;
    }

    Version version = store.getVersions().stream().max(Comparable::compareTo).get();
    if (version.getNumber() != store.getCurrentVersion() && version.getStatus().equals(status)) {
      return version.getNumber();
    }
    return NON_EXISTING_VERSION;
  }

  @Override
  public Map<String, Integer> getCurrentVersionsForMultiColos(String clusterName, String storeName) {
    return null;
  }

  /**
   * @return a new {@linkplain RepushInfo} object with specified store info.
   */
  @Override
  public RepushInfo getRepushInfo(String clusterName, String storeName, Optional<String> fabricName) {
    Store store = getStore(clusterName, storeName);
    boolean isSSL = isSSLEnabledForPush(clusterName, storeName);
    String systemSchemaClusterName = multiClusterConfigs.getSystemSchemaClusterName();
    VeniceControllerClusterConfig systemSchemaClusterConfig =
        multiClusterConfigs.getControllerConfig(systemSchemaClusterName);
    String systemSchemaClusterD2Service = systemSchemaClusterConfig.getClusterToD2Map().get(systemSchemaClusterName);
    String systemSchemaClusterD2ZkHost = systemSchemaClusterConfig.getChildControllerD2ZkHost(getRegionName());
    int currentVersionNumber = store.getCurrentVersion();
    Version version = store.getVersionOrThrow(currentVersionNumber);
    return RepushInfo.createRepushInfo(
        version,
        getKafkaBootstrapServers(isSSL),
        systemSchemaClusterD2Service,
        systemSchemaClusterD2ZkHost);
  }

  /**
   * @see Admin#getFutureVersionsForMultiColos(String, String)
   */
  @Override
  public Map<String, String> getFutureVersionsForMultiColos(String clusterName, String storeName) {
    return Collections.EMPTY_MAP;
  }

  @Override
  public Map<String, String> getBackupVersionsForMultiColos(String clusterName, String storeName) {
    return Collections.EMPTY_MAP;
  }

  /**
   * @see Admin#deleteAllVersionsInStore(String, String)
   */
  @Override
  public List<Version> deleteAllVersionsInStore(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
      Store store = repository.getStore(storeName);
      checkPreConditionForDeletion(clusterName, storeName, store);
      LOGGER.info("Deleting all versions in store: {} in cluster: {}", storeName, clusterName);
      // Set current version to NON_VERSION_AVAILABLE. Otherwise after this store is enabled again, as all of
      // version were deleted, router will get a current version which does not exist actually.
      store.setEnableWrites(true);
      store.setCurrentVersion(NON_EXISTING_VERSION);
      store.setEnableWrites(false);
      repository.updateStore(store);
      List<Version> deletingVersionSnapshot = new ArrayList<>(store.getVersions());

      for (Version version: deletingVersionSnapshot) {
        deleteOneStoreVersion(clusterName, version.getStoreName(), version.getNumber());
      }
      LOGGER.info("Deleted all versions in store: {} in cluster: {}", storeName, clusterName);
      return deletingVersionSnapshot;
    }
  }

  /**
   * @see Admin#deleteOldVersionInStore(String, String, int)
   */
  @Override
  public void deleteOldVersionInStore(String clusterName, String storeName, int versionNum) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
      Store store = repository.getStore(storeName);
      // Here we do not require the store be disabled. So it might impact reads
      // The thing is a new version is just online, now we delete the old version. So some of routers
      // might still use the old one as the current version, so when they send the request to that version,
      // they will get error response.
      // TODO the way to solve this could be: Introduce a timestamp to represent when the version is online.
      // TOOD And only allow to delete the old version that the newer version has been online for a while.
      checkPreConditionForSingleVersionDeletion(clusterName, storeName, store, versionNum);
      if (!store.containsVersion(versionNum)) {
        LOGGER.warn(
            "Ignore the deletion request. Could not find version: {} in store: {} in cluster: {}",
            versionNum,
            storeName,
            clusterName);
        return;
      }
      LOGGER.info("Deleting version: {} in store: {} in cluster: {}", versionNum, storeName, clusterName);
      deleteOneStoreVersion(clusterName, storeName, versionNum);
      LOGGER.info("Deleted version: {} in store: {} in cluster: {}", versionNum, storeName, clusterName);
    }
  }

  /**
   * Delete version from cluster, removing all related resources
   */
  @Override
  public void deleteOneStoreVersion(String clusterName, String storeName, int versionNumber) {
    deleteOneStoreVersion(clusterName, storeName, versionNumber, false);
  }

  private void deleteOneStoreVersion(String clusterName, String storeName, int versionNumber, boolean isForcedDelete) {
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      Store store = resources.getStoreMetadataRepository().getStore(storeName);
      PushMonitor pushMonitor = resources.getPushMonitor();
      if (store == null) {
        throwStoreDoesNotExist(clusterName, storeName);
      }
      Version versionToBeDeleted = store.getVersion(versionNumber);
      if (versionToBeDeleted == null) {
        LOGGER.info(
            "Version: {} doesn't exist in store: {}, will skip `deleteOneStoreVersion`",
            versionNumber,
            storeName);
        return;
      }
      String resourceName = Version.composeKafkaTopic(storeName, versionNumber);
      LOGGER.info("Deleting helix resource: {} in cluster: {}", resourceName, clusterName);
      deleteHelixResource(clusterName, resourceName);
      LOGGER.info("Killing offline push for: {} in cluster: {}", resourceName, clusterName);
      killOfflinePush(clusterName, resourceName, true);

      // Check DIV error in the push status before stopping the monitor.
      boolean hasFatalDataValidationError = hasFatalDataValidationError(pushMonitor, resourceName);
      stopMonitorOfflinePush(clusterName, resourceName, true, isForcedDelete);

      Optional<Version> deletedVersion = deleteVersionFromStoreRepository(clusterName, storeName, versionNumber);
      if (deletedVersion.isPresent()) {
        // Do not delete topic during store migration
        // In such case, the topic will be deleted after store migration, triggered by a new push job
        if (!store.isMigrating()) {
          // Not using deletedVersion.get().kafkaTopicName() because it's incorrect for Zk shared stores.
          String versionTopicName = Version.composeKafkaTopic(storeName, deletedVersion.get().getNumber());
          if (fatalDataValidationFailureRetentionMs != -1 && hasFatalDataValidationError) {
            truncateKafkaTopic(versionTopicName, fatalDataValidationFailureRetentionMs);
          } else {
            truncateKafkaTopic(versionTopicName);
          }

          if (deletedVersion.get().getPushType().isStreamReprocessing()) {
            truncateKafkaTopic(Version.composeStreamReprocessingTopic(storeName, versionNumber));
          }
          cleanUpViewResources(new Properties(), store, deletedVersion.get());
        }
        if (store.isDaVinciPushStatusStoreEnabled() && !isParent()) {
          ExecutorService executor = Executors.newSingleThreadExecutor();
          Future<?> future = executor.submit(
              () -> getPushStatusStoreWriter().deletePushStatus(
                  storeName,
                  deletedVersion.get().getNumber(),
                  Optional.empty(),
                  deletedVersion.get().getPartitionCount()));
          try {
            future.get(PUSH_STATUS_STORE_WRITER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            LOGGER.info("Deleted da-vinci push status of {}_v{}", storeName, versionNumber);
          } catch (InterruptedException e) {
            LOGGER.error(
                "Got interrupted while waiting for the completion of {}_v{} da-vinci push status deletion",
                storeName,
                versionNumber,
                e);
            future.cancel(true);
            Thread.currentThread().interrupt();
          } catch (ExecutionException e) {
            LOGGER.error("Exception thrown when deleting da-vinci push status of {}_v{}", storeName, versionNumber, e);
          } catch (TimeoutException e) {
            LOGGER.error("Timed out when deleting da-vinci push status of {}_v{}", storeName, versionNumber);
            future.cancel(true);
          } finally {
            executor.shutdownNow();
          }
        }

        PubSubTopic rtTopic = pubSubTopicRepository.getTopic(Utils.getRealTimeTopicName(deletedVersion.get()));
        if (!store.isHybrid() && getTopicManager().containsTopic(rtTopic)) {
          safeDeleteRTTopic(clusterName, deletedVersion.get());
        }
      }
    }
  }

  private boolean hasFatalDataValidationError(PushMonitor pushMonitor, String topicName) {
    try {
      OfflinePushStatus offlinePushStatus = pushMonitor.getOfflinePushOrThrow(topicName);
      return offlinePushStatus.hasFatalDataValidationError();
    } catch (VeniceException e) {
      LOGGER.warn("Failed to get offline push status for topic: {}. It might not exist anymore.", topicName);
      return false;
    }
  }

  private void safeDeleteRTTopic(String clusterName, Version version) {
    String realTimeTopicName = Utils.getRealTimeTopicName(version);
    boolean rtDeletionPermitted = isRTTopicDeletionPermittedByAllControllers(clusterName, realTimeTopicName);

    if (rtDeletionPermitted) {
      deleteRTTopicFromAllFabrics(realTimeTopicName, clusterName);
      // Check if there is incremental push topic exist. If yes, delete it and send out to let other controller to
      // delete it.
      String incrementalPushRTTopicToDelete = Utils.getSeparateRealTimeTopicName(version);
      if (getTopicManager().containsTopic(pubSubTopicRepository.getTopic(incrementalPushRTTopicToDelete))) {
        deleteRTTopicFromAllFabrics(incrementalPushRTTopicToDelete, clusterName);
      }
    }
  }

  public boolean isRTTopicDeletionPermittedByAllControllers(String clusterName, String rtTopicName) {
    // Perform RT cleanup checks for batch only store that used to be hybrid. Check versions
    // to see if any version is still using RT before deleting the RT.
    // Since we perform this check everytime when a store version is deleted we can afford to do best effort
    // approach if some fabrics are unavailable or out of sync (temporarily).
    Map<String, ControllerClient> controllerClientMap = getControllerClientMap(clusterName);

    for (Map.Entry<String, ControllerClient> controllerClientEntry: controllerClientMap.entrySet()) {
      StoreResponse storeResponse;
      String storeName = null;
      try {
        storeName = Version.parseStoreFromRealTimeTopic(rtTopicName);
        String finalStoreName = storeName;
        storeResponse = RetryUtils.executeWithMaxAttemptAndExponentialBackoff(() -> {
          StoreResponse response = controllerClientEntry.getValue().getStore(finalStoreName);
          if (response.isError() && response.getError().contains(DOES_NOT_EXISTS)) {
            throw new VeniceNoStoreException(finalStoreName);
          }
          return response;
        },
            5,
            Duration.ofMillis(10),
            Duration.ofMillis(500),
            Duration.ofSeconds(5),
            Collections.singletonList(VeniceNoStoreException.class));
        if (storeResponse.isError()) {
          if (storeResponse.getError().contains(DOES_NOT_EXISTS)) {
            LOGGER.warn(
                "Store {} does not exist in cluster {} in fabric {}, probably deleted already, skipping RT check",
                storeName,
                clusterName,
                controllerClientEntry.getKey());
            continue;
          }
          LOGGER.warn(
              "Skipping RT cleanup check for store: {} in cluster: {} due to unable to get store from fabric: {} Error: {}",
              storeName,
              clusterName,
              controllerClientEntry.getKey(),
              storeResponse.getError());
          return false;
        }
      } catch (VeniceNoStoreException e) {
        LOGGER.warn(
            "Store {} does not exist in cluster {} in fabric {}, probably deleted already, skipping RT check",
            storeName,
            clusterName,
            controllerClientEntry.getKey());
        continue;
      } catch (VeniceException e) {
        LOGGER.warn(
            "Could not parse store name from RT topic {} in cluster {} in fabric {}.",
            rtTopicName,
            clusterName,
            controllerClientEntry.getKey());
        continue;
      }
      if (Version.containsHybridVersion(storeResponse.getStore().getVersions())) {
        LOGGER.warn(
            "Topic {} cannot be deleted yet because the store {} has at least one hybrid version",
            rtTopicName,
            storeName);
        return false;
      }
    }
    return true;
  }

  private void deleteRTTopicFromAllFabrics(String topic, String clusterName) {
    Map<String, ControllerClient> controllerClientMap = getControllerClientMap(clusterName);
    truncateKafkaTopic(topic);
    for (ControllerClient controllerClient: controllerClientMap.values()) {
      ControllerResponse deleteResponse = controllerClient.deleteKafkaTopic(topic);
      if (deleteResponse.isError()) {
        LOGGER.error("Deleting real time topic " + topic + " encountered error " + deleteResponse.getError());
      }
    }
  }

  /**
   * For a given store, determine its versions to delete based on the {@linkplain BackupStrategy} settings and execute
   * the deletion in the cluster (including all its resources). It also truncates Kafka topics and Helix resources.
   * @param clusterName name of a cluster.
   * @param storeName name of the store to retire.
   * @param deleteBackupOnStartPush indicate if it is called in a start-of-push workflow.
   * @param currentVersionBeforePush current version before a new push.
   */
  @Override
  public void retireOldStoreVersions(
      String clusterName,
      String storeName,
      boolean deleteBackupOnStartPush,
      int currentVersionBeforePush) {
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      Store store = resources.getStoreMetadataRepository().getStore(storeName);

      /*
       * If deleteBackupOnStartPush is true decrement minNumberOfStoreVersionsToPreserve by one
       * as newly started push is considered as another version. The code in retrieveVersionsToDelete
       * will not return any store if we pass minNumberOfStoreVersionsToPreserve during push.
       */
      int numVersionToPreserve = minNumberOfStoreVersionsToPreserve - (deleteBackupOnStartPush ? 1 : 0);
      List<Version> versionsToDelete = store.retrieveVersionsToDelete(numVersionToPreserve);
      if (versionsToDelete.isEmpty()) {
        return;
      }

      if (deleteBackupOnStartPush) {
        LOGGER.info("Deleting backup versions as the new push started for upcoming version for store: {}", storeName);
      } else {
        LOGGER.info("Retiring old versions after successful push for store: {}", storeName);
      }

      for (Version version: versionsToDelete) {
        if (version.getNumber() == currentVersionBeforePush) {
          continue;
        }
        try {
          deleteOneStoreVersion(clusterName, storeName, version.getNumber());
        } catch (VeniceException e) {
          LOGGER.warn(
              "Could not delete store {} version number {} in cluster {}",
              storeName,
              version.getNumber(),
              clusterName,
              e);
          continue;
        }
        LOGGER.info("Retired store: {} version: {}", store.getName(), version.getNumber());
      }

      LOGGER.info("Retired {} versions for store: {}", versionsToDelete.size(), storeName);

      truncateOldTopics(clusterName, store, false);
    }
  }

  /**
   * In this function, Controller will setup proper compaction strategy when the push job is full completed, and here are the
   * reasons to set it up after the job completes:
   * 1. For batch push jobs to batch-only store, there is no impact. There could still be duplicate entries because of
   * speculative executions in map-reduce job, but we are not planning to clean them up now.
   * 2. For batch push jobs to hybrid/incremental stores, if the compaction is enabled at the beginning of the job,
   * Kafka compaction could kick in during push job, and storage node could detect DIV error, such as missing messages,
   * checksum mismatch, because speculative execution could produce duplicate entries, and we don't want to fail the push in this scenario
   * and we still want to perform the strong DIV validation in batch push, so we could only enable compaction after the batch push completes.
   * 3. For GF jobs to hybrid store, it is similar as #2, and it contains duplicate entries because there is no de-dedup
   * happening anywhere.
   *
   * With this way, when load rebalance happens for hybrid/incremental stores, DIV error could be detected during ingestion
   * at any phase since compaction might be enabled long-time ago. So in storage node, we need to add one more safeguard
   * before throwing the DIV exception to check whether the topic is compaction-enabled or not.
   * Since Venice is not going to change the compaction policy between non-compact and compact back and forth, checking
   * whether topic is compaction-enabled or not when encountering DIV error should be good enough.
   */
  @Override
  public void topicCleanupWhenPushComplete(String clusterName, String storeName, int versionNumber) {
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    VeniceControllerClusterConfig clusterConfig = resources.getConfig();
    ReadWriteStoreRepository storeRepository = resources.getStoreMetadataRepository();
    Store store = storeRepository.getStore(storeName);
    if (store.isHybrid() && clusterConfig.isKafkaLogCompactionForHybridStoresEnabled()) {
      PubSubTopic versionTopic =
          getPubSubTopicRepository().getTopic(Version.composeKafkaTopic(storeName, versionNumber));
      long minCompactionLagSeconds = store.getMinCompactionLagSeconds();
      long expectedMinCompactionLagMs =
          minCompactionLagSeconds > 0 ? minCompactionLagSeconds * Time.MS_PER_SECOND : minCompactionLagSeconds;
      long maxCompactionLagSeconds = store.getMaxCompactionLagSeconds();
      long expectedMaxCompactionLagMs =
          maxCompactionLagSeconds > 0 ? maxCompactionLagSeconds * Time.MS_PER_SECOND : maxCompactionLagSeconds;
      Consumer<PubSubTopic> updateTopicCompaction = (topic) -> getTopicManager().updateTopicCompactionPolicy(
          topic,
          true,
          expectedMinCompactionLagMs,
          expectedMaxCompactionLagMs > 0 ? Optional.of(expectedMaxCompactionLagMs) : Optional.empty());
      updateTopicCompaction.accept(versionTopic);

      // Compaction settings should also be applied to corresponding view topics
      Map<String, ViewConfig> viewConfigs = store.getVersionOrThrow(versionNumber).getViewConfigs();
      if (viewConfigs != null && !viewConfigs.isEmpty()) {
        Map<String, VeniceProperties> viewTopicNamesAndConfigs =
            ViewUtils.getViewTopicsAndConfigs(viewConfigs.values(), new Properties(), storeName, versionNumber);
        for (String topic: viewTopicNamesAndConfigs.keySet()) {
          PubSubTopic viewTopic = getPubSubTopicRepository().getTopic(topic);
          updateTopicCompaction.accept(viewTopic);
        }
      }
    }
  }

  /**
   * Delete the version specified from the store and return the deleted version. Zk shared store will only return the
   * version to be deleted (if it exists) and not actually removing it from the {@link Store} object.
   */
  private Optional<Version> deleteVersionFromStoreRepository(String clusterName, String storeName, int versionNumber) {
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    LOGGER.info("Deleting version {} in store: {} in cluster: {}", versionNumber, storeName, clusterName);
    Optional<Version> deletedVersion;
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      ReadWriteStoreRepository storeRepository = resources.getStoreMetadataRepository();
      Store store = storeRepository.getStore(storeName);
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }

      /**
       * If the system store type is {@link VeniceSystemStoreType.META_STORE}, we will use the same logic
       * as the regular Venice store here since it also contains multiple versions maintained on system store basis.
       */
      Version version = store.deleteVersion(versionNumber);
      if (version == null) {
        deletedVersion = Optional.empty();
      } else {
        deletedVersion = Optional.of(version);
        storeRepository.updateStore(store);
      }
      if (!deletedVersion.isPresent()) {
        LOGGER.warn(
            "Can not find version: {} in store: {}.  It has probably already been deleted",
            versionNumber,
            storeName);
      }
    }
    LOGGER.info("Deleted version {} in Store: {} in cluster: {}", versionNumber, storeName, clusterName);
    return deletedVersion;
  }

  /**
   * Check if a kafka topic is absent or truncated.
   * @see ConfigKeys#DEPRECATED_TOPIC_MAX_RETENTION_MS
   */
  @Override
  public boolean isTopicTruncated(String kafkaTopicName) {
    return getTopicManager()
        .isTopicTruncated(pubSubTopicRepository.getTopic(kafkaTopicName), deprecatedJobTopicMaxRetentionMs);
  }

  /**
   * Test if retention is less than the configured DEPRECATED_TOPIC_MAX_RETENTION_MS value.
   * @return <code>true</code> if the specified retention is below the configuration;
   *         <code></code> false otherwise.
   * @see ConfigKeys#DEPRECATED_TOPIC_MAX_RETENTION_MS
   */
  @Override
  public boolean isTopicTruncatedBasedOnRetention(long retention) {
    return getTopicManager().isRetentionBelowTruncatedThreshold(retention, deprecatedJobTopicMaxRetentionMs);
  }

  /**
   * Topic should also be considered to get cleaned up if:
   * retention is less than the configured {@linkplain ConfigKeys#DEPRECATED_TOPIC_MAX_RETENTION_MS} value.
   * or
   * 1. Topic retention equals fatalDataValidationFailureRetentionMs.
   * 2. Topic is a version topic.
   * 3. fatalDataValidationFailureRetentionMs has already passed since its creation.
   */
  @Override
  public boolean isTopicTruncatedBasedOnRetention(String kafkaTopicName, long retentionTime) {
    if (isTopicTruncatedBasedOnRetention(retentionTime)) {
      return true;
    }

    if (retentionTime == fatalDataValidationFailureRetentionMs && Version.isVersionTopic(kafkaTopicName)) {
      PushJobStatusRecordKey storeVersionKey = new PushJobStatusRecordKey();
      storeVersionKey.storeName = Version.parseStoreFromKafkaTopicName(kafkaTopicName);
      storeVersionKey.versionNumber = Version.parseVersionFromKafkaTopicName(kafkaTopicName);
      try {
        PushJobDetails jobDetails = getPushJobDetails(storeVersionKey);
        if (jobDetails != null) {
          /** Use {@link PushJobDetails.reportTimestamp} to approximate time of topic creation.*/
          if (LatencyUtils
              .getElapsedTimeFromMsToMs(jobDetails.reportTimestamp) > fatalDataValidationFailureRetentionMs) {
            return true;
          }
        }
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to get push job details for store: {}, version: {}",
            storeVersionKey.storeName,
            storeVersionKey.versionNumber,
            e);
      }
    }

    return false;
  }

  /**
   * @return the controller configuration value for MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE.
   * @see ConfigKeys#MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE
   */
  @Override
  public int getMinNumberOfUnusedKafkaTopicsToPreserve() {
    return multiClusterConfigs.getCommonConfig().getMinNumberOfUnusedKafkaTopicsToPreserve();
  }

  /**
   * We don't actually truncate any Kafka topic here; we just update the retention time.
   * @param kafkaTopicName
   * @return
   */
  @Override
  public boolean truncateKafkaTopic(String kafkaTopicName) {
    /**
     * Topic truncation doesn't care about whether the topic actually exists in Kafka Broker or not since
     * the truncation will only update the topic metadata in Kafka Zookeeper.
     * This logic is used to avoid the scenario that the topic exists in Kafka Zookeeper, but not fully ready in
     * Kafka Broker yet.
     */
    if (multiClusterConfigs.isParent()) {
      return truncateKafkaTopicInParentFabrics(kafkaTopicName);
    } else {
      return truncateKafkaTopic(getTopicManager(), kafkaTopicName, this.deprecatedJobTopicRetentionMs);
    }
  }

  /**
   * @see Admin#truncateKafkaTopic(String, long)
   */
  @Override
  public boolean truncateKafkaTopic(String topicName, long retentionTime) {
    return multiClusterConfigs.isParent()
        ? truncateKafkaTopicInParentFabrics(topicName)
        : truncateKafkaTopic(getTopicManager(), topicName, retentionTime);
  }

  private boolean truncateKafkaTopic(String kafkaTopicName, Map<String, PubSubTopicConfiguration> topicConfigs) {
    if (multiClusterConfigs.isParent()) {
      /**
       * topicConfigs is ignored on purpose, since we couldn't guarantee configs are in sync in
       * different Kafka clusters.
       */
      return truncateKafkaTopicInParentFabrics(kafkaTopicName);
    } else {
      return truncateKafkaTopic(getTopicManager(), kafkaTopicName, topicConfigs);
    }
  }

  /**
   * Iterate through all parent fabric Kafka clusters and truncate the given kafka topic.
   */
  private boolean truncateKafkaTopicInParentFabrics(String kafkaTopicName) {
    boolean allTopicsAreDeleted = true;
    Set<String> parentFabrics = multiClusterConfigs.getParentFabrics();
    for (String parentFabric: parentFabrics) {
      String kafkaBootstrapServerAddress = getNativeReplicationKafkaBootstrapServerAddress(parentFabric);
      allTopicsAreDeleted &= truncateKafkaTopic(
          getTopicManager(kafkaBootstrapServerAddress),
          kafkaTopicName,
          this.deprecatedJobTopicRetentionMs);
    }
    return allTopicsAreDeleted;
  }

  private boolean truncateKafkaTopic(
      TopicManager topicManager,
      String kafkaTopicName,
      long deprecatedJobTopicRetentionMs) {
    try {

      if (topicManager
          .updateTopicRetention(pubSubTopicRepository.getTopic(kafkaTopicName), deprecatedJobTopicRetentionMs)) {
        return true;
      }
    } catch (PubSubTopicDoesNotExistException e) {
      LOGGER.info(
          "Topic {} does not exist in Kafka cluster {}, will skip the truncation",
          kafkaTopicName,
          topicManager.getPubSubClusterAddress());
    } catch (Exception e) {
      LOGGER.warn(
          "Unable to update the retention for topic {} in Kafka cluster {}, will skip the truncation",
          kafkaTopicName,
          topicManager.getPubSubClusterAddress(),
          e);
    }
    return false;
  }

  private boolean truncateKafkaTopic(
      TopicManager topicManager,
      String kafkaTopicName,
      Map<String, PubSubTopicConfiguration> topicConfigs) {
    if (topicConfigs.containsKey(kafkaTopicName)) {
      if (topicManager.updateTopicRetention(
          pubSubTopicRepository.getTopic(kafkaTopicName),
          deprecatedJobTopicRetentionMs,
          topicConfigs.get(kafkaTopicName))) {
        return true;
      }
    } else {
      LOGGER.info("Topic: {} doesn't exist or not found in the configs, will skip the truncation", kafkaTopicName);
    }
    // return false to indicate the retention config has already been updated.
    return false;
  }

  /**
   * Truncate old unused Kafka topics. These could arise from resource leaking of topics during certain
   * scenarios.
   *
   * If it is for store deletion, this function will truncate all the topics;
   * Otherwise, it will only truncate topics without corresponding active version.
   *
   * N.B.: visibility is package-private to ease testing...
   *
   * @param store for which to clean up old topics
   * @param forStoreDeletion
   *
   */
  void truncateOldTopics(String clusterName, Store store, boolean forStoreDeletion) {
    if (store.isMigrating()) {
      LOGGER.info("This store {} is being migrated. Skip topic deletion.", store.getName());
      return;
    }

    Set<Integer> currentlyKnownVersionNumbers =
        store.getVersions().stream().map(version -> version.getNumber()).collect(Collectors.toSet());

    Set<PubSubTopic> allTopics = getTopicManager().listTopics();

    Set<PubSubTopic> allTopicsRelatedToThisStore = allTopics.stream()
        /** Exclude RT buffer topics, admin topics and all other special topics */
        .filter(t -> Version.isATopicThatIsVersioned(t.getName()))
        /** Keep only those topics pertaining to the store in question */
        .filter(t -> Version.parseStoreFromKafkaTopicName(t.getName()).equals(store.getName()))
        .collect(Collectors.toSet());

    if (allTopicsRelatedToThisStore.isEmpty()) {
      LOGGER.info("Searched for old topics belonging to store: {}, and did not find any.", store.getName());
      return;
    }
    Set<PubSubTopic> oldTopicsToTruncate = allTopicsRelatedToThisStore;
    if (!forStoreDeletion) {
      /**
       * For store version deprecation, controller will truncate all the topics without corresponding versions and
       * the version belonging to is smaller than the largest used version of current store.
       *
       * The reason to check whether the to-be-deleted version is smaller than the largest used version of current store or not:
       * 1. Topic could be created either by Kafka MM or addVersion function call (triggered by
       * {@link AdminConsumptionTask};
       * 2. If the topic is created by Kafka MM and the actual version creation gets delayed for some reason, the following
       * scenario could happen (assuming the current version is n):
       *   a. Topics: store_v(n-2), store_v(n-1), store_v(n), store_v(n+1) could exist at the same time because of the actual
       *     version creation gets delayed;
       *   b. The largest used version of current store is (n).
       * In this scenario, Controller should only deprecate store_v(n-2), instead of both store_v(n-2) and store_v(n+1) [because
       * of no corresponding store version], since the later one is still valid.
       */
      oldTopicsToTruncate = allTopicsRelatedToThisStore.stream().filter((topic) -> {
        int versionForCurrentTopic = Version.parseVersionFromKafkaTopicName(topic.getName());
        return !currentlyKnownVersionNumbers.contains(versionForCurrentTopic)
            && versionForCurrentTopic <= store.getLargestUsedVersionNumber();
      }).collect(Collectors.toSet());
    }

    if (oldTopicsToTruncate.isEmpty()) {
      LOGGER.info("Searched for old topics belonging to store: {}', and did not find any.", store.getName());
    } else {
      LOGGER.info("Detected the following old topics to truncate: {}", oldTopicsToTruncate);
      int numberOfNewTopicsMarkedForDelete = 0;
      Map<PubSubTopic, PubSubTopicConfiguration> pubSubTopicConfigs =
          getTopicManager().getSomeTopicConfigs(oldTopicsToTruncate);
      Map<String, PubSubTopicConfiguration> topicConfigs = new HashMap<>();
      for (Map.Entry<PubSubTopic, PubSubTopicConfiguration> entry: pubSubTopicConfigs.entrySet()) {
        topicConfigs.put(entry.getKey().getName(), entry.getValue());
      }
      for (PubSubTopic t: oldTopicsToTruncate) {
        if (truncateKafkaTopic(t.getName(), topicConfigs)) {
          ++numberOfNewTopicsMarkedForDelete;
        }
        if (!VeniceView.isViewTopic(t.getName())) {
          deleteHelixResource(clusterName, t.getName());
        }
      }
      LOGGER.info("Deleted {} old HelixResources for store: {}.", numberOfNewTopicsMarkedForDelete, store.getName());
      LOGGER.info(
          "Finished truncating old topics for store: {}'. Retention time for {} topics out of {} have been updated.",
          store.getName(),
          numberOfNewTopicsMarkedForDelete,
          oldTopicsToTruncate.size());
    }
  }

  /***
   * If you need to do mutations on the store, then you must hold onto the lock until you've persisted your mutations.
   * Only use this method if you're doing read-only operations on the store.
   * @param clusterName
   * @param storeName
   * @return
   */
  private Store getStoreForReadOnly(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreReadLock(storeName)) {
      Store store = resources.getStoreMetadataRepository().getStore(storeName);
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }
      return store; /* is a clone */
    }
  }

  /**
   * @return all versions of the specified store from a cluster.
   */
  @Override
  public List<Version> versionsForStore(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    List<Version> versions;
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreReadLock(storeName)) {
      Store store = resources.getStoreMetadataRepository().getStore(storeName);
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }
      versions = store.getVersions();
    }
    return versions;
  }

  /**
   * @return all stores in the specified cluster.
   */
  @Override
  public List<Store> getAllStores(String clusterName) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    return resources.getStoreMetadataRepository().getAllStores();
  }

  /**
   * @see Admin#getAllStoreStatuses(String)
   */
  @Override
  public Map<String, String> getAllStoreStatuses(String clusterName) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    List<Store> storeList = resources.getStoreMetadataRepository().getAllStores();
    RoutingDataRepository routingDataRepository =
        getHelixVeniceClusterResources(clusterName).getRoutingDataRepository();
    ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
    return StoreStatusDecider
        .getStoreStatues(storeList, resourceAssignment, getHelixVeniceClusterResources(clusterName).getPushMonitor());
  }

  /**
   * Test if the input store exists in a cluster.
   * @param clusterName name of a cluster.
   * @param storeName name of a store.
   * @return <code>ture</code> if store exists in the cluster.
   *         <code>false</code> otherwise.
   */
  @Override
  public boolean hasStore(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreReadLock(storeName)) {
      return resources.getStoreMetadataRepository().hasStore(storeName);
    }
  }

  /**
   * @return <code>Store</code> object reference from the input store name.
   */
  @Override
  public Store getStore(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    return repository.getStore(storeName);
  }

  StoreVersionInfo waitVersion(String clusterName, String storeName, int versionNumber, Duration timeout) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    return repository.waitVersion(storeName, versionNumber, timeout);
  }

  /**
   * Update the current version of a specified store.
   */
  @Override
  public void setStoreCurrentVersion(String clusterName, String storeName, int versionNumber) {
    this.setStoreCurrentVersion(clusterName, storeName, versionNumber, false);
  }

  /**
   * In most cases, parent region should not update the current version. This is only allowed via an update-store call
   * where the region filter list only contains one region, which is the region of the parent controller
   */
  private void setStoreCurrentVersion(
      String clusterName,
      String storeName,
      int versionNumber,
      boolean allowedInParent) {
    if (isParent() && !allowedInParent) {
      // Parent colo should not update the current version of a store unless explicitly asked to do so
      LOGGER.info(
          "Skipping current version update for store: {} in cluster: {} because it is not allowed in the "
              + "parent region",
          storeName,
          clusterName);
      return;
    }

    storeMetadataUpdate(clusterName, storeName, store -> {
      if (store.getCurrentVersion() != NON_EXISTING_VERSION) {
        if (versionNumber != NON_EXISTING_VERSION && !store.containsVersion(versionNumber)) {
          throw new VeniceException("Version: " + versionNumber + " does not exist for store:" + storeName);
        }

        if (!store.isEnableWrites()) {
          throw new VeniceException(
              "Unable to update store: " + storeName + " current version since store writeability is false");
        }
      }
      int previousVersion = store.getCurrentVersion();
      store.setCurrentVersion(versionNumber);
      if (!isParent()) {
        // Parent controller should not transmit the version swap message
        realTimeTopicSwitcher.transmitVersionSwapMessage(store, previousVersion, versionNumber);
      }
      return store;
    });
  }

  @Override
  public void rollForwardToFutureVersion(String clusterName, String storeName, String regionFilter) {
    if (!isRegionPartOfRegionsFilterList(getRegionName(), regionFilter)) {
      LOGGER.info(
          "Rolling forward will be skipped for store: {} in cluster: {}, as the region filter {} doesn't include the current region {}",
          storeName,
          clusterName,
          regionFilter,
          getRegionName());
      return;
    }

    int onlineFutureVersion = getFutureVersionWithStatus(clusterName, storeName, ONLINE);
    int pushedFutureVersion = getFutureVersionWithStatus(clusterName, storeName, PUSHED); // Check for PUSHED status too
                                                                                          // for target region pushes
    if (onlineFutureVersion == Store.NON_EXISTING_VERSION && pushedFutureVersion == NON_EXISTING_VERSION) {
      LOGGER.info(
          "Rolling forward will be skipped for store: {} in cluster: {}, as there is no future version",
          storeName,
          clusterName);
      return;
    }

    int futureVersion = onlineFutureVersion == Store.NON_EXISTING_VERSION ? pushedFutureVersion : onlineFutureVersion;
    storeMetadataUpdate(clusterName, storeName, store -> {
      if (!store.isEnableWrites()) {
        throw new VeniceException(
            "Unable to update store:" + storeName + " current version since store does not enable writes");
      }
      // check whether the future version has enough ready-to-serve instances for all partitions in CV
      int previousVersion = store.getCurrentVersion();
      Version futureVersionObj = store.getVersion(futureVersion);
      int partitionCount = futureVersionObj.getPartitionCount();
      int minActiveReplicas = futureVersionObj.getMinActiveReplicas();
      String currentVersionKafka = Version.composeKafkaTopic(storeName, futureVersion);
      HelixCustomizedViewOfflinePushRepository customizedViewRepository =
          getHelixVeniceClusterResources(clusterName).getCustomizedViewRepository();
      // Check if all partitions have enough ready-to-serve instances
      for (int partition = 0; partition < partitionCount; partition++) {
        List<Instance> readyToServeInstances;
        try {
          readyToServeInstances = customizedViewRepository.getReadyToServeInstances(currentVersionKafka, partition);
        } catch (Exception e) {
          readyToServeInstances = Collections.emptyList();
        }

        if (readyToServeInstances.size() < minActiveReplicas) {
          // fail rolling forward if any partition does not have enough ready-to-serve instances
          StringBuilder errorBuilder = new StringBuilder();
          errorBuilder.append("Rolling forward current version ")
              .append(previousVersion)
              .append(" to future version: ")
              .append(futureVersion)
              .append(" failed for store: ")
              .append(storeName)
              .append(" as partition ")
              .append(partition)
              .append(" and probably others do not have enough ready-to-serve instances");
          String errorMessage = errorBuilder.toString();
          LOGGER.error(errorMessage);
          throw new VeniceException(errorMessage);
        }
      }
      store.setCurrentVersion(futureVersion);

      if (pushedFutureVersion != Store.NON_EXISTING_VERSION) {
        store.updateVersionStatus(futureVersion, VersionStatus.ONLINE);
      }

      LOGGER.info(
          "Rolling forward current version: {} to future version: {} succeeded for store: {}",
          previousVersion,
          futureVersion,
          storeName);
      getRealTimeTopicSwitcher().transmitVersionSwapMessage(store, previousVersion, futureVersion);
      return store;
    });
  }

  /**
   * Set backup version as current version in a child region.
   */
  @Override
  public void rollbackToBackupVersion(String clusterName, String storeName, String regionFilter) {
    if (!StringUtils.isEmpty(regionFilter)) {
      Set<String> regionsFilter = parseRegionsFilterList(regionFilter);
      if (!regionsFilter.contains(multiClusterConfigs.getRegionName())) {
        LOGGER.info(
            "rollbackToBackupVersion command will be skipped for store: {} in cluster: {}, because the region filter is {}"
                + " which doesn't include the current region: {}",
            storeName,
            clusterName,
            regionsFilter,
            multiClusterConfigs.getRegionName());
        return;
      }
    }

    storeMetadataUpdate(clusterName, storeName, store -> {
      if (!store.isEnableWrites()) {
        throw new VeniceException(
            "Unable to update store:" + storeName + " current version since store does not enable write");
      }
      int backupVersion = getBackupVersionNumber(store.getVersions(), store.getCurrentVersion());
      if (backupVersion == NON_EXISTING_VERSION) {
        return store;
      }
      int previousVersion = store.getCurrentVersion();
      store.setCurrentVersion(backupVersion);
      LOGGER
          .info("Rolling back current version {} to version {} in store {}", previousVersion, backupVersion, storeName);
      realTimeTopicSwitcher.transmitVersionSwapMessage(store, previousVersion, backupVersion);
      return store;
    });
  }

  /**
   * Get backup version number, the largest online version number that is less than the current version number
   */
  public int getBackupVersionNumber(List<Version> versions, int currentVersion) {
    versions.sort(Comparator.comparingInt(Version::getNumber).reversed());
    for (Version v: versions) {
      if (v.getNumber() < currentVersion && ONLINE.equals(v.getStatus())) {
        return v.getNumber();
      }
    }
    return NON_EXISTING_VERSION;
  }

  /**
   * Update the largest used version number of a specified store.
   */
  @Override
  public void setStoreLargestUsedVersion(String clusterName, String storeName, int versionNumber) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setLargestUsedVersionNumber(versionNumber);
      return store;
    });
  }

  /**
   * Update the largest used RT version number of a specified store.
   */
  @Override
  public void setStoreLargestUsedRTVersion(String clusterName, String storeName, int versionNumber) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setLargestUsedRTVersionNumber(versionNumber);
      return store;
    });
  }

  /**
   * Update the owner of a specified store.
   */
  @Override
  public void setStoreOwner(String clusterName, String storeName, String owner) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setOwner(owner);
      return store;
    });
  }

  /**
   * Since partition check/calculation only happens when adding new store version, {@link #setStorePartitionCount(String, String, int)}
   * would only change the number of partition for the following pushes. Current version would not be changed.
   */
  @Override
  public void setStorePartitionCount(String clusterName, String storeName, int partitionCount) {
    VeniceControllerClusterConfig clusterConfig = getHelixVeniceClusterResources(clusterName).getConfig();
    storeMetadataUpdate(clusterName, storeName, store -> {
      preCheckStorePartitionCountUpdate(clusterName, store, partitionCount);
      // Do not update the partitionCount on the store.version as version config is immutable. The
      // version.getPartitionCount()
      // is read only in ensureRealTimeTopicExistsForUserSystemStores and createInternalStore creation, so modifying
      // currentVersion should not have
      // any effect.
      if (store.isHybrid()
          && multiClusterConfigs.getControllerConfig(clusterName).isHybridStorePartitionCountUpdateEnabled()) {
        generateAndUpdateRealTimeTopicName(store);
        LOGGER.info(
            "Updated largestUsedRTVersionNumber to {} for store {} in cluster {}",
            store.getLargestUsedRTVersionNumber(),
            storeName,
            clusterName);
      }
      if (partitionCount != 0) {
        store.setPartitionCount(partitionCount);
      } else {
        store.setPartitionCount(clusterConfig.getMinNumberOfPartitions());
      }

      return store;
    });
  }

  void preCheckStorePartitionCountUpdate(String clusterName, Store store, int newPartitionCount) {
    String errorMessagePrefix = "Store update error for " + store.getName() + " in cluster: " + clusterName + ": ";
    VeniceControllerClusterConfig clusterConfig = getHelixVeniceClusterResources(clusterName).getConfig();
    int maxPartitionNum = clusterConfig.getMaxNumberOfPartitions();

    if (store.isHybrid() && store.getPartitionCount() != newPartitionCount) {
      if (multiClusterConfigs.getControllerConfig(clusterName).isHybridStorePartitionCountUpdateEnabled()
          && newPartitionCount <= maxPartitionNum && newPartitionCount >= 0) {
        LOGGER.info(
            "Allow updating store " + store.getName() + " partition count to " + newPartitionCount
                + " because `updateRealTimeTopic` is true.");
        return;
      }
      // Allow the update if partition count is not configured and the new partition count matches RT partition count
      if (store.getPartitionCount() == 0) {
        TopicManager topicManager;
        if (isParent()) {
          // RT might not exist in parent colo. Get RT partition count from a child colo.
          String childDatacenter = clusterConfig.getChildDatacenters().iterator().next();
          topicManager = getTopicManager(multiClusterConfigs.getChildDataCenterKafkaUrlMap().get(childDatacenter));
        } else {
          topicManager = getTopicManager();
        }
        PubSubTopic realTimeTopic = pubSubTopicRepository.getTopic(Utils.getRealTimeTopicName(store));
        if (topicManager.containsTopic(realTimeTopic)
            && topicManager.getPartitionCount(realTimeTopic) == newPartitionCount) {
          LOGGER.info("Allow updating store " + store.getName() + " partition count to " + newPartitionCount);
          return;
        }
      }
      String errorMessage = errorMessagePrefix + "Cannot change partition count for this hybrid store";
      LOGGER.error(errorMessage);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_CONFIG);
    }

    if (newPartitionCount > maxPartitionNum) {
      String errorMessage =
          errorMessagePrefix + "Partition count: " + newPartitionCount + " should be less than max: " + maxPartitionNum;
      LOGGER.error(errorMessage);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_CONFIG);
    }
    if (newPartitionCount < 0) {
      String errorMessage = errorMessagePrefix + "Partition count: " + newPartitionCount + " should NOT be negative";
      LOGGER.error(errorMessage);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_CONFIG);
    }
  }

  private void generateAndUpdateRealTimeTopicName(Store store) {
    String newRealTimeTopicName = Utils.isRTVersioningApplicable(store.getName())
        ? Utils.composeRealTimeTopic(store.getName(), store.getLargestUsedRTVersionNumber() + 1)
        : DEFAULT_REAL_TIME_TOPIC_NAME;

    store.getHybridStoreConfig().setRealTimeTopicName(newRealTimeTopicName);
    /*
       After partition count update, new VT, if is hybrid, has to use a new RT with the same partition count, so we
       increase `largestUsedRTVersionNumber`; this will ensure that a new RT is created when/if needed
     */
    store.setLargestUsedRTVersionNumber(store.getLargestUsedRTVersionNumber() + 1);
  }

  void setStorePartitionerConfig(String clusterName, String storeName, PartitionerConfig partitionerConfig) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      // Only amplification factor is allowed to be changed if the store is a hybrid store.
      if (store.isHybrid() && !(Objects.equals(store.getPartitionerConfig(), partitionerConfig)
          || isAmplificationFactorUpdateOnly(store.getPartitionerConfig(), partitionerConfig))) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            "Partitioner config change from " + store.getPartitionerConfig() + " to " + partitionerConfig
                + " in hybrid store is not supported except amplification factor.");
      } else {
        store.setPartitionerConfig(partitionerConfig);
        return store;
      }
    });
  }

  /**
   * Update the writability of a specified store.
   */
  @Override
  public void setStoreWriteability(String clusterName, String storeName, boolean desiredWriteability) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setEnableWrites(desiredWriteability);

      return store;
    });
  }

  /**
   * Update the readability of a specified store.
   */
  @Override
  public void setStoreReadability(String clusterName, String storeName, boolean desiredReadability) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setEnableReads(desiredReadability);

      return store;
    });
  }

  /**
   * Update both readability and writability of a specified store.
   */
  @Override
  public void setStoreReadWriteability(String clusterName, String storeName, boolean isAccessible) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setEnableReads(isAccessible);
      store.setEnableWrites(isAccessible);

      return store;
    });
  }

  /**
   * We will not expose this interface to Spark server. Updating quota can only be done by #updateStore
   * TODO: remove all store attribute setters.
   */
  private void setStoreStorageQuota(String clusterName, String storeName, long storageQuotaInByte) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      if (storageQuotaInByte < 0 && storageQuotaInByte != Store.UNLIMITED_STORAGE_QUOTA) {
        throw new VeniceException("storage quota can not be less than 0");
      }
      store.setStorageQuotaInByte(storageQuotaInByte);

      return store;
    });
  }

  private void setStoreReadQuota(String clusterName, String storeName, long readQuotaInCU) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      if (readQuotaInCU < 0) {
        throw new VeniceException("read quota can not be less than 0");
      }
      store.setReadQuotaInCU(readQuotaInCU);

      return store;
    });
  }

  void setAccessControl(String clusterName, String storeName, boolean accessControlled) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setAccessControlled(accessControlled);

      return store;
    });
  }

  @Override
  public StoreMetaValue getMetaStoreValue(StoreMetaKey metaKey, String storeName) {
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    String d2Service = discoverCluster(storeName).getSecond();

    try (AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> client = ClientFactory.getAndStartSpecificAvroClient(
        ClientConfig.defaultSpecificClientConfig(metaStoreName, StoreMetaValue.class)
            .setD2ServiceName(d2Service)
            .setD2Client(this.d2Client))) {
      return client.get(metaKey).get();
    } catch (Exception e) {
      LOGGER.warn("Could not fetch meta store value for key " + metaKey, e);
      return null;
    }
  }

  @Override
  public Set<Integer> getInUseValueSchemaIds(String clusterName, String storeName) {
    if (isParent()) {
      return Collections.emptySet();
    }

    Store store = getStore(clusterName, storeName);
    Set<Integer> schemaIds = new HashSet<>();

    // Fetch value schema id used by all existing store version
    for (Version version: store.getVersions()) {
      Map<String, String> map = new HashMap<>(2);
      map.put(KEY_STRING_STORE_NAME, storeName);
      map.put(MetaStoreWriter.KEY_STRING_VERSION_NUMBER, Integer.toString(version.getNumber()));
      StoreMetaKey key = MetaStoreDataType.VALUE_SCHEMAS_WRITTEN_PER_STORE_VERSION.getStoreMetaKey(map);
      StoreMetaValue metaValue = getMetaStoreValue(key, storeName);

      if (metaValue == null) {
        String msg = "Could not find in-use value schema for store " + storeName;
        LOGGER.warn(msg);
        throw new VeniceException(msg);
      }
      schemaIds.addAll(metaValue.storeValueSchemaIdsWrittenPerStoreVersion);
    }
    return schemaIds;
  }

  public void deleteValueSchemas(String clusterName, String storeName, Set<Integer> unusedValueSchemaIds) {
    Set<Integer> inuseValueSchemaIds = getInUseValueSchemaIds(clusterName, storeName);
    boolean isCommon = unusedValueSchemaIds.stream().anyMatch(inuseValueSchemaIds::contains);
    if (isCommon) {
      String msg = "For store " + storeName + " cannot delete value schema ids they being used. schema ids: "
          + unusedValueSchemaIds;
      LOGGER.error(msg);
      throw new VeniceException(msg);
    }
    // delete the unused schemas.
    unusedValueSchemaIds.forEach(id -> {
      getHelixVeniceClusterResources(clusterName).getSchemaRepository().removeValueSchema(storeName, id);
      LOGGER.info("Removed value schema with ID " + id + " for store " + storeName);
    });
  }

  private void setStoreCompressionStrategy(
      String clusterName,
      String storeName,
      CompressionStrategy compressionStrategy) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setCompressionStrategy(compressionStrategy);

      return store;
    });
  }

  private void setClientDecompressionEnabled(String clusterName, String storeName, boolean clientDecompressionEnabled) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setClientDecompressionEnabled(clientDecompressionEnabled);
      return store;
    });
  }

  private void setChunkingEnabled(String clusterName, String storeName, boolean chunkingEnabled) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setChunkingEnabled(chunkingEnabled);
      return store;
    });
  }

  private void setRmdChunkingEnabled(String clusterName, String storeName, boolean rmdChunkingEnabled) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setRmdChunkingEnabled(rmdChunkingEnabled);
      return store;
    });
  }

  void setIncrementalPushEnabled(String clusterName, String storeName, boolean incrementalPushEnabled) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      VeniceControllerClusterConfig config = getHelixVeniceClusterResources(clusterName).getConfig();
      if (incrementalPushEnabled || store.isHybrid()) {
        // Enabling incremental push
        store.setNativeReplicationSourceFabric(config.getNativeReplicationSourceFabricAsDefaultForHybrid());
        store.setActiveActiveReplicationEnabled(
            store.isActiveActiveReplicationEnabled()
                || (config.isActiveActiveReplicationEnabledAsDefaultForHybrid() && !store.isSystemStore()));
      } else {
        // Disabling incremental push
        // This is only possible when hybrid settings are set to null before turning of incremental push for the store.
        store.setNativeReplicationSourceFabric(config.getNativeReplicationSourceFabricAsDefaultForBatchOnly());
        store.setActiveActiveReplicationEnabled(false);
      }
      store.setIncrementalPushEnabled(incrementalPushEnabled);

      return store;
    });
  }

  void setSeparateRealTimeTopicEnabled(String clusterName, String storeName, boolean separateRealTimeTopicEnabled) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setSeparateRealTimeTopicEnabled(separateRealTimeTopicEnabled);
      return store;
    });
  }

  private void setReplicationFactor(String clusterName, String storeName, int replicaFactor) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setReplicationFactor(replicaFactor);

      return store;
    });
  }

  private void setBatchGetLimit(String clusterName, String storeName, int batchGetLimit) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setBatchGetLimit(batchGetLimit);

      return store;
    });
  }

  private void setNumVersionsToPreserve(String clusterName, String storeName, int numVersionsToPreserve) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setNumVersionsToPreserve(numVersionsToPreserve);

      return store;
    });
  }

  private void setStoreMigration(String clusterName, String storeName, boolean migrating) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setMigrating(migrating);
      return store;
    });
  }

  private void setMigrationDuplicateStore(String clusterName, String storeName, boolean migrationDuplicateStore) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setMigrationDuplicateStore(migrationDuplicateStore);
      return store;
    });
  }

  private void setWriteComputationEnabled(String clusterName, String storeName, boolean writeComputationEnabled) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setWriteComputationEnabled(writeComputationEnabled);
      return store;
    });
  }

  void setReplicationMetadataVersionID(String clusterName, String storeName, int rmdVersion) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setRmdVersion(rmdVersion);
      return store;
    });
  }

  private void setReadComputationEnabled(String clusterName, String storeName, boolean computationEnabled) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setReadComputationEnabled(computationEnabled);
      return store;
    });
  }

  void setBootstrapToOnlineTimeoutInHours(String clusterName, String storeName, int bootstrapToOnlineTimeoutInHours) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setBootstrapToOnlineTimeoutInHours(bootstrapToOnlineTimeoutInHours);
      return store;
    });
  }

  private void setNativeReplicationEnabled(String clusterName, String storeName, boolean nativeReplicationEnabled) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setNativeReplicationEnabled(nativeReplicationEnabled);
      return store;
    });
  }

  private void setPushStreamSourceAddress(String clusterName, String storeName, String pushStreamSourceAddress) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setPushStreamSourceAddress(pushStreamSourceAddress);
      return store;
    });
  }

  private void addStoreViews(String clusterName, String storeName, Map<String, String> viewConfigMap) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setViewConfigs(StoreViewUtils.convertStringMapViewToViewConfigMap(viewConfigMap));
      return store;
    });
  }

  private void setBackupStrategy(String clusterName, String storeName, BackupStrategy backupStrategy) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setBackupStrategy(backupStrategy);
      return store;
    });
  }

  private void setAutoSchemaRegisterPushJobEnabled(
      String clusterName,
      String storeName,
      boolean autoSchemaRegisterPushJobEnabled) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setSchemaAutoRegisterFromPushJobEnabled(autoSchemaRegisterPushJobEnabled);
      return store;
    });
  }

  void setHybridStoreDiskQuotaEnabled(String clusterName, String storeName, boolean hybridStoreDiskQuotaEnabled) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setHybridStoreDiskQuotaEnabled(hybridStoreDiskQuotaEnabled);
      return store;
    });
  }

  private void setBackupVersionRetentionMs(String clusterName, String storeName, long backupVersionRetentionMs) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setBackupVersionRetentionMs(backupVersionRetentionMs);
      return store;
    });
  }

  private void setNativeReplicationSourceFabric(
      String clusterName,
      String storeName,
      String nativeReplicationSourceFabric) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setNativeReplicationSourceFabric(nativeReplicationSourceFabric);
      return store;
    });
  }

  void setActiveActiveReplicationEnabled(String clusterName, String storeName, boolean activeActiveReplicationEnabled) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setActiveActiveReplicationEnabled(activeActiveReplicationEnabled);
      return store;
    });
  }

  private void disableMetaSystemStore(String clusterName, String storeName) {
    LOGGER.info("Disabling meta system store for store: {} of cluster: {}", storeName, clusterName);
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setStoreMetaSystemStoreEnabled(false);
      store.setStoreMetadataSystemStoreEnabled(false);
      return store;
    });
  }

  private void disableDavinciPushStatusStore(String clusterName, String storeName) {
    LOGGER.info("Disabling davinci push status store for store: {} of cluster: {}", storeName, clusterName);
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setDaVinciPushStatusStoreEnabled(false);
      return store;
    });
  }

  private void setLatestSupersetSchemaId(String clusterName, String storeName, int latestSupersetSchemaId) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setLatestSuperSetValueSchemaId(latestSupersetSchemaId);
      return store;
    });
  }

  private void setStorageNodeReadQuotaEnabled(
      String clusterName,
      String storeName,
      boolean storageNodeReadQuotaEnabled) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      store.setStorageNodeReadQuotaEnabled(storageNodeReadQuotaEnabled);
      return store;
    });
  }

  /**
   * TODO: some logics are in parent controller {@link VeniceParentHelixAdmin} #updateStore and
   *       some are in the child controller here. Need to unify them in the future.
   */
  @Override
  public void updateStore(String clusterName, String storeName, UpdateStoreQueryParams params) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      internalUpdateStore(clusterName, storeName, params);
    }
  }

  /**
   * Update the {@linkplain LiveClusterConfig} at runtime for a specified cluster.
   * @param clusterName name of the Venice cluster.
   * @param params parameters to update.
   */
  @Override
  public void updateClusterConfig(String clusterName, UpdateClusterConfigQueryParams params) {
    checkControllerLeadershipFor(clusterName);
    Optional<Map<String, Integer>> regionToKafkaFetchQuota = params.getServerKafkaFetchQuotaRecordsPerSecond();
    Optional<Boolean> storeMigrationAllowed = params.getStoreMigrationAllowed();
    Optional<Boolean> childControllerAdminTopicConsumptionEnabled =
        params.getChildControllerAdminTopicConsumptionEnabled();

    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createClusterWriteLock()) {
      HelixReadWriteLiveClusterConfigRepository clusterConfigRepository =
          getReadWriteLiveClusterConfigRepository(clusterName);
      LiveClusterConfig clonedClusterConfig = new LiveClusterConfig(clusterConfigRepository.getConfigs());
      regionToKafkaFetchQuota.ifPresent(quota -> {
        Map<String, Integer> regionToKafkaFetchQuotaInLiveConfigs =
            clonedClusterConfig.getServerKafkaFetchQuotaRecordsPerSecond();
        if (regionToKafkaFetchQuotaInLiveConfigs == null) {
          clonedClusterConfig.setServerKafkaFetchQuotaRecordsPerSecond(quota);
        } else {
          regionToKafkaFetchQuotaInLiveConfigs.putAll(quota);
        }
      });
      storeMigrationAllowed.ifPresent(clonedClusterConfig::setStoreMigrationAllowed);
      childControllerAdminTopicConsumptionEnabled
          .ifPresent(clonedClusterConfig::setChildControllerAdminTopicConsumptionEnabled);
      clusterConfigRepository.updateConfigs(clonedClusterConfig);
    }
  }

  private void internalUpdateStore(String clusterName, String storeName, UpdateStoreQueryParams params) {
    // There are certain configs that are only allowed to be updated in child regions. We might still want the ability
    // to update such configs in the parent region via the Admin tool for operational reasons. So, we allow such updates
    // if the regions filter only specifies one region, which is the parent region.
    boolean onlyParentRegionFilter = false;

    // Check whether the command affects this region.
    if (params.getRegionsFilter().isPresent()) {
      Set<String> regionsFilter = parseRegionsFilterList(params.getRegionsFilter().get());
      if (!regionsFilter.contains(multiClusterConfigs.getRegionName())) {
        LOGGER.info(
            "UpdateStore command will be skipped for store: {} in cluster: {}, because the region filter is {}"
                + " which doesn't include the current region: {}",
            storeName,
            clusterName,
            regionsFilter,
            multiClusterConfigs.getRegionName());
        return;
      }

      if (isParent() && regionsFilter.size() == 1) {
        onlyParentRegionFilter = true;
      }
    }

    Store originalStore = getStore(clusterName, storeName);
    if (originalStore == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    if (originalStore.isHybrid()) {
      // If this is a hybrid store, always try to disable compaction if RT topic exists.
      try {
        PubSubTopic rtTopic = pubSubTopicRepository.getTopic(Utils.getRealTimeTopicName(originalStore));
        getTopicManager().updateTopicCompactionPolicy(rtTopic, false);
      } catch (PubSubTopicDoesNotExistException e) {
        LOGGER.error("Could not find realtime topic for hybrid store {}", storeName);
      }
    }

    Optional<String> owner = params.getOwner();
    Optional<Boolean> readability = params.getEnableReads();
    Optional<Boolean> writeability = params.getEnableWrites();
    Optional<Integer> partitionCount = params.getPartitionCount();
    Optional<String> partitionerClass = params.getPartitionerClass();
    Optional<Map<String, String>> partitionerParams = params.getPartitionerParams();
    Optional<Integer> amplificationFactor = params.getAmplificationFactor();
    Optional<Long> storageQuotaInByte = params.getStorageQuotaInByte();
    Optional<Long> readQuotaInCU = params.getReadQuotaInCU();
    Optional<Integer> currentVersion = params.getCurrentVersion();
    Optional<Integer> largestUsedVersionNumber = params.getLargestUsedVersionNumber();
    Optional<Integer> largestUsedRTVersionNumber = params.getLargestUsedRTVersionNumber();
    Optional<Long> hybridRewindSeconds = params.getHybridRewindSeconds();
    Optional<Long> hybridOffsetLagThreshold = params.getHybridOffsetLagThreshold();
    Optional<Long> hybridTimeLagThreshold = params.getHybridTimeLagThreshold();
    Optional<DataReplicationPolicy> hybridDataReplicationPolicy = params.getHybridDataReplicationPolicy();
    Optional<BufferReplayPolicy> hybridBufferReplayPolicy = params.getHybridBufferReplayPolicy();
    Optional<String> realTimeTopicName = params.getRealTimeTopicName();
    Optional<Boolean> accessControlled = params.getAccessControlled();
    Optional<CompressionStrategy> compressionStrategy = params.getCompressionStrategy();
    Optional<Boolean> clientDecompressionEnabled = params.getClientDecompressionEnabled();
    Optional<Boolean> chunkingEnabled = params.getChunkingEnabled();
    Optional<Boolean> rmdChunkingEnabled = params.getRmdChunkingEnabled();
    Optional<Integer> batchGetLimit = params.getBatchGetLimit();
    Optional<Integer> numVersionsToPreserve = params.getNumVersionsToPreserve();
    Optional<Boolean> incrementalPushEnabled = params.getIncrementalPushEnabled();
    Optional<Boolean> separateRealTimeTopicEnabled = params.getSeparateRealTimeTopicEnabled();
    Optional<Boolean> storeMigration = params.getStoreMigration();
    Optional<Boolean> writeComputationEnabled = params.getWriteComputationEnabled();
    Optional<Integer> replicationMetadataVersionID = params.getReplicationMetadataVersionID();
    Optional<Boolean> readComputationEnabled = params.getReadComputationEnabled();
    Optional<Integer> bootstrapToOnlineTimeoutInHours = params.getBootstrapToOnlineTimeoutInHours();
    Optional<BackupStrategy> backupStrategy = params.getBackupStrategy();
    Optional<Boolean> autoSchemaRegisterPushJobEnabled = params.getAutoSchemaRegisterPushJobEnabled();
    Optional<Boolean> hybridStoreDiskQuotaEnabled = params.getHybridStoreDiskQuotaEnabled();
    Optional<Boolean> regularVersionETLEnabled = params.getRegularVersionETLEnabled();
    Optional<Boolean> futureVersionETLEnabled = params.getFutureVersionETLEnabled();
    Optional<String> etledUserProxyAccount = params.getETLedProxyUserAccount();
    Optional<Boolean> nativeReplicationEnabled = params.getNativeReplicationEnabled();
    Optional<String> pushStreamSourceAddress = params.getPushStreamSourceAddress();
    Optional<Long> backupVersionRetentionMs = params.getBackupVersionRetentionMs();
    Optional<Integer> replicationFactor = params.getReplicationFactor();
    Optional<Boolean> migrationDuplicateStore = params.getMigrationDuplicateStore();
    Optional<String> nativeReplicationSourceFabric = params.getNativeReplicationSourceFabric();
    Optional<Boolean> activeActiveReplicationEnabled = params.getActiveActiveReplicationEnabled();
    Optional<String> personaName = params.getStoragePersona();
    Optional<Map<String, String>> storeViews = params.getStoreViews();
    Optional<Integer> latestSupersetSchemaId = params.getLatestSupersetSchemaId();
    Optional<Boolean> storageNodeReadQuotaEnabled = params.getStorageNodeReadQuotaEnabled();
    Optional<Long> minCompactionLagSeconds = params.getMinCompactionLagSeconds();
    Optional<Long> maxCompactionLagSeconds = params.getMaxCompactionLagSeconds();
    Optional<Integer> maxRecordSizeBytes = params.getMaxRecordSizeBytes();
    Optional<Integer> maxNearlineRecordSizeBytes = params.getMaxNearlineRecordSizeBytes();
    Optional<Boolean> unusedSchemaDeletionEnabled = params.getUnusedSchemaDeletionEnabled();
    Optional<Boolean> blobTransferEnabled = params.getBlobTransferEnabled();
    Optional<Boolean> nearlineProducerCompressionEnabled = params.getNearlineProducerCompressionEnabled();
    Optional<Integer> nearlineProducerCountPerWriter = params.getNearlineProducerCountPerWriter();
    Optional<String> targetSwapRegion = params.getTargetSwapRegion();
    Optional<Integer> targetSwapRegionWaitTime = params.getTargetRegionSwapWaitTime();
    Optional<Boolean> isDavinciHeartbeatReported = params.getIsDavinciHeartbeatReported();
    Optional<Boolean> globalRtDivEnabled = params.isGlobalRtDivEnabled();

    final Optional<HybridStoreConfig> newHybridStoreConfig;
    if (hybridRewindSeconds.isPresent() || hybridOffsetLagThreshold.isPresent() || hybridTimeLagThreshold.isPresent()
        || hybridDataReplicationPolicy.isPresent() || hybridBufferReplayPolicy.isPresent()) {
      HybridStoreConfig hybridConfig = mergeNewSettingsIntoOldHybridStoreConfig(
          originalStore,
          hybridRewindSeconds,
          hybridOffsetLagThreshold,
          hybridTimeLagThreshold,
          hybridDataReplicationPolicy,
          hybridBufferReplayPolicy,
          realTimeTopicName);
      newHybridStoreConfig = Optional.ofNullable(hybridConfig);
    } else {
      newHybridStoreConfig = Optional.empty();
    }

    try {
      if (owner.isPresent()) {
        setStoreOwner(clusterName, storeName, owner.get());
      }

      if (readability.isPresent()) {
        setStoreReadability(clusterName, storeName, readability.get());
      }

      if (writeability.isPresent()) {
        setStoreWriteability(clusterName, storeName, writeability.get());
      }

      if (partitionCount.isPresent()) {
        setStorePartitionCount(clusterName, storeName, partitionCount.get());
      }

      /**
       * If either of these three fields is not present, we should use store's original value to construct correct
       * updated partitioner config.
       */
      if (partitionerClass.isPresent() || partitionerParams.isPresent() || amplificationFactor.isPresent()) {
        PartitionerConfig updatedPartitionerConfig = mergeNewSettingsIntoOldPartitionerConfig(
            originalStore,
            partitionerClass,
            partitionerParams,
            amplificationFactor);
        setStorePartitionerConfig(clusterName, storeName, updatedPartitionerConfig);
      }

      if (storageQuotaInByte.isPresent()) {
        setStoreStorageQuota(clusterName, storeName, storageQuotaInByte.get());
      }

      if (readQuotaInCU.isPresent()) {
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        ZkRoutersClusterManager routersClusterManager = resources.getRoutersClusterManager();
        int routerCount = routersClusterManager.getLiveRoutersCount();
        VeniceControllerClusterConfig clusterConfig = getHelixVeniceClusterResources(clusterName).getConfig();
        int defaultReadQuotaPerRouter = clusterConfig.getDefaultReadQuotaPerRouter();

        if (Math.max(defaultReadQuotaPerRouter, routerCount * defaultReadQuotaPerRouter) < readQuotaInCU.get()) {
          throw new VeniceException(
              "Cannot update read quota for store " + storeName + " in cluster " + clusterName + ". Read quota "
                  + readQuotaInCU.get() + " requested is more than the cluster quota.");
        }
        setStoreReadQuota(clusterName, storeName, readQuotaInCU.get());
      }

      if (currentVersion.isPresent()) {
        setStoreCurrentVersion(clusterName, storeName, currentVersion.get(), onlyParentRegionFilter);
      }

      if (largestUsedVersionNumber.isPresent()) {
        setStoreLargestUsedVersion(clusterName, storeName, largestUsedVersionNumber.get());
      }

      if (largestUsedRTVersionNumber.isPresent()) {
        setStoreLargestUsedRTVersion(clusterName, storeName, largestUsedRTVersionNumber.get());
      }

      if (bootstrapToOnlineTimeoutInHours.isPresent()) {
        setBootstrapToOnlineTimeoutInHours(clusterName, storeName, bootstrapToOnlineTimeoutInHours.get());
      }

      VeniceControllerClusterConfig clusterConfig = getHelixVeniceClusterResources(clusterName).getConfig();
      if (newHybridStoreConfig.isPresent()) {
        // To fix the final variable problem in the lambda expression
        final HybridStoreConfig finalHybridConfig = newHybridStoreConfig.get();
        storeMetadataUpdate(clusterName, storeName, store -> {
          if (!isHybrid(finalHybridConfig)) {
            /**
             * If all the hybrid config values are negative, it indicates that the store is being set back to batch-only store.
             * We cannot remove the RT topic immediately because with NR and AA, existing current version is
             * still consuming the RT topic.
             */
            store.setHybridStoreConfig(null);
            store.setIncrementalPushEnabled(false);
            // Enable/disable native replication for batch-only stores if the cluster level config for new batch
            // stores is on
            store.setNativeReplicationSourceFabric(
                clusterConfig.getNativeReplicationSourceFabricAsDefaultForBatchOnly());
            store.setActiveActiveReplicationEnabled(false);
          } else {
            // Batch-only store is being converted to hybrid store.
            if (!store.isHybrid()) {
              /*
               * Enable/disable native replication for hybrid stores if the cluster level config
               * for new hybrid stores is on
               */
              store
                  .setNativeReplicationSourceFabric(clusterConfig.getNativeReplicationSourceFabricAsDefaultForHybrid());
              /*
               * Enable/disable active-active replication for user hybrid stores if the cluster level config
               * for new hybrid stores is on
               */
              store.setActiveActiveReplicationEnabled(
                  store.isActiveActiveReplicationEnabled()
                      || (clusterConfig.isActiveActiveReplicationEnabledAsDefaultForHybrid()
                          && !store.isSystemStore()));
            }
            store.setHybridStoreConfig(finalHybridConfig);
            PubSubTopic rtTopic = pubSubTopicRepository.getTopic(Utils.getRealTimeTopicName(store));
            if (getTopicManager().containsTopicAndAllPartitionsAreOnline(rtTopic)) {
              // RT already exists, ensure the retention is correct
              getTopicManager()
                  .updateTopicRetention(rtTopic, StoreUtils.getExpectedRetentionTimeInMs(store, finalHybridConfig));
            }
          }
          return store;
        });
      }

      if (accessControlled.isPresent()) {
        setAccessControl(clusterName, storeName, accessControlled.get());
      }

      if (compressionStrategy.isPresent()) {
        setStoreCompressionStrategy(clusterName, storeName, compressionStrategy.get());
      }

      if (clientDecompressionEnabled.isPresent()) {
        setClientDecompressionEnabled(clusterName, storeName, clientDecompressionEnabled.get());
      }

      if (chunkingEnabled.isPresent()) {
        setChunkingEnabled(clusterName, storeName, chunkingEnabled.get());
      }

      if (rmdChunkingEnabled.isPresent()) {
        setRmdChunkingEnabled(clusterName, storeName, rmdChunkingEnabled.get());
      }

      if (batchGetLimit.isPresent()) {
        setBatchGetLimit(clusterName, storeName, batchGetLimit.get());
      }

      if (numVersionsToPreserve.isPresent()) {
        setNumVersionsToPreserve(clusterName, storeName, numVersionsToPreserve.get());
      }

      if (incrementalPushEnabled.isPresent()) {
        if (incrementalPushEnabled.get()) {
          enableHybridModeOrUpdateSettings(clusterName, storeName);
        }
        setIncrementalPushEnabled(clusterName, storeName, incrementalPushEnabled.get());
      }

      if (separateRealTimeTopicEnabled.isPresent()) {
        setSeparateRealTimeTopicEnabled(clusterName, storeName, separateRealTimeTopicEnabled.get());
      }

      if (replicationFactor.isPresent()) {
        setReplicationFactor(clusterName, storeName, replicationFactor.get());
      }

      if (storeMigration.isPresent()) {
        setStoreMigration(clusterName, storeName, storeMigration.get());
      }

      if (migrationDuplicateStore.isPresent()) {
        setMigrationDuplicateStore(clusterName, storeName, migrationDuplicateStore.get());
      }

      if (writeComputationEnabled.isPresent()) {
        setWriteComputationEnabled(clusterName, storeName, writeComputationEnabled.get());
      }

      if (replicationMetadataVersionID.isPresent()) {
        setReplicationMetadataVersionID(clusterName, storeName, replicationMetadataVersionID.get());
      }

      if (readComputationEnabled.isPresent()) {
        setReadComputationEnabled(clusterName, storeName, readComputationEnabled.get());
      }

      if (nativeReplicationEnabled.isPresent()) {
        setNativeReplicationEnabled(clusterName, storeName, nativeReplicationEnabled.get());
      }

      if (activeActiveReplicationEnabled.isPresent()) {
        setActiveActiveReplicationEnabled(clusterName, storeName, activeActiveReplicationEnabled.get());
      }

      if (pushStreamSourceAddress.isPresent()) {
        setPushStreamSourceAddress(clusterName, storeName, pushStreamSourceAddress.get());
      }

      if (backupStrategy.isPresent()) {
        setBackupStrategy(clusterName, storeName, backupStrategy.get());
      }

      autoSchemaRegisterPushJobEnabled
          .ifPresent(value -> setAutoSchemaRegisterPushJobEnabled(clusterName, storeName, value));
      hybridStoreDiskQuotaEnabled.ifPresent(value -> setHybridStoreDiskQuotaEnabled(clusterName, storeName, value));
      if (regularVersionETLEnabled.isPresent() || futureVersionETLEnabled.isPresent()
          || etledUserProxyAccount.isPresent()) {
        ETLStoreConfig etlStoreConfig = new ETLStoreConfigImpl(
            etledUserProxyAccount.orElseGet(() -> originalStore.getEtlStoreConfig().getEtledUserProxyAccount()),
            regularVersionETLEnabled.orElseGet(() -> originalStore.getEtlStoreConfig().isRegularVersionETLEnabled()),
            futureVersionETLEnabled.orElseGet(() -> originalStore.getEtlStoreConfig().isFutureVersionETLEnabled()));
        storeMetadataUpdate(clusterName, storeName, store -> {
          store.setEtlStoreConfig(etlStoreConfig);
          return store;
        });
      }
      if (backupVersionRetentionMs.isPresent()) {
        setBackupVersionRetentionMs(clusterName, storeName, backupVersionRetentionMs.get());
      }

      if (nativeReplicationSourceFabric.isPresent()) {
        setNativeReplicationSourceFabric(clusterName, storeName, nativeReplicationSourceFabric.get());
      }

      if (params.disableMetaStore().isPresent() && params.disableMetaStore().get()) {
        disableMetaSystemStore(clusterName, storeName);
      }

      if (params.disableDavinciPushStatusStore().isPresent() && params.disableDavinciPushStatusStore().get()) {
        disableDavinciPushStatusStore(clusterName, storeName);
      }

      if (personaName.isPresent()) {
        StoragePersonaRepository repository = getHelixVeniceClusterResources(clusterName).getStoragePersonaRepository();
        repository.addStoresToPersona(personaName.get(), Arrays.asList(storeName));
      }

      if (storeViews.isPresent()) {
        addStoreViews(clusterName, storeName, storeViews.get());
      }

      if (latestSupersetSchemaId.isPresent()) {
        setLatestSupersetSchemaId(clusterName, storeName, latestSupersetSchemaId.get());
      }

      if (minCompactionLagSeconds.isPresent()) {
        storeMetadataUpdate(clusterName, storeName, store -> {
          store.setMinCompactionLagSeconds(minCompactionLagSeconds.get());
          return store;
        });
      }
      if (maxCompactionLagSeconds.isPresent()) {
        storeMetadataUpdate(clusterName, storeName, store -> {
          store.setMaxCompactionLagSeconds(maxCompactionLagSeconds.get());
          return store;
        });
      }

      maxRecordSizeBytes.ifPresent(aInt -> storeMetadataUpdate(clusterName, storeName, store -> {
        store.setMaxRecordSizeBytes(aInt);
        return store;
      }));

      maxNearlineRecordSizeBytes.ifPresent(aInt -> storeMetadataUpdate(clusterName, storeName, store -> {
        store.setMaxNearlineRecordSizeBytes(aInt);
        return store;
      }));

      unusedSchemaDeletionEnabled.ifPresent(aBoolean -> storeMetadataUpdate(clusterName, storeName, store -> {
        store.setUnusedSchemaDeletionEnabled(aBoolean);
        return store;
      }));

      storageNodeReadQuotaEnabled
          .ifPresent(aBoolean -> setStorageNodeReadQuotaEnabled(clusterName, storeName, aBoolean));

      blobTransferEnabled.ifPresent(aBoolean -> storeMetadataUpdate(clusterName, storeName, store -> {
        store.setBlobTransferEnabled(aBoolean);
        return store;
      }));

      nearlineProducerCompressionEnabled.ifPresent(aBoolean -> storeMetadataUpdate(clusterName, storeName, store -> {
        store.setNearlineProducerCompressionEnabled(aBoolean);
        return store;
      }));

      nearlineProducerCountPerWriter.ifPresent(aInt -> storeMetadataUpdate(clusterName, storeName, store -> {
        store.setNearlineProducerCountPerWriter(aInt);
        return store;
      }));

      targetSwapRegion.ifPresent(aString -> storeMetadataUpdate(clusterName, storeName, store -> {
        store.setTargetSwapRegion((aString));
        return store;
      }));

      targetSwapRegionWaitTime.ifPresent(aInt -> storeMetadataUpdate(clusterName, storeName, store -> {
        store.setTargetSwapRegionWaitTime(aInt);
        return store;
      }));

      isDavinciHeartbeatReported.ifPresent(aBool -> storeMetadataUpdate(clusterName, storeName, store -> {
        store.setIsDavinciHeartbeatReported(aBool);
        return store;
      }));

      globalRtDivEnabled.ifPresent(aBool -> storeMetadataUpdate(clusterName, storeName, store -> {
        store.setGlobalRtDivEnabled(aBool);
        return store;
      }));

      LOGGER.info("Finished updating store: {} in cluster: {}", storeName, clusterName);
    } catch (VeniceException e) {
      LOGGER.error(
          "Caught exception when updating store: {} in cluster: {}. Will attempt to rollback changes.",
          storeName,
          clusterName,
          e);
      // rollback to original store
      storeMetadataUpdate(clusterName, storeName, store -> originalStore);
      PubSubTopic rtTopic = pubSubTopicRepository.getTopic(Utils.getRealTimeTopicName(originalStore));
      if (originalStore.isHybrid() && newHybridStoreConfig.isPresent()
          && getTopicManager().containsTopicAndAllPartitionsAreOnline(rtTopic)) {
        // Ensure the topic retention is rolled back too
        getTopicManager().updateTopicRetention(
            rtTopic,
            StoreUtils.getExpectedRetentionTimeInMs(originalStore, originalStore.getHybridStoreConfig()));
      }
      LOGGER.info(
          "Successfully rolled back changes to store: {} in cluster: {}. Will now throw the original exception: {}.",
          storeName,
          clusterName,
          e.getClass().getSimpleName());
      throw e;
    }
  }

  /**
   * Enabling hybrid mode for incremental push store is moved into
   * {@link VeniceParentHelixAdmin#updateStore(String, String, UpdateStoreQueryParams)}
   * TODO: Remove the method and its usage after the deployment of parent controller updateStore change.
   */
  private void enableHybridModeOrUpdateSettings(String clusterName, String storeName) {
    storeMetadataUpdate(clusterName, storeName, store -> {
      HybridStoreConfig hybridStoreConfig = store.getHybridStoreConfig();
      if (hybridStoreConfig == null) {
        store.setHybridStoreConfig(
            new HybridStoreConfigImpl(
                DEFAULT_REWIND_TIME_IN_SECONDS,
                DEFAULT_HYBRID_OFFSET_LAG_THRESHOLD,
                DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
                null));
      }
      return store;
    });
  }

  /**
   * This method is invoked in parent controllers for store migration.
   */
  public void replicateUpdateStore(String clusterName, String storeName, UpdateStoreQueryParams params) {
    try {
      StoreConfig storeConfig = storeConfigRepo.getStoreConfigOrThrow(storeName);
      String destinationCluster = storeConfig.getMigrationDestCluster();
      String sourceCluster = storeConfig.getMigrationSrcCluster();
      if (storeConfig.getCluster().equals(destinationCluster)) {
        // Migration has completed in this colo but the overall migration is still in progress.
        if (clusterName.equals(destinationCluster)) {
          // Mirror new updates back to the source cluster in case we abort migration after completion.
          ControllerClient sourceClusterControllerClient =
              new ControllerClient(sourceCluster, getLeaderController(sourceCluster).getUrl(sslEnabled), sslFactory);
          ControllerResponse response = sourceClusterControllerClient.updateStore(storeName, params);
          if (response.isError()) {
            LOGGER.warn(
                "Replicate new update endpoint call to source cluster: {} failed for store: {}. Error: {}",
                sourceCluster,
                storeName,
                response.getError());
          }
        }
      } else if (clusterName.equals(sourceCluster)) {
        ControllerClient destClusterControllerClient = new ControllerClient(
            destinationCluster,
            getLeaderController(destinationCluster).getUrl(sslEnabled),
            sslFactory);
        ControllerResponse response = destClusterControllerClient.updateStore(storeName, params);
        if (response.isError()) {
          LOGGER.warn(
              "Replicate update store endpoint call to destination cluster: {} failed for store: {}. Error: {}",
              destinationCluster,
              storeName,
              response.getError());
        }
      }
    } catch (Exception e) {
      LOGGER
          .warn("Exception thrown when replicating new update for store: {} as part of store migration", storeName, e);
    }
  }

  /**
   * Used by both the {@link VeniceHelixAdmin} and the {@link VeniceParentHelixAdmin}
   *
   * @param oldStore Existing Store that is the source for updates. This object will not be modified by this method.
   * @param hybridRewindSeconds Optional is present if the returned object should include a new rewind time
   * @param hybridOffsetLagThreshold Optional is present if the returned object should include a new offset lag threshold
   * @return null if oldStore has no hybrid configs and optionals are not present,
   *   otherwise a fully specified {@link HybridStoreConfig}
   */
  protected static HybridStoreConfig mergeNewSettingsIntoOldHybridStoreConfig(
      Store oldStore,
      Optional<Long> hybridRewindSeconds,
      Optional<Long> hybridOffsetLagThreshold,
      Optional<Long> hybridTimeLagThreshold,
      Optional<DataReplicationPolicy> hybridDataReplicationPolicy,
      Optional<BufferReplayPolicy> bufferReplayPolicy,
      Optional<String> realTimeTopicName) {
    if (!hybridRewindSeconds.isPresent() && !hybridOffsetLagThreshold.isPresent() && !oldStore.isHybrid()) {
      return null; // For the nullable union in the avro record
    }
    HybridStoreConfig mergedHybridStoreConfig;
    if (oldStore.isHybrid()) { // for an existing hybrid store, just replace any specified values
      HybridStoreConfig oldHybridConfig = oldStore.getHybridStoreConfig().clone();
      mergedHybridStoreConfig = new HybridStoreConfigImpl(
          hybridRewindSeconds.isPresent() ? hybridRewindSeconds.get() : oldHybridConfig.getRewindTimeInSeconds(),
          hybridOffsetLagThreshold.isPresent()
              ? hybridOffsetLagThreshold.get()
              : oldHybridConfig.getOffsetLagThresholdToGoOnline(),
          hybridTimeLagThreshold.isPresent()
              ? hybridTimeLagThreshold.get()
              : oldHybridConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds(),
          hybridDataReplicationPolicy.isPresent()
              ? hybridDataReplicationPolicy.get()
              : oldHybridConfig.getDataReplicationPolicy(),
          bufferReplayPolicy.isPresent() ? bufferReplayPolicy.get() : oldHybridConfig.getBufferReplayPolicy(),
          realTimeTopicName.orElseGet(oldHybridConfig::getRealTimeTopicName));
    } else {
      // switching a non-hybrid store to hybrid; must specify:
      // 1. rewind time
      // 2. either offset lag threshold or time lag threshold, or both
      if (!(hybridRewindSeconds.isPresent()
          && (hybridOffsetLagThreshold.isPresent() || hybridTimeLagThreshold.isPresent()))) {
        throw new VeniceException(
            oldStore.getName() + " was not a hybrid store.  In order to make it a hybrid store both "
                + " rewind time in seconds and offset or time lag threshold must be specified");
      }

      String newRealTimeTopicName = oldStore.getLargestUsedRTVersionNumber() > DEFAULT_RT_VERSION_NUMBER
          && Utils.isRTVersioningApplicable(oldStore.getName())
              ? Utils.composeRealTimeTopic(oldStore.getName(), oldStore.getLargestUsedRTVersionNumber())
              : DEFAULT_REAL_TIME_TOPIC_NAME;

      mergedHybridStoreConfig = new HybridStoreConfigImpl(
          hybridRewindSeconds.get(),
          // If not specified, offset/time lag threshold will be -1 and will not be used to determine whether
          // a partition is ready to serve
          hybridOffsetLagThreshold.orElse(DEFAULT_HYBRID_OFFSET_LAG_THRESHOLD),
          hybridTimeLagThreshold.orElse(DEFAULT_HYBRID_TIME_LAG_THRESHOLD),
          hybridDataReplicationPolicy.orElse(DataReplicationPolicy.NON_AGGREGATE),
          bufferReplayPolicy.orElse(BufferReplayPolicy.REWIND_FROM_EOP),
          realTimeTopicName.orElse(newRealTimeTopicName));
    }
    if (mergedHybridStoreConfig.getRewindTimeInSeconds() > 0
        && mergedHybridStoreConfig.getOffsetLagThresholdToGoOnline() < 0
        && mergedHybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds() < 0) {
      throw new VeniceException(
          "Both offset lag threshold and time lag threshold are negative when setting hybrid" + " configs for store "
              + oldStore.getName());
    }
    return mergedHybridStoreConfig;
  }

  static PartitionerConfig mergeNewSettingsIntoOldPartitionerConfig(
      Store oldStore,
      Optional<String> partitionerClass,
      Optional<Map<String, String>> partitionerParams,
      Optional<Integer> amplificationFactor) {
    PartitionerConfig originalPartitionerConfig;
    if (oldStore.getPartitionerConfig() == null) {
      originalPartitionerConfig = new PartitionerConfigImpl();
    } else {
      originalPartitionerConfig = oldStore.getPartitionerConfig();
    }
    return new PartitionerConfigImpl(
        partitionerClass.orElseGet(originalPartitionerConfig::getPartitionerClass),
        partitionerParams.orElseGet(originalPartitionerConfig::getPartitionerParams),
        amplificationFactor.orElseGet(originalPartitionerConfig::getAmplificationFactor));
  }

  static Map<String, StoreViewConfigRecord> mergeNewViewConfigsIntoOldConfigs(
      Store oldStore,
      Map<String, String> viewParameters) throws VeniceException {
    // Merge the existing configs with the incoming configs. The new configs will override existing ones which share the
    // same key.
    Map<String, ViewConfig> oldViewConfigMap = oldStore.getViewConfigs();
    if (oldViewConfigMap == null) {
      oldViewConfigMap = new HashMap<>();
    }
    Map<String, StoreViewConfigRecord> mergedConfigs =
        StoreViewUtils.convertViewConfigMapToStoreViewRecordMap(oldViewConfigMap);
    mergedConfigs.putAll(StoreViewUtils.convertStringMapViewToStoreViewConfigRecordMap(viewParameters));
    return mergedConfigs;
  }

  static Map<String, StoreViewConfigRecord> addNewViewConfigsIntoOldConfigs(
      Store oldStore,
      String viewClass,
      ViewConfig viewConfig) throws VeniceException {
    // Add new view config into the existing config map. The new configs will override existing ones which share the
    // same key.
    Map<String, ViewConfig> oldViewConfigMap = oldStore.getViewConfigs();
    if (oldViewConfigMap == null) {
      oldViewConfigMap = new HashMap<>();
    }
    Map<String, StoreViewConfigRecord> mergedConfigs =
        StoreViewUtils.convertViewConfigMapToStoreViewRecordMap(oldViewConfigMap);

    StoreViewConfigRecord newStoreViewConfigRecord =
        StoreViewUtils.convertViewConfigToStoreViewConfigRecord(viewConfig);
    mergedConfigs.put(viewClass, newStoreViewConfigRecord);
    return mergedConfigs;
  }

  static Map<String, StoreViewConfigRecord> removeViewConfigFromStoreViewConfigMap(Store oldStore, String viewClass)
      throws VeniceException {
    Map<String, ViewConfig> oldViewConfigMap = oldStore.getViewConfigs();
    if (oldViewConfigMap == null) {
      oldViewConfigMap = new HashMap<>();
    }
    Map<String, StoreViewConfigRecord> mergedConfigs =
        StoreViewUtils.convertViewConfigMapToStoreViewRecordMap(oldViewConfigMap);
    mergedConfigs.remove(viewClass);
    return mergedConfigs;
  }

  /**
   * Update the store metadata by applying provided operation.
   * @param clusterName name of the cluster.
   * @param storeName name of the to be updated store.
   * @param operation the defined operation that update the store.
   */
  public void storeMetadataUpdate(String clusterName, String storeName, StoreMetadataOperation operation) {
    checkPreConditionForUpdateStore(clusterName, storeName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
      Store store = repository.getStore(storeName);
      Store updatedStore = operation.update(store);
      repository.updateStore(updatedStore);
    } catch (Exception e) {
      LOGGER.error("Failed to execute StoreMetadataOperation for store: {} in cluster: {}", storeName, clusterName, e);
      throw e;
    }
  }

  private void checkPreConditionForUpdateStore(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    if (repository.getStore(storeName) == null) {
      throwStoreDoesNotExist(clusterName, storeName);
    }
  }

  /**
   * @return the configuration value for {@linkplain ConfigKeys#STORAGE_ENGINE_OVERHEAD_RATIO}
   */
  @Override
  public double getStorageEngineOverheadRatio(String clusterName) {
    return multiClusterConfigs.getControllerConfig(clusterName).getStorageEngineOverheadRatio();
  }

  private void waitUntilNodesAreAssignedForResource(
      String clusterName,
      String topic,
      OfflinePushStrategy strategy,
      long maxWaitTimeMs,
      int replicationFactor) {

    HelixVeniceClusterResources clusterResources = getHelixVeniceClusterResources(clusterName);
    PushMonitor pushMonitor = clusterResources.getPushMonitor();
    RoutingDataRepository routingDataRepository = clusterResources.getRoutingDataRepository();
    PushStatusDecider statusDecider = strategy.getPushStatusDecider();

    Optional<String> notReadyReason = Optional.of("unknown");
    long startTime = System.currentTimeMillis();
    long logTime = 0;
    for (long elapsedTime = 0; elapsedTime <= maxWaitTimeMs; elapsedTime = System.currentTimeMillis() - startTime) {
      try (AutoCloseableLock ignore = clusterResources.getClusterLockManager().createClusterReadLock()) {
        if (!isLeaderControllerFor(clusterName)) {
          LOGGER.warn(
              "No longer leader controller for cluster {}; will stop waiting for the resource assignment for {}",
              clusterName,
              topic);
          return;
        }
        if (pushMonitor.getOfflinePushOrThrow(topic).getCurrentStatus().isError()) {
          throw new VeniceException("Push " + topic + " has already failed.");
        }

        ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
        notReadyReason =
            statusDecider.hasEnoughNodesToStartPush(topic, replicationFactor, resourceAssignment, notReadyReason);
        if (!notReadyReason.isPresent()) {
          LOGGER.info("After waiting for {}ms, resource allocation is completed for: {}.", elapsedTime, topic);
          pushMonitor
              .refreshAndUpdatePushStatus(topic, ExecutionStatus.STARTED, Optional.of(HELIX_ASSIGNMENT_COMPLETED));
          pushMonitor.recordPushPreparationDuration(topic, TimeUnit.MILLISECONDS.toSeconds(elapsedTime));
          return;
        }
        if ((elapsedTime - logTime) > HELIX_RESOURCE_ASSIGNMENT_LOG_INTERVAL_MS) {
          LOGGER.info(
              "After waiting for {}ms, resource assignment for: {} is still not complete, strategy: {}, "
                  + "replicationFactor: {}, reason: {}",
              elapsedTime,
              topic,
              strategy,
              replicationFactor,
              notReadyReason.get());
          logTime = elapsedTime;
        }
      }
      Utils.sleep(HELIX_RESOURCE_ASSIGNMENT_RETRY_INTERVAL_MS);
    }

    // Time out, after waiting maxWaitTimeMs, there are not enough nodes assigned.
    pushMonitor.recordPushPreparationDuration(topic, TimeUnit.MILLISECONDS.toSeconds(maxWaitTimeMs));
    throw new VeniceException(
        "After waiting for " + maxWaitTimeMs + "ms, resource assignment for: " + topic + " timed out, strategy="
            + strategy + ", replicationFactor=" + replicationFactor + ", reason=" + notReadyReason.get());
  }

  @Override
  public boolean containsHelixResource(String clusterName, String kafkaTopic) {
    checkControllerLeadershipFor(clusterName);
    return helixAdminClient.containsResource(clusterName, kafkaTopic);
  }

  @Override
  public void deleteHelixResource(String clusterName, String kafkaTopic) {
    checkControllerLeadershipFor(clusterName);

    getHelixAdminClient().dropResource(clusterName, kafkaTopic);
    LOGGER.info("Successfully dropped the resource: {} for cluster: {}", kafkaTopic, clusterName);

    enableDisabledPartition(clusterName, kafkaTopic, false);
  }

  public void enableDisabledPartition(String clusterName, String kafkaTopic, boolean enableAll) {
    Set<Instance> instances = getLiveInstanceMonitor(clusterName).getAllLiveInstances();
    Map<String, List<String>> disabledPartitions;
    for (Instance instance: instances) {
      try {
        disabledPartitions = getHelixAdminClient().getDisabledPartitionsMap(clusterName, instance.getNodeId());
      } catch (HelixException helixException) {
        String msg = "Failed to get disabled partition map in cluster " + clusterName + " for host " + instance;
        if (!EXCEPTION_FILTER.isRedundantException(msg)) {
          LOGGER.warn(msg, helixException);
        }
        continue;
      }
      for (Map.Entry<String, List<String>> entry: disabledPartitions.entrySet()) {
        if (enableAll || entry.getKey().equals(kafkaTopic)) {
          // clean up disabled partition map, so that it does not grow indefinitely with dropped resources
          getDisabledPartitionStats(clusterName).recordClearDisabledPartition(entry.getValue().size());
          getHelixAdminClient()
              .enablePartition(true, clusterName, instance.getNodeId(), entry.getKey(), entry.getValue());
          LOGGER.info("Cleaning up disabled replica of resource {}, partitions {}", entry.getKey(), entry.getValue());
        }
      }
    }
  }

  /**
   * @return the key schema for the specified store.
   */
  @Override
  public SchemaEntry getKeySchema(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    return schemaRepo.getKeySchema(storeName);
  }

  /**
   * @return the value schema for the specified store.
   */
  @Override
  public Collection<SchemaEntry> getValueSchemas(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    return schemaRepo.getValueSchemas(storeName);
  }

  /**
   * @return the derived schema for the specified store.
   */
  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    return schemaRepo.getDerivedSchemas(storeName);
  }

  /**
   * @return the schema id for the specified store and value schema.
   */
  @Override
  public int getValueSchemaId(String clusterName, String storeName, String valueSchemaStr) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    int schemaId = schemaRepo.getValueSchemaId(storeName, valueSchemaStr);
    // validate the schema as VPJ uses this method to fetch the value schema. Fail loudly if the schema user trying
    // to push is bad.
    if (schemaId != SchemaData.INVALID_VALUE_SCHEMA_ID) {
      AvroSchemaUtils.validateAvroSchemaStr(valueSchemaStr);
    }
    return schemaId;
  }

  /**
   * @return the derived schema id for the specified store and derived schema.
   */
  @Override
  public GeneratedSchemaID getDerivedSchemaId(String clusterName, String storeName, String schemaStr) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    GeneratedSchemaID schemaID = schemaRepo.getDerivedSchemaId(storeName, schemaStr);
    // validate the schema as VPJ uses this method to fetch the value schema. Fail loudly if the schema user trying
    // to push is bad.
    if (schemaID.isValid()) {
      AvroSchemaUtils.validateAvroSchemaStr(schemaStr);
    }
    return schemaID;
  }

  /**
   * @return the derived schema for the specified store and id.
   */
  @Override
  public SchemaEntry getValueSchema(String clusterName, String storeName, int id) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    return schemaRepo.getValueSchema(storeName, id);
  }

  private void validateValueSchemaUsingRandomGenerator(String schemaStr, String clusterName, String storeName) {
    VeniceControllerClusterConfig config = getHelixVeniceClusterResources(clusterName).getConfig();
    if (!config.isControllerSchemaValidationEnabled()) {
      return;
    }

    ReadWriteSchemaRepository schemaRepository = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    Collection<SchemaEntry> schemaEntries = schemaRepository.getValueSchemas(storeName);
    AvroSerializer serializer;
    Schema existingSchema = null;

    Schema newSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr);
    RandomRecordGenerator recordGenerator = new RandomRecordGenerator();
    RecordGenerationConfig genConfig = RecordGenerationConfig.newConfig().withAvoidNulls(true);

    for (int i = 0; i < RECORD_COUNT; i++) {
      // check if new records written with new schema can be read using existing older schema
      // Object record =
      Object record = recordGenerator.randomGeneric(newSchema, genConfig);
      serializer = new AvroSerializer<>(newSchema);
      byte[] bytes = serializer.serialize(record);
      for (SchemaEntry schemaEntry: schemaEntries) {
        try {
          existingSchema = schemaEntry.getSchema();
          if (!isValidAvroSchema(existingSchema)) {
            LOGGER.warn("Skip validating ill-formed schema for store: {}", storeName);
            continue;
          }
          RecordDeserializer<Object> deserializer =
              SerializerDeserializerFactory.getAvroGenericDeserializer(newSchema, existingSchema);
          deserializer.deserialize(bytes);
        } catch (Exception e) {
          if (e instanceof AvroIncompatibleSchemaException) {
            LOGGER.warn("Found incompatible avro schema with bad union branch for store: {}", storeName, e);
            continue;
          }
          throw new InvalidVeniceSchemaException(
              "Error while trying to add new schema: " + schemaStr + "  for store " + storeName
                  + " as it is incompatible with existing schema: " + existingSchema,
              e);
        }
      }
    }

    // check if records written with older schema can be read using the new schema
    for (int i = 0; i < RECORD_COUNT; i++) {
      for (SchemaEntry schemaEntry: schemaEntries) {
        try {
          Object record = recordGenerator.randomGeneric(schemaEntry.getSchema(), genConfig);
          serializer = new AvroSerializer(schemaEntry.getSchema());
          byte[] bytes = serializer.serialize(record);
          existingSchema = schemaEntry.getSchema();
          if (!isValidAvroSchema(existingSchema)) {
            LOGGER.warn("Skip validating ill-formed schema for store: {}", storeName);
            continue;
          }
          RecordDeserializer<Object> deserializer =
              SerializerDeserializerFactory.getAvroGenericDeserializer(existingSchema, newSchema);
          deserializer.deserialize(bytes);
        } catch (Exception e) {
          if (e instanceof AvroIncompatibleSchemaException) {
            LOGGER.warn("Found incompatible avro schema with bad union branch for store: {}", storeName, e);
            continue;
          }
          throw new InvalidVeniceSchemaException(
              "Error while trying to add new schema: " + schemaStr + "  for store " + storeName
                  + " as it is incompatible with existing schema: " + existingSchema,
              e);
        }
      }
    }
  }

  /**
   * @see #addValueSchema(String, String, String, int, DirectionalSchemaCompatibilityType)
   */
  @Override
  public SchemaEntry addValueSchema(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepository = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    schemaRepository.addValueSchema(storeName, valueSchemaStr, expectedCompatibilityType);
    return new SchemaEntry(schemaRepository.getValueSchemaId(storeName, valueSchemaStr), valueSchemaStr);
  }

  /**
   * Add a new value schema for the given store with all specified properties and return a new SchemaEntry object
   * containing the schema and its id.
   * @return an <code>SchemaEntry</code> object composed of a schema and its corresponding id.
   */
  @Override
  public SchemaEntry addValueSchema(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      int schemaId,
      DirectionalSchemaCompatibilityType compatibilityType) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepository = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    int newValueSchemaId =
        schemaRepository.preCheckValueSchemaAndGetNextAvailableId(storeName, valueSchemaStr, compatibilityType);
    if (newValueSchemaId != SchemaData.DUPLICATE_VALUE_SCHEMA_CODE && newValueSchemaId != schemaId) {
      throw new VeniceException(
          "Inconsistent value schema id between the caller and the local schema repository."
              + " Expected new schema id of " + schemaId + " but the next available id from the local repository is "
              + newValueSchemaId + " for store " + storeName + " in cluster " + clusterName + " Schema: "
              + valueSchemaStr);
    }
    return schemaRepository.addValueSchema(storeName, valueSchemaStr, newValueSchemaId);
  }

  /**
   * Add a new derived schema for the given store with all specified properties and return a new
   * <code>DerivedSchemaEntry</code> object containing the schema and its id.
   * @return an <code>DerivedSchemaEntry</code> object composed of specified properties.
   */
  @Override
  public DerivedSchemaEntry addDerivedSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      String derivedSchemaStr) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepository = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    schemaRepository.addDerivedSchema(storeName, derivedSchemaStr, valueSchemaId);

    return new DerivedSchemaEntry(
        valueSchemaId,
        schemaRepository.getDerivedSchemaId(storeName, derivedSchemaStr).getGeneratedSchemaVersion(),
        derivedSchemaStr);
  }

  /**
   * Add a new derived schema for the given store with all specified properties.
   * @return an <code>DerivedSchemaEntry</code> object composed of specified properties.
   */
  @Override
  public DerivedSchemaEntry addDerivedSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      int derivedSchemaId,
      String derivedSchemaStr) {
    checkControllerLeadershipFor(clusterName);
    return getHelixVeniceClusterResources(clusterName).getSchemaRepository()
        .addDerivedSchema(storeName, derivedSchemaStr, valueSchemaId, derivedSchemaId);
  }

  /**
   * @see Admin#removeDerivedSchema(String, String, int, int)
   */
  @Override
  public DerivedSchemaEntry removeDerivedSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      int derivedSchemaId) {
    checkControllerLeadershipFor(clusterName);
    return getHelixVeniceClusterResources(clusterName).getSchemaRepository()
        .removeDerivedSchema(storeName, valueSchemaId, derivedSchemaId);
  }

  /**
   * Add a new superset schema for the given store with all specified properties.
   * <p>
   *   Generate the superset schema off the current schema and latest superset schema (if any, if not pick the latest value schema) existing in the store.
   *   If the newly generated superset schema is unique add it to the store and update latestSuperSetValueSchemaId of the store.
   */
  @Override
  public SchemaEntry addSupersetSchema(
      String clusterName,
      String storeName,
      String valueSchema,
      int valueSchemaId,
      String supersetSchemaStr,
      int supersetSchemaId) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepository = getHelixVeniceClusterResources(clusterName).getSchemaRepository();

    final SchemaEntry existingSupersetSchemaEntry = schemaRepository.getValueSchema(storeName, supersetSchemaId);
    if (existingSupersetSchemaEntry == null) {
      // If the new superset schema does not exist in the schema repo, add it
      LOGGER.info("Adding superset schema: {} for store: {}", supersetSchemaStr, storeName);
      schemaRepository.addValueSchema(storeName, supersetSchemaStr, supersetSchemaId);

    } else {
      final Schema newSupersetSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(supersetSchemaStr);
      if (!AvroSchemaUtils.compareSchemaIgnoreFieldOrder(existingSupersetSchemaEntry.getSchema(), newSupersetSchema)) {
        throw new VeniceException(
            "Existing schema with id " + existingSupersetSchemaEntry.getId() + " does not match with new schema "
                + supersetSchemaStr);
      }
    }

    // add the value schema
    return schemaRepository.addValueSchema(storeName, valueSchema, valueSchemaId);
  }

  int getValueSchemaIdIgnoreFieldOrder(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      Comparator<Schema> schemaComparator) {
    checkControllerLeadershipFor(clusterName);
    SchemaEntry valueSchemaEntry = new SchemaEntry(SchemaData.UNKNOWN_SCHEMA_ID, valueSchemaStr);

    for (SchemaEntry schemaEntry: getValueSchemas(clusterName, storeName)) {
      if (schemaComparator.compare(schemaEntry.getSchema(), valueSchemaEntry.getSchema()) == 0) {
        return schemaEntry.getId();
      }
    }
    return SchemaData.INVALID_VALUE_SCHEMA_ID;

  }

  int checkPreConditionForAddValueSchemaAndGetNewSchemaId(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    AvroSchemaUtils.validateAvroSchemaStr(valueSchemaStr);
    AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr);
    validateValueSchemaUsingRandomGenerator(valueSchemaStr, clusterName, storeName);
    checkControllerLeadershipFor(clusterName);
    return getHelixVeniceClusterResources(clusterName).getSchemaRepository()
        .preCheckValueSchemaAndGetNextAvailableId(storeName, valueSchemaStr, expectedCompatibilityType);
  }

  int checkPreConditionForAddDerivedSchemaAndGetNewSchemaId(
      String clusterName,
      String storeName,
      int valueSchemaId,
      String derivedSchemaStr) {
    checkControllerLeadershipFor(clusterName);
    return getHelixVeniceClusterResources(clusterName).getSchemaRepository()
        .preCheckDerivedSchemaAndGetNextAvailableId(storeName, valueSchemaId, derivedSchemaStr);
  }

  /**
   * @return a collection of <code>ReplicationMetadataSchemaEntry</code> object for the given store and cluster.
   */
  @Override
  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    return schemaRepo.getReplicationMetadataSchemas(storeName);
  }

  boolean checkIfValueSchemaAlreadyHasRmdSchema(
      String clusterName,
      String storeName,
      final int valueSchemaID,
      final int replicationMetadataVersionId) {
    checkControllerLeadershipFor(clusterName);
    Collection<RmdSchemaEntry> schemaEntries =
        getHelixVeniceClusterResources(clusterName).getSchemaRepository().getReplicationMetadataSchemas(storeName);
    for (RmdSchemaEntry rmdSchemaEntry: schemaEntries) {
      if (rmdSchemaEntry.getValueSchemaID() == valueSchemaID
          && rmdSchemaEntry.getId() == replicationMetadataVersionId) {
        return true;
      }
    }
    return false;
  }

  boolean checkIfMetadataSchemaAlreadyPresent(
      String clusterName,
      String storeName,
      int valueSchemaId,
      RmdSchemaEntry rmdSchemaEntry) {
    checkControllerLeadershipFor(clusterName);
    try {
      Collection<RmdSchemaEntry> schemaEntries =
          getHelixVeniceClusterResources(clusterName).getSchemaRepository().getReplicationMetadataSchemas(storeName);
      for (RmdSchemaEntry schemaEntry: schemaEntries) {
        if (schemaEntry.equals(rmdSchemaEntry)) {
          return true;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Exception in checkIfMetadataSchemaAlreadyPresent ", e);
    }
    return false;
  }

  /**
   * Create a new <code>ReplicationMetadataSchemaEntry</code> object with the given properties and add it into schema
   * repository if no duplication.
   * @return <code>ReplicationMetadataSchemaEntry</code> object reference.
   */
  @Override
  public RmdSchemaEntry addReplicationMetadataSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId,
      String replicationMetadataSchemaStr) {
    checkControllerLeadershipFor(clusterName);

    RmdSchemaEntry rmdSchemaEntry =
        new RmdSchemaEntry(valueSchemaId, replicationMetadataVersionId, replicationMetadataSchemaStr);
    if (checkIfMetadataSchemaAlreadyPresent(clusterName, storeName, valueSchemaId, rmdSchemaEntry)) {
      LOGGER.info(
          "Timestamp metadata schema Already present: for store: {} in cluster: {} metadataSchema: {} "
              + "replicationMetadataVersionId: {} valueSchemaId: {}",
          storeName,
          clusterName,
          replicationMetadataSchemaStr,
          replicationMetadataVersionId,
          valueSchemaId);
      return rmdSchemaEntry;
    }

    return getHelixVeniceClusterResources(clusterName).getSchemaRepository()
        .addReplicationMetadataSchema(
            storeName,
            valueSchemaId,
            replicationMetadataSchemaStr,
            replicationMetadataVersionId);
  }

  /**
   * Check the creation results of a user store's system store. If the system store's current version is in error state,
   * re-issue a new empty push and waits for the empty push to complete.
   */
  @Override
  public void validateAndMaybeRetrySystemStoreAutoCreation(
      String clusterName,
      String storeName,
      VeniceSystemStoreType systemStoreType) {
    if (isParent()) {
      // We will only verify system store auto creation in child regions.
      return;
    }
    checkControllerLeadershipFor(clusterName);
    ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    Store store = repository.getStoreOrThrow(storeName);
    String systemStoreName = systemStoreType.getSystemStoreName(storeName);
    if (!UserSystemStoreLifeCycleHelper.isSystemStoreTypeEnabledInUserStore(store, systemStoreType)) {
      LOGGER.warn(
          "System store type {} is not enabled in user store: {}, will skip the validation.",
          systemStoreType,
          storeName);
      return;
    }
    SystemStoreAttributes systemStoreAttributes = store.getSystemStores().get(systemStoreType.getPrefix());
    if (systemStoreAttributes.getCurrentVersion() == NON_EXISTING_VERSION) {
      int latestVersionNumber = systemStoreAttributes.getLargestUsedVersionNumber();
      List<Version> filteredVersionList = systemStoreAttributes.getVersions()
          .stream()
          .filter(v -> (v.getNumber() == latestVersionNumber))
          .collect(Collectors.toList());
      if (filteredVersionList.isEmpty() || filteredVersionList.get(0).getStatus().equals(ERROR)) {
        // Latest push failed, we should issue a local empty push to create a new version.
        String pushJobId = AUTO_META_SYSTEM_STORE_PUSH_ID_PREFIX + System.currentTimeMillis();
        LOGGER.info("Empty push is triggered to store: {} in cluster: {}", storeName, clusterName);
        Version version = incrementVersionIdempotent(
            clusterName,
            systemStoreName,
            pushJobId,
            calculateNumberOfPartitions(clusterName, systemStoreName),
            getReplicationFactor(clusterName, systemStoreName));
        int versionNumber = version.getNumber();
        writeEndOfPush(clusterName, systemStoreName, versionNumber, true);
        LOGGER.warn(
            "System store: {} pushed failed. Issuing a new empty push to create version: {} ",
            systemStoreName,
            versionNumber);
      } else {
        throw new VeniceRetriableException(
            "System store:" + systemStoreName + " push is still ongoing, will check it again. This is not an error.");
      }
    } else {
      LOGGER.info(
          "System store: {} pushed with version id: {}",
          systemStoreName,
          systemStoreAttributes.getCurrentVersion());
    }
  }

  /**
   * @return a list of storage node instance names for a given cluster.
   */
  @Override
  public List<String> getStorageNodes(String clusterName) {
    checkControllerLeadershipFor(clusterName);
    return helixAdminClient.getInstancesInCluster(clusterName);
  }

  public HelixAdminClient getHelixAdminClient() {
    return helixAdminClient;
  }

  public DisabledPartitionStats getDisabledPartitionStats(String clusterName) {
    return disabledPartitionStatMap.get(clusterName);
  }

  /**
   * @return a map containing the storage node name and its connectivity status (<code>InstanceStatus</code>).
   */
  @Override
  public Map<String, String> getStorageNodesStatus(String clusterName, boolean enableReplica) {
    checkControllerLeadershipFor(clusterName);
    List<String> instances = helixAdminClient.getInstancesInCluster(clusterName);
    RoutingDataRepository routingDataRepository =
        getHelixVeniceClusterResources(clusterName).getRoutingDataRepository();
    Map<String, String> instancesStatusesMap = new HashMap<>();
    for (String instance: instances) {
      if (routingDataRepository.isLiveInstance(instance)) {
        instancesStatusesMap.put(instance, InstanceStatus.CONNECTED.toString());
      } else {
        instancesStatusesMap.put(instance, InstanceStatus.DISCONNECTED.toString());
      }
      if (enableReplica) {
        Map<String, List<String>> disabledPartitions = helixAdminClient.getDisabledPartitionsMap(clusterName, instance);
        for (Map.Entry<String, List<String>> entry: disabledPartitions.entrySet()) {
          helixAdminClient.enablePartition(true, clusterName, instance, entry.getKey(), entry.getValue());
          getDisabledPartitionStats(clusterName).recordClearDisabledPartition(entry.getValue().size());
          LOGGER.info(
              "Enabled disabled replica of resource {}, partitions {} in cluster {}",
              entry.getKey(),
              entry.getValue(),
              clusterName);
        }
      }
    }
    return instancesStatusesMap;
  }

  Schema getSupersetOrLatestValueSchema(String clusterName, Store store) {
    ReadWriteSchemaRepository schemaRepository = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    // If already a superset schema exists, try to generate the new superset from that and the input value schema
    SchemaEntry existingSchema = schemaRepository.getSupersetOrLatestValueSchema(store.getName());
    return existingSchema == null ? null : existingSchema.getSchema();
  }

  /**
   * Remove one storage node from the given cluster.
   * <p>
   *   It removes the given helix nodeId from the allowlist in ZK and its associated resource in Helix.
   */
  @Override
  public void removeStorageNode(String clusterName, String instanceId) {
    checkControllerLeadershipFor(clusterName);
    LOGGER.info("Removing storage node: {} from cluster: {}", instanceId, clusterName);
    RoutingDataRepository routingDataRepository =
        getHelixVeniceClusterResources(clusterName).getRoutingDataRepository();
    if (routingDataRepository.isLiveInstance(instanceId)) {
      // The given storage node is still connected to cluster.
      throw new VeniceException(
          "Storage node: " + instanceId + " is still connected to cluster: " + clusterName
              + ", could not be removed from that cluster.");
    }

    // Remove storage node from both allowlist and helix instances list.
    removeInstanceFromAllowList(clusterName, instanceId);
    helixAdminClient.dropStorageInstance(clusterName, instanceId);
    LOGGER.info("Removed storage node: {} from cluster: {}", instanceId, clusterName);
  }

  /**
   * @see Admin#stop(String)
   */
  @Override
  public synchronized void stop(String clusterName) {
    // Instead of disconnecting the sub-controller for the given cluster, we should disable it for this controller,
    // then the LEADER->STANDBY and STANDBY->OFFLINE will be triggered, our handler will handle the resource collection.
    List<String> partitionNames = new ArrayList<>();
    partitionNames.add(VeniceControllerStateModel.getPartitionNameFromVeniceClusterName(clusterName));
    helixAdminClient.enablePartition(false, controllerClusterName, controllerName, clusterName, partitionNames);
  }

  /**
   * @see Admin#stopVeniceController()
   */
  @Override
  public void stopVeniceController() {
    try {
      controllerStateModelFactory.close();
      helixManager.disconnect();
      topicManagerRepository.close();
      zkClient.close();
      admin.close();
      helixAdminClient.close();
    } catch (Exception e) {
      throw new VeniceException("Can not stop controller correctly.", e);
    }
  }

  /**
   * @see Admin#getOffLinePushStatus(String, String)
   */
  @Override
  public OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic) {
    return getOffLinePushStatus(clusterName, kafkaTopic, Optional.empty(), null, null);
  }

  @Override
  public OfflinePushStatusInfo getOffLinePushStatus(
      String clusterName,
      String kafkaTopic,
      Optional<String> incrementalPushVersion,
      String region,
      String targetedRegions,
      boolean isTargetRegionPushWithDeferredSwap) {
    return getOffLinePushStatus(clusterName, kafkaTopic, incrementalPushVersion, region, targetedRegions);
  }

  /**
   * @see Admin#getOffLinePushStatus(String, String, Optional, String, String, boolean).
   */
  public OfflinePushStatusInfo getOffLinePushStatus(
      String clusterName,
      String kafkaTopic,
      Optional<String> incrementalPushVersion,
      String region,
      String targetedRegions) {
    checkControllerLeadershipFor(clusterName);
    if (region != null) {
      checkCurrentFabricMatchesExpectedFabric(region);
    }
    PushMonitor monitor = getHelixVeniceClusterResources(clusterName).getPushMonitor();
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    Store store = getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName);
    }
    int versionNumber = Version.parseVersionFromVersionTopicName(kafkaTopic);

    if (!incrementalPushVersion.isPresent()) {
      OfflinePushStatusInfo offlinePushStatusInfo = getOfflinePushStatusInfo(
          clusterName,
          kafkaTopic,
          incrementalPushVersion,
          monitor,
          store,
          versionNumber,
          !StringUtils.isEmpty(targetedRegions));
      if (region != null) {
        offlinePushStatusInfo.setUncompletedPartitions(monitor.getUncompletedPartitions(kafkaTopic));
      }
      return offlinePushStatusInfo;
    }

    List<OfflinePushStatusInfo> list = new ArrayList<>();
    // for incremental for push status. check all the incremental enabled versions of the store
    for (Version version: store.getVersions()) {
      if (!version.isIncrementalPushEnabled() || version.getStatus() == ERROR) {
        continue;
      }
      try {
        String kafkaTopicToTry = Version.composeKafkaTopic(storeName, version.getNumber());
        OfflinePushStatusInfo offlinePushStatusInfoOtherVersion = getOfflinePushStatusInfo(
            clusterName,
            kafkaTopicToTry,
            incrementalPushVersion,
            monitor,
            store,
            version.getNumber(),
            !StringUtils.isEmpty(targetedRegions));
        list.add(offlinePushStatusInfoOtherVersion);
      } catch (VeniceNoHelixResourceException e) {
        LOGGER.warn("Resource for store: {} version: {} not found!", storeName, version, e);
      }

    }

    if (list.size() == 0) {
      LOGGER.warn(
          "Could not find any valid incremental push status for store: {}, returning NOT_CREATED status.",
          storeName);
      return new OfflinePushStatusInfo(ExecutionStatus.NOT_CREATED, null, "Offline job hasn't been created yet.");
    }
    // higher priority of EOIP followed by SOIP and NOT_CREATED
    list.sort(((o1, o2) -> {
      int val1 = o1.getExecutionStatus().getValue();
      int val2 = o2.getExecutionStatus().getValue();
      return val2 - val1;
    }));

    ExecutionStatus status = list.get(0).getExecutionStatus();
    // if status is not SOIP remove incremental push version from the supposedlyOngoingIncrementalPushVersions
    if (incrementalPushVersion.isPresent()
        && (status == ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED || status == ExecutionStatus.NOT_CREATED)
        && store.isDaVinciPushStatusStoreEnabled() && !isParent()) {
      getPushStatusStoreWriter().removeFromSupposedlyOngoingIncrementalPushVersions(
          store.getName(),
          versionNumber,
          incrementalPushVersion.get());
    }
    return list.get(0);
  }

  private ExecutionStatusWithDetails getIncrementalPushStatus(
      String clusterName,
      String kafkaTopic,
      String incrementalPushVersion,
      PushMonitor monitor,
      Store store) {
    HelixCustomizedViewOfflinePushRepository cvRepo =
        getHelixVeniceClusterResources(clusterName).getCustomizedViewRepository();
    boolean usePushStatusStoreForIncrementalPush =
        multiClusterConfigs.getControllerConfig(clusterName).usePushStatusStoreForIncrementalPush();
    if (usePushStatusStoreForIncrementalPush) {
      if (store.isDaVinciPushStatusStoreEnabled()) {
        return monitor.getIncrementalPushStatusFromPushStatusStore(
            kafkaTopic,
            incrementalPushVersion,
            cvRepo,
            getPushStatusStoreReader());
      } else {
        LOGGER.warn(
            "Push status system store is not enabled for store: {} in cluster: {}, using ZK for incremental push status reads",
            store.getName(),
            clusterName);
      }
    }
    return monitor.getIncrementalPushStatusAndDetails(kafkaTopic, incrementalPushVersion, cvRepo);
  }

  private OfflinePushStatusInfo getOfflinePushStatusInfo(
      String clusterName,
      String kafkaTopic,
      Optional<String> incrementalPushVersion,
      PushMonitor monitor,
      Store store,
      int versionNumber,
      boolean isTargetPush) {
    ExecutionStatusWithDetails statusAndDetails;

    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    MaintenanceSignal maintenanceSignal =
        HelixUtils.getClusterMaintenanceSignal(clusterName, resources.getHelixManager());

    // Check if cluster is in maintenance mode due to too many offline instances or too many partitions per instance
    if (isTargetPush && maintenanceSignal != null && maintenanceSignal
        .getAutoTriggerReason() == MaintenanceSignal.AutoTriggerReason.MAX_OFFLINE_INSTANCES_EXCEEDED) {
      String msg = "Push errored out for " + kafkaTopic + "due to too many offline instances. Reason: "
          + maintenanceSignal.getReason();
      LOGGER.error(msg);
      return new OfflinePushStatusInfo(ExecutionStatus.ERROR, null, msg);
    }

    if (incrementalPushVersion.isPresent()) {
      statusAndDetails =
          getIncrementalPushStatus(clusterName, kafkaTopic, incrementalPushVersion.get(), monitor, store);
    } else {
      statusAndDetails = monitor.getPushStatusAndDetails(kafkaTopic);
    }
    ExecutionStatus executionStatus = statusAndDetails.getStatus();
    String details = statusAndDetails.getDetails();
    Long statusUpdateTimestamp = statusAndDetails.getStatusUpdateTimestamp();
    if (executionStatus.equals(ExecutionStatus.NOT_CREATED)) {
      StringBuilder moreDetailsBuilder = new StringBuilder(details == null ? "" : details + " and ");
      Version version = store.getVersion(versionNumber);

      // Check whether cluster is in maintenance mode or not
      if (maintenanceSignal != null) {
        moreDetailsBuilder.append("Cluster: ").append(clusterName).append(" is in maintenance mode");
      } else if (version != null && version.getStatus() == KILLED) {
        moreDetailsBuilder.append("Push job was killed for: ").append(kafkaTopic);
        return new OfflinePushStatusInfo(ExecutionStatus.ERROR, statusUpdateTimestamp, moreDetailsBuilder.toString());
      } else {
        moreDetailsBuilder.append("Version creation for topic: ").append(kafkaTopic).append(" got delayed");
      }
      details = moreDetailsBuilder.toString();
    }

    // Retrieve Da Vinci push status
    // Da Vinci can only subscribe to an existing version, so skip 1st push
    if (store.isDaVinciPushStatusStoreEnabled() && (versionNumber > 1 || incrementalPushVersion.isPresent())) {
      if (monitor.isOfflinePushMonitorDaVinciPushStatusEnabled() && !incrementalPushVersion.isPresent()) {
        // The offline push status will contain Da Vinci push status when either server or Da Vinci push status becomes
        // terminal.
        return new OfflinePushStatusInfo(executionStatus, statusUpdateTimestamp, details);
      }
      Version version = store.getVersion(versionNumber);
      if (version != null) {
        ExecutionStatusWithDetails daVinciStatusAndDetails = PushMonitorUtils.getDaVinciPushStatusAndDetails(
            getPushStatusStoreReader(),
            version.kafkaTopicName(),
            version.getPartitionCount(),
            incrementalPushVersion,
            multiClusterConfigs.getControllerConfig(clusterName).getDaVinciPushStatusScanMaxOfflineInstanceCount(),
            multiClusterConfigs.getControllerConfig(clusterName).getDaVinciPushStatusScanMaxOfflineInstanceRatio(),
            multiClusterConfigs.getCommonConfig().useDaVinciSpecificExecutionStatusForError());
        ExecutionStatus daVinciStatus = daVinciStatusAndDetails.getStatus();
        String daVinciDetails = daVinciStatusAndDetails.getDetails();
        ExecutionStatus overallExecutionStatus = getOverallPushStatus(executionStatus, daVinciStatus);
        if (overallExecutionStatus != executionStatus) {
          executionStatus = overallExecutionStatus;
          statusUpdateTimestamp = null;
        }
        if (details != null || daVinciDetails != null) {
          String overallDetails = "";
          if (details != null) {
            overallDetails += details;
          }
          if (daVinciDetails != null) {
            overallDetails += (overallDetails.isEmpty() ? "" : " ") + daVinciDetails;
          }
          details = overallDetails;
        }
      } else {
        LOGGER
            .info("Version {} of {} does not exist, will not check push status store", versionNumber, store.getName());
      }
    }
    return new OfflinePushStatusInfo(executionStatus, statusUpdateTimestamp, details);
  }

  // The method merges push status from Venice Server replicas and online Da Vinci hosts and return the unified status.
  protected static ExecutionStatus getOverallPushStatus(ExecutionStatus veniceStatus, ExecutionStatus daVinciStatus) {
    List<ExecutionStatus> statuses = Arrays.asList(veniceStatus, daVinciStatus);
    statuses.sort(Comparator.comparingInt(STATUS_PRIORITIES::indexOf));
    return statuses.get(0);
  }

  // TODO remove this method once we are fully on HaaS
  // Create the controller cluster for venice cluster assignment if required.
  private void createControllerClusterIfRequired() {
    if (admin.getClusters().contains(controllerClusterName)) {
      LOGGER.info("Cluster: {} already exists.", controllerClusterName);
      return;
    }

    boolean isClusterCreated = admin.addCluster(controllerClusterName, false);
    if (isClusterCreated == false) {
      /**
       * N.B.: {@link HelixAdmin#addCluster(String, boolean)} has a somewhat quirky implementation:
       *
       * When it returns true, it does not necessarily mean the cluster is fully created, because it
       * short-circuits the rest of its work if it sees the top-level znode is present.
       *
       * When it returns false, it means the cluster is either not created at all or is only partially
       * created.
       *
       * Therefore, when calling this function twice in a row, it is possible that the first invocation
       * may do a portion of the setup work, and then fail on a subsequent step (thus returning false);
       * and that the second invocation would short-circuit when seeing that the initial portion of the
       * work is done (thus returning true). In this case, however, the cluster is actually not created.
       *
       * Because the function swallows any errors (though still logs them, thankfully) and only returns
       * a boolean, it is impossible to catch the specific exception that prevented the first invocation
       * from working, hence why our own logs instruct the operator to look for previous Helix logs for
       * the details...
       *
       * In the main code, I (FGV) believe we don't retry the #addCluster() call, so we should not fall
       * in the scenario where this returns true and we mistakenly think the cluster exists. This does
       * happen in the integration test suite, however, which retries service creation several times
       * if it gets any exception.
       *
       * In any case, if we call this function twice and progress past this code block, another Helix
       * function below, {@link HelixAdmin#setConfig(HelixConfigScope, Map)}, fails with the following
       * symptoms:
       *
       * org.apache.helix.HelixException: fail to set config. cluster: venice-controllers is NOT setup.
       *
       * Thus, if you see this, it is actually because {@link HelixAdmin#addCluster(String, boolean)}
       * returned true even though the Helix cluster is only partially setup.
       */
      throw new VeniceException(
          "admin.addCluster() for '" + controllerClusterName + "' returned false. "
              + "Look for previous errors logged by Helix for more details...");
    }
    HelixConfigScope configScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(controllerClusterName)
            .build();
    Map<String, String> helixClusterProperties = new HashMap<String, String>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    // Topology and fault zone type fields are used by CRUSH alg. Helix would apply the constraints on CRUSH alg to
    // choose proper instance to hold the replica.
    helixClusterProperties
        .put(ClusterConfig.ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name(), String.valueOf(false));
    /**
     * This {@link HelixAdmin#setConfig(HelixConfigScope, Map)} function will throw a HelixException if
     * the previous {@link HelixAdmin#addCluster(String, boolean)} call failed silently (details above).
     */
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(controllerClusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());
  }

  private void setupStorageClusterAsNeeded(String clusterName) {
    if (!helixAdminClient.isVeniceStorageClusterCreated(clusterName)) {
      ClusterConfig helixClusterConfig = new ClusterConfig(clusterName);
      helixClusterConfig.getRecord().setBooleanField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, true);
      VeniceControllerClusterConfig clusterConfigs = multiClusterConfigs.getControllerConfig(clusterName);
      long delayRebalanceTimeMs = clusterConfigs.getDelayToRebalanceMS();
      if (delayRebalanceTimeMs > 0) {
        helixClusterConfig.setRebalanceDelayTime(delayRebalanceTimeMs);
        helixClusterConfig.setDelayRebalaceEnabled(true);
      }
      helixClusterConfig.setPersistBestPossibleAssignment(true);
      // Topology and fault zone type fields are used by CRUSH alg. Helix would apply the constraints on CRUSH alg to
      // choose proper instance to hold the replica.
      helixClusterConfig.setTopology("/" + HelixUtils.TOPOLOGY_CONSTRAINT);
      helixClusterConfig.setFaultZoneType(HelixUtils.TOPOLOGY_CONSTRAINT);

      RESTConfig restConfig = null;
      if (!StringUtils.isEmpty(clusterConfigs.getHelixRestCustomizedHealthUrl())) {
        restConfig = new RESTConfig(clusterName);
        restConfig.set(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL, clusterConfigs.getHelixRestCustomizedHealthUrl());
      }

      helixAdminClient.createVeniceStorageCluster(clusterName, helixClusterConfig, restConfig);
    }
    if (!helixAdminClient.isClusterInGrandCluster(clusterName)) {
      helixAdminClient.addClusterToGrandCluster(clusterName);
    }
    if (!helixAdminClient.isVeniceStorageClusterInControllerCluster(clusterName)) {
      helixAdminClient.addVeniceStorageClusterToControllerCluster(clusterName);
    }
  }

  // TODO remove this method once we are fully on HaaS
  private void createClusterIfRequired(String clusterName) {
    if (admin.getClusters().contains(clusterName)) {
      LOGGER.info("Cluster: {} already exists.", clusterName);
      return;
    }

    boolean isClusterCreated = admin.addCluster(clusterName, false);
    if (!isClusterCreated) {
      LOGGER.info("Cluster: {} creation returned false.", clusterName);
      return;
    }

    VeniceControllerClusterConfig config = multiClusterConfigs.getControllerConfig(clusterName);
    HelixConfigScope configScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
    Map<String, String> helixClusterProperties = new HashMap<>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    long delayedTime = config.getDelayToRebalanceMS();
    if (delayedTime > 0) {
      helixClusterProperties
          .put(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name(), String.valueOf(delayedTime));
    }
    helixClusterProperties
        .put(ClusterConfig.ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.name(), String.valueOf(true));
    // Topology and fault zone type fields are used by CRUSH alg. Helix would apply the constrains on CRUSH alg to
    // choose proper instance to hold the replica.
    helixClusterProperties
        .put(ClusterConfig.ClusterConfigProperty.TOPOLOGY.name(), "/" + HelixUtils.TOPOLOGY_CONSTRAINT);
    helixClusterProperties
        .put(ClusterConfig.ClusterConfigProperty.FAULT_ZONE_TYPE.name(), HelixUtils.TOPOLOGY_CONSTRAINT);
    admin.setConfig(configScope, helixClusterProperties);
    LOGGER.info(
        "Cluster creation: {} completed, auto join to true. Delayed rebalance time: {}ms",
        clusterName,
        delayedTime);
    admin.addStateModelDef(clusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());

    admin.addResource(
        controllerClusterName,
        clusterName,
        CONTROLLER_CLUSTER_NUMBER_OF_PARTITION,
        LeaderStandbySMD.name,
        IdealState.RebalanceMode.FULL_AUTO.toString(),
        AutoRebalanceStrategy.class.getName());
    IdealState idealState = admin.getResourceIdealState(controllerClusterName, clusterName);
    // Use crush alg to allocate controller as well.
    idealState.setMinActiveReplicas(controllerClusterReplica);
    idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
    idealState.setRebalanceStrategy(CrushRebalanceStrategy.class.getName());
    admin.setResourceIdealState(controllerClusterName, clusterName, idealState);
    admin.rebalance(controllerClusterName, clusterName, controllerClusterReplica);
  }

  private void throwStoreAlreadyExists(String clusterName, String storeName) {
    String errorMessage = "Store:" + storeName + " already exists. Can not add it to cluster:" + clusterName;
    LOGGER.error(errorMessage);
    throw new VeniceStoreAlreadyExistsException(storeName, clusterName);
  }

  private void throwStoreDoesNotExist(String clusterName, String storeName) {
    String errorMessage = "Store:" + storeName + " does not exist in cluster:" + clusterName;
    LOGGER.error(errorMessage);
    throw new VeniceNoStoreException(storeName, clusterName);
  }

  private void throwVersionAlreadyExists(String storeName, int version) {
    String errorMessage = "Version" + version + " already exists in Store:" + storeName + ". Can not add it to store.";
    logAndThrow(errorMessage);
  }

  private void throwClusterNotInitialized(String clusterName) {
    throw new VeniceNoClusterException(clusterName);
  }

  private void logAndThrow(String msg) {
    LOGGER.info(msg);
    throw new VeniceException(msg);
  }

  /**
   * @see Admin#getKafkaBootstrapServers(boolean)
   */
  @Override
  public String getKafkaBootstrapServers(boolean isSSL) {
    if (isSSL) {
      return kafkaSSLBootstrapServers;
    } else {
      return kafkaBootstrapServers;
    }
  }

  @Override
  public String getRegionName() {
    return multiClusterConfigs.getRegionName();
  }

  /**
   * @return KafkaUrl for the given fabric.
   * @see ConfigKeys#CHILD_DATA_CENTER_KAFKA_URL_PREFIX
   */
  @Override
  public String getNativeReplicationKafkaBootstrapServerAddress(String sourceFabric) {
    return multiClusterConfigs.getChildDataCenterKafkaUrlMap().get(sourceFabric);
  }

  /**
   * Source fabric selection priority:
   * 1. Parent controller emergency source fabric config.
   * 2. VPJ plugin targeted region config, however it will compute all selections based on the criteria below to select the source region.
   * 3. VPJ plugin source grid fabric config.
   * 4. Store level source fabric config.
   * 5. Cluster level source fabric config.
   * @return the selected source fabric for a given store.
   */
  @Override
  public String getNativeReplicationSourceFabric(
      String clusterName,
      Store store,
      Optional<String> sourceGridFabric,
      Optional<String> emergencySourceRegion,
      String targetedRegions) {
    String sourceFabric = emergencySourceRegion.orElse(null);

    // always respect emergency region so return directly if it is not empty
    if (StringUtils.isNotEmpty(sourceFabric)) {
      return sourceFabric;
    }

    Set<String> regions = targetedRegions != null ? parseRegionsFilterList(targetedRegions) : null;

    if (sourceGridFabric.isPresent()) {
      sourceFabric = getPreferredRegion(sourceGridFabric.get(), regions);
    }

    if (StringUtils.isEmpty(sourceFabric)) {
      sourceFabric = getPreferredRegion(store.getNativeReplicationSourceFabric(), regions);
    }

    if (StringUtils.isEmpty(sourceFabric)) {
      sourceFabric = getPreferredRegion(
          getMultiClusterConfigs().getControllerConfig(clusterName).getNativeReplicationSourceFabric(),
          regions);
    }

    if (StringUtils.isEmpty(sourceFabric) && regions != null) {
      String selection = regions.iterator().next();
      LOGGER.warn(
          "User specified targeted regions: {} for store {} but default source fabric selection is not in the list. Use {} from the list instead",
          targetedRegions,
          store.getName(),
          selection);
      sourceFabric = selection;
    }
    return sourceFabric;
  }

  /**
   * Select the preferred regions from the given set if the set is provided.
   * @param candidate
   * @param regions
   * @return
   */
  private String getPreferredRegion(String candidate, Set<String> regions) {
    if (regions == null || regions.contains(candidate)) {
      return candidate;
    }
    return null;
  }

  /**
   * @see Admin#isSSLEnabledForPush(String, String)
   */
  @Override
  public boolean isSSLEnabledForPush(String clusterName, String storeName) {
    if (isSslToKafka()) {
      Store store = getStore(clusterName, storeName);
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }
      if (store.isHybrid()) {
        if (multiClusterConfigs.getCommonConfig().isEnableNearlinePushSSLAllowlist()
            && (!multiClusterConfigs.getCommonConfig().getPushSSLAllowlist().contains(storeName))) {
          // allowlist is enabled but the given store is not in that list, so ssl is not enabled for this store.
          return false;
        }
      } else {
        if (multiClusterConfigs.getCommonConfig().isEnableOfflinePushSSLAllowlist()
            && (!multiClusterConfigs.getCommonConfig().getPushSSLAllowlist().contains(storeName))) {
          // allowlist is enabled but the given store is not in that list, so ssl is not enabled for this store.
          return false;
        }
      }
      // allowlist is not enabled, or allowlist is enabled and the given store is in that list, so ssl is enabled for
      // this store for push.
      return true;
    } else {
      return false;
    }
  }

  /**
   * Test if ssl is enabled to Kafka.
   * @see ConfigKeys#SSL_TO_KAFKA_LEGACY
   * @see ConfigKeys#KAFKA_OVER_SSL
   */
  @Override
  public boolean isSslToKafka() {
    return this.multiClusterConfigs.isSslToKafka();
  }

  TopicManagerRepository getTopicManagerRepository() {
    return this.topicManagerRepository;
  }

  /**
   * @see Admin#getTopicManager()
   */
  @Override
  public TopicManager getTopicManager() {
    return this.topicManagerRepository.getLocalTopicManager();
  }

  /**
   * @see Admin#getTopicManager(String)
   */
  @Override
  public TopicManager getTopicManager(String pubSubServerAddress) {
    return this.topicManagerRepository.getTopicManager(pubSubServerAddress);
  }

  /**
   * @see Admin#isLeaderControllerFor(String)
   */
  @Override
  public boolean isLeaderControllerFor(String clusterName) {
    VeniceControllerStateModel model = controllerStateModelFactory.getModel(clusterName);
    if (model == null) {
      return false;
    }
    return model.isLeader();
  }

  @Override
  public InstanceRemovableStatuses getAggregatedHealthStatus(
      String cluster,
      List<String> instances,
      List<String> toBeStoppedInstances,
      boolean isSSLEnabled) {
    InstanceRemovableStatuses statuses = new InstanceRemovableStatuses();

    // If current controller is not the leader, redirect with leader URL
    if (!isLeaderControllerFor(cluster)) {
      statuses.setRedirectUrl(getLeaderController(cluster).getUrl(isSSLEnabled));
      return statuses;
    }

    Map<String, String> nonStoppableInstances = new HashMap<>();
    List<String> stoppableInstances = new ArrayList<>();
    statuses.setNonStoppableInstancesWithReasons(nonStoppableInstances);
    statuses.setStoppableInstances(stoppableInstances);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(cluster);

    List<NodeRemovableResult> removableResults =
        InstanceStatusDecider.getInstanceStoppableStatuses(resources, cluster, instances, toBeStoppedInstances);
    for (NodeRemovableResult nodeRemovableResult: removableResults) {
      if (nodeRemovableResult.isRemovable()) {
        stoppableInstances.add(nodeRemovableResult.getInstanceId());
      } else {
        nonStoppableInstances.put(nodeRemovableResult.getInstanceId(), nodeRemovableResult.getFormattedMessage());
      }
    }
    return statuses;
  }

  /**
   * Calculate number of partition for given store.
   */
  @Override
  public int calculateNumberOfPartitions(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    Store store = resources.getStoreMetadataRepository().getStoreOrThrow(storeName);
    VeniceControllerClusterConfig config = resources.getConfig();
    return PartitionUtils.calculatePartitionCount(
        storeName,
        store.getStorageQuotaInByte(),
        store.getPartitionCount(),
        config.getPartitionSize(),
        config.getMinNumberOfPartitions(),
        config.getMaxNumberOfPartitions(),
        config.isPartitionCountRoundUpEnabled(),
        config.getPartitionCountRoundUpSize());
  }

  /**
   * @return the replication factor of the given store.
   */
  @Override
  public int getReplicationFactor(String clusterName, String storeName) {
    int replicationFactor = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository()
        .getStore(storeName)
        .getReplicationFactor();
    if (replicationFactor <= 0) {
      throw new VeniceException(
          "Unexpected replication factor: " + replicationFactor + " for store: " + storeName + " in cluster: "
              + clusterName);
    }
    return replicationFactor;
  }

  /**
   * @return a list of <code>Replica</code> created for the given resource.
   */
  @Override
  public List<Replica> getReplicas(String clusterName, String kafkaTopic) {
    checkControllerLeadershipFor(clusterName);
    List<Replica> replicas = new ArrayList<>();
    PartitionAssignment partitionAssignment =
        getHelixVeniceClusterResources(clusterName).getRoutingDataRepository().getPartitionAssignments(kafkaTopic);

    partitionAssignment.getAllPartitions().forEach(partition -> {
      int partitionId = partition.getId();
      partition.getAllInstancesByHelixState().forEach((helixState, instanceList) -> {
        instanceList.forEach(instance -> {
          Replica replica = new Replica(instance, partitionId, kafkaTopic);
          replica.setStatus(helixState);
          replicas.add(replica);
        });
      });
    });

    return replicas;
  }

  /**
   * @see Admin#getReplicasOfStorageNode(String, String)
   */
  @Override
  public List<Replica> getReplicasOfStorageNode(String cluster, String instanceId) {
    checkControllerLeadershipFor(cluster);
    return InstanceStatusDecider.getReplicasForInstance(getHelixVeniceClusterResources(cluster), instanceId);
  }

  /**
   * @see Admin#isInstanceRemovable(String, String, List)
   */
  @Override
  public NodeRemovableResult isInstanceRemovable(String clusterName, String helixNodeId, List<String> lockedNodes) {
    checkControllerLeadershipFor(clusterName);
    return InstanceStatusDecider
        .isRemovable(getHelixVeniceClusterResources(clusterName), clusterName, helixNodeId, lockedNodes);
  }

  /**
   * @see Admin#getLeaderController(String)
   *
   * Get the Venice controller leader for a storage cluster. We look at the external view of the controller cluster to
   * find the Venice controller leader for a storage cluster. Because in both Helix as a library or Helix as a service
   * (HaaS), the leader in the controller cluster external view is the Venice controller leader. During HaaS transition,
   * controller leader property will become a HaaS controller, which is not the Venice controller that we want.
   * Therefore, we don't refer to controller leader property to get leader controller.
   */
  @Override
  public Instance getLeaderController(String clusterName) {
    List<Instance> leaderControllers = getControllersByHelixState(clusterName, HelixState.LEADER_STATE);
    return leaderControllers.get(0);
  }

  /**
   * Get controllers instance based on the given helix state.
   * We look at the external view of the controller cluster to find the venice controller by the wanted state.
   */
  public List<Instance> getControllersByHelixState(String clusterName, String helixState) {
    // Validate helix state
    if (!HelixState.isValidHelixState(helixState)) {
      throw new VeniceException("Invalid Helix state: " + helixState);
    }

    if (!multiClusterConfigs.getClusters().contains(clusterName)) {
      throw new VeniceNoClusterException(clusterName);
    }
    final int maxAttempts = 10;
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(controllerClusterName);
    String partitionName = HelixUtils.getPartitionName(clusterName, 0);
    for (int attempt = 1; attempt <= maxAttempts; ++attempt) {
      ExternalView externalView = helixManager.getHelixDataAccessor().getProperty(keyBuilder.externalView(clusterName));
      List<Instance> controllers = new ArrayList<>();
      if (externalView == null || externalView.getStateMap(partitionName) == null) {
        // Assignment is incomplete, try again later
        continue;
      }
      Map<String, String> veniceClusterStateMap = externalView.getStateMap(partitionName);
      for (Map.Entry<String, String> instanceNameAndState: veniceClusterStateMap.entrySet()) {
        if (instanceNameAndState.getValue().equals(helixState)) {
          // Found the Venice controller, adding to the return list
          String id = instanceNameAndState.getKey();
          // TODO: Update the instance constructor when Helix NodeId is changed to use secure port.
          controllers.add(
              new Instance(
                  id,
                  Utils.parseHostFromHelixNodeIdentifier(id),
                  Utils.parsePortFromHelixNodeIdentifier(id),
                  multiClusterConfigs.getAdminSecurePort(),
                  multiClusterConfigs.getAdminGrpcPort(),
                  multiClusterConfigs.getAdminSecureGrpcPort()));
        }
      }

      // Return the controllers if found
      if (!controllers.isEmpty()) {
        return controllers;
      }

      if (attempt < maxAttempts) {
        LOGGER.warn(
            "Venice controller with state {} does not exist for cluster: {}, attempt: {}/{}",
            helixState,
            clusterName,
            attempt,
            maxAttempts);
        Utils.sleep(5 * Time.MS_PER_SECOND);
      }
    }
    String message = "Unable to find Venice controller " + helixState + " for cluster: " + clusterName + " after "
        + maxAttempts + " attempts";
    LOGGER.error(message);
    throw new VeniceException(message);
  }

  /**
   * Get all live instance controllers from ZK /LIVEINSTANCES
   */
  public List<Instance> getAllLiveInstanceControllers() {
    final int maxAttempts = 10;
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(getControllerClusterName());
    for (int attempt = 1; attempt <= maxAttempts; ++attempt) {
      List<String> liveInstances = getHelixManager().getHelixDataAccessor().getChildNames(keyBuilder.liveInstances());
      if (liveInstances == null || liveInstances.isEmpty()) {
        // Assignment is incomplete, try again later
        LOGGER.warn("No live instance controllers found, attempt: {}/{}", attempt, maxAttempts);
        Utils.sleep(5 * Time.MS_PER_SECOND);
        continue;
      }

      List<Instance> controllers = new ArrayList<>();
      for (String id: liveInstances) {
        controllers.add(
            new Instance(
                id,
                Utils.parseHostFromHelixNodeIdentifier(id),
                Utils.parsePortFromHelixNodeIdentifier(id),
                getMultiClusterConfigs().getAdminSecurePort(),
                getMultiClusterConfigs().getAdminGrpcPort(),
                getMultiClusterConfigs().getAdminSecureGrpcPort()));
      }
      return controllers;
    }
    String message = "Unable to find live instance controllers in ZK after " + maxAttempts + " attempts";
    if (!EXCEPTION_FILTER.isRedundantException(message)) {
      LOGGER.error(message);
    }
    throw new VeniceException(message);
  }

  /**
   * Add the given helix nodeId into the allowlist in ZK.
   */
  @Override
  public void addInstanceToAllowlist(String clusterName, String helixNodeId) {
    checkControllerLeadershipFor(clusterName);
    allowlistAccessor.addInstanceToAllowList(clusterName, helixNodeId);
  }

  /**
   * Remove the given helix nodeId from the allowlist in ZK.
   */
  @Override
  public void removeInstanceFromAllowList(String clusterName, String helixNodeId) {
    checkControllerLeadershipFor(clusterName);
    allowlistAccessor.removeInstanceFromAllowList(clusterName, helixNodeId);
  }

  /**
   * @return a list of all helix nodeIds in the allowlist for the given cluster from ZK.
   */
  @Override
  public Set<String> getAllowlist(String clusterName) {
    checkControllerLeadershipFor(clusterName);
    return allowlistAccessor.getAllowList(clusterName);
  }

  void checkPreConditionForKillOfflinePush(String clusterName, String kafkaTopic) {
    checkControllerLeadershipFor(clusterName);
    if (!Version.isVersionTopicOrStreamReprocessingTopic(kafkaTopic)) {
      throw new VeniceException("Topic: " + kafkaTopic + " is not a valid Venice version topic.");
    }
  }

  private Version getStoreVersion(String clusterName, String topic) {
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int versionId = Version.parseVersionFromKafkaTopicName(topic);
    Store store = getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    return store.getVersion(versionId);
  }

  /**
   * @see Admin#killOfflinePush(String, String, boolean)
   */
  @Override
  public void killOfflinePush(String clusterName, String kafkaTopic, boolean isForcedKill) {
    if (!isResourceStillAlive(kafkaTopic)) {
      /**
       * To avoid sending kill job messages if the resource is already removed, and this
       * could happen in the following scenario:
       * 1. The first try of resource deletion fails in the middle, which could be caused by
       * sending killing job message timeout;
       * 2. The second try of deleting the same resource will try to send the killing job message
       * again even the resource has already been deleted (kill job messages are eventually processed
       * by every participant);
       * So the killing job message is not necessary since the resource doesn't exist, and we should
       * not send kill job message since it is expensive and easy to time-out.
       */
      LOGGER.info("Resource: {} doesn't exist, kill job will be skipped", kafkaTopic);
      return;
    }
    checkPreConditionForKillOfflinePush(clusterName, kafkaTopic);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    if (!isForcedKill) {
      String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
      try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLockOnly(storeName)) {
        /**
         * Check whether the specified store is already terminated or not.
         * If yes, the kill job message will be skipped.
         */
        Version version;
        try {
          version = getStoreVersion(clusterName, kafkaTopic);
        } catch (VeniceNoStoreException e) {
          LOGGER.warn(
              "Kill job will be skipped since the corresponding store for topic: {} doesn't exist in cluster: {}",
              kafkaTopic,
              clusterName);
          return;
        }
        if (version != null) {
          VersionStatus status = version.getStatus();
          if (VersionStatus.isBootstrapCompleted(status)) {
            /**
             * This is trying to avoid kill job entry in participant store if the version is already online.
             * This won't solve all the edge cases since the following race condition could still happen, but it is fine.
             * The race condition could be:
             * 1. The version status is still not 'ONLINE' yet from Controller's view, then kill job message will be sent to storage node;
             * 2. When storage node receives the kill job message, the version from storage node's perspective could be already
             *    'online';
             *
             * The reason not to fix this race condition:
             * 1. It should happen rarely;
             * 2. This will change the lifecycle of store version greatly, which might introduce other potential issues;
             *
             * The target use case is for new fabric build out.
             * In Blueshift project, we are planning to leverage participant store to short-circuit
             * the bootstrap if the corresponding version has been killed in the source fabric.
             * So in the new fabric, we need the kill-job message to be as accurate as possible to
             * avoid the discrepancy.
             */
            LOGGER.info("Resource: {} has finished bootstrapping, so kill job will be skipped", kafkaTopic);
            return;
          }

          // If the version has already been killed/errored in the past, skip updating.
          if (VersionStatus.isVersionKilled(status) || VersionStatus.isVersionErrored(status)) {
            LOGGER.info(
                "Resource: {} has been updated to {} previously, so kill job will be skipped",
                kafkaTopic,
                status.name());
            return;
          }

          // Update version status to KILLED on ZkNode.
          ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
          Store store = repository.getStore(storeName);
          store.updateVersionStatus(version.getNumber(), KILLED);
          repository.updateStore(store);
        }
      }
    }

    if (multiClusterConfigs.getControllerConfig(clusterName).isAdminHelixMessagingChannelEnabled()) {
      StatusMessageChannel messageChannel = resources.getMessageChannel();
      // As we should already have retry outside of this function call, so we do not need to retry again inside.
      int retryCount = 1;
      // Broadcast kill message to all of storage nodes assigned to given resource. Helix will help us to only send
      // message to the live instances.
      // The alternative way here is that get the storage nodes in BOOTSTRAP state of given resource, then send the
      // kill message node by node. Considering the simplicity, broadcast is a better.
      // In prospective of performance, each time helix sending a message needs to read the whole LIVE_INSTANCE and
      // EXTERNAL_VIEW from ZK, so sending message nodes by nodes would generate lots of useless read requests. Of
      // course
      // broadcast would generate useless write requests to ZK(N-M useless messages, N=number of nodes assigned to
      // resource,
      // M=number of nodes have completed the ingestion or have not started). But considering the number of nodes in
      // our cluster is not too big, so it's not a big deal here.
      messageChannel.sendToStorageNodes(clusterName, new KillOfflinePushMessage(kafkaTopic), kafkaTopic, retryCount);
    }
    if (multiClusterConfigs.getControllerConfig(clusterName).isParticipantMessageStoreEnabled()
        && participantMessageStoreRTTMap.containsKey(clusterName)) {
      sendKillMessageToParticipantStore(clusterName, kafkaTopic);
    }
  }

  /**
   * Compose a <code>ParticipantMessageKey</code> message and execute a delete operation on the key to the cluster's participant store.
   */
  public void deleteParticipantStoreKillMessage(String clusterName, String kafkaTopic) {
    LOGGER.info(
        "Delete KILL ingestion message for topic: {} from participant store in cluster: {}",
        kafkaTopic,
        clusterName);
    VeniceWriter writer = participantStoreClientsManager.getWriter(clusterName);
    ParticipantMessageKey key = new ParticipantMessageKey();
    key.resourceName = kafkaTopic;
    key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
    writer.delete(key, null);
    writer.flush();
  }

  public void sendKillMessageToParticipantStore(String clusterName, String kafkaTopic) {
    LOGGER.info("Send kill message for topic: {} to participant store of cluster: {}", kafkaTopic, clusterName);
    VeniceWriter writer = participantStoreClientsManager.getWriter(clusterName);
    ParticipantMessageType killPushJobType = ParticipantMessageType.KILL_PUSH_JOB;
    ParticipantMessageKey key = new ParticipantMessageKey();
    key.resourceName = kafkaTopic;
    key.messageType = killPushJobType.getValue();
    KillPushJob message = new KillPushJob();
    message.timestamp = System.currentTimeMillis();
    ParticipantMessageValue value = new ParticipantMessageValue();
    value.messageType = killPushJobType.getValue();
    value.messageUnion = message;
    writer.put(key, value, PARTICIPANT_MESSAGE_STORE_SCHEMA_ID);
  }

  /**
   * @see Admin#getStorageNodesStatus(String, boolean)
   */
  @Override
  public StorageNodeStatus getStorageNodesStatus(String clusterName, String instanceId) {
    checkControllerLeadershipFor(clusterName);
    List<Replica> replicas = getReplicasOfStorageNode(clusterName, instanceId);
    StorageNodeStatus status = new StorageNodeStatus();
    for (Replica replica: replicas) {
      status.addStatusForReplica(
          HelixUtils.getPartitionName(replica.getResource(), replica.getPartitionId()),
          replica.getStatus());
    }
    return status;
  }

  /**
   * @see Admin#isStorageNodeNewerOrEqualTo(String, String, StorageNodeStatus)
   */
  // TODO we don't use this function to check the storage node status. The isRemovable looks enough to ensure the
  // TODO upgrading would not affect the push and online reading request. Leave this function here to see do we need it
  // in the future.
  @Override
  public boolean isStorageNodeNewerOrEqualTo(String clusterName, String instanceId, StorageNodeStatus oldStatus) {
    checkControllerLeadershipFor(clusterName);
    StorageNodeStatus currentStatus = getStorageNodesStatus(clusterName, instanceId);
    return currentStatus.isNewerOrEqual(oldStatus);
  }

  /**
   * Package private on purpose, only used by tests.
   *
   * Enable or disable the delayed rebalance for the given cluster. By default, the delayed reblance is enabled/disabled
   * depends on the cluster's configuration. Through this method, SRE/DEV could enable or disable the delayed reblance
   * temporarily or set a different delayed rebalance time temporarily.
   *
   * @param  delayedTime how long the helix will not rebalance after a server is disconnected. If the given value
   *                     equal or smaller than 0, we disable the delayed rebalance.
   */
  void setDelayedRebalanceTime(String clusterName, long delayedTime) {
    boolean enable = delayedTime > 0;
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    PropertyKey clusterConfigPath = keyBuilder.clusterConfig();
    ClusterConfig clusterConfig = helixManager.getHelixDataAccessor().getProperty(clusterConfigPath);
    if (clusterConfig == null) {
      throw new VeniceException("Got a null clusterConfig from: " + clusterConfigPath);
    }
    clusterConfig.getRecord()
        .setLongField(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name(), delayedTime);
    helixManager.getHelixDataAccessor().setProperty(keyBuilder.clusterConfig(), clusterConfig);
    // TODO use the helix new config API below once it's ready. Right now helix has a bug that controller would not get
    // the update from the new config.
    /* ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    HelixConfigScope clusterScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
    configAccessor.set(clusterScope, ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_DISABLED.name(),
        String.valueOf(disable));*/
    String message = enable
        ? "Enabled delayed rebalance for cluster: " + clusterName + " with delayed time " + delayedTime
        : "Disabled delayed rebalance for cluster: " + clusterName;
    LOGGER.info(message);
  }

  /**
   * Package private on purpose, only used by tests.
   *
   * @return current delayed rebalance time value for the given cluster
   */
  long getDelayedRebalanceTime(String clusterName) {
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    PropertyKey clusterConfigPath = keyBuilder.clusterConfig();
    ClusterConfig clusterConfig = helixManager.getHelixDataAccessor().getProperty(clusterConfigPath);
    if (clusterConfig == null) {
      throw new VeniceException("Got a null clusterConfig from: " + clusterConfigPath);
    }
    return clusterConfig.getRecord().getLongField(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name(), 0L);
  }

  /**
   * @see Admin#setAdminConsumerService(String, AdminConsumerService)
   */
  @Override
  public void setAdminConsumerService(String clusterName, AdminConsumerService service) {
    adminConsumerServices.put(clusterName, service);
  }

  /**
   * @see Admin#skipAdminMessage(String, long, boolean)
   */
  @Override
  public void skipAdminMessage(String clusterName, long offset, boolean skipDIV) {
    if (adminConsumerServices.containsKey(clusterName)) {
      adminConsumerServices.get(clusterName).setOffsetToSkip(clusterName, offset, skipDIV);
    } else {
      throw new VeniceException("Cannot skip execution, must first setAdminConsumerService for cluster " + clusterName);
    }

  }

  /**
   * @see Admin#getLastSucceedExecutionId(String)
   */
  @Override
  public Long getLastSucceedExecutionId(String clusterName) {
    if (adminConsumerServices.containsKey(clusterName)) {
      return adminConsumerServices.get(clusterName).getLastSucceededExecutionIdInCluster(clusterName);
    } else {
      throw new VeniceException(
          "Cannot get the last succeed execution Id, must first setAdminConsumerService for cluster " + clusterName);
    }
  }

  /**
   * Get last succeeded execution id for a given store; if storeName is null, return the last succeeded execution id
   * for the cluster
   * @param clusterName
   * @param storeName
   * @return the last succeeded execution id or null if the cluster/store is invalid or the admin consumer service
   *         for the given cluster is not up and running yet.
   */
  public Long getLastSucceededExecutionId(String clusterName, String storeName) {
    if (storeName == null) {
      return getLastSucceedExecutionId(clusterName);
    } else {
      String userStoreName = VeniceSystemStoreType.extractUserStoreName(storeName);
      return adminConsumerServices.containsKey(clusterName)
          ? adminConsumerServices.get(clusterName).getLastSucceededExecutionId(userStoreName)
          : null;
    }
  }

  Exception getLastExceptionForStore(String clusterName, String storeName) {
    return adminConsumerServices.containsKey(clusterName)
        ? adminConsumerServices.get(clusterName).getLastExceptionForStore(storeName)
        : null;
  }

  /**
   * @see Admin#getAdminCommandExecutionTracker(String)
   */
  @Override
  public Optional<AdminCommandExecutionTracker> getAdminCommandExecutionTracker(String clusterName) {
    return Optional.empty();
  }

  /**
   * @return cluster-level execution id, offset, upstream offset, and admin operation protocol version.
   *        If store name is specified, it returns store-level execution id.
   */
  public Map<String, Long> getAdminTopicMetadata(String clusterName, Optional<String> storeName) {
    if (storeName.isPresent()) {
      Long executionId = getExecutionIdAccessor().getLastSucceededExecutionIdMap(clusterName).get(storeName.get());
      return executionId == null
          ? Collections.emptyMap()
          : AdminTopicMetadataAccessor
              .generateMetadataMap(Optional.of(-1L), Optional.of(-1L), Optional.of(executionId), Optional.of(-1L));
    }
    return getAdminConsumerService(clusterName).getAdminTopicMetadata(clusterName);
  }

  /**
   * Update cluster-level execution id, offset and upstream offset.
   * If store name is specified, it updates the store-level execution id.
   */
  public void updateAdminTopicMetadata(
      String clusterName,
      long executionId,
      Optional<String> storeName,
      Optional<Long> offset,
      Optional<Long> upstreamOffset) {
    if (storeName.isPresent()) {
      getExecutionIdAccessor().updateLastSucceededExecutionIdMap(clusterName, storeName.get(), executionId);
    } else {
      if (!offset.isPresent() || !upstreamOffset.isPresent()) {
        throw new VeniceException("Offsets must be provided to update cluster-level admin topic metadata");
      }
      getAdminConsumerService(clusterName)
          .updateAdminTopicMetadata(clusterName, executionId, offset.get(), upstreamOffset.get());
    }
  }

  /**
   * Update AdminOperationProtocolVersion in metadata
   */
  public void updateAdminOperationProtocolVersion(String clusterName, Long adminOperationProtocolVersion) {
    checkControllerLeadershipFor(clusterName);
    getAdminConsumerService(clusterName)
        .updateAdminOperationProtocolVersion(clusterName, adminOperationProtocolVersion);
  }

  /**
   * Get the admin operation protocol versions from all controllers for specific cluster.
   * @param clusterName: the cluster name
   * @return map (controllerName: version). Example: {localhost_1234=1, localhost_1235=1}*/
  @Override
  public Map<String, Long> getAdminOperationVersionFromControllers(String clusterName) {
    checkControllerLeadershipFor(clusterName);

    Map<String, Long> controllerNameToAdminOperationVersionMap = new HashMap<>();

    // Get the version from the current controller - leader
    Instance leaderController = getLeaderController(clusterName);

    controllerNameToAdminOperationVersionMap.put(getControllerName(), getLocalAdminOperationProtocolVersion());

    // Create the controller client to reuse
    // (this is controller client to communicate with other controllers in the same cluster, the same region)
    ControllerClient localControllerClient = ControllerClient
        .constructClusterControllerClient(clusterName, leaderController.getUrl(sslEnabled), getSslFactory());

    // Get version for all controllers
    List<Instance> liveInstances = getAllLiveInstanceControllers();

    for (Instance instance: liveInstances) {
      // In production, leader and standby share the secure port but have different hostnames—no conflict.
      // In tests, both run on 'localhost' with the same secure port, which confuses routing.
      // Hence, we need to disable SSL for local integration tests.
      // RCA: Helix uses an insecure port from the instance ID, while secure port comes from shared multiClusterConfig.
      if (Objects.equals(instance.getNodeId(), leaderController.getNodeId())) {
        // If the instance is the leader controller, skip it as we already have its version.
        continue;
      }

      String liveInstanceUrl = instance.getUrl(getSslFactory().isPresent());

      // Get the admin operation protocol version from standby controller
      AdminOperationProtocolVersionControllerResponse response =
          localControllerClient.getLocalAdminOperationProtocolVersion(liveInstanceUrl);
      if (response.isError()) {
        String msg = "Failed to get admin operation protocol version from controller: " + liveInstanceUrl
            + ", error message: " + response.getError();
        if (!EXCEPTION_FILTER.isRedundantException(msg)) {
          LOGGER.warn(msg);
        }
        continue;
      }

      controllerNameToAdminOperationVersionMap
          .put(response.getLocalControllerName(), response.getLocalAdminOperationProtocolVersion());
    }

    return controllerNameToAdminOperationVersionMap;
  }

  /**
   * Get the local admin operation protocol version.
   */
  @Override
  public long getLocalAdminOperationProtocolVersion() {
    return AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;
  }

  /**
   * @see Admin#getRoutersClusterConfig(String)
   */
  @Override
  public RoutersClusterConfig getRoutersClusterConfig(String clusterName) {
    checkControllerLeadershipFor(clusterName);
    ZkRoutersClusterManager routersClusterManager =
        getHelixVeniceClusterResources(clusterName).getRoutersClusterManager();
    return routersClusterManager.getRoutersClusterConfig();
  }

  /**
   * @see Admin#updateRoutersClusterConfig(String, Optional, Optional, Optional, Optional)
   */
  @Override
  public void updateRoutersClusterConfig(
      String clusterName,
      Optional<Boolean> isThrottlingEnable,
      Optional<Boolean> isQuotaRebalancedEnable,
      Optional<Boolean> isMaxCapacityProtectionEnabled,
      Optional<Integer> expectedRouterCount) {
    ZkRoutersClusterManager routersClusterManager =
        getHelixVeniceClusterResources(clusterName).getRoutersClusterManager();

    checkControllerLeadershipFor(clusterName);
    if (isThrottlingEnable.isPresent()) {
      routersClusterManager.enableThrottling(isThrottlingEnable.get());
    }
    if (isMaxCapacityProtectionEnabled.isPresent()) {
      routersClusterManager.enableMaxCapacityProtection(isMaxCapacityProtectionEnabled.get());
    }
  }

  /**
   * Unsupported operation in the child controller.
   */
  @Override
  public Map<String, String> getAllStorePushStrategyForMigration() {
    throw new VeniceUnsupportedOperationException("getAllStorePushStrategyForMigration");
  }

  /**
   * Unsupported operation in the child controller.
   */
  @Override
  public void setStorePushStrategyForMigration(String voldemortStoreName, String strategy) {
    throw new VeniceUnsupportedOperationException("setStorePushStrategyForMigration");
  }

  /**
   * @see Admin#discoverCluster(String)
   */
  @Override
  public Pair<String, String> discoverCluster(String storeName) {
    StoreConfig config = storeConfigRepo.getStoreConfigOrThrow(storeName);
    if (config == null || StringUtils.isEmpty(config.getCluster())) {
      throw new VeniceNoStoreException(
          storeName,
          null,
          "Make sure the store is created and the provided store name is correct");
    }
    String clusterName = config.getCluster();
    String d2Service = multiClusterConfigs.getClusterToD2Map().get(clusterName);
    if (d2Service == null) {
      throw new VeniceException("Could not find d2 service by given cluster: " + clusterName);
    }
    return new Pair<>(clusterName, d2Service);
  }

  /**
   * @see Admin#getServerD2Service(String)
   */
  @Override
  public String getServerD2Service(String clusterName) {
    return multiClusterConfigs.getClusterToServerD2Map().get(clusterName);
  }

  /**
   * @see Admin#findAllBootstrappingVersions(String)
   * TODO: With L/F we need to deprecate this function OR augment it to read the customized view as opposed to helix states
   */
  @Override
  public Map<String, String> findAllBootstrappingVersions(String clusterName) {
    checkControllerLeadershipFor(clusterName);
    Map<String, String> result = new HashMap<>();
    // Find all ongoing offline pushes at first.
    PushMonitor monitor = getHelixVeniceClusterResources(clusterName).getPushMonitor();
    monitor.getTopicsOfOngoingOfflinePushes().forEach(topic -> result.put(topic, STARTED.toString()));
    // Find the versions which had been ONLINE, but some of replicas are still bootstrapping due to:
    // 1. As we use N-1 strategy, so there might be some slow replicas caused by kafka or other issues.
    // 2. Storage node was added/removed/disconnected, so replicas need to bootstrap again on the same or other node.
    RoutingDataRepository routingDataRepository =
        getHelixVeniceClusterResources(clusterName).getRoutingDataRepository();
    ReadWriteStoreRepository storeRepository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
    for (String topic: resourceAssignment.getAssignedResources()) {
      if (result.containsKey(topic)) {
        continue;
      }
      PartitionAssignment partitionAssignment = resourceAssignment.getPartitionAssignment(topic);
      for (Partition p: partitionAssignment.getAllPartitions()) {
        if (p.getInstancesInState(ExecutionStatus.STARTED).size() > 0) {
          String storeName = Version.parseStoreFromKafkaTopicName(topic);
          VersionStatus status;
          Store store = storeRepository.getStore(storeName);
          if (store != null) {
            status = store.getVersionStatus(Version.parseVersionFromKafkaTopicName(topic));
          } else {
            status = NOT_CREATED;
          }
          result.put(topic, status.toString());
          // Found at least one bootstrap replica, skip to next topic.
          break;
        }
      }
    }
    return result;
  }

  /**
   * @return a <code>VeniceWriterFactory</code> object used by the Venice controller to create the venice writer.
   */
  public VeniceWriterFactory getVeniceWriterFactory() {
    return veniceWriterFactory;
  }

  @Override
  public VeniceProperties getPubSubSSLProperties(String pubSubBrokerAddress) {
    return this.getPubSubSSLPropertiesFromControllerConfig(pubSubBrokerAddress);
  }

  // public for testing purpose
  public AdminConsumerService getAdminConsumerService(String clusterName) {
    return adminConsumerServices.get(clusterName);
  }

  private void startMonitorOfflinePush(
      String clusterName,
      String kafkaTopic,
      int numberOfPartition,
      int replicationFactor,
      OfflinePushStrategy strategy) {
    PushMonitorDelegator offlinePushMonitor = getHelixVeniceClusterResources(clusterName).getPushMonitor();
    offlinePushMonitor.startMonitorOfflinePush(kafkaTopic, numberOfPartition, replicationFactor, strategy);
  }

  public void stopMonitorOfflinePush(
      String clusterName,
      String topic,
      boolean deletePushStatus,
      boolean isForcedDelete) {
    PushMonitor offlinePushMonitor = getHelixVeniceClusterResources(clusterName).getPushMonitor();
    offlinePushMonitor.stopMonitorOfflinePush(topic, deletePushStatus, isForcedDelete);
  }

  Store checkPreConditionForUpdateStoreMetadata(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    Store store = repository.getStore(storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName);
    }
    return store;
  }

  /**
   * Cause VeniceHelixAdmin and its associated services to stop executing.
   */
  @Override
  public void close() {
    long closeStartTime = System.currentTimeMillis();
    ExecutorService ex = Executors.newSingleThreadExecutor(new DaemonThreadFactory("HelixManagerDisconnect"));
    try {
      // This disconnect sometimes hangs... hence why we treat it this way...
      Future helixManagerDisconnectFuture = ex.submit(this.helixManager::disconnect);

      // Close everything else, which usually works fine...
      Utils.closeQuietlyWithErrorLogged(getPushStatusStoreReader());
      this.pushStatusStoreWriter.ifPresent(Utils::closeQuietlyWithErrorLogged);
      Utils.closeQuietlyWithErrorLogged(this.zkSharedSystemStoreRepository);
      Utils.closeQuietlyWithErrorLogged(this.zkSharedSchemaRepository);
      try {
        zkClient.close();
      } catch (Exception e) {
        LOGGER.error("Failed to close zkClient. Swallowing and moving on.", e);
      }
      this.jobTrackingVeniceWriterMap.values().forEach(Utils::closeQuietlyWithErrorLogged);
      this.jobTrackingVeniceWriterMap.clear();
      Utils.closeQuietlyWithErrorLogged(this.dataRecoveryManager);
      Utils.closeQuietlyWithErrorLogged(this.participantStoreClientsManager);
      Utils.closeQuietlyWithErrorLogged(this.topicManagerRepository);
      Utils.closeQuietlyWithErrorLogged(this.pushJobDetailsStoreClient);
      Utils.closeQuietlyWithErrorLogged(this.livenessHeartbeatStoreClient);
      this.clusterControllerClientPerColoMap.values()
          .forEach(ccMap -> ccMap.values().forEach(Utils::closeQuietlyWithErrorLogged));
      D2ClientUtils.shutdownClient(this.d2Client);

      long elapsedTime = System.currentTimeMillis() - closeStartTime;
      long remainingTime = Math.max(1, HELIX_MANAGER_DISCONNECT_TIMEOUT_MS - elapsedTime);
      helixManagerDisconnectFuture.get(remainingTime, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOGGER.error("Timed out waiting for helixManagerDisconnectFuture. Swallowing and moving on.", e);
    } finally {
      ex.shutdownNow();
      long elapsedTime = System.currentTimeMillis() - closeStartTime;
      LOGGER.info("{} took {} ms to close.", VeniceHelixAdmin.class.getSimpleName(), elapsedTime);
    }
  }

  /**
   * Check whether this controller is the leader controller for a cluster. If not, throw the VeniceException.
   *
   * @param clusterName
   */
  void checkControllerLeadershipFor(String clusterName) {
    if (!isLeaderControllerFor(clusterName)) {
      throw new VeniceException(
          "This controller:" + controllerName + " is not the leader controller for cluster: " + clusterName);
    }
  }

  @Override
  public HelixVeniceClusterResources getHelixVeniceClusterResources(String cluster) {
    Optional<HelixVeniceClusterResources> resources = controllerStateModelFactory.getModel(cluster).getResources();
    if (!resources.isPresent()) {
      throwClusterNotInitialized(cluster);
    }
    return resources.get();
  }

  void addConfig(VeniceControllerClusterConfig config) {
    multiClusterConfigs.addClusterConfig(config);
  }

  @Override
  public String getControllerName() {
    return controllerName;
  }

  @Override
  public HelixReadOnlyStoreConfigRepository getStoreConfigRepo() {
    return storeConfigRepo;
  }

  private HelixReadWriteLiveClusterConfigRepository getReadWriteLiveClusterConfigRepository(String cluster) {
    return clusterToLiveClusterConfigRepo.computeIfAbsent(cluster, clusterName -> {
      HelixReadWriteLiveClusterConfigRepository clusterConfigRepository =
          new HelixReadWriteLiveClusterConfigRepository(zkClient, adapterSerializer, clusterName);
      clusterConfigRepository.refresh();
      return clusterConfigRepository;
    });
  }

  public interface StoreMetadataOperation {
    /**
     * define the operation that update a store. Return the store after metadata being updated so that it could
     * be updated by metadataRepository
     */
    Store update(Store store);
  }

  /**
   * This function is used to detect whether current node is the leader controller of controller cluster.
   *
   * Be careful to use this function since it will talk to Zookeeper directly every time.
   *
   * @return
   */
  @Override
  public boolean isLeaderControllerOfControllerCluster() {
    if (isControllerClusterHAAS) {
      return isLeaderControllerFor(coloLeaderClusterName);
    }
    LiveInstance leader =
        helixManager.getHelixDataAccessor().getProperty(controllerClusterKeyBuilder.controllerLeader());
    if (leader == null || leader.getId() == null) {
      LOGGER.warn("Cannot determine the controller cluster leader or leader id");
      return false;
    }
    return leader.getId().equals(this.controllerName);
  }

  /**
   * Update "migrationDestCluster" and "migrationSrcCluster" fields of the "/storeConfigs/storeName" znode.
   * @param storeName name of the store.
   * @param srcClusterName name of the source cluster.
   * @param destClusterName name of the destination cluster.
   */
  public void setStoreConfigForMigration(String storeName, String srcClusterName, String destClusterName) {
    ZkStoreConfigAccessor storeConfigAccessor = getHelixVeniceClusterResources(srcClusterName).getStoreConfigAccessor();
    StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
    storeConfig.setMigrationSrcCluster(srcClusterName);
    storeConfig.setMigrationDestCluster(destClusterName);
    Store store = getStore(srcClusterName, storeName);
    storeConfigAccessor.updateConfig(storeConfig, store.isStoreMetaSystemStoreEnabled());
  }

  /**
   * @see Admin#updateAclForStore(String, String, String)
   */
  @Override
  public void updateAclForStore(String clusterName, String storeName, String accessPermissions) {
    throw new VeniceUnsupportedOperationException("updateAclForStore is not supported in child controller!");
  }

  /**
   * @see Admin#getAclForStore(String, String)
   */
  @Override
  public String getAclForStore(String clusterName, String storeName) {
    throw new VeniceUnsupportedOperationException("getAclForStore is not supported!");
  }

  /**
   * @see Admin#deleteAclForStore(String, String)
   */
  @Override
  public void deleteAclForStore(String clusterName, String storeName) {
    throw new VeniceUnsupportedOperationException("deleteAclForStore is not supported!");
  }

  /**
   * @see Admin#configureActiveActiveReplication(String, VeniceUserStoreType, Optional, boolean, Optional)
   */
  @Override
  public void configureActiveActiveReplication(
      String clusterName,
      VeniceUserStoreType storeType,
      Optional<String> storeName,
      boolean enableActiveActiveReplicationForCluster,
      Optional<String> regionsFilter) {
    /**
     * Check whether the command affects this fabric.
     */
    if (regionsFilter.isPresent()) {
      Set<String> fabrics = parseRegionsFilterList(regionsFilter.get());
      if (!fabrics.contains(multiClusterConfigs.getRegionName())) {
        LOGGER.info(
            "EnableActiveActiveReplicationForCluster command will be skipped for cluster: {}, because the "
                + "fabrics filter is {} which doesn't include the current fabric: {}",
            clusterName,
            fabrics,
            multiClusterConfigs.getRegionName());
        return;
      }

    }

    if (storeName.isPresent()) {
      /**
       * Legacy stores venice_system_store_davinci_push_status_store_<cluster_name> still exist.
       * But {@link HelixReadOnlyStoreRepositoryAdapter#getStore(String)} cannot find
       * them by store names. Skip davinci push status stores until legacy znodes are cleaned up.
       */
      VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName.get());
      if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE)) {
        LOGGER.info("Will not enable active active replication for davinci push status store: {}", storeName.get());
        return;
      }

      /**
       * The function is invoked by {@link AdminExecutionTask} if the
       * storeName is present.
       */
      Store originalStore = getStore(clusterName, storeName.get());
      if (originalStore == null) {
        throw new VeniceNoStoreException(storeName.get(), clusterName);
      }
      boolean shouldUpdateActiveActiveReplication = false;
      switch (storeType) {
        case BATCH_ONLY:
          shouldUpdateActiveActiveReplication =
              !originalStore.isHybrid() && !originalStore.isIncrementalPushEnabled() && !originalStore.isSystemStore();
          break;
        case HYBRID_ONLY:
          shouldUpdateActiveActiveReplication =
              originalStore.isHybrid() && !originalStore.isIncrementalPushEnabled() && !originalStore.isSystemStore();
          break;
        case INCREMENTAL_PUSH:
          shouldUpdateActiveActiveReplication =
              originalStore.isIncrementalPushEnabled() && !originalStore.isSystemStore();
          break;
        case HYBRID_OR_INCREMENTAL:
          shouldUpdateActiveActiveReplication =
              (originalStore.isHybrid() || originalStore.isIncrementalPushEnabled()) && !originalStore.isSystemStore();
          break;
        case SYSTEM:
          shouldUpdateActiveActiveReplication = originalStore.isSystemStore();
          break;
        case ALL:
          shouldUpdateActiveActiveReplication = true;
          break;
        default:
          break;
      }
      if (shouldUpdateActiveActiveReplication) {
        LOGGER.info("Will enable active active replication for store: {}", storeName.get());
        setActiveActiveReplicationEnabled(clusterName, storeName.get(), enableActiveActiveReplicationForCluster);
      } else {
        LOGGER.info("Will not enable active active replication for store: {}", storeName.get());
      }
    } else {
      /**
       * The batch update command hits child controller directly; all stores in the cluster will be updated
       */
      List<Store> storesToBeConfigured;
      switch (storeType) {
        case BATCH_ONLY:
          storesToBeConfigured = getAllStores(clusterName).stream()
              .filter(s -> (!s.isHybrid() && !s.isIncrementalPushEnabled()))
              .collect(Collectors.toList());
          break;
        case HYBRID_ONLY:
          storesToBeConfigured = getAllStores(clusterName).stream()
              .filter(s -> (s.isHybrid() && !s.isIncrementalPushEnabled()))
              .collect(Collectors.toList());
          break;
        case INCREMENTAL_PUSH:
          storesToBeConfigured =
              getAllStores(clusterName).stream().filter(Store::isIncrementalPushEnabled).collect(Collectors.toList());
          break;
        case HYBRID_OR_INCREMENTAL:
          storesToBeConfigured = getAllStores(clusterName).stream()
              .filter(store -> (store.isHybrid() || store.isIncrementalPushEnabled()))
              .collect(Collectors.toList());
          break;
        case SYSTEM:
          storesToBeConfigured =
              getAllStores(clusterName).stream().filter(Store::isSystemStore).collect(Collectors.toList());
          break;
        case ALL:
          storesToBeConfigured = getAllStores(clusterName);
          break;
        default:
          throw new VeniceException("Unsupported store type." + storeType);
      }

      storesToBeConfigured = storesToBeConfigured.stream()
          .filter(
              store -> !((VeniceSystemStoreType.getSystemStoreType(store.getName()) != null)
                  && (VeniceSystemStoreType.getSystemStoreType(store.getName()).isStoreZkShared())))
          .collect(Collectors.toList());
      storesToBeConfigured.forEach(store -> {
        LOGGER.info("Will enable active active replication for store: {}", store.getName());
        setActiveActiveReplicationEnabled(clusterName, store.getName(), enableActiveActiveReplicationForCluster);
      });
    }
  }

  /**
   * @return a list of <code>StoreInfo</code> of all stores in the specified cluster.
   */
  @Override
  public ArrayList<StoreInfo> getClusterStores(String clusterName) {
    // Return all stores at this step in the process
    return (ArrayList<StoreInfo>) getAllStores(clusterName).stream()
        /**
         * This filtering should not be needed because {@link #getAllStores(String)} should not return any null items,
         * but just in case, we have it here as defensive code...
         */
        .filter(Objects::nonNull)
        .map(StoreInfo::fromStore)
        .collect(Collectors.toList());
  }

  @Override
  public Map<String, StoreDataAudit> getClusterStaleStores(String clusterName) {
    throw new UnsupportedOperationException("This function has not been implemented.");
  }

  /**
   * - intermediary between {@link LogCompactionService} and {@link CompactionManager}
   * - injects the child controller's {@link ControllerClient} into the function {@link CompactionManager#getStoresForCompaction(String, Map)}
   * - serves as API endpoint to query stores ready for log compaction
   * @param clusterName
   * @return a list of <code>StoreInfo</code> of stores in clusterName that are ready for log compaction.
   */
  @Override
  public List<StoreInfo> getStoresForCompaction(String clusterName) {
    Assert.isTrue(
        multiClusterConfigs.getControllerConfig(clusterName).isLogCompactionEnabled(),
        "[log-compaction] Log compaction is not enabled for this cluster!");
    try {
      Map<String, ControllerClient> childControllers = getControllerClientMap(clusterName);
      return compactionManager.getStoresForCompaction(clusterName, childControllers);
    } catch (Exception e) {
      throw new VeniceException("Something went wrong trying to fetch stores for compaction.", e);
    }
  }

  /**
   * triggers repush for storeName for log compaction of store topic
   * <p>
   * - intermediary between {@link LogCompactionService} and
   * {@link CompactionManager} - serves as API endpoint to trigger scheduled & adhoc log compaction
   *
   * @param repushJobRequest@return
   */
  @Override
  public RepushJobResponse repushStore(RepushJobRequest repushJobRequest) throws Exception {
    Assert.isTrue(
        getMultiClusterConfigs().getControllerConfig(repushJobRequest.getClusterName()).isLogCompactionEnabled(),
        "[log-compaction] Log compaction is not enabled for this cluster!");
    try {
      RepushJobResponse response = getCompactionManager().repushStore(repushJobRequest);
      getLogCompactionStatsMap().get(repushJobRequest.getClusterName())
          .recordRepushStoreCall(
              repushJobRequest.getStoreName(),
              repushJobRequest.getTriggerSource(),
              response.isError() ? VeniceResponseStatusCategory.FAIL : VeniceResponseStatusCategory.SUCCESS);
      return response;
    } catch (Exception e) {
      LOGGER.error("[log-compaction] Error while triggering repush for store: {}", repushJobRequest.getStoreName(), e);
      getLogCompactionStatsMap().get(repushJobRequest.getClusterName())
          .recordRepushStoreCall(
              repushJobRequest.getStoreName(),
              repushJobRequest.getTriggerSource(),
              VeniceResponseStatusCategory.FAIL);
      throw e; // this method is the first common point for scheduled & adhoc log compaction, each has different error
    }
  }

  @VisibleForTesting
  Map<String, LogCompactionStats> getLogCompactionStatsMap() {
    return logCompactionStatsMap;
  }

  // for testing
  @Override
  public CompactionManager getCompactionManager() {
    return compactionManager;
  }

  @Override
  public Map<String, RegionPushDetails> listStorePushInfo(
      String clusterName,
      String storeName,
      boolean isPartitionDetailEnabled) {
    throw new UnsupportedOperationException("This function has not been implemented.");
  }

  public void preFetchDeadStoreStats(String clusterName, List<StoreInfo> storeInfos) {
    checkControllerLeadershipFor(clusterName);
    deadStoreStatsMap.get(clusterName).preFetchStats(storeInfos);
  }

  /**
   * @see Admin#getDeadStores(String, String, boolean)
   */
  @Override
  public List<StoreInfo> getDeadStores(String clusterName, String storeName, boolean includeSystemStores) {
    checkControllerLeadershipFor(clusterName);
    if (!multiClusterConfigs.getControllerConfig(clusterName).isDeadStoreEndpointEnabled()) {
      throw new VeniceUnsupportedOperationException("Dead store stats is not enabled.");
    }

    if (storeName == null) {
      List<StoreInfo> clusterStoreInfos = getAllStores(clusterName).stream()
          .filter(Objects::nonNull)
          .filter(store -> includeSystemStores || !store.isSystemStore())
          .map(StoreInfo::fromStore)
          .collect(Collectors.toList());
      return deadStoreStatsMap.get(clusterName).getDeadStores(clusterStoreInfos);
    } else {
      StoreInfo store = StoreInfo.fromStore(getStore(clusterName, storeName));
      if (store == null) {
        throw new VeniceNoStoreException(storeName, clusterName);
      }
      return deadStoreStatsMap.get(clusterName).getDeadStores(Collections.singletonList(store));
    }
  }

  /**
   * @return <code>RegionPushDetails</code> object containing the specified store's push status.
   */
  @Override
  public RegionPushDetails getRegionPushDetails(String clusterName, String storeName, boolean isPartitionDetailAdded) {
    StoreInfo store = StoreInfo.fromStore(getStore(clusterName, storeName));
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }

    RegionPushDetails ret = new RegionPushDetails();
    OfflinePushStatus zkData = retrievePushStatus(clusterName, store);

    for (StatusSnapshot status: zkData.getStatusHistory()) {
      if (shouldUpdateEndTime(ret, status)) {
        ret.setPushEndTimestamp(status.getTime());
      } else if (shouldUpdateStartTime(ret, status)) {
        ret.setPushStartTimestamp(status.getTime());
      } else if (status.getStatus().isError()) {
        ret.setErrorMessage(zkData.getStatusDetails());
        ret.setLatestFailedPush(status.getTime());
      }
    }

    for (Version v: store.getVersions()) {
      ret.addVersion(v.getNumber());
    }
    ret.setCurrentVersion(store.getCurrentVersion());
    if (isPartitionDetailAdded) {
      ret.addPartitionDetails(zkData);
    }
    return ret;
  }

  public OfflinePushStatus retrievePushStatus(String clusterName, StoreInfo store) {
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        clusterName,
        getZkClient(),
        getAdapterSerializer(),
        multiClusterConfigs.getLogContext(),
        multiClusterConfigs.getCommonConfig().getRefreshAttemptsForZkReconnect());

    Optional<Version> currentVersion = store.getVersion(store.getCurrentVersion());
    String kafkaTopic = currentVersion.isPresent() ? currentVersion.get().kafkaTopicName() : "";
    OfflinePushStatus status = accessor.getOfflinePushStatusAndItsPartitionStatuses(kafkaTopic);
    return status;
  }

  private boolean shouldUpdateStartTime(final RegionPushDetails curResult, final StatusSnapshot status) {
    LocalDateTime timestamp = LocalDateTime.parse(status.getTime());
    return status.getStatus() == ExecutionStatus.STARTED && (curResult.getPushStartTimestamp() == null
        || timestamp.isBefore(LocalDateTime.parse(curResult.getPushStartTimestamp())));
  }

  private boolean shouldUpdateEndTime(final RegionPushDetails curResult, final StatusSnapshot status) {
    LocalDateTime timestamp = LocalDateTime.parse(status.getTime());
    return status.getStatus() == ExecutionStatus.COMPLETED && (curResult.getPushEndTimestamp() == null
        || timestamp.isAfter(LocalDateTime.parse(curResult.getPushEndTimestamp())));
  }

  /**
   * @see Admin#checkResourceCleanupBeforeStoreCreation(String, String)
   */
  @Override
  public void checkResourceCleanupBeforeStoreCreation(String clusterName, String storeName) {
    checkResourceCleanupBeforeStoreCreation(clusterName, storeName, true);
  }

  /**
   * Delete stores from the cluster including both store data and metadata.
   * <p>The API provides the flexibility to delete a single store or a single version.
   * Cluster name and fabric are required parameters, but store name and version number are optional.
   * If store name is empty, all stores in the cluster are deleted.
   * @param clusterName name of the Venice cluster.
   * @param fabric name of the fabric.
   * @param storeName name of the to be deleted store, if value is absent, all stores in the cluster are deleted.
   * @param versionNum the number of the version to be deleted, if present, only the specified version is deleted.
   */
  @Override
  public void wipeCluster(String clusterName, String fabric, Optional<String> storeName, Optional<Integer> versionNum) {
    checkControllerLeadershipFor(clusterName);
    checkCurrentFabricMatchesExpectedFabric(fabric);
    if (!isClusterWipeAllowed(clusterName)) {
      throw new VeniceException("Current fabric " + fabric + " does not allow cluster wipe");
    }
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    if (storeName.isPresent()) {
      if (versionNum.isPresent()) {
        deleteOneStoreVersion(clusterName, storeName.get(), versionNum.get(), true);
      } else {
        setStoreReadWriteability(clusterName, storeName.get(), false);
        deleteStore(clusterName, storeName.get(), Store.IGNORE_VERSION, false, true, false);
      }
    } else {
      try (AutoCloseableLock ignore = resources.getClusterLockManager().createClusterWriteLock()) {
        for (Store store: resources.getStoreMetadataRepository().getAllStores()) {
          // Do not delete system stores as some are initialized when controller starts and will not be copied from
          // source fabric
          if (store.isSystemStore()) {
            continue;
          }
          setStoreReadWriteability(clusterName, store.getName(), false);
          deleteStore(clusterName, store.getName(), Store.IGNORE_VERSION, false, true, false);
        }
      }
    }
  }

  /**
   * @see Admin#compareStore(String, String, String, String)
   */
  @Override
  public StoreComparisonInfo compareStore(String clusterName, String storeName, String fabricA, String fabricB) {
    throw new VeniceUnsupportedOperationException("compareStore is not supported in child controller!");
  }

  /**
   * @see Admin#copyOverStoreSchemasAndConfigs(String, String, String, String)
   */
  @Override
  public StoreInfo copyOverStoreSchemasAndConfigs(
      String clusterName,
      String srcFabric,
      String destFabric,
      String storeName) {
    throw new VeniceUnsupportedOperationException(
        "copyOverStoreSchemasAndConfigs is not supported in child controller!");
  }

  ZkStoreConfigAccessor getStoreConfigAccessor(String clusterName) {
    return getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor();
  }

  ReadWriteStoreRepository getMetadataRepository(String clusterName) {
    return getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
  }

  void checkResourceCleanupBeforeStoreCreation(String clusterName, String storeName, boolean checkHelixResource) {
    checkControllerLeadershipFor(clusterName);
    ZkStoreConfigAccessor storeConfigAccess = getStoreConfigAccessor(clusterName);
    StoreConfig storeConfig = storeConfigAccess.getStoreConfig(storeName);
    if (storeConfig != null) {
      throw new VeniceException("Store: " + storeName + " still exists in cluster: " + storeConfig.getCluster());
    }
    ReadWriteStoreRepository repository = getMetadataRepository(clusterName);
    Store store = repository.getStore(storeName);
    if (store != null) {
      throw new VeniceException("Store: " + storeName + " still exists in cluster: " + clusterName);
    }

    /**
     * All store version topics from before deletion will not be used since we will create a new version number
     * that has not been used previously. We let TopicCleanupService take care of the cleanups.
     *
     * The version check skip was introduced when Venice was running with Kafka MirrorMaker to avoid a KMM crash.
     * TODO: Evaluate if this code needs to change now that KMM logic has been deprecated.
     *
     * So for topic check, we ensure RT topics for the Venice store and its system stores are deleted.
     */
    checkKafkaTopicAndHelixResource(clusterName, storeName, false, checkHelixResource, false);
  }

  void checkKafkaTopicAndHelixResource(
      String clusterName,
      String storeName,
      boolean checkVersionTopic,
      boolean checkHelixResource,
      boolean checkOfflinePush) {
    Set<String> allRelevantStores = Arrays.stream(VeniceSystemStoreType.values())
        .filter(VeniceSystemStoreType::isStoreZkShared)
        .map(s -> s.getSystemStoreName(storeName))
        .collect(Collectors.toSet());
    allRelevantStores.add(storeName);

    // Check Kafka topics belonging to this store.
    Set<PubSubTopic> topics = getTopicManager().listTopics();
    topics.forEach(topic -> {
      String storeNameForTopic = null;
      if (topic.isRealTime()) {
        storeNameForTopic = topic.getStoreName();
      } else if (checkVersionTopic) {
        storeNameForTopic = topic.getStoreName();
      }
      if (storeNameForTopic != null && allRelevantStores.contains(storeNameForTopic)) {
        throw new ResourceStillExistsException(
            "Topic: " + topic + " still exists for store: " + storeName + ", please make sure all "
                + (checkVersionTopic ? "" : "real-time ") + "topics are removed.");
      }
    });
    // Check all helix resources.
    if (checkHelixResource) {
      List<String> helixAliveResources = getAllLiveHelixResources(clusterName);
      helixAliveResources.forEach(resource -> {
        if (Version.isVersionTopic(resource)) {
          String storeNameForResource = Version.parseStoreFromVersionTopic(resource);
          if (allRelevantStores.contains(storeNameForResource)) {
            throw new ResourceStillExistsException(
                "Helix Resource: " + resource + " still exists for store: " + storeName
                    + ", please make sure all helix resources are removed.");
          }
        }
      });
    }
    // Check all offline push zk nodes.
    if (checkOfflinePush) {
      VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
          clusterName,
          zkClient,
          adapterSerializer,
          multiClusterConfigs.getLogContext(),
          multiClusterConfigs.getCommonConfig().getRefreshAttemptsForZkReconnect());
      List<String> offlinePushes = zkClient.getChildren(accessor.getOfflinePushStatuesParentPath());
      offlinePushes.forEach(resource -> {
        if (Version.isVersionTopic(resource)) {
          String storeNameForResource = Version.parseStoreFromVersionTopic(resource);
          if (allRelevantStores.contains(storeNameForResource)) {
            throw new ResourceStillExistsException(
                "Offline push: " + resource + " still exists for store: " + storeName
                    + ", please make sure all offline push nodes are removed.");
          }
        }
      });
    }
  }

  Store checkPreConditionForAclOp(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    Store store = repository.getStore(storeName);
    if (store == null) {
      throwStoreDoesNotExist(clusterName, storeName);
    }
    return store;
  }

  /**
   * A store is not hybrid in the following two scenarios:
   * If hybridStoreConfig is null, it means store is not hybrid.
   * If all the hybrid config values are negative, it indicates that the store is being set back to batch-only store.
   */
  boolean isHybrid(HybridStoreConfig hybridStoreConfig) {
    return hybridStoreConfig != null
        && (hybridStoreConfig.getRewindTimeInSeconds() >= 0 || hybridStoreConfig.getOffsetLagThresholdToGoOnline() >= 0
            || hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds() >= 0);
  }

  /**
   * @see VeniceHelixAdmin#isHybrid(HybridStoreConfig)
   */
  boolean isHybrid(HybridStoreConfigRecord hybridStoreConfigRecord) {
    HybridStoreConfig hybridStoreConfig = null;
    if (hybridStoreConfigRecord != null) {
      hybridStoreConfig = new HybridStoreConfigImpl(
          hybridStoreConfigRecord.rewindTimeInSeconds,
          hybridStoreConfigRecord.offsetLagThresholdToGoOnline,
          hybridStoreConfigRecord.producerTimestampLagThresholdToGoOnlineInSeconds,
          DataReplicationPolicy.valueOf(hybridStoreConfigRecord.dataReplicationPolicy),
          BufferReplayPolicy.valueOf(hybridStoreConfigRecord.bufferReplayPolicy),
          hybridStoreConfigRecord.realTimeTopicName.toString());
    }
    return isHybrid(hybridStoreConfig);
  }

  /**
   * @see Admin#isParent()
   */
  @Override
  public boolean isParent() {
    return multiClusterConfigs.isParent();
  }

  /**
   * @see Admin#getParentControllerRegionState()
   */
  @Override
  public ParentControllerRegionState getParentControllerRegionState() {
    return multiClusterConfigs.getParentControllerRegionState();
  }

  /**
   * @see Admin#getChildDataCenterControllerUrlMap(String)
   */
  @Override
  public Map<String, String> getChildDataCenterControllerUrlMap(String clusterName) {
    /**
     * According to {@link VeniceControllerClusterConfig#VeniceControllerClusterConfig(VeniceProperties)}, the map is empty
     * if this is a child controller.
     */
    return multiClusterConfigs.getControllerConfig(clusterName).getChildDataCenterControllerUrlMap();
  }

  /**
   * @see Admin#getChildDataCenterControllerD2Map(String)
   */
  @Override
  public Map<String, String> getChildDataCenterControllerD2Map(String clusterName) {
    return multiClusterConfigs.getControllerConfig(clusterName).getChildDataCenterControllerD2Map();
  }

  /**
   * @see Admin#getChildControllerD2ServiceName(String)
   */
  @Override
  public String getChildControllerD2ServiceName(String clusterName) {
    return multiClusterConfigs.getControllerConfig(clusterName).getD2ServiceName();
  }

  /**
   * @see Admin#getReadOnlyZKSharedSystemStoreRepository()
   */
  @Override
  public HelixReadOnlyZKSharedSystemStoreRepository getReadOnlyZKSharedSystemStoreRepository() {
    return zkSharedSystemStoreRepository;
  }

  /**
   * @see Admin#getReadOnlyZKSharedSchemaRepository()
   */
  @Override
  public HelixReadOnlyZKSharedSchemaRepository getReadOnlyZKSharedSchemaRepository() {
    return zkSharedSchemaRepository;
  }

  /**
   * @see Admin#getMetaStoreWriter()
   */
  @Override
  public MetaStoreWriter getMetaStoreWriter() {
    return metaStoreWriter;
  }

  @Override
  public MetaStoreReader getMetaStoreReader() {
    return metaStoreReader;
  }

  /**
   * @see Admin#getEmergencySourceRegion(String)
   */
  @Override
  public Optional<String> getEmergencySourceRegion(@Nonnull String clusterName) {
    String emergencySourceRegion = multiClusterConfigs.getEmergencySourceRegion(clusterName);
    if (StringUtils.isNotEmpty(emergencySourceRegion)) {
      return Optional.of(emergencySourceRegion);
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see Admin#getAggregateRealTimeTopicSource(String)
   */
  @Override
  public Optional<String> getAggregateRealTimeTopicSource(String clusterName) {
    String sourceRegion = multiClusterConfigs.getControllerConfig(clusterName).getAggregateRealTimeSourceRegion();
    if (!StringUtils.isEmpty(sourceRegion)) {
      return Optional.of(getNativeReplicationKafkaBootstrapServerAddress(sourceRegion));
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see Admin#isActiveActiveReplicationEnabledInAllRegion(String, String, boolean)
   */
  @Override
  public boolean isActiveActiveReplicationEnabledInAllRegion(
      String clusterName,
      String storeName,
      boolean checkCurrentVersion) {
    throw new VeniceUnsupportedOperationException(
        "isActiveActiveReplicationEnabledInAllRegion is not supported in child controller!");
  }

  /**
   * @see Admin#getClustersLeaderOf()
   */
  @Override
  public List<String> getClustersLeaderOf() {
    List<String> clusters = new ArrayList<>();
    for (VeniceControllerStateModel model: controllerStateModelFactory.getAllModels()) {
      if (model.getCurrentState().equals(LeaderStandbySMD.States.LEADER.toString())) {
        clusters.add(model.getClusterName());
      }
    }
    return clusters;
  }

  /**
   * Return the topic creation time if it has not been persisted to Zk yet.
   * @param topic The topic whose creation time is needed.
   * @return the topic creation time if it has not yet persisted to Zk yet. 0 if topic information has persisted to Zk or if the topic doesn't exist.
   */
  Long getInMemoryTopicCreationTime(String topic) {
    return topicToCreationTime.get(topic);
  }

  private void setUpDaVinciPushStatusStore(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    Store store = repository.getStore(storeName);
    if (store == null) {
      throwStoreDoesNotExist(clusterName, storeName);
    }
    String daVinciPushStatusStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);

    if (!isParent()) {
      // We do not materialize PS3 for parent region. Hence, skip RT topic creation.
      ensureRealTimeTopicExistsForUserSystemStores(clusterName, daVinciPushStatusStoreName);
    }
    if (!store.isDaVinciPushStatusStoreEnabled()) {
      storeMetadataUpdate(clusterName, storeName, (s) -> {
        s.setDaVinciPushStatusStoreEnabled(true);
        return s;
      });
    }
  }

  /**
   * Set up the meta store and produce snapshot to meta store RT. Should be called in the child controllers.
   * @param clusterName The cluster name.
   * @param regularStoreName The regular user store name.
   */
  void setUpMetaStoreAndMayProduceSnapshot(String clusterName, String regularStoreName) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    Store store = repository.getStore(regularStoreName);
    if (store == null) {
      throwStoreDoesNotExist(clusterName, regularStoreName);
    }

    // Make sure RT topic exists before producing.
    if (!isParent()) {
      // We do not materialize meta store for parent region. Hence, skip RT topic creation.
      ensureRealTimeTopicExistsForUserSystemStores(
          clusterName,
          VeniceSystemStoreType.META_STORE.getSystemStoreName(regularStoreName));
    }

    // Update the store flag to enable meta system store.
    if (!store.isStoreMetaSystemStoreEnabled()) {
      storeMetadataUpdate(clusterName, regularStoreName, (s) -> {
        s.setStoreMetaSystemStoreEnabled(true);
        return s;
      });
    }

    Optional<MetaStoreWriter> metaStoreWriter = getHelixVeniceClusterResources(clusterName).getMetaStoreWriter();
    if (!metaStoreWriter.isPresent()) {
      LOGGER.info(
          "MetaStoreWriter from VeniceHelixResource is absent, will skip producing snapshot to meta store RT for store: {}",
          regularStoreName);
      return;
    }
    // Get updated store
    store = repository.getStore(regularStoreName);
    // Local store properties
    metaStoreWriter.get().writeStoreProperties(clusterName, store);
    // Store cluster configs
    metaStoreWriter.get()
        .writeStoreClusterConfig(
            getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor().getStoreConfig(regularStoreName));
    LOGGER.info(
        "Wrote store property snapshot to meta system store for venice store: {} in cluster: {}",
        regularStoreName,
        clusterName);
    // Key/value schemas
    Collection<SchemaEntry> keySchemas = new HashSet<>();
    keySchemas.add(getKeySchema(clusterName, regularStoreName));
    metaStoreWriter.get().writeStoreKeySchemas(regularStoreName, keySchemas);
    LOGGER.info(
        "Wrote key schema to meta system store for venice store: " + regularStoreName + " in cluster: " + clusterName);
    Collection<SchemaEntry> valueSchemas = getValueSchemas(clusterName, regularStoreName);
    metaStoreWriter.get().writeStoreValueSchemas(regularStoreName, valueSchemas);
    LOGGER.info(
        "Wrote value schemas to meta system store for venice store: {} in cluster: {}",
        regularStoreName,
        clusterName);
  }

  private boolean isAmplificationFactorUpdateOnly(
      PartitionerConfig originalPartitionerConfig,
      PartitionerConfig newPartitionerConfig) {
    if (newPartitionerConfig == null) {
      throw new VeniceException(
          "New partitioner config is null, in theory it will never happen as we should pre-fill new partitioner config.");
    }
    // We verify if only amp factor is changed by updating original partition config's amp factor and compare it with
    // given partitioner config.
    PartitionerConfig ampFactorUpdatedPartitionerConfig =
        originalPartitionerConfig == null ? new PartitionerConfigImpl() : originalPartitionerConfig.clone();
    ampFactorUpdatedPartitionerConfig.setAmplificationFactor(newPartitionerConfig.getAmplificationFactor());
    return Objects.equals(ampFactorUpdatedPartitionerConfig, newPartitionerConfig);
  }

  /**
   * @see Admin#getBackupVersionDefaultRetentionMs()
   */
  @Override
  public long getBackupVersionDefaultRetentionMs() {
    return backupVersionDefaultRetentionMs;
  }

  /** @see Admin#getDefaultMaxRecordSizeBytes() */
  @Override
  public int getDefaultMaxRecordSizeBytes() {
    return defaultMaxRecordSizeBytes;
  }

  private Pair<NodeReplicasReadinessState, List<Replica>> areAllCurrentVersionReplicasReady(
      HelixCustomizedViewOfflinePushRepository customizedViewRepo,
      ReadWriteStoreRepository storeRepo,
      String nodeId) {
    List<Replica> unreadyReplicas = new ArrayList<>();
    List<Replica> localReplicas = Utils.getReplicasForInstance(customizedViewRepo, nodeId);

    ResourceAssignment resourceAssn = customizedViewRepo.getResourceAssignment();
    for (Replica replica: localReplicas) {
      // Skip if replica is a stale version.
      if (!Utils.isCurrentVersion(replica.getResource(), storeRepo)) {
        continue;
      }

      List<Instance> readyToServeInstances = customizedViewRepo.getReadyToServeInstances(
          resourceAssn.getPartitionAssignment(replica.getResource()),
          replica.getPartitionId());

      // A current replica is unready if its running instance is unready and it is not an extra replica.
      if (!readyToServeInstances.contains(replica.getInstance())
          && !Utils.isExtraReplica(storeRepo, replica, readyToServeInstances)) {
        unreadyReplicas.add(replica);
      }
    }
    return new Pair<>(
        unreadyReplicas.isEmpty() ? NodeReplicasReadinessState.READY : NodeReplicasReadinessState.UNREADY,
        unreadyReplicas);
  }

  /**
   * @see Admin#nodeReplicaReadiness(String, String)
   */
  @Override
  public Pair<NodeReplicasReadinessState, List<Replica>> nodeReplicaReadiness(String cluster, String helixNodeId) {
    checkControllerLeadershipFor(cluster);
    List<String> instances = helixAdminClient.getInstancesInCluster(cluster);
    HelixCustomizedViewOfflinePushRepository customizedViewRepo =
        getHelixVeniceClusterResources(cluster).getCustomizedViewRepository();
    ReadWriteStoreRepository storeRepo = getHelixVeniceClusterResources(cluster).getStoreMetadataRepository();

    if (!instances.contains(helixNodeId)) {
      throw new VeniceException("Node: " + helixNodeId + " is not in the cluster: " + cluster);
    }

    if (!HelixUtils.isLiveInstance(cluster, helixNodeId, getHelixVeniceClusterResources(cluster).getHelixManager())) {
      return new Pair<>(NodeReplicasReadinessState.INANIMATE, Collections.emptyList());
    }
    return areAllCurrentVersionReplicasReady(customizedViewRepo, storeRepo, helixNodeId);
  }

  private void checkCurrentFabricMatchesExpectedFabric(String fabric) {
    if (!multiClusterConfigs.getRegionName().equals(fabric)) {
      throw new VeniceException(
          "Current fabric: " + multiClusterConfigs.getRegionName() + " does not match with request parameter fabric: "
              + fabric);
    }
  }

  /**
   * @see Admin#initiateDataRecovery(String, String, int, String, String, boolean, Optional)
   */
  @Override
  public void initiateDataRecovery(
      String clusterName,
      String storeName,
      int version,
      String sourceFabric,
      String destinationFabric,
      boolean copyAllVersionConfigs,
      Optional<Version> sourceFabricVersion) {
    RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
        () -> checkControllerLeadershipFor(clusterName),
        1,
        Duration.ofMillis(100),
        Duration.ofMillis(200),
        Duration.ofSeconds(10),
        RETRY_FAILURE_TYPES);
    checkCurrentFabricMatchesExpectedFabric(destinationFabric);
    if (!sourceFabricVersion.isPresent()) {
      throw new VeniceException("Source fabric version object is required for data recovery");
    }
    dataRecoveryManager.verifyStoreVersionIsReadyForDataRecovery(clusterName, storeName, version);
    dataRecoveryManager.initiateDataRecovery(
        clusterName,
        storeName,
        version,
        sourceFabric,
        copyAllVersionConfigs,
        sourceFabricVersion.get());
  }

  /**
   * @see Admin#prepareDataRecovery(String, String, int, String, String, Optional)
   */
  @Override
  public void prepareDataRecovery(
      String clusterName,
      String storeName,
      int version,
      String sourceFabric,
      String destinationFabric,
      Optional<Integer> sourceAmplificationFactor) {
    checkControllerLeadershipFor(clusterName);
    checkCurrentFabricMatchesExpectedFabric(destinationFabric);
    dataRecoveryManager.prepareStoreVersionForDataRecovery(clusterName, storeName, destinationFabric, version);
  }

  /**
   * @see Admin#isStoreVersionReadyForDataRecovery(String, String, int, String, String, Optional)
   */
  @Override
  public Pair<Boolean, String> isStoreVersionReadyForDataRecovery(
      String clusterName,
      String storeName,
      int version,
      String sourceFabric,
      String destinationFabric,
      Optional<Integer> sourceAmplificationFactor) {
    checkControllerLeadershipFor(clusterName);
    boolean isReady = true;
    String reason = "";
    try {
      checkCurrentFabricMatchesExpectedFabric(destinationFabric);
      dataRecoveryManager.verifyStoreVersionIsReadyForDataRecovery(clusterName, storeName, version);
    } catch (Exception e) {
      isReady = false;
      reason = e.getMessage();
    }
    return new Pair<>(isReady, reason);
  }

  /**
   * @see Admin#isAdminTopicConsumptionEnabled(String)
   */
  @Override
  public boolean isAdminTopicConsumptionEnabled(String clusterName) {
    if (!isLeaderControllerFor(clusterName)) {
      // Defensive code: disable admin topic consumption on standby controllers
      return false;
    }
    if (isParent()) {
      return true;
    }
    boolean adminTopicConsumptionEnabled;
    // HelixVeniceClusterResources should exist on leader controller
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createClusterReadLock()) {
      HelixReadWriteLiveClusterConfigRepository liveConfigRepository =
          getReadWriteLiveClusterConfigRepository(clusterName);
      // Enable child controller admin topic consumption when configs applied at startup and live config are true.
      // The live configs are used during store migration.
      adminTopicConsumptionEnabled = liveConfigRepository.getConfigs().isChildControllerAdminTopicConsumptionEnabled()
          && multiClusterConfigs.getControllerConfig(clusterName).isMultiRegion();
    }
    return adminTopicConsumptionEnabled;
  }

  @Override
  public int getLargestUsedVersionFromStoreGraveyard(String clusterName, String storeName) {
    return getStoreGraveyard().getLargestUsedVersionNumber(storeName);
  }

  @Override
  public int getLargestUsedVersion(String clusterName, String storeName) {
    Store store = getStore(clusterName, storeName);
    // If the store does not exist, check the store graveyard.
    if (store == null) {
      return getStoreGraveyard().getLargestUsedVersionNumber(storeName);
    }
    return Math.max(store.getLargestUsedVersionNumber(), getStoreGraveyard().getLargestUsedVersionNumber(storeName));
  }

  /**
   * @see StoragePersonaRepository#addPersona(String, long, Set, Set)
   */
  @Override
  public void createStoragePersona(
      String clusterName,
      String name,
      long quotaNumber,
      Set<String> storesToEnforce,
      Set<String> owners) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try {
      StoragePersonaRepository repository = resources.getStoragePersonaRepository();
      repository.addPersona(name, quotaNumber, storesToEnforce, owners);
    } catch (Exception e) {
      LOGGER.error("Failed to execute CreateStoragePersonaOperation.", e);
      throw e;
    }
  }

  /**
   * @see StoragePersonaRepository#getPersona(String)
   */
  @Override
  public StoragePersona getStoragePersona(String clusterName, String name) {
    checkControllerLeadershipFor(clusterName);
    StoragePersonaRepository repository = getHelixVeniceClusterResources(clusterName).getStoragePersonaRepository();
    return repository.getPersona(name);
  }

  /**
   * @see StoragePersonaRepository#deletePersona(String)
   */
  @Override
  public void deleteStoragePersona(String clusterName, String name) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try {
      StoragePersonaRepository repository = resources.getStoragePersonaRepository();
      repository.deletePersona(name);
    } catch (Exception e) {
      LOGGER.error("Failed to execute DeleteStoragePersonaOperation.", e);
      throw e;
    }
  }

  /**
   * @see StoragePersonaRepository#updatePersona(String, UpdateStoragePersonaQueryParams)
   */
  @Override
  public void updateStoragePersona(String clusterName, String name, UpdateStoragePersonaQueryParams queryParams) {
    checkControllerLeadershipFor(clusterName);
    HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
    try {
      StoragePersonaRepository repository = resources.getStoragePersonaRepository();
      repository.updatePersona(name, queryParams);
    } catch (Exception e) {
      LOGGER.error("Failed to execute UpdateStoragePersonaOperation.", e);
      throw e;
    }
  }

  /**
   * @see StoragePersonaRepository#getPersonaContainingStore(String)
   */
  @Override
  public StoragePersona getPersonaAssociatedWithStore(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    StoragePersonaRepository repository = getHelixVeniceClusterResources(clusterName).getStoragePersonaRepository();
    return repository.getPersonaContainingStore(storeName);
  }

  @Override
  public List<StoragePersona> getClusterStoragePersonas(String clusterName) {
    checkControllerLeadershipFor(clusterName);
    StoragePersonaRepository repository = getHelixVeniceClusterResources(clusterName).getStoragePersonaRepository();
    return repository.getAllPersonas();
  }

  @Override
  public List<String> cleanupInstanceCustomizedStates(String clusterName) {
    checkControllerLeadershipFor(clusterName);
    ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    List<String> deletedZNodes = new ArrayList<>();
    Set<String> irrelevantStoreVersions = new HashSet<>();
    ZkBaseDataAccessor<ZNRecord> znRecordAccessor = new ZkBaseDataAccessor<>(zkClient);
    String instancesZkPath = HelixUtils.getHelixClusterZkPath(clusterName) + "/" + ZK_INSTANCES_SUB_PATH;
    List<String> instances = znRecordAccessor.getChildNames(instancesZkPath, AccessOption.PERSISTENT);
    for (String instance: instances) {
      String instanceCustomizedStatesPath = instancesZkPath + "/" + instance + "/" + ZK_CUSTOMIZEDSTATES_SUB_PATH;
      List<String> storeVersions =
          znRecordAccessor.getChildNames(instanceCustomizedStatesPath, AccessOption.PERSISTENT);
      if (storeVersions != null) {
        for (String storeVersion: storeVersions) {
          boolean delete = irrelevantStoreVersions.contains(storeVersion);
          if (!delete) {
            // Check if the store version is still relevant
            try {
              Store store = repository.getStoreOrThrow(Version.parseStoreFromVersionTopic(storeVersion));
              if (store.getVersion(Version.parseVersionFromKafkaTopicName(storeVersion)) == null) {
                irrelevantStoreVersions.add(storeVersion);
                delete = true;
              }
            } catch (VeniceNoStoreException e) {
              irrelevantStoreVersions.add(storeVersion);
              delete = true;
            }
          }
          if (delete) {
            String deleteInstanceCustomizedStatePath = instanceCustomizedStatesPath + "/" + storeVersion;
            HelixUtils.remove(znRecordAccessor, deleteInstanceCustomizedStatePath);
            LOGGER.info(
                "Deleted lingering instance level customized state ZNode: {} in cluster {}",
                deleteInstanceCustomizedStatePath,
                clusterName);
            deletedZNodes.add(deleteInstanceCustomizedStatePath);
          }
        }
      }
    }
    return deletedZNodes;
  }

  @Override
  public StoreGraveyard getStoreGraveyard() {
    return storeGraveyard;
  }

  @Override
  public void removeStoreFromGraveyard(String clusterName, String storeName) {
    checkControllerLeadershipFor(clusterName);
    checkKafkaTopicAndHelixResource(clusterName, storeName, true, true, true);
    storeGraveyard.removeStoreFromGraveyard(clusterName, storeName);
  }

  @Override
  public PushStatusStoreReader getPushStatusStoreReader() {
    return pushStatusStoreReader;
  }

  @Override
  public PushStatusStoreWriter getPushStatusStoreWriter() {
    return pushStatusStoreWriter.get();
  }

  @Override
  public void sendHeartbeatToSystemStore(String clusterName, String storeName, long heartbeatTimeStamp) {
    if (isParent()) {
      return;
    }
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    String userStoreName = systemStoreType.extractRegularStoreName(storeName);
    if (VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.equals(systemStoreType)) {
      getPushStatusStoreWriter().writeHeartbeat(userStoreName, heartbeatTimeStamp);
    } else {
      getMetaStoreWriter().writeHeartbeat(userStoreName, heartbeatTimeStamp);
    }
  }

  @Override
  public long getHeartbeatFromSystemStore(String clusterName, String systemStoreName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(systemStoreName);
    String userStoreName = systemStoreType.extractRegularStoreName(systemStoreName);
    try {
      return RetryUtils.executeWithMaxRetriesAndFixedAttemptDuration(() -> {
        long retrievedTimestamp;
        if (systemStoreType == VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE) {
          retrievedTimestamp =
              getPushStatusStoreReader().getHeartbeat(userStoreName, CONTROLLER_HEARTBEAT_INSTANCE_NAME);
        } else {
          retrievedTimestamp = getMetaStoreReader().getHeartbeat(userStoreName);
        }
        return retrievedTimestamp;
      }, 3, Duration.ofSeconds(1), Collections.singletonList(VeniceException.class));
    } catch (VeniceException e) {
      return -1;
    }
  }

  @Override
  public void autoMigrateStore(
      String srcClusterName,
      String destClusterName,
      String storeName,
      Optional<Integer> currStep,
      Optional<Boolean> abortOnFailure) {
    checkControllerLeadershipFor(srcClusterName);
    Optional<MultiTaskSchedulerService> multiTaskSchedulerService =
        getHelixVeniceClusterResources(srcClusterName).getMultiTaskSchedulerService();
    if (multiTaskSchedulerService.isPresent()) {
      StoreMigrationManager storeMigrationManager = multiTaskSchedulerService.get().getStoreMigrationManager();
      storeMigrationManager.scheduleMigration(
          storeName,
          srcClusterName,
          destClusterName,
          currStep.orElse(0),
          0,
          abortOnFailure.orElse(false));
    } else {
      throw new VeniceException(
          "Store migration is not supported in this cluster: " + srcClusterName,
          ErrorType.INCORRECT_CONTROLLER);
    }
  }

  public Optional<SSLFactory> getSslFactory() {
    return sslFactory;
  }

  public boolean isClusterWipeAllowed(String clusterName) {
    return multiClusterConfigs.getControllerConfig(clusterName).isClusterWipeAllowed();
  }

  @VisibleForTesting
  VeniceControllerMultiClusterConfig getMultiClusterConfigs() {
    return multiClusterConfigs;
  }

  @Override
  public VeniceControllerClusterConfig getControllerConfig(String clusterName) {
    return multiClusterConfigs.getControllerConfig(clusterName);
  }

  // Only for testing
  public void setPushJobDetailsStoreClient(AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> client) {
    pushJobDetailsStoreClient = client;
  }

  @Override
  public PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }

  SafeHelixManager getHelixManager() {
    return helixManager;
  }

  String getControllerClusterName() {
    return controllerClusterName;
  }

  String getPushJobStatusStoreClusterName() {
    return pushJobStatusStoreClusterName;
  }

  InternalAvroSpecificSerializer<PushJobDetails> getPushJobDetailsSerializer() {
    return pushJobDetailsSerializer;
  }

  public LogContext getLogContext() {
    return logContext;
  }

  // visible for testing
  RealTimeTopicSwitcher getRealTimeTopicSwitcher() {
    return realTimeTopicSwitcher;
  }

  Map<String, D2Client> getD2Clients() {
    return d2Clients;
  }
}
