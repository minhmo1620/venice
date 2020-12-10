package com.linkedin.venice.helix;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import java.util.HashSet;
import java.util.Set;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;

public class SubscriptionBasedStoreRepository extends HelixReadOnlyStoreRepository implements SubscriptionBasedReadOnlyStoreRepository {
  private static final Logger logger = Logger.getLogger(HelixReadOnlyStoreRepository.class);

  private final Set<String> subscription = new HashSet<>();

  public SubscriptionBasedStoreRepository(ZkClient zkClient, HelixAdapterSerializer compositeSerializer, String clusterName) {
    super(zkClient, compositeSerializer, clusterName, 0, 0);
  }

  @Override
  public void subscribe(String storeName) throws InterruptedException {
    updateLock.lock();
    try {
      subscription.add(storeName);
      Store store = refreshOneStore(storeName);
      if (store == null) {
        subscription.remove(storeName);
        throw new VeniceNoStoreException(storeName, clusterName);
      }
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public void unsubscribe(String storeName) {
    updateLock.lock();
    try {
      if (!subscription.remove(storeName)) {
        throw new VeniceException("Cannot unsubscribe from not-subscribed store, storeName=" + storeName);
      }
      removeStore(storeName);
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  protected Store putStore(Store newStore) {
    updateLock.lock();
    try {
      if (subscription.contains(newStore.getName()) || VeniceSystemStoreUtils.isSystemStore(newStore.getName())) {
        return super.putStore(newStore);
      }
      logger.info("Ignoring not-subscribed store, storeName=" + newStore.getName());
      return null;
    } finally {
      updateLock.unlock();
    }
  }
}
