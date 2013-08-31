package org.infinispan.loaders.offheap.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractLockSupportStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.offheap.OffheapCacheStore;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * 
 */
@ConfigurationFor(OffheapCacheStore.class)
@BuiltBy(OffheapCacheStoreConfigurationBuilder.class)
public class OffheapCacheStoreConfiguration extends AbstractLockSupportStoreConfiguration {
   final private boolean compression;
   final private int expiryQueueSize;

   protected OffheapCacheStoreConfiguration(boolean compression, int expiryQueueSize,
         long lockAcquistionTimeout, int lockConcurrencyLevel, boolean purgeOnStartup, boolean purgeSynchronously,
         int purgerThreads, boolean fetchPersistentState, boolean ignoreModifications, TypedProperties properties, AsyncStoreConfiguration async,
         SingletonStoreConfiguration singletonStore) {
      super(lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, properties, async,
            singletonStore);

      this.compression = compression;
      this.expiryQueueSize = expiryQueueSize;
   }
   
   public boolean compression() {
      return compression;
   }

   public int expiryQueueSize() {
      return expiryQueueSize;
   }
}
