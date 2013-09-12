package org.infinispan.loaders.offheap.configuration;

import java.util.Properties;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.offheap.OffheapStore;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * 
 */
@ConfigurationFor(OffheapStore.class)
@BuiltBy(OffheapStoreConfigurationBuilder.class)
public class OffheapStoreConfiguration extends AbstractStoreConfiguration {
   final private boolean compression;
   final private int expiryQueueSize;
   
   public OffheapStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload, boolean shared, Properties properties,
         boolean compression, int expiryQueueSize) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
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
