package org.infinispan.loaders.offheap.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractLockSupportStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * 
 */
public class OffheapCacheStoreConfigurationBuilder extends AbstractLockSupportStoreConfigurationBuilder<OffheapCacheStoreConfiguration, OffheapCacheStoreConfigurationBuilder> {

   protected boolean compression = false;
   protected int expiryQueueSize = 10000;

   public OffheapCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }
   
   public OffheapCacheStoreConfigurationBuilder expiryQueueSize(int expiryQueueSize) {
      this.expiryQueueSize = expiryQueueSize;
      return self();
   }
   
   public OffheapCacheStoreConfigurationBuilder compression(boolean compression) {
      this.compression = compression;
      return self();
   }

   @Override
   public void validate() {
      // how do you validate required attributes?
      super.validate();
   }

   @Override
   public OffheapCacheStoreConfiguration create() {
      return new OffheapCacheStoreConfiguration(compression, expiryQueueSize,
            lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications,
            TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public Builder<?> read(OffheapCacheStoreConfiguration template) {
      compression = template.compression();
      expiryQueueSize = template.expiryQueueSize();
      
      // LockSupportStore-specific configuration
      lockAcquistionTimeout = template.lockAcquistionTimeout();
      lockConcurrencyLevel = template.lockConcurrencyLevel();

      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      purgeSynchronously = template.purgeSynchronously();
      this.async.read(template.async());
      this.singletonStore.read(template.singletonStore());

      return self();
   }

   @Override
   public OffheapCacheStoreConfigurationBuilder self() {
      return this;
   }

}
