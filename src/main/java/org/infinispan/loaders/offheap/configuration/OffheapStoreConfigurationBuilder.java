package org.infinispan.loaders.offheap.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * 
 */
public class OffheapStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<OffheapStoreConfiguration, OffheapStoreConfigurationBuilder> {

   protected boolean compression = false;
   protected int expiryQueueSize = 10000;

   public OffheapStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }
   
   public OffheapStoreConfigurationBuilder expiryQueueSize(int expiryQueueSize) {
      this.expiryQueueSize = expiryQueueSize;
      return self();
   }
   
   public OffheapStoreConfigurationBuilder compression(boolean compression) {
      this.compression = compression;
      return self();
   }

   @Override
   public void validate() {
      // how do you validate required attributes?
      super.validate();
   }

   @Override
   public OffheapStoreConfiguration create() {
      return new OffheapStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
            singletonStore.create(), preload, shared, properties,
            compression, expiryQueueSize);
            
   }

   @Override
   public Builder<?> read(OffheapStoreConfiguration template) {
      compression = template.compression();
      expiryQueueSize = template.expiryQueueSize();
      
      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      this.async.read(template.async());
      this.singletonStore.read(template.singletonStore());

      return self();
   }

   @Override
   public OffheapStoreConfigurationBuilder self() {
      return this;
   }

}
