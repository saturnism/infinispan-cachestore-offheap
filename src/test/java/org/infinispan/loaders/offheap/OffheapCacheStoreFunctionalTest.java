package org.infinispan.loaders.offheap;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.loaders.offheap.configuration.OffheapStoreConfigurationBuilder;
import org.infinispan.persistence.BaseCacheStoreFunctionalTest;

public abstract class OffheapCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {
   OffheapStoreConfigurationBuilder createStoreBuilder(PersistenceConfigurationBuilder loaders) {
      return loaders.addStore(OffheapStoreConfigurationBuilder.class);
   }
}
