package org.infinispan.loaders.offheap;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.loaders.offheap.configuration.OffheapCacheStoreConfigurationBuilder;
import org.infinispan.persistence.BaseCacheStoreFunctionalTest;

public abstract class OffheapCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {
   OffheapCacheStoreConfigurationBuilder createStoreBuilder(PersistenceConfigurationBuilder loaders) {
      return loaders.addStore(OffheapCacheStoreConfigurationBuilder.class);
   }
}
