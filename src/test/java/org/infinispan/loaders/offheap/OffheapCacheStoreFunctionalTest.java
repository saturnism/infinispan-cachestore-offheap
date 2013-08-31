package org.infinispan.loaders.offheap;

import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.offheap.configuration.OffheapCacheStoreConfigurationBuilder;

public abstract class OffheapCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {
   OffheapCacheStoreConfigurationBuilder createStoreBuilder(LoadersConfigurationBuilder loaders) {
      return loaders.addStore(OffheapCacheStoreConfigurationBuilder.class)
            .purgeSynchronously(true);
   }
}
