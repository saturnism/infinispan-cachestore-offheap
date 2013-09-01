package org.infinispan.loaders.offheap;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.offheap.OffheapCacheStore;
import org.infinispan.loaders.offheap.configuration.OffheapCacheStoreConfiguration;
import org.infinispan.loaders.offheap.configuration.OffheapCacheStoreConfigurationBuilder;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.offheap.OffheapCacheStoreTest")
public class OffheapCacheStoreTest extends BaseCacheStoreTest {

   private OffheapCacheStore fcs;

   protected OffheapCacheStoreConfiguration createCacheStoreConfig(LoadersConfigurationBuilder lcb) throws CacheLoaderException {
      OffheapCacheStoreConfigurationBuilder cfg = new OffheapCacheStoreConfigurationBuilder(lcb);
      cfg.purgeSynchronously(true); // for more accurate unit testing
      return cfg.create();
   }

   @Override
   protected CacheStore createCacheStore() throws CacheLoaderException {
      fcs = new OffheapCacheStore();
      ConfigurationBuilder cb = new ConfigurationBuilder();
      OffheapCacheStoreConfiguration cfg = createCacheStoreConfig(cb.loaders());
      fcs.init(cfg, getCache(), getMarshaller());
      fcs.start();
      return fcs;
   }
   
   @Override
   @Test(enabled=false, description="We can't really test this with offheap store")
   public void testStopStartDoesNotNukeValues() throws InterruptedException, CacheLoaderException {
   }

}
