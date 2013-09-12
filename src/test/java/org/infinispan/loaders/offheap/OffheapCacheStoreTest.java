package org.infinispan.loaders.offheap;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.loaders.offheap.configuration.OffheapStoreConfiguration;
import org.infinispan.loaders.offheap.configuration.OffheapStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.BaseCacheStoreTest;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.DummyLoaderContext;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.offheap.OffheapCacheStoreTest")
public class OffheapCacheStoreTest extends BaseCacheStoreTest {
   private EmbeddedCacheManager cacheManager;
   private OffheapStore fcs;
   
   protected OffheapStoreConfiguration createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      OffheapStoreConfigurationBuilder cfg = new OffheapStoreConfigurationBuilder(lcb);
      return cfg.create();
   }

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(CacheMode.LOCAL, false);
      fcs = new OffheapStore();
      ConfigurationBuilder cb = new ConfigurationBuilder();
      OffheapStoreConfiguration cfg = createCacheStoreConfig(cb.persistence());
      fcs.init(new DummyLoaderContext(cfg, getCache(), getMarshaller()));
      fcs.start();
      return fcs;
   }
   
   @Override
   protected StreamingMarshaller getMarshaller() {
      return cacheManager.getCache().getAdvancedCache().getComponentRegistry().getCacheMarshaller();
   }

   @AfterMethod
   @Override
   public void tearDown() throws CacheLoaderException {
      super.tearDown();
      TestingUtil.killCacheManagers(cacheManager);
   }
   
   @Override
   @Test(enabled=false, description="We can't really test this with offheap store")
   public void testStopStartDoesNotNukeValues() throws InterruptedException, CacheLoaderException {
   }
}
