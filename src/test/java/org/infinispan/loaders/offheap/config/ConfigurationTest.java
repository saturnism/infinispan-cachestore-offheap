package org.infinispan.loaders.offheap.config;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.loaders.offheap.configuration.OffheapCacheStoreConfiguration;
import org.infinispan.loaders.offheap.configuration.OffheapCacheStoreConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Test(groups = "unit", testName = "loaders.offheap.configuration.ConfigurationTest")
public class ConfigurationTest extends AbstractInfinispanTest {
   public void testConfigBuilder() {
      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder().globalJmxStatistics().transport().defaultTransport().build();

      Configuration cacheConfig = new ConfigurationBuilder().loaders().addLoader(OffheapCacheStoreConfigurationBuilder.class)
            .compression(true)
            .build();

      CacheLoaderConfiguration cacheLoaderConfig = cacheConfig.loaders().cacheLoaders().get(0);
      assertTrue(cacheLoaderConfig instanceof OffheapCacheStoreConfiguration);
      OffheapCacheStoreConfiguration config = (OffheapCacheStoreConfiguration) cacheLoaderConfig;
      assertEquals(true, config.compression());
      
      EmbeddedCacheManager cacheManager = new DefaultCacheManager(globalConfig);

      cacheManager.defineConfiguration("testCache", cacheConfig);

      cacheManager.start();
      Cache<String, String> cache = cacheManager.getCache("testCache");

      cache.put("hello", "there");
      cache.stop();
      cacheManager.stop();
   }

   public void testXmlConfig60() throws IOException {
      EmbeddedCacheManager cacheManager = new DefaultCacheManager("config/offheap-config-60.xml");

      Cache<String, String> cache = cacheManager.getCache("testCache");

      cache.put("hello", "there 52 xml");
      cache.stop();
      cacheManager.stop();
   }

}
