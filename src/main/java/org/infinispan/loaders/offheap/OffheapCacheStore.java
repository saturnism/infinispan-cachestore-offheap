package org.infinispan.loaders.offheap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.offheap.configuration.OffheapCacheStoreConfiguration;
import org.infinispan.loaders.offheap.logging.Log;
import org.infinispan.loaders.spi.LockSupportCacheStore;
import org.infinispan.util.logging.LogFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;

/**
 * A cache store that can store entries off-heap in direct memory.
 * This implementation uses MapDB's Direct Memory DB.
 * 
 * At most one offheap store can be used by one cache.
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 6.0
 */
public class OffheapCacheStore extends LockSupportCacheStore<Integer> {
   private static final Log log = LogFactory.getLog(OffheapCacheStore.class, Log.class);
   
   private static final String STORE_DB_NAME = "store-";
   private static final String EXPIRY_DB_NAME = "expiry-";
   
   private OffheapCacheStoreConfiguration configuration;
   private DB db;
   private Map<Object, byte[]> store;
   private Map<Long, Object> expired;
   private BlockingQueue<ExpiryEntry> expiryEntryQueue;

   @Override
   public void init(CacheLoaderConfiguration config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      this.configuration = validateConfigurationClass(config, OffheapCacheStoreConfiguration.class);
      super.init(config, cache, m);
   }

   @Override
   public void start() throws CacheLoaderException {
      expiryEntryQueue = new LinkedBlockingQueue<ExpiryEntry>(configuration.expiryQueueSize());
      DBMaker<?> dbMaker = DBMaker.newDirectMemoryDB()
            .transactionDisable().asyncWriteDisable();
            //.asyncFlushDelay(100);
      
      if (configuration.compression())
         dbMaker.compressionEnable();
      
      this.db = dbMaker.make();
      
      this.store = db.createHashMap(STORE_DB_NAME + cache.getName())
            .makeOrGet();
      this.expired = db.createHashMap(EXPIRY_DB_NAME + cache.getName())
            .makeOrGet();

      super.start();
   }

   @Override
   public void stop() throws CacheLoaderException {
      db.close();
      
      super.stop();
   }

   @Override
   protected void clearLockSafe() throws CacheLoaderException {
      store.clear();
   }

   @Override
   protected Set<InternalCacheEntry> loadAllLockSafe() throws CacheLoaderException {
      Set<InternalCacheEntry> entries = new HashSet<InternalCacheEntry>();
      
      try {
         for (Map.Entry<Object, byte[]> entry : store.entrySet()) {
            entries.add(unmarshall(entry));
         }
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }

      return entries;
   }

   @Override
   protected Set<InternalCacheEntry> loadLockSafe(int maxEntries) throws CacheLoaderException {
      if (maxEntries <= 0)
         return InfinispanCollections.emptySet();
      
      Set<InternalCacheEntry> entries = new HashSet<InternalCacheEntry>();
      
      try {
         for (Map.Entry<Object, byte[]> entry : store.entrySet()) {
            entries.add(unmarshall(entry));
            if (entries.size() == maxEntries)
               return entries;
         }
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }

      return entries;
   }

   @Override
   protected Set<Object> loadAllKeysLockSafe(Set<Object> keysToExclude) throws CacheLoaderException {
      if (!cache.getStatus().allowInvocations())
         return InfinispanCollections.emptySet();

      Set<Object> keys = new HashSet<Object>();
      
      try {
         for (Object key : store.keySet()) {
            if (keysToExclude == null || keysToExclude.isEmpty() || !keysToExclude.contains(key))
               keys.add(key);
         }

         return keys;
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   protected void toStreamLockSafe(ObjectOutput oos) throws CacheLoaderException {
      try {
         for (Map.Entry<Object, byte[]> entry : store.entrySet()) {
            InternalCacheEntry ice = unmarshall(entry);
            getMarshaller().objectToObjectStream(ice, oos);
         }
         getMarshaller().objectToObjectStream(null, oos);
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   protected void fromStreamLockSafe(ObjectInput ois) throws CacheLoaderException {
      try {
         while (true) {
            InternalCacheEntry ice = (InternalCacheEntry) getMarshaller().objectFromObjectStream(ois);
            if (ice == null)
               break;

            store.put(ice.getKey(), marshall(ice));
         }
      } catch (InterruptedException ie) {
         Thread.currentThread().interrupt();
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }

   }

   @Override
   protected boolean removeLockSafe(Object key, Integer lockingKey) throws CacheLoaderException {
      return store.remove(key) != null;
   }

   @Override
   protected void storeLockSafe(InternalCacheEntry ed, Integer lockingKey) throws CacheLoaderException {
      try {
         store.put(ed.getKey(), marshall(ed));
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
      if (ed.canExpire()) {
         addNewExpiry(ed);
      }
   }

   @Override
   protected InternalCacheEntry loadLockSafe(Object key, Integer lockingKey) throws CacheLoaderException {
      try {
         InternalCacheValue icv = (InternalCacheValue) unmarshall(store.get(key));
         if (icv == null)
            return null;
         
         if (icv != null && icv.isExpired(System.currentTimeMillis())) {
            removeLockSafe(key, lockingKey);
            return null;
         }
         return icv.toInternalCacheEntry(key);
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   protected Integer getLockFromKey(Object key) throws CacheLoaderException {
      return key.hashCode();
   }

   @SuppressWarnings("unchecked")
   @Override
   protected void purgeInternal() throws CacheLoaderException {
      try {
         // Drain queue and update expiry tree
         List<ExpiryEntry> entries = new ArrayList<ExpiryEntry>();
         expiryEntryQueue.drainTo(entries);
         for (ExpiryEntry entry : entries) {
            //final byte[] expiryBytes = marshall(entry.expiry);
            //final byte[] keyBytes = marshall(entry.key);
            //final byte[] existingBytes = expiredDb.get(expiryBytes);
            
            final Object existing = expired.get(entry.expiry);

            if (existing != null) {
               // in the case of collision make the key a List ...
               if (existing instanceof List) {
                  ((List<Object>) existing).add(entry.key);
                  expired.put(entry.expiry, existing);
               } else {
                  List<Object> al = new ArrayList<Object>(2);
                  al.add(existing);
                  al.add(entry.key);
                  expired.put(entry.expiry, al);
               }
            } else {
               expired.put(entry.expiry, entry.key);
            }
         }

         List<Long> times = new ArrayList<Long>();
         List<Object> keys = new ArrayList<Object>();
         try {
            for (Map.Entry<Long, Object> entry : expired.entrySet()) {
               Long time = (Long) entry.getKey();
               if (time > System.currentTimeMillis())
                  break;
               times.add(time);
               Object key = entry.getValue();
               if (key instanceof List)
                  keys.addAll((List<?>) key);
               else
                  keys.add(key);
            }

            for (Long time : times) {
               expired.remove(time);
            }

            if (!keys.isEmpty())
               log.debugf("purge (up to) %d entries", keys.size());
            int count = 0;
            long currentTimeMillis = System.currentTimeMillis();
            for (Object key : keys) {
               byte [] bytes = store.get(key);
               
               if (bytes == null)
                  continue;
               
               InternalCacheValue icv = (InternalCacheValue) getMarshaller().objectFromByteBuffer(bytes);
               if (icv.isExpired(currentTimeMillis)) {
                  store.remove(key);
                  count++;
               }
            }
            if (count != 0)
               log.debugf("purged %d entries", count);
         } catch (Exception e) {
            throw new CacheLoaderException(e);
         }
      } catch (CacheLoaderException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   private void addNewExpiry(InternalCacheEntry entry) {
      long expiry = entry.getExpiryTime();
      if (entry.getMaxIdle() > 0) {
         // Coding getExpiryTime() for transient entries has the risk of
         // being a moving target
         // which could lead to unexpected results, hence, InternalCacheEntry
         // calls are required
         expiry = entry.getMaxIdle() + System.currentTimeMillis();
      }
      Long at = expiry;
      Object key = entry.getKey();

      try {
         expiryEntryQueue.put(new ExpiryEntry(at, key));
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt(); // Restore interruption status
      }
   }

   private static final class ExpiryEntry {
      private final Long expiry;
      private final Object key;

      private ExpiryEntry(long expiry, Object key) {
         this.expiry = expiry;
         this.key = key;
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + ((key == null) ? 0 : key.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj)
            return true;
         if (obj == null)
            return false;
         if (getClass() != obj.getClass())
            return false;
         ExpiryEntry other = (ExpiryEntry) obj;
         if (key == null) {
            if (other.key != null)
               return false;
         } else if (!key.equals(other.key))
            return false;
         return true;
      }
   }
   
   private byte[] marshall(InternalCacheEntry entry) throws IOException, InterruptedException {
      return marshall(entry.toInternalCacheValue());
   }

   private byte[] marshall(Object entry) throws IOException, InterruptedException {
      return getMarshaller().objectToByteBuffer(entry);
   }

   private Object unmarshall(byte[] bytes) throws IOException, ClassNotFoundException {
      if (bytes == null)
         return null;

      return getMarshaller().objectFromByteBuffer(bytes);
   }

   private InternalCacheEntry unmarshall(Map.Entry<Object, byte[]> entry) throws IOException, ClassNotFoundException {
      if (entry == null || entry.getValue() == null)
         return null;

      InternalCacheValue v = (InternalCacheValue) unmarshall(entry.getValue());
      return v.toInternalCacheEntry(entry.getKey());
   }

   private InternalCacheEntry unmarshall(byte[] value, Object key) throws IOException, ClassNotFoundException {
      if (value == null)
         return null;

      InternalCacheValue v = (InternalCacheValue) unmarshall(value);
      return v.toInternalCacheEntry(key);
   }
}
