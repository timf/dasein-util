/**
 * Copyright (C) 1998-2013 enStratus Networks Inc
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.util;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.Statistics;
import net.sf.ehcache.management.ManagementService;

import javax.annotation.Nonnull;

/**
 * <p>
 *   A concurrent multi-cache caches objects along multiple
 *   unique keys. You provide the list of unique keys and then this object will
 *   manage the concurrent access of multiple threads into the cache.
 * </p>
 * <p>
 *   This class is backed up by multiple {@link net.sf.ehcache.Ehcache} instances (one for
 *   each unique key). Example of creating a memory cache of employees:
 * </p>
 * <p>
 * <code>
 * public class EmployeeFactory {<br/>
 *     private ConcurrentMultiEhache&lt;Employee&gt; cache = new ConcurrentMultiEhache&lt;Employee&gt;(Employee.class, "employeeId", "email");<br/>
 *     <br/>
 *     public Employee getEmployeeById(Number id) {<br/>
 *       return cache.find("employeeId", id, getIdLoader(id));<br/>
 *     }<br/>
 *     <br/>
 *     public Employee getEmployeeByEmail(String email) {<br/>
 *       return cache.find("email", email, getEmailLoader(email));</br>
 *     }<br/>
 * }
 * </code>
 * </p>
 * <p>
 *   In the above example, the <code>getIdLoader()</code> and <code>getEmailLoader()</code>
 *   methods would be methods you would create to return an instance of
 *   {@link org.dasein.util.CacheLoader} that would load the desired employee from the database
 *   based on the employee ID or email address, respectively.
 * </p>
 * @param <T> the type for objects being stored in instances of the cache
 */
public class ConcurrentMultiEhcache<T> extends ConcurrentMultiCache<T> {

    /**
     * A mapping of the unique identifer names to concurrent caches.
     */
    private final Map<String,EhcacheWrapper<T>> caches;
    /**
     * The ordered list of unique identifiers supported by this cache.
     */
    private final ArrayList<String> order  = new ArrayList<String>();
    /**
     * The class being managed by this cache, if any.
     */
    private final Class<T> target;
    /**
     * Config file on resource path.
     */
    public static final String CACHE_FILENAME = "/dsn-ehcache.xml";
    /**
     * Backup config file in the jar.
     */
    public static final String BACKUP_CACHE_FILENAME = "/dsn-ehcache-backup.xml";
    /**
     * CacheManager for all dasein-util ConcurrentMultiEhcache instances in this JVM
     */
    private static final CacheManager dsnCacheManager;
    static {
        CacheManager cacheManager = null;
        try {
            URL url = ConcurrentMultiEhcache.class.getResource(CACHE_FILENAME);
            if (url != null) {
                cacheManager = new CacheManager(url);
            }
            if (cacheManager == null) {
                url = ConcurrentMultiEhcache.class.getResource(BACKUP_CACHE_FILENAME);
                if (url != null) {
                    cacheManager = new CacheManager(url);
                }
            }
        } catch (Throwable t) {
            throw new IllegalStateException("Could not setup backend cache manager: " + t.getMessage());
        }
        if (cacheManager == null) {
            throw new IllegalStateException("Could not setup backend cache manager, no configuration file");
        }
        ManagementService.registerMBeans(cacheManager, ManagementFactory.getPlatformMBeanServer(), true, true, true, true);
        dsnCacheManager = cacheManager;
    }

    /**
     * Constructs a concurrent multi-cache that caches for unique keys specified by the
     * attributes. This constructor allows for automated loading into the cache.
     * @param cls the class object for objects stored in this cache
     * @param attrs the unique keys that this cache will cache objects on
     */
    public ConcurrentMultiEhcache(Class<T> cls, Collection<String> attrs) {
        this(cls, fromCollection(attrs));
    }

    /**
     * Constructs a concurrent multi-cache that caches for unique keys specified by the
     * attributes. This constructor allows for automated loading into the cache.
     * @param cls the class object for objects stored in this cache
     * @param attrs the unique keys that this cache will cache objects on
     */
    public ConcurrentMultiEhcache(Class<T> cls, String... attrs) {
        if (cls == null) {
            throw new IllegalArgumentException("No class");
        }
        target = cls;
        if (attrs == null || attrs.length == 0) {
            throw new IllegalArgumentException("No attrs");
        }
        caches = new HashMap<String,EhcacheWrapper<T>>(attrs.length);
        for( String attr : attrs ) {
            if (attr == null || attr.trim().isEmpty()) {
                throw new IllegalArgumentException("Attribute is empty or missing");
            }
            caches.put(attr, newEhcache(cls, attr));
            order.add(attr);
        }
    }

    /**
     * Finds the object from the cache with the specified unique identifier value
     * for the default unique identifier attribute. This method will throw an
     * exception if there is more than one unique identifier associated with this
     * cache.
     * @param val the value matching the desired object from the cache
     * @return the matching object from the cache
     * @throws org.dasein.util.CacheManagementException this multi-cache supports multiple unique IDs
     */
    public T find(Object val) {
        return find(val, null);
    }

    /**
     * Finds the object in the cache matching the values in the specified value map.
     * If a matching argument is not in the cache, this method will instantiate an
     * instance of the object and assign the mapping values to that instantiated
     * object. In order for this to work, the keys in this mapping must have the same
     * names as the attributes for the instantiated object
     * @param vals the mapping that includes the attribute/value pairs
     * @return an object matching those values in the cache
     */
    public T find(Map<String,Object> vals) {
        String key = order.get(0);
        Object val = vals.get(key);
        CacheLoader<T> loader;

        if( val == null ) {
            throw new CacheManagementException("No value specified  for key: " + key);
        }
        loader = new MapLoader<T>(target);
        return find(key, val, loader, vals);
    }

    /**
     * Returns the object identified by the specified key/value pair if it is currently
     * in memory in the cache. Just because this value returns <code>null</code> does
     * not mean the object does not exist. Instead, it may be that it is simply not
     * cached in memory.
     * @param key they unique identifier attribute on which you are searching
     * @param val the value of the unique identifier on which you are searching
     * @return the matching object from the cache
     */
    public T find(String key, Object val) {
        return find(key, val, null);
    }

    /**
     * Calls {@link #find(String,Object, org.dasein.util.CacheLoader)} using the only unique
     * identifier attribute as passed to this cache's constructor. If more than one
     * unique identifier attribute was passed, this method will throw an exception
     * @param val the value of the unique key being sought
     * @param loader the loader to load new instances from the persistent store
     * @return a matching object, if any
     * @throws org.dasein.util.CacheManagementException this cache supports multiple unique identifiers
     */
    public T find(Object val, CacheLoader<T> loader) {
        if( order.size() != 1 ) {
            throw new CacheManagementException("You may only call this method when the cache is managing one unique identifier.");
        }
        return find(order.get(0), val, loader);
    }

    /**
     * Seeks the item from the cache that is identified by the specified key having
     * the specified value. If no match is found, the specified loader will be called to
     * place an item in the cache. You may pass in  <code>null</code> for the loader.
     * If you do, only an object in active memory will be returned.
     * @param key the name of the unique identifier attribute whose value you have
     * @param val the value of the unique identifier that identifiers the desired item
     * @param loader a loader to load the desired object from the persistence store if it
     * is not in memory
     * @return the object that matches the specified key/value
     */
    public T find(String key, Object val, CacheLoader<T> loader) {
        return find(key, val, loader, null, null);
    }

    /**
     * Seeks the item from the cache that is identified by the specified key having
     * the specified value. If no match is found, the specified loader will be called
     * with the specified arguments in order to place an instantiated item into the cache.
     * @param key the name of the unique identifier attribute whose value you have
     * @param val the value of the unique identifier that identifies the desired item
     * @param loader a loader to load the desired object from the persistence store if it
     * is not in memory. Should not be possible for loader to hang indefinitely.
     * @param args any arguments to pass to the loader
     * @return the object that matches the specified key/value, or null
     */
    public T find(String key, Object val, CacheLoader<T> loader, Object ... args) {
        if (key == null) {
            throw new IllegalArgumentException("key is missing");
        }
        if (val == null) {
            throw new IllegalArgumentException("val is missing");
        }
        if( val instanceof BigDecimal ) {
            val = Long.valueOf(((Number) val).longValue());
        }

        final EhcacheWrapper<T> wrapper = caches.get(key);
        T item = wrapper.getItem(val);

        if (item != null) {
            // Cache hit.
            if( item instanceof CachedItem ) {
                if( ((CachedItem)item).isValidForCache() ) {
                    return item;
                } else {
                    release(item);
                }
            } else {
                return item;
            }
        }

        if (loader == null) {
            // Nothing we can do.
            return null;
        }

        // Cache miss.

        // First we get a lock per-ID-value of the object being loaded.
        // This is stampede protection: many simultaneous cache-misses on the
        // same value can otherwise cause many loaders to go off to get the
        // item from the definitive source. That's unecessary work and extra
        // time spent waiting by each of the threads involved in the cache-miss.
        final Lock lock = wrapper.getLoadingLock(val);

        // Could add a timeout here ('tryLock' method), even a configurable one.
        // For now suggesting that loader never be able to hang indefinitely.
        lock.lock();
        try {
            // Did another thread just load this in the meantime?
            item = wrapper.getItem(val);
            if (item != null) {
                return null;
            }

            item = loader.load(args);
            if( item == null ) {
                return null;
            }

            wrapper.getCache().put(new Element(val, item));
            return item;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Provides the values for all of the unique identifiers managed by the cache.
     * @param item the item whose key values are being sought
     * @return a mapping of key names to item values
     */
    public HashMap<String,Object> getKeys(T item) {
        HashMap<String,Object> keys = new HashMap<String,Object>();

        for( String key: caches.keySet() ) {
            keys.put(key, getValue(key, item));
        }
        return keys;
    }

    public Class<T> getTarget() {
        return target;
    }

    /**
     * Provides the actual value for the specified unique ID key for the specified object.
     * @param key the name of the unique identifier
     * @param item the object who's unique identifier value is sought
     * @return the unique identifier for the object's attribute matching the key
     */
    private Object getValue(String key, T item) {
        Class cls = item.getClass();

        while( true ) {
            try {
                Field f = cls.getDeclaredField(key);
                int m = f.getModifiers();

                if( Modifier.isTransient(m) || Modifier.isStatic(m) ) {
                    return null;
                }
                f.setAccessible(true);
                return f.get(item);
            }
            catch( Exception e ) {
                Class[] params = new Class[0];
                String mname;

                mname = "get" + key.substring(0,1).toUpperCase() + key.substring(1);
                try {
                    Method method = cls.getDeclaredMethod(mname, params);
                    Object[] args = new Object[0];

                    return method.invoke(item, args);
                }
                catch( IllegalAccessException e2 ) {
                    // ignore
                }
                catch( SecurityException e2 ) {
                    // ignore
                }
                catch( NoSuchMethodException e2 ) {
                    // ignore
                }
                catch( IllegalArgumentException e2 ) {
                    // ignore
                }
                catch( InvocationTargetException e2 ) {
                    // ignore
                }
                cls = cls.getSuperclass();
                if( cls == null || cls.getName().getClass().equals(Object.class.getName()) ) {
                    throw new CacheManagementException("No such property: " + key);
                }
            }
        }
    }

    /**
     * Releases the specified item from the cache. If it is still in the persistent
     * store, it will be retrieved back into the cache on next query. Otherwise,
     * subsequent attempts to search for it in the cache will result in <code>null</code>.
     * @param item the item to be released from the cache.
     */
    public void release(T item) {
        HashMap<String,Object> keys = getKeys(item);
        for(String key : order) {
            EhcacheWrapper<T> wrapper = caches.get(key);
            wrapper.getCache().remove(keys.get(key));
        }
    }

    /**
     * Releases all cached keys and objects. Please use this sparingly as you will cause all of your data to
     * reload.  In certain cases, this is a very useful method that will avoid key leakage.  Take, for example, users
     * hitting a website and you want to track each click.  If you keep all the clicks around, you will eventually
     * run out of RAM as Java will clear out the SoftReferences to the clicks, but the caches will keep the keys
     * in the internal Maps.  Not good.
     */
    public void releaseAll() {
        for (EhcacheWrapper<T> wrapper: caches.values()) {
            wrapper.getCache().removeAll();
        }
    }

    public String toString() {
        return caches.toString();
    }

    private static final String[] EMPTY_STRINGS = new String[]{};
    private static String[] fromCollection(Collection<String> attrs) {
        if (attrs == null) {
            return EMPTY_STRINGS;
        }
        return attrs.toArray(new String[attrs.size()]);
    }

    private EhcacheWrapper<T> newEhcache(@Nonnull Class<T> cls, @Nonnull String attr) {
        final String canonical = cls.getCanonicalName();
        if (canonical == null) {
            throw new IllegalArgumentException("Class will not work with anonymous classes etc.");
        }
        final Ehcache cache = dsnCacheManager.addCacheIfAbsent("dsncache-" + canonical + "##" + attr);
        cache.setStatisticsEnabled(true);
        cache.setStatisticsAccuracy(Statistics.STATISTICS_ACCURACY_NONE);
        return new EhcacheWrapper<T>(cache);
    }
}
