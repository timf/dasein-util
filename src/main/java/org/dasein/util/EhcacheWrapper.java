/**
 * ========= CONFIDENTIAL =========
 *
 * Copyright (C) 2010-2013 enStratus Networks Inc - ALL RIGHTS RESERVED
 *
 * ====================================================================
 *  NOTICE: All information contained herein is, and remains the
 *  property of enStratus Networks Inc. The intellectual and technical
 *  concepts contained herein are proprietary to enStratus Networks LLC
 *  and may be covered by U.S. and Foreign Patents, patents in process,
 *  and are protected by trade secret or copyright law. Dissemination
 *  of this information or reproduction of this material is strictly
 *  forbidden unless prior written permission is obtained from
 *  enStratus Networks Inc.
 * ====================================================================
 */

package org.dasein.util;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class EhcacheWrapper<T> {

    /**
     * The Ehcache instance.
     */
    private final Ehcache cache;

    /**
     * Locks to protect multiple loaders racing. Currently this will grow with each new
     * unique key, but that is a RAM vs. stampede tradeoff.
     */
    private final ConcurrentHashMap<Object,ReentrantLock> loadingLocks;

    public EhcacheWrapper(Ehcache cache) {
        if (cache == null) {
            throw new IllegalArgumentException("cache is missing");
        }
        this.cache = cache;
        loadingLocks = new ConcurrentHashMap<Object, ReentrantLock>();
    }

    @Nullable
    public <T> T getItem(Object key) {
        final Element e = cache.get(key);
        if (e != null) {
            return (T)e.getObjectValue();
        }
        return null;
    }

    @Nonnull
    public Ehcache getCache() {
        return cache;
    }

    /**
     * @param lockKey unique value to prevent loaders of same object from doing extra work
     * @return lock
     */
    @Nonnull
    public Lock getLoadingLock(Object lockKey) {
        Lock lock = loadingLocks.get(lockKey);
        if (lock == null) {
            loadingLocks.putIfAbsent(lockKey, new ReentrantLock());
            lock = loadingLocks.get(lockKey);
        }
        return lock;
    }

    @Override
    public String toString() {
        return "EhcacheWrapper{" +
                "cache=" + cache +
                ", loadingLocks=" + loadingLocks +
                '}';
    }
}
