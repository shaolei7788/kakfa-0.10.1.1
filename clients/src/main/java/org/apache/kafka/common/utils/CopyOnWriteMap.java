/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple read-optimized map implementation that synchronizes only writes and does a full copy on each modification
 */

/**
 *  读写分离的设计方案:适合读多写少的场景
 *
 *
 *
 *
 * @param <K>
 * @param <V>
 */
public class CopyOnWriteMap<K, V> implements ConcurrentMap<K, V> {
    /**
     * 核心的变量就是一个map
     * 这个map有个特点,它有一个volatile关键字
     * 在多线程的场景下,如果这个map中的元素发生变化,其他线程是可见的
     */
    private volatile Map<K, V> map;

    public CopyOnWriteMap() {
        this.map = Collections.emptyMap();
    }

    public CopyOnWriteMap(Map<K, V> map) {
        this.map = Collections.unmodifiableMap(map);
    }

    @Override
    public boolean containsKey(Object k) {
        return map.containsKey(k);
    }

    @Override
    public boolean containsValue(Object v) {
        return map.containsValue(v);
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    /**
     * 没有加锁,读取数据的时候性能很高
     * 并且是线程安全的
     * @param k
     * @return
     */
    @Override
    public V get(Object k) {
        return map.get(k);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public synchronized void clear() {
        this.map = Collections.emptyMap();
    }

    /**
     *    1): 整个方法采用的是synchronized关键字修饰的,说明这个方法是线程安全的
     * 即使加了锁,这段代码的性能依然非常好,因为整个过程是纯内存操作的
     *    2): 这种设计方式,采用的是读写分离的设计思想
     *        读操作和写操作,是相互不影响的
     *    3): this.map = Collections.unmodifiableMap(copy);
     *          最后把值赋值给map,map是用volatile关键字修饰的,说明这个map具有可见性,
     *          所以当从map中get的时候,如果map中的数据发生了变化,也是可以感知到的
     *
     * @param k
     * @param v
     * @return
     */
    @Override
    public synchronized V put(K k, V v) {
        //每插入一条数据,就会开辟新的内存空间,说明读写是分离的
        //写数据的时候,是往新的内存空间插入
        //读数据的时候,就从老的空间里面去读
        Map<K, V> copy = new HashMap<K, V>(this.map);
        //插入数据
        V prev = copy.put(k, v);
        //赋值给map
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    @Override
    public synchronized void putAll(Map<? extends K, ? extends V> entries) {
        Map<K, V> copy = new HashMap<K, V>(this.map);
        copy.putAll(entries);
        this.map = Collections.unmodifiableMap(copy);
    }

    @Override
    public synchronized V remove(Object key) {
        Map<K, V> copy = new HashMap<K, V>(this.map);
        V prev = copy.remove(key);
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    @Override
    public synchronized V putIfAbsent(K k, V v) {
        //如果我们传进来的key不存在
        if (!containsKey(k))
            //那么就调用里面内部的put方法
            return put(k, v);
        else
            //直接返回
            return get(k);
    }

    @Override
    public synchronized boolean remove(Object k, Object v) {
        if (containsKey(k) && get(k).equals(v)) {
            remove(k);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized boolean replace(K k, V original, V replacement) {
        if (containsKey(k) && get(k).equals(original)) {
            put(k, replacement);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized V replace(K k, V v) {
        if (containsKey(k)) {
            return put(k, v);
        } else {
            return null;
        }
    }

}
