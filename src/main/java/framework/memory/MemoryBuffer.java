/**
 * MapReduce框架内存缓冲区管理类
 * 
 * 该类提供内存缓冲区的管理功能，支持大小限制、溢出处理和线程安全操作。
 * 当缓冲区达到指定阈值时，会自动将数据溢出到磁盘文件中。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.memory;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class MemoryBuffer<K, V> {
    
    private final int maxSizeInKB;
    private final double spillThreshold;
    private final List<BufferEntry<K, V>> buffer;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final SpillManager spillManager;
    private final String taskId;
    private volatile int currentSizeInBytes = 0;
    private volatile int spillCount = 0;
    
    public MemoryBuffer(int maxSizeInKB, double spillThreshold, SpillManager spillManager, String taskId) {
        this.maxSizeInKB = maxSizeInKB;
        this.spillThreshold = spillThreshold;
        this.buffer = new ArrayList<>();
        this.spillManager = spillManager;
        this.taskId = taskId;
    }
    
    /**
     * 向缓存中添加键值对
     * @param key 键
     * @param value 值
     * @throws Exception 缓存溢出异常
     */
    public void put(K key, V value) throws Exception {
        BufferEntry<K, V> entry = new BufferEntry<>(key, value);
        
        lock.writeLock().lock();
        try {
            // 先添加数据到缓存
            buffer.add(entry);
            currentSizeInBytes += entry.getEstimatedSize();
            
            // DEBUG: 输出缓存状态
            double currentSizeInKB = currentSizeInBytes / 1024.0;
            // System.out.printf("DEBUG: Buffer put - key: %s, value: %s, buffer size: %d, current size: %.2f KB, max: %d KB, threshold: %.2f%n", 
            //     key, value, buffer.size(), currentSizeInKB, maxSizeInKB, spillThreshold);
            
            // 然后检查是否需要溢出
            if (shouldSpill()) {
                System.out.println("DEBUG: Spill threshold reached, triggering spill operation");
                spill();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 判断是否需要溢出
     */
    private boolean shouldSpill() {
        double currentSizeInKB = currentSizeInBytes / 1024.0;
        return currentSizeInKB >= maxSizeInKB * spillThreshold;
    }
    
    /**
     * 执行溢出操作
     */
    private void spill() throws Exception {
        if (buffer.isEmpty()) {
            System.out.println("DEBUG: Spill called but buffer is empty");
            return;
        }
        
        System.out.printf("DEBUG: Starting spill operation - buffer size: %d, current size: %.2f KB%n", 
            buffer.size(), currentSizeInBytes / 1024.0);
        
        // 创建要溢出的数据副本
        List<BufferEntry<K, V>> dataToSpill = new ArrayList<>(buffer);
        
        // DEBUG: 输出前几个要溢出的数据
        System.out.println("DEBUG: First few entries to spill:");
        for (int i = 0; i < Math.min(5, dataToSpill.size()); i++) {
            BufferEntry<K, V> entry = dataToSpill.get(i);
            System.out.printf("  [%d] %s -> %s%n", i, entry.getKey(), entry.getValue());
        }
        
        // 排序数据
        Collections.sort(dataToSpill, (e1, e2) -> {
            if (e1.getKey() instanceof Comparable && e2.getKey() instanceof Comparable) {
                return ((Comparable) e1.getKey()).compareTo(e2.getKey());
            }
            return e1.getKey().hashCode() - e2.getKey().hashCode();
        });
        
        // 写入溢出文件
        String spillFile = spillManager.getTempDir() + "/spill_" + taskId + "_" + spillCount + ".tmp";
        @SuppressWarnings("unchecked")
        List<BufferEntry<?, ?>> spillData = (List<BufferEntry<?, ?>>) (List<?>) dataToSpill;
        spillManager.spill(spillData, spillFile);
        
        // 清空缓存
        buffer.clear();
        currentSizeInBytes = 0;
        spillCount++;
        
        System.out.printf("DEBUG: Spill completed - file: %s, spill count: %d, entries spilled: %d%n", 
            spillFile, spillCount, dataToSpill.size());
    }
    
    /**
     * 获取所有数据并清空缓存
     */
    public List<BufferEntry<K, V>> getAndClear() {
        lock.writeLock().lock();
        try {
            List<BufferEntry<K, V>> result = new ArrayList<>(buffer);
            buffer.clear();
            currentSizeInBytes = 0;
            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 强制溢出所有数据
     */
    public void forceSpill() throws Exception {
        lock.writeLock().lock();
        try {
            System.out.printf("DEBUG: Force spill called - buffer size: %d, current size: %.2f KB%n", 
                buffer.size(), currentSizeInBytes / 1024.0);
            if (!buffer.isEmpty()) {
                spill();
            } else {
                System.out.println("DEBUG: Force spill - buffer is already empty");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public boolean isFull() {
        lock.readLock().lock();
        try {
            return shouldSpill();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public int size() {
        lock.readLock().lock();
        try {
            return buffer.size();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public double getCurrentSizeInMB() {
        lock.readLock().lock();
        try {
            return currentSizeInBytes / (1024.0 * 1024.0);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public int getSpillCount() {
        return spillCount;
    }
}