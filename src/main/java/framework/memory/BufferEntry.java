/**
 * MapReduce框架缓冲区条目类
 * 
 * 该类表示内存缓冲区中的单个数据条目，包含键值对、时间戳
 * 和大小估算等信息，用于内存管理和溢出控制。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.memory;

/**
 * 缓存条目类
 */
public class BufferEntry<K, V> {
    
    private final K key;
    private final V value;
    private final long timestamp;
    private final int estimatedSize;
    
    public BufferEntry(K key, V value) {
        this.key = key;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
        this.estimatedSize = calculateEstimatedSize();
    }
    
    /**
     * 估算条目大小（字节）
     * 使用更合理的估算方法，避免过度估算导致频繁溢出
     */
    private int calculateEstimatedSize() {
        int size = 0;
        
        // 估算键的大小
        if (key instanceof String) {
            size += ((String) key).length(); // 简化为1字节每字符
        } else {
            size += 8; // 其他对象估算8字节
        }
        
        // 估算值的大小
        if (value instanceof String) {
            size += ((String) value).length();
        } else if (value instanceof Integer) {
            size += 4;
        } else if (value instanceof Long) {
            size += 8;
        } else {
            size += 8; // 其他对象估算8字节
        }
        
        // 加上基本的对象开销（减少到16字节）
        size += 16;
        
        return size;
    }
    
    public K getKey() {
        return key;
    }
    
    public V getValue() {
        return value;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public int getEstimatedSize() {
        return estimatedSize;
    }
    
    @Override
    public String toString() {
        return "BufferEntry{" +
                "key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                ", estimatedSize=" + estimatedSize +
                '}';
    }
}