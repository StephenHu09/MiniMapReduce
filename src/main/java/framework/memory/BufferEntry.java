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
     * 计算条目的估算大小
     * @return 估算的内存大小（字节）
     */
    private int calculateEstimatedSize() {
        int size = 32; // 基本开销：对象头 + 引用字段 + timestamp字段

        // Key大小估算
        size += estimateObjectSize(key);

        // Value大小估算
        size += estimateObjectSize(value);

        // 8字节对齐
        return (size + 7) & (~7);
    }

    // 估算单个对象的大小
    private int estimateObjectSize(Object obj) {
        if (obj == null) {
            return 0;
        }

        // 对于字节数组，直接使用其长度（不包含对象头，因为已在基本开销中计算）
        if (obj instanceof byte[]) {
            return ((byte[]) obj).length;
        }

        // 对于其他对象，使用toString()长度作为估算基础（不包含对象头）
        String str = obj.toString();
        return str.length() * 2; // 字符串内容估算（UTF-16编码）
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