/**
 * MapReduce框架上下文接口的具体实现类
 * 
 * 该类实现了Context接口，提供Map和Reduce任务执行过程中的
 * 具体上下文功能，包括数据写入缓冲区、配置管理和统计信息。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.core;

import framework.memory.MemoryBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Context的具体实现类
 */
public class ContextImpl<KEYOUT, VALUEOUT> implements Context<KEYOUT, VALUEOUT> {
    
    private final Configuration configuration;
    private final String taskId;
    private final MemoryBuffer<KEYOUT, VALUEOUT> buffer;
    private final AtomicLong writeCount = new AtomicLong(0);
    
    public ContextImpl(Configuration configuration, String taskId, MemoryBuffer<KEYOUT, VALUEOUT> buffer) {
        this.configuration = configuration;
        this.taskId = taskId;
        this.buffer = buffer;
    }
    
    @Override
    public void write(KEYOUT key, VALUEOUT value) throws Exception {
        buffer.put(key, value);
        writeCount.incrementAndGet();
    }
    
    @Override
    public Configuration getConfiguration() {
        return configuration;
    }
    
    @Override
    public String getTaskId() {
        return taskId;
    }
    
    public long getWriteCount() {
        return writeCount.get();
    }
}