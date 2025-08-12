/**
 * MapReduce框架Map任务实现类
 * 
 * 该类实现了Map阶段的具体任务执行逻辑，包括Mapper实例创建、
 * 输入数据处理、内存缓冲区管理、溢出处理和输出文件生成。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.task;

import framework.core.*;
import framework.memory.*;
import framework.scheduler.JobConfig;
import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

public class MapTask implements Callable<Boolean> {
    
    private final String taskId;
    private final JobConfig jobCfg;
    private final InputSplit inputSplit;
    private final BiConsumer<String, String> completionCallback;
    private TaskStatus status = TaskStatus.PENDING;
    private long startTime;
    private long endTime;
    private String outputFile;
    
    public MapTask(String taskId, JobConfig jobCfg, InputSplit inputSplit, 
                    BiConsumer<String, String> completionCallback) {
        this.taskId = taskId;
        this.jobCfg = jobCfg;
        this.inputSplit = inputSplit;
        this.completionCallback = completionCallback;
        this.outputFile = jobCfg.getTempDir() + "/map-output-" + taskId + ".txt";
    }
    
    @Override
    public Boolean call() throws Exception {
        startTime = System.currentTimeMillis();
        status = TaskStatus.RUNNING;
        
        System.out.println("Starting MapTask: " + taskId + ", processing " + 
                          inputSplit.getLineCount() + " lines");
        
        try {
            // 创建Mapper实例
            Mapper<Object, String, Object, Object> mapper = createMapperInstance();
            
            // 创建内存缓存和溢出管理器
            SpillManager spillManager = new SpillManager(jobCfg.getTempDir());
            MemoryBuffer<Object, Object> buffer = new MemoryBuffer<>(
                jobCfg.getBufferSizeKB(), 
                jobCfg.getSpillThreshold(), 
                spillManager,
                taskId
            );
            
            // 创建上下文
            Configuration config = jobCfg.getConfiguration();
            ContextImpl<Object, Object> context = new ContextImpl<>(config, taskId, buffer);
            
            // 执行setup
            mapper.setup(context);
            
            // 处理输入数据
            int lineIndex = 0;
            int processedLines = 0;
            for (String line : inputSplit.getLines()) {
                if (line != null && !line.trim().isEmpty()) {
                    mapper.map(lineIndex++, line, context);
                    processedLines++;
                }
            }
            System.out.println("MapTask " + taskId + " processed " + processedLines + " lines, buffer size: " + buffer.size());
            
            // 执行cleanup
            mapper.cleanup(context);
            
            // 检查是否有溢出文件
            List<String> spillFilesList = spillManager.getSpillFiles();
            if (!spillFilesList.isEmpty()) {
                // 有溢出文件，先强制溢出剩余数据（如果有的话），然后合并所有溢出文件
                if (buffer.size() > 0) {
                    buffer.forceSpill();
                }
                System.out.println("MapTask " + taskId + " merging " + spillManager.getSpillFiles().size() + " spill files");
                spillManager.mergeSpillFiles(outputFile);
            } else {
                // 没有溢出文件，直接写入缓存数据
                List<BufferEntry<Object, Object>> entries = buffer.getAndClear();
                System.out.println("MapTask " + taskId + " writing " + entries.size() + " entries to file: " + outputFile);
                writeBufferToFile(entries, outputFile);
            }
            
            endTime = System.currentTimeMillis();
            status = TaskStatus.COMPLETED;
            
            System.out.println("MapTask " + taskId + " completed in " + 
                              (endTime - startTime) + "ms, output: " + outputFile);
            
            // 调用完成回调
            if (completionCallback != null) {
                completionCallback.accept(taskId, outputFile);
            }
            
            return true;
            
        } catch (Exception e) {
            endTime = System.currentTimeMillis();
            status = TaskStatus.FAILED;
            System.err.println("MapTask " + taskId + " failed: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 创建Mapper实例
     */
    @SuppressWarnings("unchecked")
    private Mapper<Object, String, Object, Object> createMapperInstance() throws Exception {
        Class<? extends Mapper> mapperClass = jobCfg.getMapperClass();
        return (Mapper<Object, String, Object, Object>) mapperClass.getDeclaredConstructor().newInstance();
    }
    
    /**
     * 将缓存数据写入文件
     */
    private void writeBufferToFile(List<BufferEntry<Object, Object>> entries, String outputFile) throws IOException {
        new File(outputFile).getParentFile().mkdirs();
        
        // 排序数据
        Collections.sort(entries, (e1, e2) -> {
            if (e1.getKey() instanceof Comparable && e2.getKey() instanceof Comparable) {
                return ((Comparable) e1.getKey()).compareTo(e2.getKey());
            }
            return e1.getKey().hashCode() - e2.getKey().hashCode();
        });
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
            for (BufferEntry<Object, Object> entry : entries) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    writer.println(entry.getKey() + "\t" + entry.getValue());
                }
            }
        }
    }
    
    // Getter方法
    public String getTaskId() {
        return taskId;
    }
    
    public TaskStatus getStatus() {
        return status;
    }
    
    public long getStartTime() {
        return startTime;
    }
    
    public long getEndTime() {
        return endTime;
    }
    
    public String getOutputFile() {
        return outputFile;
    }
    
    public InputSplit getInputSplit() {
        return inputSplit;
    }
}