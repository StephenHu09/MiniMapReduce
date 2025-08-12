/**
 * MapReduce框架作业配置类
 * 
 * 该类封装了MapReduce作业的所有配置信息，包括Mapper/Reducer类、
 * 输入输出目录、任务数量、缓冲区大小、线程池配置等参数。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.scheduler;

import framework.core.*;
import java.io.File;

public class JobConfig {
    
    private final Configuration configuration;
    private Class<? extends Mapper> mapperClass;
    private Class<? extends Reducer> reducerClass;
    private String inputDir;
    private String outputDir;
    private String tempDir;
    private int mapTaskCount;
    private int reduceTaskCount;
    private int bufferSizeKB;
    private double spillThreshold;
    private int maxThreads;
    private boolean enableMonitoring;
    
    public JobConfig(Configuration configuration) {
        this.configuration = configuration;
        loadDefaultSettings();
    }
    
    /**
     * 从配置文件加载默认设置
     */
    private void loadDefaultSettings() {
        this.mapTaskCount = configuration.getInt("mapreduce.map.tasks", 4);
        this.reduceTaskCount = configuration.getInt("mapreduce.reduce.tasks", 2);
        this.bufferSizeKB = configuration.getInt("mapreduce.buffer.size.kb", 1);
        this.spillThreshold = configuration.getDouble("mapreduce.spill.threshold", 0.8);
        this.maxThreads = configuration.getInt("mapreduce.thread.pool.size", 8);
        this.enableMonitoring = configuration.get("mapreduce.monitoring.enabled", "true").equals("true");
        this.tempDir = configuration.get("mapreduce.temp.dir", "temp");
        
        // 确保临时目录存在
        new File(tempDir).mkdirs();
    }
    
    /**
     * 设置Mapper类
     */
    public void setMapperClass(Class<? extends Mapper> mapperClass) {
        this.mapperClass = mapperClass;
    }
    
    /**
     * 设置Reducer类
     */
    public void setReducerClass(Class<? extends Reducer> reducerClass) {
        this.reducerClass = reducerClass;
    }
    
    /**
     * 设置输入目录
     */
    public void setInputDir(String inputDir) {
        this.inputDir = inputDir;
    }
    
    /**
     * 设置输出目录
     */
    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
        // 确保输出目录存在
        new File(outputDir).mkdirs();
    }
    
    /**
     * 设置Map任务数量
     */
    public void setMapTaskCount(int mapTaskCount) {
        this.mapTaskCount = mapTaskCount;
    }
    
    /**
     * 设置Reduce任务数量
     */
    public void setReduceTaskCount(int reduceTaskCount) {
        this.reduceTaskCount = reduceTaskCount;
    }
    
    /**
     * 设置缓存大小（KB）
     */
    public void setBufferSizeKB(int bufferSizeKB) {
        this.bufferSizeKB = bufferSizeKB;
    }
    
    /**
     * 设置溢出阈值
     */
    public void setSpillThreshold(double spillThreshold) {
        this.spillThreshold = spillThreshold;
    }
    
    /**
     * 设置最大线程数
     */
    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }
    
    /**
     * 设置是否启用监控
     */
    public void setEnableMonitoring(boolean enableMonitoring) {
        this.enableMonitoring = enableMonitoring;
    }
    
    /**
     * 验证配置的有效性
     */
    public void validate() throws IllegalArgumentException {
        if (mapperClass == null) {
            throw new IllegalArgumentException("Mapper class is not set");
        }
        if (reducerClass == null) {
            throw new IllegalArgumentException("Reducer class is not set");
        }
        if (inputDir == null || inputDir.trim().isEmpty()) {
            throw new IllegalArgumentException("Input directory is not set");
        }
        if (outputDir == null || outputDir.trim().isEmpty()) {
            throw new IllegalArgumentException("Output directory is not set");
        }
        if (!new File(inputDir).exists()) {
            throw new IllegalArgumentException("Input directory does not exist: " + inputDir);
        }
        if (mapTaskCount <= 0) {
            throw new IllegalArgumentException("Map task count must be positive");
        }
        if (reduceTaskCount <= 0) {
            throw new IllegalArgumentException("Reduce task count must be positive");
        }
        if (bufferSizeKB <= 0) {
            throw new IllegalArgumentException("Buffer size must be positive");
        }
        if (spillThreshold <= 0 || spillThreshold > 1) {
            throw new IllegalArgumentException("Spill threshold must be between 0 and 1");
        }
        if (maxThreads <= 0) {
            throw new IllegalArgumentException("Max threads must be positive");
        }
    }
    
    // Getter方法
    public Configuration getConfiguration() {
        return configuration;
    }
    
    public Class<? extends Mapper> getMapperClass() {
        return mapperClass;
    }
    
    public Class<? extends Reducer> getReducerClass() {
        return reducerClass;
    }
    
    public String getInputDir() {
        return inputDir;
    }
    
    public String getOutputDir() {
        return outputDir;
    }
    
    public String getTempDir() {
        return tempDir;
    }
    
    public int getMapTaskCount() {
        return mapTaskCount;
    }
    
    public int getReduceTaskCount() {
        return reduceTaskCount;
    }
    
    public int getBufferSizeKB() {
        return bufferSizeKB;
    }
    
    public double getSpillThreshold() {
        return spillThreshold;
    }
    
    public int getMaxThreads() {
        return maxThreads;
    }
    
    public boolean isEnableMonitoring() {
        return enableMonitoring;
    }
    
    @Override
    public String toString() {
        return "JobConfig{" +
                "mapperClass=" + (mapperClass != null ? mapperClass.getSimpleName() : "null") +
                ", reducerClass=" + (reducerClass != null ? reducerClass.getSimpleName() : "null") +
                ", inputDir='" + inputDir + '\'' +
                ", outputDir='" + outputDir + '\'' +
                ", mapTaskCount=" + mapTaskCount +
                ", reduceTaskCount=" + reduceTaskCount +
                ", bufferSizeKB=" + bufferSizeKB +
                ", spillThreshold=" + spillThreshold +
                ", maxThreads=" + maxThreads +
                ", enableMonitoring=" + enableMonitoring +
                '}';
    }
}