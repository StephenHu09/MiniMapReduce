/**
 * MapReduce框架Reduce任务实现类
 * 
 * 该类实现了Reduce阶段的具体任务执行逻辑，包括Reducer实例创建、
 * 输入文件读取和分组、数据聚合处理和最终结果输出。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.task;

import framework.core.*;
import framework.scheduler.JobConfig;
import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

public class ReduceTask implements Callable<Boolean> {
    
    private final String taskId;
    private final JobConfig jobCfg;
    private final List<String> inputFiles;
    private final BiConsumer<String, String> completionCallback;
    private TaskStatus status = TaskStatus.PENDING;
    private long startTime;
    private long endTime;
    private String outputFile;
    
    public ReduceTask(String taskId, JobConfig jobCfg, List<String> inputFiles,
                       BiConsumer<String, String> completionCallback) {
        this.taskId = taskId;
        this.jobCfg = jobCfg;
        this.inputFiles = inputFiles;
        this.completionCallback = completionCallback;
        this.outputFile = jobCfg.getOutputDir() + "/part-r-" + 
                         String.format("%05d", Integer.parseInt(taskId.split("-")[2])) + ".txt";
    }
    
    @Override
    public Boolean call() throws Exception {
        startTime = System.currentTimeMillis();
        status = TaskStatus.RUNNING;
        
        System.out.println("Starting ReduceTask: " + taskId + ", processing " + 
                          inputFiles.size() + " input files");
        
        try {
            // 创建Reducer实例
            Reducer<Object, Object, Object, Object> reducer = createReducerInstance();
            
            // 创建上下文
            Configuration config = jobCfg.getConfiguration();
            ReduceContextImpl context = new ReduceContextImpl(config, taskId, outputFile);
            
            // 执行setup
            reducer.setup(context);
            
            // 读取并合并所有输入文件
            Map<Object, List<Object>> groupedData = readAndGroupInputFiles();
            
            // 对每个key执行reduce操作
            for (Map.Entry<Object, List<Object>> entry : groupedData.entrySet()) {
                reducer.reduce(entry.getKey(), entry.getValue(), context);
            }
            
            // 执行cleanup
            reducer.cleanup(context);
            
            // 关闭输出文件
            context.close();
            
            endTime = System.currentTimeMillis();
            status = TaskStatus.COMPLETED;
            
            System.out.println("ReduceTask " + taskId + " completed in " + 
                              (endTime - startTime) + "ms, output: " + outputFile);
            
            // 调用完成回调
            if (completionCallback != null) {
                completionCallback.accept(taskId, outputFile);
            }
            
            return true;
            
        } catch (Exception e) {
            endTime = System.currentTimeMillis();
            status = TaskStatus.FAILED;
            System.err.println("ReduceTask " + taskId + " failed: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 创建Reducer实例
     */
    @SuppressWarnings("unchecked")
    private Reducer<Object, Object, Object, Object> createReducerInstance() throws Exception {
        Class<? extends Reducer> reducerClass = jobCfg.getReducerClass();
        return (Reducer<Object, Object, Object, Object>) reducerClass.getDeclaredConstructor().newInstance();
    }
    
    /**
     * 读取并分组输入文件数据
     */
    private Map<Object, List<Object>> readAndGroupInputFiles() throws IOException {
        Map<Object, List<Object>> groupedData = new TreeMap<>((o1, o2) -> {
            if (o1 instanceof Comparable && o2 instanceof Comparable) {
                return ((Comparable) o1).compareTo(o2);
            }
            return o1.hashCode() - o2.hashCode();
        });
        
        for (String inputFile : inputFiles) {
            if (new File(inputFile).exists()) {
                try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (!line.trim().isEmpty()) {
                            String[] parts = line.split("\t", 2);
                            if (parts.length == 2) {
                                Object key = parts[0];
                                Object value;
                                
                                // 尝试将value转换为Integer，如果失败则保持为String
                                try {
                                    value = Integer.parseInt(parts[1]);
                                } catch (NumberFormatException e) {
                                    value = parts[1];
                                }
                                
                                groupedData.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
                            }
                        }
                    }
                }
            }
        }
        
        return groupedData;
    }
    
    /**
     * Reduce任务专用的Context实现
     */
    private static class ReduceContextImpl implements Context {
        private final Configuration config;
        private final String taskId;
        private final PrintWriter writer;
        
        public ReduceContextImpl(Configuration config, String taskId, String outputFile) throws IOException {
            this.config = config;
            this.taskId = taskId;
            new File(outputFile).getParentFile().mkdirs();
            this.writer = new PrintWriter(new FileWriter(outputFile));
        }
        
        @Override
        public void write(Object key, Object value) {
            writer.println(key + "\t" + value);
        }
        
        @Override
        public Configuration getConfiguration() {
            return config;
        }
        
        @Override
        public String getTaskId() {
            return taskId;
        }
        
        public void close() {
            if (writer != null) {
                writer.close();
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
    
    public List<String> getInputFiles() {
        return inputFiles;
    }
}