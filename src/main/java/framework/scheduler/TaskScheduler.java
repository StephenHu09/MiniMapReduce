/**
 * MapReduce框架任务调度器
 *
 * 该类是MapReduce框架的核心调度组件，负责协调和管理Map和Reduce任务的执行，
 * 包括输入分片创建、任务分配、线程池管理和作业统计等功能。
 *
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.scheduler;

import framework.task.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

public class TaskScheduler {

    private final JobConfig jobCfg;
    private final ThreadPoolManager threadPoolManager;
    private final List<MapTask> mapTasks = new ArrayList<>();
    private final List<ReduceTask> reduceTasks = new ArrayList<>();
    private final Map<String, String> mapOutputFiles = new ConcurrentHashMap<>();
    private final Map<String, String> reduceOutputFiles = new ConcurrentHashMap<>();
    private long jobStartTime;
    private long jobEndTime;

    public TaskScheduler(JobConfig jobCfg) {
        this.jobCfg = jobCfg;
        this.threadPoolManager = new ThreadPoolManager(jobCfg.getMaxThreads());
    }

    /**
     * 执行完整的MapReduce作业
     */
    public boolean executeJob() {
        try {
            jobStartTime = System.currentTimeMillis();
            System.out.println("Starting MapReduce job with configuration: " + jobCfg);

            // 验证配置
            jobCfg.validate();

            // 清理输出目录
            cleanOutputDirectory();

            // 第一阶段：执行Map任务
            if (!executeMapPhase()) {
                System.err.println("Map phase failed");
                return false;
            }

            // 第二阶段：执行Reduce任务
            if (!executeReducePhase()) {
                System.err.println("Reduce phase failed");
                return false;
            }

            jobEndTime = System.currentTimeMillis();
            System.out.println("MapReduce job completed successfully in " +
                              (jobEndTime - jobStartTime) + "ms");

            // 清理临时文件
            cleanupTempFiles();

            return true;

        } catch (Exception e) {
            System.err.println("MapReduce job failed: " + e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            threadPoolManager.shutdown();
        }
    }

    /**
     * 执行Map阶段
     */
    private boolean executeMapPhase() throws Exception {
        System.out.println("Starting Map phase...");

        // 创建输入分片
        List<InputSplit> inputSplits = createInputSplits();
        System.out.println("Created " + inputSplits.size() + " input splits");

        // 创建Map任务
        List<Future<Boolean>> mapFutures = new ArrayList<>();
        BiConsumer<String, String> mapCompletionCallback = (taskId, outputFile) -> {
            mapOutputFiles.put(taskId, outputFile);
            System.out.println("Map task " + taskId + " completed, output: " + outputFile);
        };

        for (int i = 0; i < inputSplits.size(); i++) {
            String taskId = "map-task-" + i;
            MapTask mapTask = new MapTask(taskId, jobCfg, inputSplits.get(i), mapCompletionCallback);
            mapTasks.add(mapTask);

            Future<Boolean> future = threadPoolManager.submitMapTask(mapTask);
            mapFutures.add(future);
        }

        // 等待所有Map任务完成
        boolean allMapTasksSucceeded = true;
        for (Future<Boolean> future : mapFutures) {
            try {
                Boolean result = future.get();
                if (!result) {
                    allMapTasksSucceeded = false;
                }
            } catch (Exception e) {
                System.err.println("Map task failed: " + e.getMessage());
                allMapTasksSucceeded = false;
            }
        }

        threadPoolManager.waitForMapTasks();

        System.out.println("Map phase completed. Success: " + allMapTasksSucceeded +
                          ", Output files: " + mapOutputFiles.size());

        return allMapTasksSucceeded;
    }

    /**
     * 执行Reduce阶段
     */
    private boolean executeReducePhase() throws Exception {
        System.out.println("Starting Reduce phase...");

        // 分配Map输出文件给Reduce任务
        List<List<String>> reduceInputFiles = distributeMapOutputs();

        // 创建Reduce任务
        List<Future<Boolean>> reduceFutures = new ArrayList<>();
        BiConsumer<String, String> reduceCompletionCallback = (taskId, outputFile) -> {
            reduceOutputFiles.put(taskId, outputFile);
            System.out.println("Reduce task " + taskId + " completed, output: " + outputFile);
        };

        for (int i = 0; i < jobCfg.getReduceTaskCount(); i++) {
            String taskId = "reduce-task-" + i;
            ReduceTask reduceTask = new ReduceTask(taskId, jobCfg,
                                                  reduceInputFiles.get(i), reduceCompletionCallback);
            reduceTasks.add(reduceTask);

            Future<Boolean> future = threadPoolManager.submitReduceTask(reduceTask);
            reduceFutures.add(future);
        }

        // 等待所有Reduce任务完成
        boolean allReduceTasksSucceeded = true;
        for (Future<Boolean> future : reduceFutures) {
            try {
                Boolean result = future.get();
                if (!result) {
                    allReduceTasksSucceeded = false;
                }
            } catch (Exception e) {
                System.err.println("Reduce task failed: " + e.getMessage());
                allReduceTasksSucceeded = false;
            }
        }

        threadPoolManager.waitForReduceTasks();

        System.out.println("Reduce phase completed. Success: " + allReduceTasksSucceeded +
                          ", Output files: " + reduceOutputFiles.size());

        return allReduceTasksSucceeded;
    }

    /**
     * 创建输入分片
     */
    private List<InputSplit> createInputSplits() throws IOException {
        List<InputSplit> splits = new ArrayList<>();
        File inputDir = new File(jobCfg.getInputDir());

        if (!inputDir.exists() || !inputDir.isDirectory()) {
            throw new IOException("Input directory does not exist: " + jobCfg.getInputDir());
        }

        // 读取所有输入文件
        List<String> allLines = new ArrayList<>();
        File[] files = inputDir.listFiles((dir, name) -> name.endsWith(".txt"));

        if (files != null) {
            for (File file : files) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (!line.trim().isEmpty()) {
                            allLines.add(line);
                        }
                    }
                }
            }
        }

        // 将数据分割成指定数量的分片
        int totalLines = allLines.size();
        int linesPerSplit = Math.max(1, totalLines / jobCfg.getMapTaskCount());

        for (int i = 0; i < jobCfg.getMapTaskCount(); i++) {
            int startIndex = i * linesPerSplit;
            int endIndex = (i == jobCfg.getMapTaskCount() - 1) ?
                          totalLines : Math.min(startIndex + linesPerSplit, totalLines);

            if (startIndex < totalLines) {
                List<String> splitLines = allLines.subList(startIndex, endIndex);
                splits.add(new InputSplit(i, new ArrayList<>(splitLines)));
            }
        }

        return splits;
    }

    /**
     * 分配Map输出文件给Reduce任务
     */
    private List<List<String>> distributeMapOutputs() {
        List<List<String>> reduceInputFiles = new ArrayList<>();

        // 初始化每个Reduce任务的输入文件列表
        for (int i = 0; i < jobCfg.getReduceTaskCount(); i++) {
            reduceInputFiles.add(new ArrayList<>());
        }

        // 将Map输出文件分配给Reduce任务
        // 简单的轮询分配策略
        int reduceIndex = 0;
        for (String outputFile : mapOutputFiles.values()) {
            reduceInputFiles.get(reduceIndex).add(outputFile);
            reduceIndex = (reduceIndex + 1) % jobCfg.getReduceTaskCount();
        }

        return reduceInputFiles;
    }

    /**
     * 清理输出目录
     */
    private void cleanOutputDirectory() {
        File outputDir = new File(jobCfg.getOutputDir());
        if (outputDir.exists()) {
            // 调试模式：保留输出目录中的所有文件
            File[] files = outputDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        System.out.println("  - Keeping file: " + file.getName());
                        // file.delete(); // 代码不做删除
                    }
                }
            }
        } else {
            outputDir.mkdirs();
        }
    }

    /**
     * 清理临时文件
     */
    private void cleanupTempFiles() {
        File tempDir = new File(jobCfg.getTempDir());
        if (tempDir.exists()) {
            // 调试模式：保留所有临时文件
            File[] files = tempDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile() && (file.getName().startsWith("map-output-") ||
                                         file.getName().startsWith("spill-"))) {
                        System.out.println("  - Keeping temp file: " + file.getName());
                        // file.delete(); // 代码不做删除
                    }
                }
            }
        }
    }

    /**
     * 获取作业执行统计信息
     */
    public JobStatistics getJobStatistics() {
        return new JobStatistics(
            mapTasks.size(),
            reduceTasks.size(),
            mapOutputFiles.size(),
            reduceOutputFiles.size(),
            jobStartTime,
            jobEndTime,
            threadPoolManager.getStatus()
        );
    }

    /**
     * 作业统计信息类
     */
    public static class JobStatistics {
        private final int totalMapTasks;
        private final int totalReduceTasks;
        private final int completedMapTasks;
        private final int completedReduceTasks;
        private final long startTime;
        private final long endTime;
        private final ThreadPoolManager.ThreadPoolStatus threadPoolStatus;

        public JobStatistics(int totalMapTasks, int totalReduceTasks,
                           int completedMapTasks, int completedReduceTasks,
                           long startTime, long endTime,
                           ThreadPoolManager.ThreadPoolStatus threadPoolStatus) {
            this.totalMapTasks = totalMapTasks;
            this.totalReduceTasks = totalReduceTasks;
            this.completedMapTasks = completedMapTasks;
            this.completedReduceTasks = completedReduceTasks;
            this.startTime = startTime;
            this.endTime = endTime;
            this.threadPoolStatus = threadPoolStatus;
        }

        public long getExecutionTime() {
            return endTime - startTime;
        }

        @Override
        public String toString() {
            return String.format(
                "Job Statistics - Map Tasks: %d/%d, Reduce Tasks: %d/%d, " +
                "Execution Time: %dms, \n%s",
                completedMapTasks, totalMapTasks,
                completedReduceTasks, totalReduceTasks,
                getExecutionTime(),
                threadPoolStatus
            );
        }

        // Getter方法
        public int getTotalMapTasks() { return totalMapTasks; }
        public int getTotalReduceTasks() { return totalReduceTasks; }
        public int getCompletedMapTasks() { return completedMapTasks; }
        public int getCompletedReduceTasks() { return completedReduceTasks; }
        public long getStartTime() { return startTime; }
        public long getEndTime() { return endTime; }
        public ThreadPoolManager.ThreadPoolStatus getThreadPoolStatus() { return threadPoolStatus; }
    }
}