/**
 * MapReduce框架驱动基类
 *
 * 该抽象类提供MapReduce作业的配置、执行和管理功能，
 * 包括命令行参数解析、作业调度、输出文件处理等核心功能。
 *
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.driver;

import framework.core.*;
import framework.scheduler.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;


public abstract class Driver {

    protected JobConfig jobCfg;
    protected TaskScheduler scheduler;

    /**
     * 构造函数，初始化配置
     */
    public Driver() {
        // 加载默认配置
        Configuration config = new Configuration();
        this.jobCfg = new JobConfig(config);
    }

    /**
     * 构造函数，使用自定义配置
     */
    public Driver(Configuration config) {
        this.jobCfg = new JobConfig(config);
    }

    /**
     * 抽象方法：配置作业参数
     * 子类必须实现此方法来设置Mapper、Reducer类等
     */
    protected abstract void configureJob() throws Exception;

    /**
     * 运行MapReduce作业
     */
    public boolean run(String[] args) {
        try {
            // 解析命令行参数
            if (!parseArguments(args)) {
                printUsage();
                return false;
            }

            // 配置作业
            configureJob();

            // 验证配置
            jobCfg.validate();

            // 创建任务调度器
            scheduler = new TaskScheduler(jobCfg);

            // 执行作业
            System.out.println("Starting MapReduce job...");
            boolean success = scheduler.executeJob();

            if (success) {
                System.out.println("Job completed successfully!");

                // 合并输出文件并重命名
                try {
                    mergeAndRenameOutputFiles();
                } catch (IOException e) {
                    System.err.println("Warning: Failed to merge output files: " + e.getMessage());
                }

                // 打印统计信息
                if (jobCfg.isEnableMonitoring()) {
                    TaskScheduler.JobStatistics stats = scheduler.getJobStatistics();
                    System.out.println("\n" + stats);
                }
            } else {
                System.err.println("Job failed!");
            }

            return success;

        } catch (Exception e) {
            System.err.println("Driver execution failed: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 解析命令行参数
     * 默认实现解析输入和输出目录
     */
    protected boolean parseArguments(String[] args) {
        if (args.length < 2) {
            return false;
        }

        String inputDir = args[0];
        String outputDir = args[1];

        jobCfg.setInputDir(inputDir);
        jobCfg.setOutputDir(outputDir);

        // 解析可选参数
        for (int i = 2; i < args.length; i++) {
            String arg = args[i];

            if (arg.startsWith("-mapTasks=")) {
                int mapTasks = Integer.parseInt(arg.substring("-mapTasks=".length()));
                jobCfg.setMapTaskCount(mapTasks);
            } else if (arg.startsWith("-reduceTasks=")) {
                int reduceTasks = Integer.parseInt(arg.substring("-reduceTasks=".length()));
                jobCfg.setReduceTaskCount(reduceTasks);
            } else if (arg.startsWith("-bufferSize=")) {
                int bufferSize = Integer.parseInt(arg.substring("-bufferSize=".length()));
                jobCfg.setBufferSizeKB(bufferSize);
            } else if (arg.startsWith("-maxThreads=")) {
                int maxThreads = Integer.parseInt(arg.substring("-maxThreads=".length()));
                jobCfg.setMaxThreads(maxThreads);
            } else if (arg.equals("-noMonitoring")) {
                jobCfg.setEnableMonitoring(false);
            }
        }

        return true;
    }

    /**
     * 打印使用说明
     */
    protected void printUsage() {
        System.err.println("Usage: " + this.getClass().getSimpleName() + " <input_dir> <output_dir> [options]");
        System.err.println("Options:");
        System.err.println("  -mapTasks=<num>      Number of map tasks (default: 4)");
        System.err.println("  -reduceTasks=<num>   Number of reduce tasks (default: 2)");
        System.err.println("  -bufferSize=<mb>     Buffer size in MB (default: 100)");
        System.err.println("  -maxThreads=<num>    Maximum number of threads (default: 8)");
        System.err.println("  -noMonitoring        Disable performance monitoring");
    }

    /**
     * 设置Mapper类
     */
    protected void setMapperClass(Class<? extends Mapper> mapperClass) {
        jobCfg.setMapperClass(mapperClass);
    }

    /**
     * 设置Reducer类
     */
    protected void setReducerClass(Class<? extends Reducer> reducerClass) {
        jobCfg.setReducerClass(reducerClass);
    }

    /**
     * 设置Map任务数量
     */
    protected void setMapTaskCount(int count) {
        jobCfg.setMapTaskCount(count);
    }

    /**
     * 设置Reduce任务数量
     */
    protected void setReduceTaskCount(int count) {
        jobCfg.setReduceTaskCount(count);
    }

    /**
     * 设置缓存大小
     */
    protected void setBufferSizeKB(int sizeKB) {
        jobCfg.setBufferSizeKB(sizeKB);
    }

    /**
     * 设置溢出阈值
     */
    protected void setSpillThreshold(double threshold) {
        jobCfg.setSpillThreshold(threshold);
    }

    /**
     * 设置最大线程数
     */
    protected void setMaxThreads(int maxThreads) {
        jobCfg.setMaxThreads(maxThreads);
    }

    /**
     * 启用或禁用监控
     */
    protected void setEnableMonitoring(boolean enable) {
        jobCfg.setEnableMonitoring(enable);
    }

    /**
     * 获取作业配置
     */
    protected JobConfig getJobConfiguration() {
        return jobCfg;
    }

    /**
     * 获取任务调度器
     */
    protected TaskScheduler getScheduler() {
        return scheduler;
    }

    /**
     * 合并输出文件并重命名
     * 本方法将多个part文件中的词频统计结果进行汇总合并，而不是简单的文本拼接
     */
    private void mergeAndRenameOutputFiles() throws IOException {
        String outputDir = jobCfg.getOutputDir();
        String inputDir = jobCfg.getInputDir();

        // 获取输入目录中的第一个.txt文件名作为基础名称
        String baseFileName = getFirstInputFileName(inputDir);
        if (baseFileName == null) {
            System.out.println("Warning: No input .txt files found, using default output name");
            baseFileName = "result";
        }

        // 查找所有part-r-*.txt文件
        File outputDirFile = new File(outputDir);
        File[] partFiles = outputDirFile.listFiles((dir, name) ->
            name.startsWith("part-r-") && name.endsWith(".txt"));

        if (partFiles == null || partFiles.length == 0) {
            System.out.println("Warning: No output part files found to merge");
            return;
        }

        // 排序文件以确保合并顺序
        Arrays.sort(partFiles, Comparator.comparing(File::getName));

        // 创建合并后的文件名
        String mergedFileName = baseFileName + "_out.txt";
        File mergedFile = new File(outputDir, mergedFileName);

        System.out.println("Merging " + partFiles.length + " output files into: " + mergedFileName);
        System.out.println("Performing word frequency aggregation...");

        // 使用Map来汇总词频统计
        Map<String, Integer> wordCountMap = new TreeMap<>(); // TreeMap自动按key排序

        // 读取所有part文件并汇总词频
        for (File partFile : partFiles) {
            System.out.println("Processing: " + partFile.getName());
            try (BufferedReader reader = new BufferedReader(new FileReader(partFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty()) {
                        // 解析词频数据：格式为 "单词\t数量"
                        String[] parts = line.split("\t");
                        if (parts.length == 2) {
                            String word = parts[0];
                            try {
                                int count = Integer.parseInt(parts[1]);
                                // 汇总相同单词的词频
                                wordCountMap.merge(word, count, Integer::sum);
                            } catch (NumberFormatException e) {
                                System.out.println("Warning: Invalid count format in line: " + line);
                            }
                        } else {
                            System.out.println("Warning: Invalid line format: " + line);
                        }
                    }
                }
            }
        }

        // 写入合并后的结果
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(mergedFile))) {
            for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
                writer.write(entry.getKey() + "\t" + entry.getValue());
                writer.newLine();
            }
        }

        // 移动原始的part文件移到temp目录
        File tempDir = new File("temp");
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }

        for (File partFile : partFiles) {
            File targetFile = new File(tempDir, partFile.getName());
            try {
                // 如果目标文件已存在，先删除
                if (targetFile.exists()) {
                    targetFile.delete();
                }
                // 移动文件
                partFile.renameTo(targetFile);
            } catch (Exception e) {
                System.out.println("Warning: Error moving " + partFile.getName() + ": " + e.getMessage());
            }
        }

        System.out.println("Word frequency aggregation completed!");
        System.out.println("Total unique words: " + wordCountMap.size());
        System.out.println("Output files merged successfully into: " + mergedFileName);
    }

    /**
     * 获取输入目录中第一个.txt文件的基础名称（不含扩展名）
     */
    private String getFirstInputFileName(String inputDir) {
        File inputDirFile = new File(inputDir);
        if (!inputDirFile.exists() || !inputDirFile.isDirectory()) {
            return null;
        }

        File[] txtFiles = inputDirFile.listFiles((dir, name) ->
            name.toLowerCase().endsWith(".txt"));

        if (txtFiles != null && txtFiles.length > 0) {
            String fileName = txtFiles[0].getName();
            // 移除.txt扩展名
            return fileName.substring(0, fileName.lastIndexOf('.'));
        }

        return null;
    }

    /**
     * 主方法模板，子类可以直接使用
     */
    public static void runJob(Driver driver, String[] args) {
        long startTime = System.currentTimeMillis();

        boolean success = driver.run(args);

        long endTime = System.currentTimeMillis();
        System.out.println("\nTotal execution time: " + (endTime - startTime) + "ms");

        System.exit(success ? 0 : 1);
    }
}