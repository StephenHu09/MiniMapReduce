/**
 * MapReduce框架性能监控器
 * 
 * 该类提供实时性能监控功能，包括内存使用、CPU使用率、线程数量等指标的监控，
 * 支持定时采样、数据导出和性能报告生成，用于MapReduce作业的性能分析。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package test;

import java.io.*;
import java.lang.management.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class PerformanceMonitor {
    
    private final MemoryMXBean memoryBean;
    private final ThreadMXBean threadBean;
    private final OperatingSystemMXBean osBean;
    private final RuntimeMXBean runtimeBean;
    
    private final List<PerformanceSnapshot> snapshots;
    private final ScheduledExecutorService scheduler;
    private final SimpleDateFormat dateFormat;
    
    private long startTime;
    private boolean monitoring;
    
    public PerformanceMonitor() {
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.threadBean = ManagementFactory.getThreadMXBean();
        this.osBean = ManagementFactory.getOperatingSystemMXBean();
        this.runtimeBean = ManagementFactory.getRuntimeMXBean();
        
        this.snapshots = new ArrayList<>();
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        this.monitoring = false;
    }
    
    /**
     * 开始监控
     */
    public void startMonitoring(long intervalMs) {
        if (monitoring) {
            return;
        }
        
        startTime = System.currentTimeMillis();
        monitoring = true;
        snapshots.clear();
        
        System.out.println("Performance monitoring started with interval: " + intervalMs + "ms");
        
        scheduler.scheduleAtFixedRate(() -> {
            if (monitoring) {
                takeSnapshot();
            }
        }, 0, intervalMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 停止监控
     */
    public void stopMonitoring() {
        monitoring = false;
        scheduler.shutdown();
        
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        System.out.println("Performance monitoring stopped. Total snapshots: " + snapshots.size());
    }
    
    /**
     * 拍摄性能快照
     */
    private void takeSnapshot() {
        long timestamp = System.currentTimeMillis();
        long elapsedTime = timestamp - startTime;
        
        // 内存使用情况
        MemoryUsage heapMemory = memoryBean.getHeapMemoryUsage();
        MemoryUsage nonHeapMemory = memoryBean.getNonHeapMemoryUsage();
        
        // 线程信息
        int threadCount = threadBean.getThreadCount();
        int peakThreadCount = threadBean.getPeakThreadCount();
        
        // CPU使用率（如果支持）
        double cpuUsage = -1;
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            com.sun.management.OperatingSystemMXBean sunOsBean = 
                (com.sun.management.OperatingSystemMXBean) osBean;
            cpuUsage = sunOsBean.getProcessCpuLoad() * 100;
        }
        
        // 系统负载
        double systemLoad = osBean.getSystemLoadAverage();
        
        PerformanceSnapshot snapshot = new PerformanceSnapshot(
            timestamp, elapsedTime,
            heapMemory.getUsed(), heapMemory.getMax(),
            nonHeapMemory.getUsed(), nonHeapMemory.getMax(),
            threadCount, peakThreadCount,
            cpuUsage, systemLoad
        );
        
        snapshots.add(snapshot);
    }
    
    /**
     * 获取性能统计信息
     */
    public PerformanceStatistics getStatistics() {
        if (snapshots.isEmpty()) {
            return new PerformanceStatistics();
        }
        
        // 计算统计信息
        long maxHeapUsed = 0;
        long maxNonHeapUsed = 0;
        int maxThreads = 0;
        double maxCpuUsage = 0;
        double totalCpuUsage = 0;
        int cpuSamples = 0;
        
        for (PerformanceSnapshot snapshot : snapshots) {
            maxHeapUsed = Math.max(maxHeapUsed, snapshot.heapUsed);
            maxNonHeapUsed = Math.max(maxNonHeapUsed, snapshot.nonHeapUsed);
            maxThreads = Math.max(maxThreads, snapshot.threadCount);
            
            if (snapshot.cpuUsage >= 0) {
                maxCpuUsage = Math.max(maxCpuUsage, snapshot.cpuUsage);
                totalCpuUsage += snapshot.cpuUsage;
                cpuSamples++;
            }
        }
        
        double avgCpuUsage = cpuSamples > 0 ? totalCpuUsage / cpuSamples : -1;
        
        PerformanceSnapshot firstSnapshot = snapshots.get(0);
        PerformanceSnapshot lastSnapshot = snapshots.get(snapshots.size() - 1);
        
        return new PerformanceStatistics(
            snapshots.size(),
            lastSnapshot.elapsedTime,
            maxHeapUsed, firstSnapshot.heapMax,
            maxNonHeapUsed, firstSnapshot.nonHeapMax,
            maxThreads,
            avgCpuUsage, maxCpuUsage
        );
    }
    
    /**
     * 将监控数据保存到文件
     */
    public void saveToFile(String filePath) throws IOException {
        File file = new File(filePath);
        file.getParentFile().mkdirs();
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            // 写入头部
            writer.println("Timestamp,ElapsedTime(ms),HeapUsed(MB),HeapMax(MB)," +
                          "NonHeapUsed(MB),NonHeapMax(MB),ThreadCount,PeakThreadCount," +
                          "CPUUsage(%),SystemLoad");
            
            // 写入数据
            for (PerformanceSnapshot snapshot : snapshots) {
                writer.printf("%s,%d,%.2f,%.2f,%.2f,%.2f,%d,%d,%.2f,%.2f%n",
                    dateFormat.format(new Date(snapshot.timestamp)),
                    snapshot.elapsedTime,
                    snapshot.heapUsed / (1024.0 * 1024.0),
                    snapshot.heapMax / (1024.0 * 1024.0),
                    snapshot.nonHeapUsed / (1024.0 * 1024.0),
                    snapshot.nonHeapMax / (1024.0 * 1024.0),
                    snapshot.threadCount,
                    snapshot.peakThreadCount,
                    snapshot.cpuUsage,
                    snapshot.systemLoad
                );
            }
        }
        
        System.out.println("Performance data saved to: " + filePath);
    }
    
    /**
     * 打印性能报告
     */
    public void printReport() {
        PerformanceStatistics stats = getStatistics();
        
        System.out.println("\n=== Performance Report ===");
        System.out.println("Monitoring Duration: " + stats.totalTime + "ms");
        System.out.println("Total Snapshots: " + stats.snapshotCount);
        System.out.println("\nMemory Usage:");
        System.out.printf("  Max Heap Used: %.2f MB / %.2f MB (%.1f%%)%n",
            stats.maxHeapUsed / (1024.0 * 1024.0),
            stats.heapMax / (1024.0 * 1024.0),
            (stats.maxHeapUsed * 100.0) / stats.heapMax);
        System.out.printf("  Max Non-Heap Used: %.2f MB / %.2f MB%n",
            stats.maxNonHeapUsed / (1024.0 * 1024.0),
            stats.nonHeapMax / (1024.0 * 1024.0));
        System.out.println("\nThread Usage:");
        System.out.println("  Max Thread Count: " + stats.maxThreads);
        
        if (stats.avgCpuUsage >= 0) {
            System.out.println("\nCPU Usage:");
            System.out.printf("  Average CPU: %.2f%%%n", stats.avgCpuUsage);
            System.out.printf("  Peak CPU: %.2f%%%n", stats.maxCpuUsage);
        }
        
        System.out.println("========================\n");
    }
    
    /**
     * 性能快照类
     */
    private static class PerformanceSnapshot {
        final long timestamp;
        final long elapsedTime;
        final long heapUsed;
        final long heapMax;
        final long nonHeapUsed;
        final long nonHeapMax;
        final int threadCount;
        final int peakThreadCount;
        final double cpuUsage;
        final double systemLoad;
        
        PerformanceSnapshot(long timestamp, long elapsedTime,
                           long heapUsed, long heapMax,
                           long nonHeapUsed, long nonHeapMax,
                           int threadCount, int peakThreadCount,
                           double cpuUsage, double systemLoad) {
            this.timestamp = timestamp;
            this.elapsedTime = elapsedTime;
            this.heapUsed = heapUsed;
            this.heapMax = heapMax;
            this.nonHeapUsed = nonHeapUsed;
            this.nonHeapMax = nonHeapMax;
            this.threadCount = threadCount;
            this.peakThreadCount = peakThreadCount;
            this.cpuUsage = cpuUsage;
            this.systemLoad = systemLoad;
        }
    }
    
    /**
     * 性能统计信息类
     */
    public static class PerformanceStatistics {
        final int snapshotCount;
        final long totalTime;
        final long maxHeapUsed;
        final long heapMax;
        final long maxNonHeapUsed;
        final long nonHeapMax;
        final int maxThreads;
        final double avgCpuUsage;
        final double maxCpuUsage;
        
        PerformanceStatistics() {
            this(0, 0, 0, 0, 0, 0, 0, -1, -1);
        }
        
        PerformanceStatistics(int snapshotCount, long totalTime,
                             long maxHeapUsed, long heapMax,
                             long maxNonHeapUsed, long nonHeapMax,
                             int maxThreads,
                             double avgCpuUsage, double maxCpuUsage) {
            this.snapshotCount = snapshotCount;
            this.totalTime = totalTime;
            this.maxHeapUsed = maxHeapUsed;
            this.heapMax = heapMax;
            this.maxNonHeapUsed = maxNonHeapUsed;
            this.nonHeapMax = nonHeapMax;
            this.maxThreads = maxThreads;
            this.avgCpuUsage = avgCpuUsage;
            this.maxCpuUsage = maxCpuUsage;
        }
    }
    
    /**
     * 主方法，用于独立测试性能监控
     */
    public static void main(String[] args) throws InterruptedException {
        PerformanceMonitor monitor = new PerformanceMonitor();
        
        // 开始监控
        monitor.startMonitoring(1000); // 每秒采样一次
        
        // 模拟一些工作负载
        System.out.println("Simulating workload...");
        for (int i = 0; i < 10; i++) {
            // 创建一些对象来消耗内存
            List<String> data = new ArrayList<>();
            for (int j = 0; j < 10000; j++) {
                data.add("Test data " + j);
            }
            
            Thread.sleep(500);
            
            // 清理数据
            data.clear();
            System.gc();
            
            Thread.sleep(500);
        }
        
        // 停止监控
        monitor.stopMonitoring();
        
        // 打印报告
        monitor.printReport();
        
        // 保存数据
        try {
            monitor.saveToFile("logs/performance_test.csv");
        } catch (IOException e) {
            System.err.println("Failed to save performance data: " + e.getMessage());
        }
    }
}