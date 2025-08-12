/**
 * MapReduce框架线程池管理器
 *
 * 该类负责管理MapReduce任务的线程池资源，为Map和Reduce任务分别
 * 提供独立的线程池，并提供任务提交、状态监控和资源清理功能。
 *
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.scheduler;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolManager {

    private final ExecutorService mapExecutor;
    private final ExecutorService reduceExecutor;
    private final int maxThreads;
    private final AtomicInteger activeMapTasks = new AtomicInteger(0);
    private final AtomicInteger activeReduceTasks = new AtomicInteger(0);
    private final AtomicInteger completedMapTasks = new AtomicInteger(0);
    private final AtomicInteger completedReduceTasks = new AtomicInteger(0);

    public ThreadPoolManager(int maxThreads) {
        this.maxThreads = maxThreads;

        // 为Map和Reduce任务分别创建线程池
        int mapThreads = Math.max(1, maxThreads * 2 / 3); // Map任务占2/3线程
        int reduceThreads = Math.max(1, maxThreads - mapThreads); // Reduce任务占剩余线程

        this.mapExecutor = new ThreadPoolExecutor(
            mapThreads / 2,  // 核心线程数
            mapThreads,      // 最大线程数
            60L,             // 空闲线程存活时间
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new MapTaskThreadFactory(),
            new ThreadPoolExecutor.CallerRunsPolicy() // 拒绝策略
        );

        this.reduceExecutor = new ThreadPoolExecutor(
            reduceThreads / 2,  // 核心线程数
            reduceThreads,      // 最大线程数
            60L,                // 空闲线程存活时间
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ReduceTaskThreadFactory(),
            new ThreadPoolExecutor.CallerRunsPolicy() // 拒绝策略
        );

        System.out.println("ThreadPoolManager initialized with " + mapThreads +
                          " map threads and " + reduceThreads + " reduce threads");
    }

    /**
     * 提交Map任务
     */
    public <T> Future<T> submitMapTask(Callable<T> task) {
        activeMapTasks.incrementAndGet();
        return mapExecutor.submit(() -> {
            try {
                T result = task.call();
                completedMapTasks.incrementAndGet();
                return result;
            } finally {
                activeMapTasks.decrementAndGet();
            }
        });
    }

    /**
     * 提交Reduce任务
     */
    public <T> Future<T> submitReduceTask(Callable<T> task) {
        activeReduceTasks.incrementAndGet();
        return reduceExecutor.submit(() -> {
            try {
                T result = task.call();
                completedReduceTasks.incrementAndGet();
                return result;
            } finally {
                activeReduceTasks.decrementAndGet();
            }
        });
    }

    /**
     * 等待所有Map任务完成
     */
    public void waitForMapTasks() throws InterruptedException {
        while (activeMapTasks.get() > 0) {
            Thread.sleep(100);
        }
    }

    /**
     * 等待所有Reduce任务完成
     */
    public void waitForReduceTasks() throws InterruptedException {
        while (activeReduceTasks.get() > 0) {
            Thread.sleep(100);
        }
    }

    /**
     * 关闭线程池
     */
    public void shutdown() {
        System.out.println("Shutting down thread pools...");

        mapExecutor.shutdown();
        reduceExecutor.shutdown();

        try {
            // 等待任务完成，最多等待30秒
            if (!mapExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                mapExecutor.shutdownNow();
            }
            if (!reduceExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                reduceExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            mapExecutor.shutdownNow();
            reduceExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println("Thread pools shut down. Completed tasks - Map: " +
                          completedMapTasks.get() + ", Reduce: " + completedReduceTasks.get());
    }

    /**
     * 强制关闭线程池
     */
    public void shutdownNow() {
        mapExecutor.shutdownNow();
        reduceExecutor.shutdownNow();
    }

    /**
     * 获取线程池状态信息
     */
    public ThreadPoolStatus getStatus() {
        ThreadPoolExecutor mapPool = (ThreadPoolExecutor) mapExecutor;
        ThreadPoolExecutor reducePool = (ThreadPoolExecutor) reduceExecutor;

        return new ThreadPoolStatus(
            mapPool.getActiveCount(),
            mapPool.getCompletedTaskCount(),
            mapPool.getTaskCount(),
            reducePool.getActiveCount(),
            reducePool.getCompletedTaskCount(),
            reducePool.getTaskCount(),
            activeMapTasks.get(),
            activeReduceTasks.get(),
            completedMapTasks.get(),
            completedReduceTasks.get()
        );
    }

    /**
     * Map任务线程工厂
     */
    private static class MapTaskThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "MapTask-" + threadNumber.getAndIncrement());
            t.setDaemon(false);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

    /**
     * Reduce任务线程工厂
     */
    private static class ReduceTaskThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "ReduceTask-" + threadNumber.getAndIncrement());
            t.setDaemon(false);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

    /**
     * 线程池状态信息类
     */
    public static class ThreadPoolStatus {
        private final int mapActiveThreads;
        private final long mapCompletedTasks;
        private final long mapTotalTasks;
        private final int reduceActiveThreads;
        private final long reduceCompletedTasks;
        private final long reduceTotalTasks;
        private final int activeMapTasks;
        private final int activeReduceTasks;
        private final int completedMapTasks;
        private final int completedReduceTasks;

        public ThreadPoolStatus(int mapActiveThreads, long mapCompletedTasks, long mapTotalTasks,
                               int reduceActiveThreads, long reduceCompletedTasks, long reduceTotalTasks,
                               int activeMapTasks, int activeReduceTasks,
                               int completedMapTasks, int completedReduceTasks) {
            this.mapActiveThreads = mapActiveThreads;
            this.mapCompletedTasks = mapCompletedTasks;
            this.mapTotalTasks = mapTotalTasks;
            this.reduceActiveThreads = reduceActiveThreads;
            this.reduceCompletedTasks = reduceCompletedTasks;
            this.reduceTotalTasks = reduceTotalTasks;
            this.activeMapTasks = activeMapTasks;
            this.activeReduceTasks = activeReduceTasks;
            this.completedMapTasks = completedMapTasks;
            this.completedReduceTasks = completedReduceTasks;
        }

        @Override
        public String toString() {
            return String.format(
                "ThreadPool Status - Map: [Active: %d, Completed: %d/%d, Tasks: %d/%d], \n" +
                "Reduce: [Active: %d, Completed: %d/%d, Tasks: %d/%d]",
                mapActiveThreads, mapCompletedTasks, mapTotalTasks, completedMapTasks, activeMapTasks + completedMapTasks,
                reduceActiveThreads, reduceCompletedTasks, reduceTotalTasks, completedReduceTasks, activeReduceTasks + completedReduceTasks
            );
        }

        // Getter方法
        public int getMapActiveThreads() { return mapActiveThreads; }
        public long getMapCompletedTasks() { return mapCompletedTasks; }
        public long getMapTotalTasks() { return mapTotalTasks; }
        public int getReduceActiveThreads() { return reduceActiveThreads; }
        public long getReduceCompletedTasks() { return reduceCompletedTasks; }
        public long getReduceTotalTasks() { return reduceTotalTasks; }
        public int getActiveMapTasks() { return activeMapTasks; }
        public int getActiveReduceTasks() { return activeReduceTasks; }
        public int getCompletedMapTasks() { return completedMapTasks; }
        public int getCompletedReduceTasks() { return completedReduceTasks; }
    }
}