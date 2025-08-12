/**
 * MapReduce框架任务状态枚举
 * 
 * 该枚举定义了MapReduce任务在执行过程中的各种状态，
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.task;

public enum TaskStatus {
    PENDING,    // 等待执行
    RUNNING,    // 正在执行
    COMPLETED,  // 执行完成
    FAILED      // 执行失败
}