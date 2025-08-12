/**
 * MapReduce框架上下文接口
 * 
 * 该接口定义了Map和Reduce任务执行过程中的上下文环境，
 * 提供数据输出、配置获取和任务标识等核心功能。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.core;

/**
 * 上下文接口，提供输入输出操作
 */
public interface Context<KEYOUT, VALUEOUT> {
    
    /**
     * 写入键值对到输出
     * @param key 输出键
     * @param value 输出值
     * @throws Exception 写入异常
     */
    void write(KEYOUT key, VALUEOUT value) throws Exception;
    
    /**
     * 获取配置对象
     * @return 配置对象
     */
    Configuration getConfiguration();
    
    /**
     * 获取任务ID
     * @return 任务ID
     */
    String getTaskId();
}