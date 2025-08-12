/**
 * MapReduce框架的Mapper抽象基类
 * 
 * 该抽象类定义了Map阶段的核心接口，用户需要继承此类并实现
 * map方法来处理输入数据。提供了setup和cleanup钩子方法。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.core;

public abstract class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    
    /**
     * Map方法，处理输入的键值对
     * @param key 输入键
     * @param value 输入值
     * @param context 上下文对象，用于输出中间结果
     * @throws Exception 处理异常
     */
    public abstract void map(KEYIN key, VALUEIN value, Context<KEYOUT, VALUEOUT> context) throws Exception;
    
    /**
     * 初始化方法，在map任务开始前调用
     * @param context 上下文对象
     */
    public void setup(Context<KEYOUT, VALUEOUT> context) throws Exception {
        // 默认空实现，子类可以重写
    }
    
    /**
     * 清理方法，在map任务结束后调用
     * @param context 上下文对象
     */
    public void cleanup(Context<KEYOUT, VALUEOUT> context) throws Exception {
        // 默认空实现，子类可以重写
    }
}