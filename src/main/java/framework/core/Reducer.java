/**
 * MapReduce框架的Reducer抽象基类
 * 
 * 该抽象类定义了Reduce阶段的核心接口，用户需要继承此类并实现
 * reduce方法来处理相同键的所有值。提供了setup和cleanup钩子方法。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.core;

import java.util.Iterator;

public abstract class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    
    /**
     * Reduce方法，处理相同键的所有值
     * @param key 输入键
     * @param values 相同键的所有值的迭代器
     * @param context 上下文对象，用于输出最终结果
     * @throws Exception 处理异常
     */
    public abstract void reduce(KEYIN key, Iterable<VALUEIN> values, Context<KEYOUT, VALUEOUT> context) throws Exception;
    
    /**
     * 初始化方法，在reduce任务开始前调用
     * @param context 上下文对象
     */
    public void setup(Context<KEYOUT, VALUEOUT> context) throws Exception {
        // 默认空实现，子类可以重写
    }
    
    /**
     * 清理方法，在reduce任务结束后调用
     * @param context 上下文对象
     */
    public void cleanup(Context<KEYOUT, VALUEOUT> context) throws Exception {
        // 默认空实现，子类可以重写
    }
}