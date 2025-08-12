/**
 * WordCount MapReduce应用的Reducer实现类
 * 
 * 该类负责Reduce阶段的数据处理，将Map阶段输出的相同单词的计数
 * 进行累加，最终输出每个单词的总出现次数。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package app.wordcount;

import framework.core.*;
import java.io.IOException;

public class WordCountReducer extends Reducer<String, Integer, String, Integer> {
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        // 可以在这里进行初始化操作
        System.out.println("WordCountReducer setup for task: " + context.getTaskId());
    }
    
    @Override
    public void reduce(String key, Iterable<Integer> values, Context<String, Integer> context) 
            throws IOException, InterruptedException, Exception {
        
        int sum = 0;
        
        // 累加所有值
        for (Integer value : values) {
            if (value != null) {
                sum += value;
            }
        }
        
        // 输出单词和其总计数
        context.write(key, sum);
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        // 可以在这里进行清理操作
        System.out.println("WordCountReducer cleanup for task: " + context.getTaskId());
    }
}