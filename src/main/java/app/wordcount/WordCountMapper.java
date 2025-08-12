/**
 * WordCount MapReduce应用的Mapper实现类
 * 
 * 该类负责Map阶段的数据处理，将输入的文本行按单词分割，
 * 对每个单词输出键值对(word, 1)，为后续的Reduce阶段做准备。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package app.wordcount;

import framework.core.*;
import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<Object, String, String, Integer> {
    
    private final static Integer ONE = 1;
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        // 可以在这里进行初始化操作
        System.out.println("WordCountMapper setup for task: " + context.getTaskId());
    }
    
    @Override
    public void map(Object key, String value, Context<String, Integer> context) 
            throws IOException, InterruptedException, Exception {
        
        if (value == null || value.trim().isEmpty()) {
            return;
        }
        
        // 转换为小写并移除标点符号
        String cleanedLine = value.toLowerCase().replaceAll("[^a-zA-Z0-9\\s]", " ");
        
        // 使用StringTokenizer分割单词
        StringTokenizer tokenizer = new StringTokenizer(cleanedLine);
        
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().trim();
            
            // 只过滤掉空字符串，保留所有有效单词
            if (!word.isEmpty()) {
                context.write(word, ONE);
            }
        }
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        // 可以在这里进行清理操作
        System.out.println("WordCountMapper cleanup for task: " + context.getTaskId());
    }
}