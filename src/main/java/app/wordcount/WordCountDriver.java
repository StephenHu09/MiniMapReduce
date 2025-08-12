/**
 * WordCount MapReduce应用驱动类
 * 
 * 该类是WordCount应用的主入口，继承自框架的Driver类，
 * 负责配置和启动WordCount MapReduce作业。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package app.wordcount;

import framework.driver.Driver;
import framework.core.Configuration;

public class WordCountDriver extends Driver {
    
    /**
     * 默认构造函数
     */
    public WordCountDriver() {
        super();
    }
    
    /**
     * 使用自定义配置的构造函数
     */
    public WordCountDriver(Configuration config) {
        super(config);
    }
    
    @Override
    protected void configureJob() throws Exception {
        // 设置Mapper和Reducer类
        setMapperClass(WordCountMapper.class);
        setReducerClass(WordCountReducer.class);
        
        // 可以根据需要调整任务数量
        // setMapTaskCount(4);
        // setReduceTaskCount(2);
        
        System.out.println("WordCount job configured:");
        System.out.println("  Mapper: " + WordCountMapper.class.getSimpleName());
        System.out.println("  Reducer: " + WordCountReducer.class.getSimpleName());
        System.out.println("  Map tasks: " + getJobConfiguration().getMapTaskCount());
        System.out.println("  Reduce tasks: " + getJobConfiguration().getReduceTaskCount());
        System.out.println("  Input directory: " + getJobConfiguration().getInputDir());
        System.out.println("  Output directory: " + getJobConfiguration().getOutputDir());
    }
    
    @Override
    protected void printUsage() {
        System.err.println("Usage: WordCountDriver <input_dir> <output_dir> [options]");
        System.err.println("\nDescription:");
        System.err.println("  Counts the occurrences of each word in the input files.");
        System.err.println("  Input files should be text files (.txt) in the input directory.");
        System.err.println("  Output will be written to part-r-XXXXX.txt files in the output directory.");
        System.err.println("\nOptions:");
        System.err.println("  -mapTasks=<num>      Number of map tasks (default: 4)");
        System.err.println("  -reduceTasks=<num>   Number of reduce tasks (default: 2)");
        System.err.println("  -bufferSize=<mb>     Buffer size in MB (default: 100)");
        System.err.println("  -maxThreads=<num>    Maximum number of threads (default: 8)");
        System.err.println("  -noMonitoring        Disable performance monitoring");
        System.err.println("\nExample:");
        System.err.println("  java app.wordcount.WordCountDriver input output -mapTasks=6 -reduceTasks=3");
    }
    
    /**
     * 主方法
     */
    public static void main(String[] args) {
        System.out.println("=== WordCount MapReduce Application ===");
        System.out.println("Starting WordCount job...");
        
        WordCountDriver driver = new WordCountDriver();
        runJob(driver, args);
    }
}