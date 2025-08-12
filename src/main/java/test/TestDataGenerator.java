/**
 * MapReduce框架测试数据生成器
 * 
 * 该类提供多种测试数据生成功能，包括随机文本文件生成、
 * 指定单词频率文件生成和多文件批量生成，用于MapReduce框架的测试。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package test;

import java.io.*;
import java.util.*;

public class TestDataGenerator {
    
    private static final String[] SAMPLE_WORDS = {
        "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
        "hello", "world", "java", "programming", "mapreduce", "framework",
        "big", "data", "processing", "distributed", "computing", "algorithm",
        "software", "development", "university", "student", "project",
        "implementation", "multithreading", "parallel", "execution", "performance"
    };
    
    private static final String[] SAMPLE_SENTENCES = {
        "The quick brown fox jumps over the lazy dog.",
        "Hello world from Java programming.",
        "MapReduce is a powerful framework for big data processing.",
        "Distributed computing enables parallel execution of algorithms.",
        "Software development requires careful implementation and testing.",
        "University students work on challenging programming projects.",
        "Multithreading improves application performance significantly.",
        "Big data analytics helps organizations make better decisions.",
        "Java provides excellent support for concurrent programming.",
        "Framework design patterns simplify software architecture."
    };
    
    private final Random random;
    
    public TestDataGenerator() {
        this.random = new Random();
    }
    
    public TestDataGenerator(long seed) {
        this.random = new Random(seed);
    }
    
    /**
     * 生成指定行数的随机文本文件
     */
    public void generateTextFile(String filePath, int lineCount) throws IOException {
        File file = new File(filePath);
        file.getParentFile().mkdirs();
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            for (int i = 0; i < lineCount; i++) {
                String line = generateRandomLine();
                writer.println(line);
            }
        }
        
        System.out.println("Generated test file: " + filePath + " with " + lineCount + " lines");
    }
    
    /**
     * 生成多个测试文件
     */
    public void generateMultipleFiles(String directory, int fileCount, int linesPerFile) throws IOException {
        File dir = new File(directory);
        dir.mkdirs();
        
        for (int i = 0; i < fileCount; i++) {
            String fileName = String.format("test%03d.txt", i + 1);
            String filePath = directory + File.separator + fileName;
            generateTextFile(filePath, linesPerFile);
        }
        
        System.out.println("Generated " + fileCount + " test files in directory: " + directory);
    }
    
    /**
     * 生成包含指定单词频率的测试文件
     */
    public void generateWordFrequencyFile(String filePath, Map<String, Integer> wordFrequencies) throws IOException {
        File file = new File(filePath);
        file.getParentFile().mkdirs();
        
        List<String> words = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : wordFrequencies.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                words.add(entry.getKey());
            }
        }
        
        // 打乱单词顺序
        Collections.shuffle(words, random);
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            int wordsPerLine = 5 + random.nextInt(10); // 每行5-14个单词
            int currentLineWords = 0;
            
            for (String word : words) {
                if (currentLineWords > 0) {
                    writer.print(" ");
                }
                writer.print(word);
                currentLineWords++;
                
                if (currentLineWords >= wordsPerLine) {
                    writer.println();
                    currentLineWords = 0;
                    wordsPerLine = 5 + random.nextInt(10);
                }
            }
            
            if (currentLineWords > 0) {
                writer.println();
            }
        }
        
        System.out.println("Generated word frequency file: " + filePath);
    }
    
    /**
     * 生成随机行
     */
    private String generateRandomLine() {
        int type = random.nextInt(3);
        
        switch (type) {
            case 0:
                // 使用预定义句子
                return SAMPLE_SENTENCES[random.nextInt(SAMPLE_SENTENCES.length)];
            case 1:
                // 生成随机单词组合
                return generateRandomWords(3 + random.nextInt(8));
            default:
                // 混合模式
                return generateMixedLine();
        }
    }
    
    /**
     * 生成指定数量的随机单词
     */
    private String generateRandomWords(int wordCount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < wordCount; i++) {
            if (i > 0) {
                sb.append(" ");
            }
            sb.append(SAMPLE_WORDS[random.nextInt(SAMPLE_WORDS.length)]);
        }
        return sb.toString();
    }
    
    /**
     * 生成混合内容行
     */
    private String generateMixedLine() {
        StringBuilder sb = new StringBuilder();
        
        // 添加一些随机单词
        int wordCount = 2 + random.nextInt(5);
        for (int i = 0; i < wordCount; i++) {
            if (i > 0) {
                sb.append(" ");
            }
            sb.append(SAMPLE_WORDS[random.nextInt(SAMPLE_WORDS.length)]);
        }
        
        // 可能添加标点符号
        if (random.nextBoolean()) {
            char[] punctuation = {'.', ',', '!', '?', ';', ':'};
            sb.append(punctuation[random.nextInt(punctuation.length)]);
        }
        
        // 可能添加更多单词
        if (random.nextBoolean()) {
            sb.append(" ");
            sb.append(generateRandomWords(1 + random.nextInt(4)));
        }
        
        return sb.toString();
    }
    
    /**
     * 创建默认测试数据
     */
    public void createDefaultTestData(String inputDir) throws IOException {
        // 创建基本测试文件
        generateTextFile(inputDir + "/test.txt", 100);
        
        // 创建包含已知单词频率的文件
        Map<String, Integer> knownFrequencies = new HashMap<>();
        knownFrequencies.put("hello", 10);
        knownFrequencies.put("world", 8);
        knownFrequencies.put("java", 15);
        knownFrequencies.put("mapreduce", 5);
        knownFrequencies.put("test", 12);
        
        generateWordFrequencyFile(inputDir + "/frequency_test.txt", knownFrequencies);
        
        // 创建多个小文件
        generateMultipleFiles(inputDir, 3, 50);
        
        System.out.println("Default test data created in: " + inputDir);
    }
    
    /**
     * 主方法，用于独立运行测试数据生成
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: TestDataGenerator <output_directory> [line_count]");
            System.err.println("Example: TestDataGenerator input 1000");
            return;
        }
        
        String outputDir = args[0];
        int lineCount = args.length > 1 ? Integer.parseInt(args[1]) : 500;
        
        TestDataGenerator generator = new TestDataGenerator();
        
        try {
            if (lineCount > 0) {
                generator.generateTextFile(outputDir + "/generated_test.txt", lineCount);
            }
            
            generator.createDefaultTestData(outputDir);
            
            System.out.println("Test data generation completed successfully!");
            
        } catch (IOException e) {
            System.err.println("Failed to generate test data: " + e.getMessage());
            e.printStackTrace();
        }
    }
}