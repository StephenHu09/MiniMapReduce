/**
 * MapReduce框架溢出文件管理类
 * 
 * 该类负责管理内存缓冲区溢出到磁盘的文件操作，包括数据溢出、
 * 文件合并、多路归并排序和临时文件清理等功能。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.memory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SpillManager {
    
    private final Queue<String> spillFiles = new ConcurrentLinkedQueue<>();
    private final String tempDir;
    
    public SpillManager(String tempDir) {
        this.tempDir = tempDir;
        // 确保临时目录存在
        new File(tempDir).mkdirs();
    }
    
    /**
     * 获取临时目录路径
     */
    public String getTempDir() {
        return tempDir;
    }
    
    /**
     * 将数据溢出到文件
     * @param data 要溢出的数据
     * @param spillFile 溢出文件路径
     */
    public void spill(List<BufferEntry<?, ?>> data, String spillFile) throws IOException {
        File file = new File(spillFile);
        file.getParentFile().mkdirs();
        
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file))) {
            oos.writeInt(data.size());
            for (BufferEntry<?, ?> entry : data) {
                oos.writeObject(entry.getKey());
                oos.writeObject(entry.getValue());
            }
        }
        
        spillFiles.offer(spillFile);
        System.out.println("Spilled " + data.size() + " entries to " + spillFile);
    }
    
    /**
     * 获取所有溢出文件
     */
    public List<String> getSpillFiles() {
        return new ArrayList<>(spillFiles);
    }
    
    /**
     * 合并所有溢出文件到输出文件
     * @param outputFile 输出文件路径
     */
    public void mergeSpillFiles(String outputFile) throws IOException {
        if (spillFiles.isEmpty()) {
            System.out.println("No spill files to merge for output: " + outputFile);
            return;
        }
        
        System.out.println("Merging " + spillFiles.size() + " spill files to: " + outputFile);
        
        // 使用优先队列进行多路归并
        PriorityQueue<SpillFileReader> readers = new PriorityQueue<>(
            Comparator.comparing(SpillFileReader::getCurrentKey, (k1, k2) -> {
                if (k1 instanceof Comparable && k2 instanceof Comparable) {
                    return ((Comparable) k1).compareTo(k2);
                }
                return k1.hashCode() - k2.hashCode();
            })
        );
        
        // 初始化所有溢出文件的读取器
        for (String spillFile : spillFiles) {
            try {
                SpillFileReader reader = new SpillFileReader(spillFile);
                if (reader.getCurrentKey() != null) {
                    readers.offer(reader);
                } else {
                    reader.close();
                }
            } catch (Exception e) {
                System.err.println("Failed to read spill file: " + spillFile + ", error: " + e.getMessage());
            }
        }
        
        // 合并写入输出文件
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
            while (!readers.isEmpty()) {
                SpillFileReader reader = readers.poll();
                Object key = reader.getCurrentKey();
                Object value = reader.getCurrentValue();
                
                if (key != null && value != null) {
                    writer.println(key + "\t" + value);
                }
                
                try {
                    if (reader.next()) {
                        readers.offer(reader);
                    } else {
                        reader.close();
                    }
                } catch (Exception e) {
                    System.err.println("Error reading next entry from spill file, closing reader: " + e.getMessage());
                    reader.close();
                }
            }
        }
        
        // 清理溢出文件
        cleanupSpillFiles();
    }
    
    /**
     * 清理溢出文件
     */
    private void cleanupSpillFiles() {
        for (String spillFile : spillFiles) {
            System.out.println("  - " + spillFile);
        }
        
        // 代码不做删除
        /*
        for (String spillFile : spillFiles) {
            File file = new File(spillFile);
            if (file.exists()) {
                if (file.delete()) {
                    System.out.println("Deleted spill file: " + spillFile);
                } else {
                    System.err.println("Failed to delete spill file: " + spillFile);
                }
            } else {
                System.err.println("Spill file not found for cleanup: " + spillFile);
            }
        }
        */
        
        // 保留spillFiles列表用于调试
        // spillFiles.clear();
    }
    
    /**
     * 溢出文件读取器
     */
    private static class SpillFileReader implements Closeable {
        private final ObjectInputStream ois;
        private final int totalEntries;
        private int currentIndex = 0;
        private Object currentKey;
        private Object currentValue;
        
        public SpillFileReader(String spillFile) throws IOException {
            this.ois = new ObjectInputStream(new FileInputStream(spillFile));
            this.totalEntries = ois.readInt();
            next(); // 读取第一个条目
        }
        
        public boolean hasNext() {
            return currentIndex < totalEntries;
        }
        
        public boolean next() {
            if (currentIndex >= totalEntries) {
                currentKey = null;
                currentValue = null;
                return false;
            }
            
            try {
                currentKey = ois.readObject();
                currentValue = ois.readObject();
                currentIndex++;
                return true;
            } catch (Exception e) {
                System.err.println("Error reading spill file entry at index " + currentIndex + ": " + e.getMessage());
                currentKey = null;
                currentValue = null;
                return false;
            }
        }
        
        public Object getCurrentKey() {
            return currentKey;
        }
        
        public Object getCurrentValue() {
            return currentValue;
        }
        
        @Override
        public void close() throws IOException {
            ois.close();
        }
    }
}