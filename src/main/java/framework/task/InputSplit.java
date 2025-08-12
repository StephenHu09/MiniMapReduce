/**
 * MapReduce框架输入分片类
 * 
 * 该类表示Map任务的输入数据分片，包含分片索引、数据行列表
 * 和大小信息，用于将大数据集分割成可并行处理的小块。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.task;

import java.util.List;

public class InputSplit {
    
    private final int splitIndex;
    private final List<String> lines;
    private final long size;
    
    public InputSplit(int splitIndex, List<String> lines) {
        this.splitIndex = splitIndex;
        this.lines = lines;
        this.size = calculateSize();
    }
    
    /**
     * 计算分片大小（字节数）
     */
    private long calculateSize() {
        long totalSize = 0;
        for (String line : lines) {
            totalSize += line.length() + 1; // +1 for newline character
        }
        return totalSize;
    }
    
    public int getSplitIndex() {
        return splitIndex;
    }
    
    public List<String> getLines() {
        return lines;
    }
    
    public long getSize() {
        return size;
    }
    
    public int getLineCount() {
        return lines.size();
    }
    
    @Override
    public String toString() {
        return "InputSplit{" +
                "splitIndex=" + splitIndex +
                ", lineCount=" + lines.size() +
                ", size=" + size +
                " bytes}";
    }
}