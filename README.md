# MiniMapReduce 框架

一个基于Java实现的轻量级MapReduce框架，支持分布式数据处理和词频统计等应用。

## 项目概述

本项目是一个简单的 Mini-MapReduce 框架实现，包含以下核心功能：
- 可配置的Map和Reduce任务调度
- 多线程并行与分页处理
- 模拟缓存溢出处理
- 性能监控和统计
- 智能输出文件合并

## 项目结构

```
MyMapReduce/
├── src/main/java/
│   ├── framework/           # 框架核心代码
│   │   ├── core/           # 核心配置和数据结构
│   │   ├── driver/         # 驱动基类
│   │   └── scheduler/      # 任务调度器
│   └── app/                # 应用实现
│       └── wordcount/      # WordCount示例应用
├── src/main/resources/
│   └── config.properties   # 框架配置文件
├── build/                  # 编译输出目录
├── temp/                   # 临时文件目录
├── compile.bat            # 编译脚本
├── run.bat               # 运行脚本
└── README.md             # 项目说明
```

## 快速开始

### 1. 编译项目

```bash
compile.bat
```

### 2. 运行WordCount示例

```bash
run.bat input output
```

其中：
- `input` - 输入目录，包含要处理的文本文件
- `output` - 输出目录，存放处理结果

### 3. 查看结果

运行完成后，在输出目录中会生成：
- `*_out.txt` - 合并后的最终结果文件

## 配置说明

### 核心配置 (`config.properties`)

```properties
# 任务配置
mapreduce.map.tasks=8          # Map任务数量
mapreduce.reduce.tasks=4       # Reduce任务数量（决定part文件数量）

# 性能配置
mapreduce.buffer.size.kb=1     # 模拟内存缓存大小(单位KB)
mapreduce.spill.threshold=0.8  # 溢出阈值
thread.pool.max.size=8         # 最大线程数

# 监控配置
mapreduce.monitor.enabled=true # 启用性能监控
```

### 命令行参数

```bash
run.bat <input_dir> <output_dir> [options]
```

**可选参数：**
- `-mapTasks=<num>` - Map任务数量（默认：4）
- `-reduceTasks=<num>` - Reduce任务数量（默认：2）
- `-bufferSize=<kb>` - 缓存大小KB（默认：1）
- `-maxThreads=<num>` - 最大线程数（默认：8）
- `-noMonitoring` - 禁用性能监控

**示例：**
```bash
run.bat input output -mapTasks=4 -reduceTasks=2 -bufferSize=2
```

## 使用示例

### WordCount应用

统计文本文件中每个单词的出现次数：

1. **准备输入数据**
   ```
   input/
   └── text.txt
   ```

2. **运行处理**
   ```bash
   run.bat input output
   ```

3. **查看结果**
   ```
   output/
   └── text_out.txt    # 合并后的词频统计结果

   temp/               # 保留中间生成文件
   ├── part-r-00000.txt
   ├── part-r-00001.txt
   ├── part-r-00002.txt
   └── part-r-00003.txt
   ```

### 输出格式

最终结果文件格式（制表符分隔）：
```
word1	count1
word2	count2
word3	count3
...
```

## 开发自定义应用

### 1. 创建Mapper类

```java
public class MyMapper extends Mapper {
    @Override
    public void map(String key, String value, Context context) throws IOException {
        // 实现map逻辑
        context.write(outputKey, outputValue);
    }
}
```

### 2. 创建Reducer类

```java
public class MyReducer extends Reducer {
    @Override
    public void reduce(String key, Iterable<String> values, Context context) throws IOException {
        // 实现reduce逻辑
        context.write(key, result);
    }
}
```

### 3. 创建Driver类

```java
public class MyDriver extends Driver {
    @Override
    protected void configureJob() throws Exception {
        setMapperClass(MyMapper.class);
        setReducerClass(MyReducer.class);
    }

    public static void main(String[] args) {
        MyDriver driver = new MyDriver();
        runJob(driver, args);
    }
}
```

## 性能调优

### 1. 任务数量调优

- **Map任务数量**：建议不超过CPU核心数
- **Reduce任务数量**：影响输出文件数量，根据数据量调整

### 2. 内存配置

- **缓存大小**：增大可减少磁盘I/O，但占用更多内存
- **溢出阈值**：控制内存使用，避免OOM

### 3. 线程池配置

- **核心线程数**：建议等于CPU核心数
- **最大线程数**：根据系统资源调整

## 监控和调试

### 性能统计

启用监控后会显示：
```
Job Statistics - Map Tasks: 8/8, Reduce Tasks: 4/4, Execution Time: 125ms
ThreadPool Status - Map: [Active: 0, Completed: 8/8, Tasks: 8/8]
```

### 调试技巧

1. **查看part文件**：在temp目录中检查各个Reduce任务的输出
2. **调整任务数量**：通过修改配置观察性能变化
3. **启用详细日志**：设置`log.level=DEBUG`获取更多信息

## 系统要求

- **Java版本**：JDK 8或更高版本
- **操作系统**：Windows（支持批处理脚本）
- **内存**：建议至少512MB可用内存
- **磁盘空间**：根据数据量确定

## 许可证 MIT

本项目仅供学习和研究使用。
