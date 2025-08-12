/**
 * MapReduce框架配置管理类
 * 
 * 该类负责管理MapReduce框架的各种配置参数，包括从配置文件加载
 * 默认配置以及提供配置参数的读取和设置功能。
 * 
 * @author hucj
 * @date 2025-08-10
 * @version 1.0
 */
package framework.core;

import java.util.Properties;
import java.io.InputStream;
import java.io.IOException;

/**
 * 配置管理类
 */
public class Configuration {
    
    private Properties properties;
    
    public Configuration() {
        this.properties = new Properties();
        loadDefaultConfig();
    }
    
    /**
     * 加载默认配置
     */
    private void loadDefaultConfig() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            System.err.println("Failed to load config.properties: " + e.getMessage());
        }
        
        // 设置默认值
        setIfNotExists("mapreduce.map.tasks", "4");
        setIfNotExists("mapreduce.reduce.tasks", "2");
        setIfNotExists("mapreduce.buffer.size.mb", "100");
        setIfNotExists("mapreduce.spill.threshold", "0.8");
        setIfNotExists("mapreduce.temp.dir", "temp");
    }
    
    private void setIfNotExists(String key, String value) {
        if (!properties.containsKey(key)) {
            properties.setProperty(key, value);
        }
    }
    
    public String get(String key) {
        return properties.getProperty(key);
    }
    
    public String get(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
    
    public int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }
    
    public double getDouble(String key, double defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Double.parseDouble(value) : defaultValue;
    }
    
    public void set(String key, String value) {
        properties.setProperty(key, value);
    }
}