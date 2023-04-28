package io.micrometer.tdengine;

/**
 * @Author xuxiaobing
 * @Description TODO
 * @Date 2023/4/28 3:24 下午
 * @Version 1.0
 */
public class KeyValueOptions {
    private String key;
    private String value;

    public KeyValueOptions(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
