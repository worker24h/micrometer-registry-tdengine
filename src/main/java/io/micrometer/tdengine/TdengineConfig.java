/**
 * Refer to TdengineConfig for this class
 */

package io.micrometer.tdengine;

import static io.micrometer.core.instrument.config.MeterRegistryConfigValidator.checkAll;
import static io.micrometer.core.instrument.config.MeterRegistryConfigValidator.checkRequired;
import io.micrometer.core.instrument.config.validate.InvalidReason;
import static io.micrometer.core.instrument.config.validate.PropertyValidator.getBoolean;
import static io.micrometer.core.instrument.config.validate.PropertyValidator.getEnum;
import static io.micrometer.core.instrument.config.validate.PropertyValidator.getInteger;
import static io.micrometer.core.instrument.config.validate.PropertyValidator.getSecret;
import static io.micrometer.core.instrument.config.validate.PropertyValidator.getString;
import static io.micrometer.core.instrument.config.validate.PropertyValidator.getUrlString;
import io.micrometer.core.instrument.config.validate.Validated;
import io.micrometer.core.instrument.step.StepRegistryConfig;
import io.micrometer.core.instrument.util.StringUtils;
import io.micrometer.core.lang.Nullable;
import io.micrometer.influx.InfluxApiVersion;
import io.micrometer.influx.InfluxConfig;
import io.micrometer.influx.InfluxConsistency;

import com.taosdata.jdbc.enums.SchemalessTimestampType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author xuxiaobing
 * @Description TODO
 * @Date 2023/4/28 2:50 下午
 * @Version 1.0
 */
public class TdengineConfig implements StepRegistryConfig {
    private String username;
    private String password;
    private String db;
    private String host;
    private int port;
    private String precision;
    private boolean useServerTimestamp = true; // use taod system timestamp
    private int step;
    private long readTimeout = 10; // for seconds
    private long connectTimeout = 10; // for seconds
    private List<KeyValueOptions> options = new ArrayList<>();

    public String getUsername() {
        return username;
    }

    public TdengineConfig setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public TdengineConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getDb() {
        return db;
    }

    public TdengineConfig setDb(String db) {
        this.db = db;
        return this;
    }

    public String getHost() {
        return host;
    }

    public TdengineConfig setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public TdengineConfig setPort(int port) {
        this.port = port;
        return this;
    }

    public List<KeyValueOptions> getOptions() {
        return options;
    }

    public void setOptions(List<KeyValueOptions> options) {
        this.options = options;
    }

    public TdengineConfig addOption(String key, String value) {
        options.add(new KeyValueOptions(key, value));
        return this;
    }

    public String getPrecision() {
        return precision;
    }

    public SchemalessTimestampType getPrecisionType() {
        if (precision.equals("us")) {
            return SchemalessTimestampType.MICRO_SECONDS;
        } else if (precision.equals("ns")) {
            return SchemalessTimestampType.NANO_SECONDS;
        } else {
            return SchemalessTimestampType.MILLI_SECONDS;
        }
    }

    public TdengineConfig setPrecision(String precision) {
        this.precision = precision;
        return this;
    }

    public boolean useServerTimestamp() {
        return useServerTimestamp;
    }

    public void setUseServerTimestamp(boolean useServerTimestamp) {
        this.useServerTimestamp = useServerTimestamp;
    }

    @Override
    public Duration step() {
        return Duration.ofSeconds(step);
    }

    public TdengineConfig setStep(int step) {
        this.step = step;
        return this;
    }

    public Duration getReadTimeout() {
        return Duration.ofSeconds(readTimeout);
    }

    public TdengineConfig setReadTimeout(long readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }

    public Duration getConnectTimeout() {
        return Duration.ofSeconds(connectTimeout);
    }

    public TdengineConfig setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    @Override
    public String prefix() {
        return "tdengine";
    }

    @Override
    public String get(String key) {
        return null; //do nothing
    }

    @Override
    public Validated<?> validate() {
        return checkAll(this, c -> StepRegistryConfig.validate(c), checkRequired("db", TdengineConfig::getDb),
                checkRequired("host", TdengineConfig::getHost),
                checkRequired("port", TdengineConfig::getPort),
                checkRequired("username", TdengineConfig::getUsername),
                checkRequired("password", TdengineConfig::getPassword));
    }
}
