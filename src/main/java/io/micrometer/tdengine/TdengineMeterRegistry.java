/**
 * Refer to TdengineMeterRegistry for this class
 */
package io.micrometer.tdengine;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.DoubleFormat;
import io.micrometer.core.instrument.util.MeterPartition;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import io.micrometer.core.instrument.util.StringUtils;
import static java.util.stream.Collectors.joining;


import com.taosdata.jdbc.SchemalessWriter;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Author xuxiaobing
 * @Description TODO
 * @Date 2023/4/28 11:46 上午
 * @Version 1.0
 */
public class TdengineMeterRegistry extends StepMeterRegistry {
    private final Logger logger = LoggerFactory.getLogger(TdengineMeterRegistry.class);
    private static final ThreadFactory DEFAULT_THREAD_FACTORY = new NamedThreadFactory("tdengine-metrics-publisher");

    private final TdengineConfig config;

    private Connection connection;

    public TdengineMeterRegistry(TdengineConfig config) {
        this(config, Clock.SYSTEM);
    }

    public TdengineMeterRegistry(TdengineConfig config, Clock clock) {
        this(config, clock, DEFAULT_THREAD_FACTORY);
    }

    private TdengineMeterRegistry(TdengineConfig config, Clock clock, ThreadFactory threadFactory) {
        super(config, clock);
        config().namingConvention(new TdengineNamingConvention());
        this.config = config;

        // init connection for Tdengine
        getConnection();

        // create db
        createDatabaseIfNecessary();

        //backend thread
        start(threadFactory);
    }

    private void getConnection() {
        String jdbcUrl = String.format("jdbc:TAOS://%s:%d?user=%s&password=%s",
                config.getHost(), config.getPort(), config.getUsername(), config.getPassword());
        try {
            connection = DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start(ThreadFactory threadFactory) {
        super.start(threadFactory);
    }

    private void createDatabaseIfNecessary() {
        try (Statement stmt = connection.createStatement()) {
            // the default precision is ms (millisecond), but we use us(microsecond) here.
            String createDatabaseSql = new CreateDatabaseQueryBuilder(config.getDb())
                    .setOptions(config.getOptions())
                    .setPrecision(config.getPrecision()).build();
            stmt.execute(createDatabaseSql);
            stmt.execute("USE " + config.getDb());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void publish() {
        try {
            for (List<Meter> batch : MeterPartition.partition(this, config.batchSize())) {
                List<String> data = batch.stream()
                                .flatMap(m -> m.match(
                                        gauge -> writeGauge(gauge.getId(), gauge.value()),
                                        counter -> writeCounter(counter.getId(), counter.count()),
                                        this::writeTimer,
                                        this::writeSummary,
                                        this::writeLongTaskTimer,
                                        gauge -> writeGauge(gauge.getId(), gauge.value(getBaseTimeUnit())),
                                        counter -> writeCounter(counter.getId(), counter.count()),
                                        this::writeFunctionTimer,
                                        this::writeMeter))
                                .collect(Collectors.toList());
                SchemalessWriter writer = new SchemalessWriter(connection);
                writer.write(data, SchemalessProtocolType.LINE, config.getPrecisionType());
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // VisibleForTesting
    Stream<String> writeMeter(Meter m) {
        List<TdengineMeterRegistry.Field> fields = new ArrayList<>();
        for (Measurement measurement : m.measure()) {
            double value = measurement.getValue();
            if (!Double.isFinite(value)) {
                continue;
            }
            String fieldKey = measurement.getStatistic().getTagValueRepresentation()
                    .replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();
            fields.add(new TdengineMeterRegistry.Field(fieldKey, value));
        }
        if (fields.isEmpty()) {
            return Stream.empty();
        }
        Meter.Id id = m.getId();
        return Stream.of(influxLineProtocol(id, id.getType().name().toLowerCase(), fields.stream()));
    }

    private Stream<String> writeLongTaskTimer(LongTaskTimer timer) {
        Stream<TdengineMeterRegistry.Field> fields = Stream.of(new TdengineMeterRegistry.Field("active_tasks", timer.activeTasks()),
                new TdengineMeterRegistry.Field("duration", timer.duration(getBaseTimeUnit())));
        return Stream.of(influxLineProtocol(timer.getId(), "long_task_timer", fields));
    }

    // VisibleForTesting
    Stream<String> writeCounter(Meter.Id id, double count) {
        if (Double.isFinite(count)) {
            return Stream.of(influxLineProtocol(id, "counter", Stream.of(new TdengineMeterRegistry.Field("value", count))));
        }
        return Stream.empty();
    }

    // VisibleForTesting
    Stream<String> writeGauge(Meter.Id id, Double value) {
        if (Double.isFinite(value)) {
            return Stream.of(influxLineProtocol(id, "gauge", Stream.of(new TdengineMeterRegistry.Field("value", value))));
        }
        return Stream.empty();
    }

    // VisibleForTesting
    Stream<String> writeFunctionTimer(FunctionTimer timer) {
        double sum = timer.totalTime(getBaseTimeUnit());
        if (Double.isFinite(sum)) {
            Stream.Builder<TdengineMeterRegistry.Field> builder = Stream.builder();
            builder.add(new TdengineMeterRegistry.Field("sum", sum));
            builder.add(new TdengineMeterRegistry.Field("count", timer.count()));
            double mean = timer.mean(getBaseTimeUnit());
            if (Double.isFinite(mean)) {
                builder.add(new TdengineMeterRegistry.Field("mean", mean));
            }
            return Stream.of(influxLineProtocol(timer.getId(), "histogram", builder.build()));
        }
        return Stream.empty();
    }

    private Stream<String> writeTimer(Timer timer) {
        final Stream<TdengineMeterRegistry.Field> fields = Stream.of(new TdengineMeterRegistry.Field("sum", timer.totalTime(getBaseTimeUnit())),
                new TdengineMeterRegistry.Field("count", timer.count()), new TdengineMeterRegistry.Field("mean", timer.mean(getBaseTimeUnit())),
                new TdengineMeterRegistry.Field("upper", timer.max(getBaseTimeUnit())));

        return Stream.of(influxLineProtocol(timer.getId(), "histogram", fields));
    }

    private Stream<String> writeSummary(DistributionSummary summary) {
        final Stream<TdengineMeterRegistry.Field> fields = Stream.of(new TdengineMeterRegistry.Field("sum", summary.totalAmount()),
                new TdengineMeterRegistry.Field("count", summary.count()), new TdengineMeterRegistry.Field("mean", summary.mean()),
                new TdengineMeterRegistry.Field("upper", summary.max()));

        return Stream.of(influxLineProtocol(summary.getId(), "histogram", fields));
    }

    private String influxLineProtocol(Meter.Id id, String metricType, Stream<TdengineMeterRegistry.Field> fields) {
        String tags = getConventionTags(id).stream().filter(t -> StringUtils.isNotBlank(t.getValue()))
                .map(t -> "," + t.getKey() + "=" + t.getValue()).collect(joining(""));

        String line = getConventionName(id) + tags + ",metric_type=" + metricType + " "
                + fields.map(TdengineMeterRegistry.Field::toString).collect(joining(","));
        if (!config.useServerTimestamp()) {
            if (config.getPrecisionType() != SchemalessTimestampType.MILLI_SECONDS) {
                throw new RuntimeException("invalid timestamp precision");
            } else {
                line = line + " " + clock.wallTime();
            }
        }

        return line;
    }

    @Override
    protected final TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    static class Field {

        final String key;

        final double value;

        Field(String key, double value) {
            // `time` cannot be a field key or tag key
            if (key.equals("time")) {
                throw new IllegalArgumentException("'time' is an invalid field key in TDengine");
            }
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return key + "=" + DoubleFormat.decimalOrNan(value);
        }

    }
}
