package io.micrometer.tdengine;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.influx.InfluxConfig;
import io.micrometer.influx.InfluxMeterRegistry;
import org.junit.Test;

import java.util.function.ToDoubleFunction;

/**
 * @Author xuxiaobing
 * @Description TODO
 * @Date 2023/4/28 6:01 下午
 * @Version 1.0
 */
public class SimpleTest {

    @Test
    public void tdengineRegistryTest() throws Exception {
        TdengineConfig config = new TdengineConfig().setHost("127.0.0.1").setPort(6030)
                .setDb("micrometer_tdengine_test").setUsername("root").setPassword("root")
                .setPrecision("ns").setStep(10).setConnectTimeout(10).setReadTimeout(10);
        TdengineMeterRegistry registry = new TdengineMeterRegistry(config);
        Metrics.addRegistry(registry);

        Counter counter =
                Metrics.counter("td.counter", Tags.of("tag1", "value1").and("tag2", "value2"));
        counter.increment();
        Timer timer = Metrics.timer("td.request", Tags.of("tag1", "value1").and("tag2", "value2"));

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000L);
            counter.increment();
            timer.record(new Runnable() {

                @Override
                public void run() {
                    try {
                        Thread.sleep(3000L);
                    } catch (Exception e) {

                    }
                }
            });

        }

    }
}
