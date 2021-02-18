package com.uber.profiling.reporters;

import com.uber.profiling.Reporter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.util.Map;

public class PrometheusOutputReporter implements Reporter {

    @Override
    public void report(String profilerName, Map<String, Object> metrics) {
        CollectorRegistry registry = new CollectorRegistry();
        Gauge lastSuccessMetric = Gauge.build()
                .name("spark_application_jvm_mem_usage_pct")
                .labelNames("namespace", "customer", "source", "role")
                .help("memory usage percent for a JVM which is a part of some spark application.")
                .register(registry);
        lastSuccessMetric.labels("jr_dev", "jr_cigna", "jr_athc", "executor").set(1312);
        String jobName = "spark_transform_john_dev_instance";
        PushGateway pg = new PushGateway("prometheus-pushgateway.marathon.l4lb.thisdcos.directory:9091");
        try {
            pg.pushAdd(registry, jobName);
            System.out.println("Successfully pushed to Prometheus");
        } catch (IOException ex) {
            System.err.println("io exception: " + ex.getMessage());
        }
    }

    @Override
    public void close() {}

}
