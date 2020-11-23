package com.uber.profiling.reporters;

import com.uber.profiling.Reporter;
import com.uber.profiling.util.JsonUtils;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Map;

public class AuroraOutputReporter implements Reporter {

    public AuroraOutputReporter() {
    }

    private final String FUNNY_PASSWORD = "password";

    public void report(String profilerName, Map<String, Object> metrics) {
     /*   if (metrics.containsKey("stacktrace")) {
            System.out.println("AuroraOutputReporter(): Storing stacktrace.");
            String startEpoch = metrics.get("startEpoch").toString();
            String endEpoch = metrics.get("endEpoch").toString();
            String stackTraceRaw = JsonUtils.serialize(metrics);
            storeStacktrace(startEpoch, endEpoch, stackTraceRaw.trim());
        }*/
        if (profilerName.equalsIgnoreCase("cpuandmemory")) {
            System.out.println("AuroraOutputReporter(): Storing CPU and memory stats.");
            storeCpuAndMemory(metrics);

        }

    }

    public void close() {
        System.out.println("close()");
    }

    public void storeStacktrace(String startEpoch, String endEpoch, String stackTraceRaw) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            System.out.println("AuroraOutputReporter(): jdbc driver not found");
        }
        try {

            Connection con = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/timeseries_profiler_data", "root", FUNNY_PASSWORD);

            String insert = String.format("INSERT INTO stacktrace (timestamps, stacktrace_raw) VALUES (?, ?)");
            PreparedStatement ps = con.prepareStatement(insert);
            ps.setString(1, startEpoch + "." + endEpoch);
            ps.setString(2, stackTraceRaw);

            System.out.println("stackTraceRaw:\n" + stackTraceRaw);

            int res = ps.executeUpdate();
            con.close();
        } catch (SQLException ex) {
            System.out.println("AuroraOutputReporter(): SQLException thrown.");
            System.out.println(ex.getMessage());
        }
    }

    public void storeCpuAndMemory(Map<String, Object> metrics) {
        String transform_name = metrics.get("tag").toString();
        String timestamp      = metrics.get("epochMillis").toString();
        String system_cpu_usage = metrics.get("systemCpuLoad").toString();
        String process_cpu_usage = metrics.get("processCpuLoad").toString();
        String nonheap_memory_in_use   = metrics.get("nonHeapMemoryTotalUsed").toString();
        String nonheap_memory_committed_in_jvm = metrics.get("nonHeapMemoryCommitted").toString();
        String heap_memory_in_use   = metrics.get("heapMemoryTotalUsed").toString();
        String heap_memory_committed_in_jvm = metrics.get("heapMemoryCommitted").toString();


        BigDecimal total_memory_in_use = new BigDecimal(new BigDecimal(heap_memory_in_use).intValue() + new BigDecimal(nonheap_memory_in_use).intValue());
        BigDecimal total_memory_committed = new BigDecimal(new BigDecimal(heap_memory_committed_in_jvm).intValue() + new BigDecimal(nonheap_memory_committed_in_jvm).intValue());


        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            System.out.println("AuroraOutputReporter(): jdbc driver not found");
        }
        try {

            Connection con = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/timeseries_profiler_data", "root", FUNNY_PASSWORD);

            String insert = String.format("INSERT INTO cpuandmemory " +
                    "(transform_name, timestamp_epoch_ms, system_cpu_usage, process_cpu_usage, memory_in_use, memory_committed_to_jvm)" +
                    "VALUES (?,?,?,?,?,?)");



            PreparedStatement ps = con.prepareStatement(insert);
            ps.setString(1, transform_name);
            ps.setInt(2, Integer.valueOf(timestamp));
            ps.setBigDecimal(3, new BigDecimal(system_cpu_usage));
            ps.setBigDecimal(4, new BigDecimal((process_cpu_usage)));
            ps.setInt(5, total_memory_in_use.intValue());
            ps.setInt(6, total_memory_committed.intValue());

            int res = ps.executeUpdate();
            con.close();
        } catch (SQLException ex) {
            System.out.println("AuroraOutputReporter(): SQLException thrown.");
            System.out.println(ex.getMessage());
        }
    }
}