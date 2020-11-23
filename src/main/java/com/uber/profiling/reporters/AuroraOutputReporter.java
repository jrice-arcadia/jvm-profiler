package com.uber.profiling.reporters;

import com.uber.profiling.Reporter;
import com.uber.profiling.util.JsonUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedDeque;

public class AuroraOutputReporter implements Reporter {

    private static ConcurrentLinkedDeque<Connection> connPool;

    public AuroraOutputReporter() {
    }

    private final String FUNNY_PASSWORD = "password";

    public void report(String profilerName, Map<String, Object> metrics) {
        if (connPool == null) initConnectionPool();
        if (metrics.containsKey("stacktrace")) {
            System.out.println("AuroraOutputReporter(): Storing stacktrace.");
            String startEpoch = metrics.get("startEpoch").toString();
            String endEpoch = metrics.get("endEpoch").toString();
            String stackTraceRaw = JsonUtils.serialize(metrics);
            storeStacktrace(startEpoch, endEpoch, stackTraceRaw.trim());
        }
        if (profilerName.equalsIgnoreCase("cpuandmemory")) {
            System.out.println("AuroraOutputReporter(): Storing CPU and memory stats.");
            storeCpuAndMemory(metrics);

        }

    }

    private void initConnectionPool() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            System.out.println("AuroraOutputReporter(): jdbc driver not found");
        }
        connPool = new ConcurrentLinkedDeque<>();
        try {
            for (int i = 0; i < 5; i++) {
                connPool.push(DriverManager.getConnection(
                        "jdbc:mysql://localhost:3306/timeseries_profiler_data", "root", FUNNY_PASSWORD));
            }
        } catch (SQLException ex) {
            System.out.println("AuroraOutputReporter(): couldn't init pool.");
            System.out.print(ex.getMessage());
        }
    }

    public void close() {
        try {
            for (Connection c : connPool) {
                c.close();
            }
        } catch (SQLException ex) {
            System.out.println("couldnt close");
        }
    }

    public void storeStacktrace(String startEpoch, String endEpoch, String stackTraceRaw) {

        try {
            while (connPool.isEmpty()) {
                Thread.sleep(5000);
            }
            Connection con = connPool.pop();

            String insert = String.format("INSERT INTO stacktrace (timestamps, stacktrace_raw) VALUES (?, ?)");
            PreparedStatement ps = con.prepareStatement(insert);
            ps.setString(1, startEpoch + "." + endEpoch);
            ps.setString(2, stackTraceRaw);

            int res = ps.executeUpdate();
            connPool.push(con);
        } catch (SQLException ex) {
            System.out.println("AuroraOutputReporter(): SQLException thrown.");
            System.out.println(ex.getMessage());
        } catch (InterruptedException ex) {
            System.out.print("InterruptedException");
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
        String host = metrics.get("name").toString();


        BigDecimal total_memory_in_use = new BigDecimal(new BigDecimal(heap_memory_in_use).intValue() + new BigDecimal(nonheap_memory_in_use).intValue());
        BigDecimal total_memory_committed = new BigDecimal(new BigDecimal(heap_memory_committed_in_jvm).intValue() + new BigDecimal(nonheap_memory_committed_in_jvm).intValue());

        try {
            while (connPool.isEmpty()) {
                Thread.sleep(5000);
            }
            Connection con = connPool.pop();

            String insert = String.format("INSERT INTO cpuandmemory " +
                    "(transform_name, timestamp_epoch_ms, system_cpu_usage, process_cpu_usage, memory_in_use, memory_committed_to_jvm, host)" +
                    "VALUES (?,?,?,?,?,?,?)");


            PreparedStatement ps = con.prepareStatement(insert);
            ps.setString(1, transform_name);
            ps.setBigDecimal(2, new BigDecimal(timestamp));
            ps.setBigDecimal(3, new BigDecimal(system_cpu_usage));
            ps.setBigDecimal(4, new BigDecimal((process_cpu_usage)));
            ps.setInt(5, total_memory_in_use.intValue());
            ps.setInt(6, total_memory_committed.intValue());
            ps.setString(7, host);

            int res = ps.executeUpdate();
            connPool.push(con);
        } catch (SQLException ex) {
            System.out.println("AuroraOutputReporter(): SQLException thrown.");
            System.out.println(ex.getMessage());
        } catch (InterruptedException ex) {
            System.out.print("InterruptedException");
            System.out.println(ex.getMessage());
        }
    }
}