package com.arcadia.parseconsolemetrics;

import com.google.gson.Gson;

import java.io.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Stream;

public class ParseConsoleMetrics {

    private static ConcurrentLinkedDeque<Connection> connPool;

    private static final String FUNNY_PASSWORD = "password";

    private static Gson g;

    private static int total_datapoints_inserted = 0;

    private static ConcurrentHashMap<Integer, String> DEBUG_LOG_CACHE = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<Integer, String> METRIC_CACHE = new ConcurrentHashMap<>();


    public static void main(String... args) throws Exception{
        String path = args[0];
        initConnectionPool();
        g = new Gson();

        Stream<Path> allLogs = Files.list(Paths.get(path));
        allLogs.forEach(p ->  {

            System.out.println("Processing Path: " + p);
            try {
                String[] tmp = p.getFileName().toString().split("\\.");
                String taskId = tmp[tmp.length-2];
                Stream<String> s = Files.lines(p);
                s.filter(l -> l.contains("ConsoleOutputReporter") || l.contains("JR"))
                        .map( l -> {
                            if (l.contains("JR")) return l;
                            else return l.substring(l.indexOf(": {", l.indexOf("ConsoleOutputReporter"))+1).trim();
                        })
                        .forEach(l -> ParseConsoleMetrics.processLog(l, taskId));
                s.close();
                // Files.delete(p); NOT DELETING AT THE MOMENT. KEEPING FOR LOGS/ANNOTATIONS
            } catch (IOException ex) {
                System.out.println("EXCEPTION: " + ex.getMessage());
                throw new UncheckedIOException(ex);
            }
            System.out.println("Finished processing Path: " + path);
        });
        allLogs.close();
        System.out.println("Added " + total_datapoints_inserted + " datapoints to the database.");

        return;
    }

    private static void processLog(String line, String taskId) throws UncheckedIOException {
       // if (DEBUG_LOG_CACHE.containsKey(line.hashCode())) System.out.println("DEBUG LOG CACHE HIT!");
       // if (METRIC_CACHE.containsKey(line.hashCode())) System.out.println("METRIC CACHE HIT!");

        try {
            String dest = "/Users/johnrice/Development/macig_filtered_logs/";

            Path debugPath = Paths.get(dest+"DEBUG_LOG_" + taskId +".txt");
            Path metricsPath = Paths.get(dest+"METRICS_"+taskId+".txt");
            if (Files.notExists(debugPath)) Files.createFile(debugPath);
            if (Files.notExists(metricsPath)) Files.createFile(metricsPath);
            if (line.contains("JR") && !DEBUG_LOG_CACHE.containsKey(line.hashCode())) {
                DEBUG_LOG_CACHE.put(line.hashCode(), "");
                Files.write(debugPath, (line+"\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            } else if (line.contains("heapMemoryMax") && !METRIC_CACHE.containsKey(line.hashCode())){
                METRIC_CACHE.put(line.hashCode(), "");
                ParseConsoleMetrics.storeCpuAndMemory(g.fromJson(line, Map.class), taskId);
                Files.write(metricsPath, (line+"\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            } else if (line.contains("stacktrace") && !METRIC_CACHE.containsKey(line.hashCode())) {
                METRIC_CACHE.put(line.hashCode(), "");
                Files.write(metricsPath, (line+"\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            }
        } catch (IOException ex) {
            System.out.println("EXCEPTION: " + ex.getMessage());
            throw new UncheckedIOException(ex);
        }
    }

    private static void initConnectionPool() throws SQLException {
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
            throw ex;
        }
    }
    public static void storeCpuAndMemory(Map<String, Object> metrics, String taskId) throws IOException {
        String transform_name                   = metrics.get("tag").toString();
        String timestamp                        = metrics.get("epochMillis").toString();
        String system_cpu_usage                 = metrics.get("systemCpuLoad").toString();
        String process_cpu_usage                = metrics.get("processCpuLoad").toString();
        String nonheap_memory_in_use            = metrics.get("nonHeapMemoryTotalUsed").toString();
        String nonheapmax                       = metrics.get("nonHeapMemoryMax").toString();
        String nonheap_memory_committed_in_jvm  = metrics.get("nonHeapMemoryCommitted").toString();
        String heap_memory_in_use               = metrics.get("heapMemoryTotalUsed").toString();
        String heapmax                          = metrics.get("heapMemoryMax").toString();
        String heap_memory_committed_in_jvm     = metrics.get("heapMemoryCommitted").toString();
        String vmsize_incl_swap                 = metrics.get("vmSize").toString();
        String host                             = metrics.get("name").toString();
        String role                             = metrics.get("role").toString();
        String processUUID                      = metrics.get("processUuid").toString();

        BigDecimal total_memory_in_use = new BigDecimal(heap_memory_in_use).add(new BigDecimal(nonheap_memory_in_use));
        BigDecimal total_memory_committed = new BigDecimal(heap_memory_committed_in_jvm).add(new BigDecimal(nonheap_memory_committed_in_jvm));
        BigDecimal total_memory_max = new BigDecimal(heapmax).add(new BigDecimal(nonheapmax));
        BigDecimal total_mem_usage = total_memory_in_use.divide(total_memory_max, 10, RoundingMode.HALF_UP);
        String insert = String.format("INSERT INTO cpuandmemory " +
                "(transform_name, timestamp_epoch, system_cpu_usage, process_cpu_usage, memory_in_use, " +
                "memory_committed, host, role, task_id, process_uuid, timestamp_epoch_ms, nonheapmax, heapmax " +
                ", vmsize_incl_swap, max_memory_available, mem_usage)" +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

        Connection con = null;
        try {

            con = getConnectionBlocking();
            /** ALL MEMORY AND DISK VALUES ARE IN BYTES. **/
            PreparedStatement ps = con.prepareStatement(insert);
            ps.setString(1, transform_name);
            ps.setBigDecimal(2, new BigDecimal(timestamp).divide(BigDecimal.valueOf(1000)));
            ps.setBigDecimal(3, new BigDecimal(system_cpu_usage));
            ps.setBigDecimal(4, new BigDecimal((process_cpu_usage)));
            ps.setBigDecimal(5, total_memory_in_use);
            ps.setBigDecimal(6, total_memory_committed);
            ps.setString(7, host);
            ps.setString(8, role);
            ps.setString(9, taskId);
            ps.setString(10, processUUID);
            ps.setBigDecimal(11, new BigDecimal(timestamp));
            ps.setBigDecimal(12, new BigDecimal(nonheapmax));
            ps.setBigDecimal(13, new BigDecimal(heapmax));
            ps.setBigDecimal(14, new BigDecimal(vmsize_incl_swap));
            ps.setBigDecimal(15, total_memory_max);
            ps.setBigDecimal(16, total_mem_usage);

            int res = ps.executeUpdate();
            total_datapoints_inserted++;
            relinquishConnection(con);
        } catch (SQLException ex) {
            System.out.println("AuroraOutputReporter(): SQLException thrown.");
            System.out.println(ex.getMessage());
            relinquishConnection(con);
            throw new IOException(ex.getMessage());

        } catch (InterruptedException ex) {
            System.out.print("InterruptedException");
            System.out.println(ex.getMessage());
            throw new IOException(ex.getMessage());
        }
    }

    private static synchronized Connection getConnectionBlocking() throws InterruptedException {
        while(true) {
            if (connPool.isEmpty()) {
                Thread.sleep(500);
            } else return connPool.pop();
        }
    }

    private static synchronized void relinquishConnection(Connection con) {
        connPool.push(con);
    }
}