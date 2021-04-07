package com.arcadia.metricsprocessor;

import com.google.gson.*;

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
import java.util.*;
import java.util.stream.*;
import java.lang.*;

public class MetricsProcessor {

    private static ArrayList<String> CPU_AND_MEM_SCHEMA = new ArrayList<String>();
    private static ArrayList<String>  IO_SCHEMA          = new ArrayList<String>();

    private static Gson g;
    private static String RAW_METRICS_DIRECTORY = "";
    private static StringBuffer cpuMemoryBuffer; // thread safe
    private static StringBuffer stackTraceBuffer;
    private static StringBuffer ioBuffer;
    private static final String STAGING         = "/staging/";
    private static final String COMPLETED       = "/completed/";
    private static final String NORMALIZED      = "/normalized";
    private static final String ERROR           = "/errored/";
    private static final String CPU_MEMORY_PATH = "/cpuandmemory/";
    private static final String STACKTRACE_PATH = "/stacktraces/";
    private static final String IO_PATH         = "/io/";

    /** Path examples. **/
    // /cpuandmemory/staging/cpuandmemory_jsons.txt
    // /cpuandmemory/completed/cpumemmetrics5.csv


    public static void main(String... args) throws Exception{
        String path = args[0];
        RAW_METRICS_DIRECTORY = path;

        String cpuAndMemoryDest = RAW_METRICS_DIRECTORY + CPU_MEMORY_PATH;
        String stacktraceDest   = RAW_METRICS_DIRECTORY + STACKTRACE_PATH;
        String ioDest           = RAW_METRICS_DIRECTORY + IO_PATH + COMPLETED;
        System.out.println("Creating directories.");
        Files.createDirectories(Paths.get(cpuAndMemoryDest));
        Files.createDirectories(Paths.get(stacktraceDest));
        Files.createDirectories(Paths.get(ioDest));
        cpuAndMemoryDest = cpuAndMemoryDest + System.currentTimeMillis() + "_cpuandmemory.csv";
        stacktraceDest   = stacktraceDest + System.currentTimeMillis() + "_stacktraces.txt";
        ioDest           = ioDest + System.currentTimeMillis() + "_io.csv";

        FileWriter cpuAndMemoryWriter = new FileWriter(cpuAndMemoryDest, true);
        FileWriter stacktraceWriter   = new FileWriter(stacktraceDest, true);
        FileWriter ioWriter            = new FileWriter(ioDest, true);



        cpuMemoryBuffer  = new StringBuffer("");
        stackTraceBuffer = new StringBuffer("");
        ioBuffer         = new StringBuffer("");
        g = new GsonBuilder().setLenient().create();
        initCpuMemSchema();
        initIoSchema();

        cpuMemoryBuffer.append(String.join(",", CPU_AND_MEM_SCHEMA) + "\n");
        ioBuffer.append(String.join(",", IO_SCHEMA) + "\n");
        /** I think for this application's scale we gotta do all of the routing and actual writing to files in the streams. **/
        /** Well, we are routing in the foreach. That's fine. Have processLine write to FileOutputStreams that get flushed. **/
        Stream<Path> metricsFiles = Files.list(Paths.get(path));
        metricsFiles.forEach(p ->  {

            System.out.println("Processing Path: " + p);
            try {
                Stream<String> s = Files.lines(p);
                String[] lines = Files.lines(p).toArray(String[]::new); // this will be small.

                s.filter(l -> l.contains("Stacktrace:") || l.contains("CpuAndMemory:") || l.contains("IO:"))
                        .forEach(l -> processLine(l));
                s.close();
            } catch (IOException ex) {
                System.out.println("EXCEPTION: " + ex.getMessage());
                throw new UncheckedIOException(ex);
            }
            System.out.println("Finished processing Path: " + p);
        });
        metricsFiles.close();
        System.out.println("Size of CPU and Memory output: " + cpuMemoryBuffer.length());
        System.out.println("Size of Stacktrace output: " + stackTraceBuffer.length());
        System.out.println("Size of IO output: " + ioBuffer.length());

        String cpuAndMemoryDest = RAW_METRICS_DIRECTORY + CPU_MEMORY_PATH + System.currentTimeMillis() + "_cpuandmemory.csv";
        String stacktraceDest = RAW_METRICS_DIRECTORY + STACKTRACE_PATH + System.currentTimeMillis() + "_stacktraces.txt";
        System.out.println("Creating directories.");
        Files.createDirectories(Paths.get(RAW_METRICS_DIRECTORY + CPU_MEMORY_PATH));
        Files.createDirectories(Paths.get(RAW_METRICS_DIRECTORY + STACKTRACE_PATH));
        System.out.println("Writing output files:\n" + cpuAndMemoryDest + "\n" + stacktraceDest);

        /** Write Cpu and Memory metrics. **/
        Path p = Paths.get(cpuAndMemoryDest);
        Files.createFile(p);
        Files.write(p, cpuMemoryBuffer.toString().getBytes());

        /** Write Stacktraces. **/
        p = Paths.get(stacktraceDest);
        Files.createFile(p);
        Files.write(p, stackTraceBuffer.toString().getBytes());
        return;
    }

    private static void initCpuMemSchema() {
        CPU_AND_MEM_SCHEMA.add("transform_name");
        CPU_AND_MEM_SCHEMA.add("timestamp_epoch");
        CPU_AND_MEM_SCHEMA.add("timestamp_epoch_ms");
        CPU_AND_MEM_SCHEMA.add("host");
        CPU_AND_MEM_SCHEMA.add("role");
        CPU_AND_MEM_SCHEMA.add("executor_number");
        CPU_AND_MEM_SCHEMA.add("process_uuid");
        CPU_AND_MEM_SCHEMA.add("infrastructure");
        CPU_AND_MEM_SCHEMA.add("system_cpu_usage");
        CPU_AND_MEM_SCHEMA.add("process_cpu_usage");
        CPU_AND_MEM_SCHEMA.add("memory_in_use");
        CPU_AND_MEM_SCHEMA.add("memory_committed");
        CPU_AND_MEM_SCHEMA.add("virtmemsize_incl_swap");
        CPU_AND_MEM_SCHEMA.add("max_memory_available");
        CPU_AND_MEM_SCHEMA.add("virt_mem_hwm");
        CPU_AND_MEM_SCHEMA.add("mem_usage");
    }

    private static void initIoSchema() {
        IO_SCHEMA.add("transform_name");
        IO_SCHEMA.add("timestamp_epoch");
        IO_SCHEMA.add("timestamp_epoch_ms");
        IO_SCHEMA.add("host");
        IO_SCHEMA.add("role");
        IO_SCHEMA.add("executor_number");
        IO_SCHEMA.add("process_uuid");
        IO_SCHEMA.add("infrastructure");
        IO_SCHEMA.add("system");
        IO_SCHEMA.add("idle");
        IO_SCHEMA.add("iowait");
        IO_SCHEMA.add("user");
        IO_SCHEMA.add("nice");
        IO_SCHEMA.add("jvmDiskBytesWritten");
        IO_SCHEMA.add("jvmBytesWrittenToStorage");
        IO_SCHEMA.add("jvmDiskBytesRead");
        IO_SCHEMA.add("jvmBytesReadFromStorage");
    }

    private static void processLine(String line) throws UncheckedIOException {

        try {
           if (line.contains("CpuAndMemory:")) {
                storeCpuAndMemory(g.fromJson(line.replace("CpuAndMemory:", ""), Map.class));
            } else if (line.contains("Stacktrace:")) {
               storeStackTrace(line.replace("Stacktrace:", ""));
            } else if (line.contains("IO:")) {
               storeIO(g.fromJson(line.replace("IO:", ""), Map.class));
           }
        } catch (IOException ex) {
            System.out.println("EXCEPTION: " + ex.getMessage());
            throw new UncheckedIOException(ex);
        } catch (JsonIOException ex) {
            return;
        }
    }
    public static void storeIO(Map<String, Object> metrics) {
        String transform_name                   = metrics.get("tag").toString();
        Double timestamp_ms                     = Double.valueOf(metrics.get("epochMillis").toString());
        String host                             = metrics.get("name").toString();
        String role                             = metrics.get("role").toString();
        String processUUID                      = metrics.get("processUuid").toString();
        String executor_number                  = metrics.get("executor_number").toString();
        Long system             = new Long(metrics.get("system").toString());
        Long idle               = new Long(metrics.get("idle").toString());
        Long iowait             = new Long(metrics.get("iowait").toString());
        Long user               = new Long(metrics.get("user").toString());
        Long nice               = new Long(metrics.get("nice").toString());
        Long jvmDiskBytesWritten  = new Long(metrics.get("jvmDiskBytesWritten").toString());
        Long jvmBytesWrittenToStorage = new Long(metrics.get("jvmBytesWrittenToStorage").toString());
        Long jvmDiskBytesRead = new Long(metrics.get("jvmDiskBytesRead").toString());
        Long jvmBytesReadFromStorage = new Long (metrics.get("jvmBytesReadFromStorage").toString());

        String[] line = new String[IO_SCHEMA.size()];

        line[IO_SCHEMA.indexOf("transform_name")] = transform_name;
        line[IO_SCHEMA.indexOf("timestamp_epoch")] = Long.toString(Math.round(timestamp_ms.doubleValue() / 1000.0));
        line[IO_SCHEMA.indexOf("timestamp_epoch_ms")] = Long.toString(Math.round(timestamp_ms));
        line[IO_SCHEMA.indexOf("host")] = host;
        line[IO_SCHEMA.indexOf("role")] = role;
        line[IO_SCHEMA.indexOf("executor_number")] = executor_number;// we need to set this in the profiler code. we used to get this from dcos in a log file name. taskId.toString();
        line[IO_SCHEMA.indexOf("process_uuid")] = processUUID;
        line[IO_SCHEMA.indexOf("infrastructure")] = "implementation2";
        line[IO_SCHEMA.indexOf("system")] = system.toString();
        line[IO_SCHEMA.indexOf("idle")] = idle.toString();
        line[IO_SCHEMA.indexOf("iowait")] = iowait.toString();
        line[IO_SCHEMA.indexOf("user")] = user.toString();
        line[IO_SCHEMA.indexOf("nice")] = nice.toString();
        line[IO_SCHEMA.indexOf("jvmDiskBytesWritten")] = jvmDiskBytesWritten.toString();
        line[IO_SCHEMA.indexOf("jvmBytesWrittenToStorage")] = jvmBytesWrittenToStorage.toString();
        line[IO_SCHEMA.indexOf("jvmDiskBytesRead")] = jvmDiskBytesRead.toString();
        line[IO_SCHEMA.indexOf("jvmBytesReadFromStorage")] = jvmBytesReadFromStorage.toString();
    }

    public static void storeStackTrace(String line) {
        stackTraceBuffer.append(line+"\n");
    }


    public static void storeCpuAndMemory(Map<String, Object> metrics) throws IOException {
        String transform_name                   = metrics.get("tag").toString();
        Double timestamp_ms                      = Double.valueOf(metrics.get("epochMillis").toString());
        String system_cpu_usage                 = metrics.get("systemCpuLoad").toString();
        String process_cpu_usage                = metrics.get("processCpuLoad").toString();
        String nonheap_memory_in_use            = metrics.get("nonHeapMemoryTotalUsed").toString();
        String nonheapmax                       = metrics.get("nonHeapMemoryMax").toString();
        String nonheap_memory_committed_in_jvm  = metrics.get("nonHeapMemoryCommitted").toString();
        String heap_memory_in_use               = metrics.get("heapMemoryTotalUsed").toString();
        String heapmax                          = metrics.get("heapMemoryMax").toString();
        String heap_memory_committed_in_jvm     = metrics.get("heapMemoryCommitted").toString();
        String vmsize_incl_swap                 = Long.toString(Math.round(Double.valueOf(metrics.get("vmSize").toString())));
        String host                             = metrics.get("name").toString();
        String role                             = metrics.get("role").toString();
        String processUUID                      = metrics.get("processUuid").toString();
        String virt_mem_hwm                     = Long.toString(Math.round(Double.valueOf(metrics.get("vmHWM").toString())));
        String executor_number                  = metrics.get("executor_number").toString();

        BigDecimal total_memory_in_use = new BigDecimal(heap_memory_in_use).add(new BigDecimal(nonheap_memory_in_use));
        BigDecimal total_memory_committed = new BigDecimal(heap_memory_committed_in_jvm).add(new BigDecimal(nonheap_memory_committed_in_jvm));
        BigDecimal total_memory_max = new BigDecimal(heapmax).add(new BigDecimal(nonheapmax));
        BigDecimal total_mem_usage = total_memory_in_use.divide(total_memory_max, 10, RoundingMode.HALF_UP);
        String[] line = new String[CPU_AND_MEM_SCHEMA.size()];

        line[CPU_AND_MEM_SCHEMA.indexOf("transform_name")] = transform_name;
        line[CPU_AND_MEM_SCHEMA.indexOf("timestamp_epoch")] = Long.toString(Math.round(timestamp_ms.doubleValue() / 1000.0));
        line[CPU_AND_MEM_SCHEMA.indexOf("timestamp_epoch_ms")] = Long.toString(Math.round(timestamp_ms));
        line[CPU_AND_MEM_SCHEMA.indexOf("host")] = host;
        line[CPU_AND_MEM_SCHEMA.indexOf("role")] = role;
        line[CPU_AND_MEM_SCHEMA.indexOf("executor_number")] = executor_number;// we need to set this in the profiler code. we used to get this from dcos in a log file name. taskId.toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("process_uuid")] = processUUID.toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("infrastructure")] = "implementation2";
        line[CPU_AND_MEM_SCHEMA.indexOf("system_cpu_usage")] = new BigDecimal(system_cpu_usage).toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("process_cpu_usage")] = new BigDecimal(process_cpu_usage).toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("memory_in_use")] = Long.toString(total_memory_in_use.longValue());
        line[CPU_AND_MEM_SCHEMA.indexOf("memory_committed")] = Long.toString(total_memory_committed.longValue());
        line[CPU_AND_MEM_SCHEMA.indexOf("virtmemsize_incl_swap")] = vmsize_incl_swap;
        line[CPU_AND_MEM_SCHEMA.indexOf("max_memory_available")] = Long.toString(total_memory_max.longValue());
        line[CPU_AND_MEM_SCHEMA.indexOf("virt_mem_hwm")] = virt_mem_hwm;
        line[CPU_AND_MEM_SCHEMA.indexOf("mem_usage")] = total_mem_usage.toString();

        cpuMemoryBuffer.append(String.join(",", line));
        cpuMemoryBuffer.append("\n");


        /** ALL MEMORY AND DISK VALUES ARE IN BYTES. **/
         /*   ps.setString(1, transform_name);
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
*/

    }
}

