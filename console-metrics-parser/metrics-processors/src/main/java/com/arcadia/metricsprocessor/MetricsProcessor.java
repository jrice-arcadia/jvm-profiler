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

    private static Gson g = new GsonBuilder().setLenient().create();
    private static String RAW_METRICS_DIRECTORY = "";
    private static final String STAGING         = "/staging/";
    private static final String COMPLETED       = "/completed/";
    private static final String NORMALIZED      = "/normalized";
    private static final String ERROR           = "/errored/";
    private static final String CPU_MEMORY_PATH = "/cpuandmemory/";
    private static final String STACKTRACE_PATH = "/stacktraces/";
    private static final String IO_PATH         = "/io/";

    protected static Writer cpuAndMemoryWriter, stacktraceWriter, ioWriter;

    /** Path examples. **/
    // /cpuandmemory/staging/cpuandmemory_jsons.txt
    // /cpuandmemory/completed/cpumemmetrics5.csv


    public static void main(String... args) throws Exception{
        String path = args[0];
        RAW_METRICS_DIRECTORY = path;

        String cpuAndMemoryDest = RAW_METRICS_DIRECTORY + CPU_MEMORY_PATH + COMPLETED;
        String stacktraceDest   = RAW_METRICS_DIRECTORY + STACKTRACE_PATH;
        String ioDest           = RAW_METRICS_DIRECTORY + IO_PATH + COMPLETED;
        System.out.println("Creating directories.");
        Files.createDirectories(Paths.get(cpuAndMemoryDest));
        Files.createDirectories(Paths.get(stacktraceDest));
        Files.createDirectories(Paths.get(ioDest));
        cpuAndMemoryDest = cpuAndMemoryDest + System.currentTimeMillis() + "_cpuandmemory.csv";
        stacktraceDest   = stacktraceDest + System.currentTimeMillis() + "_stacktraces.txt";
        ioDest           = ioDest + System.currentTimeMillis() + "_io.csv";

        cpuAndMemoryWriter = new FileWriter(cpuAndMemoryDest, true);
        stacktraceWriter   = new FileWriter(stacktraceDest, true);
        ioWriter           = new FileWriter(ioDest, true);


        initCpuMemSchema();
        initIoSchema();

        cpuAndMemoryWriter.append(String.join(",", CPU_AND_MEM_SCHEMA) + "\n");
        ioWriter.append(String.join(",", IO_SCHEMA) + "\n");

        /** I think for this application's scale we gotta do all of the routing and actual writing to files in the streams. **/
        /** Well, we are routing in the foreach. That's fine. Have processLine write to FileOutputStreams that get flushed. **/
        Stream<Path> metricsFiles = Files.list(Paths.get(path));
        metricsFiles.forEach(p ->  {
            if (Files.isDirectory(p)) return;
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
        return;
    }

    protected static void initCpuMemSchema() {
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

    protected static void initIoSchema() {
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
               System.out.println("Line for IO:\n" + line);
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
        String stat = metrics.get("stat").toString();
        Object[] cpus = g.fromJson(stat, Object[].class);
        Map<String, Object> aggregateCpu = null;
        for(Object cpu: cpus) {
            Map<String, Object> c = g.fromJson(cpu.toString(), Map.class);
            if ("cpu".equals(c.get("cpu").toString())) {
                aggregateCpu = c;
                break;
            }
        }

        String transform_name                   = metrics.get("tag").toString();
        Double timestamp_ms                     = Double.valueOf(metrics.get("epochMillis").toString());
        String host                             = metrics.get("name").toString();
        String role                             = metrics.get("role").toString();
        String processUUID                      = metrics.get("processUuid").toString();
        String executor_number                  = metrics.get("executor_number").toString();
        String system             = aggregateCpu.get("system").toString();  //new Long(aggregateCpu.get("system").toString());
        String idle               = aggregateCpu.get("idle").toString();
        String iowait             = aggregateCpu.get("iowait").toString();
        String user               = aggregateCpu.get("user").toString();
        String nice               = aggregateCpu.get("nice").toString();

        Map<String, Object> self = g.fromJson(metrics.get("self").toString(), Map.class);
        Map<String, Object> io = g.fromJson(self.get("io").toString(), Map.class);

        String jvmDiskBytesWritten  = io.get("wchar").toString();
        String jvmBytesWrittenToStorage = io.get("write_bytes").toString();
        String jvmDiskBytesRead = io.get("rchar").toString();
        String jvmBytesReadFromStorage = io.get("read_bytes").toString();

        String[] line = new String[IO_SCHEMA.size()];

        line[IO_SCHEMA.indexOf("transform_name")] = transform_name;
        line[IO_SCHEMA.indexOf("timestamp_epoch")] = Long.toString(Math.round(timestamp_ms.doubleValue() / 1000.0));
        line[IO_SCHEMA.indexOf("timestamp_epoch_ms")] = Long.toString(Math.round(timestamp_ms));
        line[IO_SCHEMA.indexOf("host")] = host;
        line[IO_SCHEMA.indexOf("role")] = role;
        line[IO_SCHEMA.indexOf("executor_number")] = executor_number;// we need to set this in the profiler code. we used to get this from dcos in a log file name. taskId.toString();
        line[IO_SCHEMA.indexOf("process_uuid")] = processUUID;
        line[IO_SCHEMA.indexOf("infrastructure")] = "implementation2";
        line[IO_SCHEMA.indexOf("system")] = Long.toString(new Double(system).longValue());
        line[IO_SCHEMA.indexOf("idle")] = Long.toString(new Double(idle).longValue());
        line[IO_SCHEMA.indexOf("iowait")] = Long.toString(new Double(iowait).longValue());
        line[IO_SCHEMA.indexOf("user")] = Long.toString(new Double(user).longValue());
        line[IO_SCHEMA.indexOf("nice")] = Long.toString(new Double(nice).longValue());
        line[IO_SCHEMA.indexOf("jvmDiskBytesWritten")] = Long.toString(new Double(jvmDiskBytesWritten).longValue());
        line[IO_SCHEMA.indexOf("jvmBytesWrittenToStorage")] = Long.toString(new Double(jvmBytesWrittenToStorage).longValue());
        line[IO_SCHEMA.indexOf("jvmDiskBytesRead")] = Long.toString(new Double(jvmDiskBytesRead).longValue());
        line[IO_SCHEMA.indexOf("jvmBytesReadFromStorage")] = Long.toString(new Double(jvmBytesReadFromStorage).longValue());

        writeLine(ioWriter, String.join(",", line));

    }

    private synchronized static void writeLine(Writer fw, String line) {
        try {
            fw.write(line);
            fw.write("\n");
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }


    public static void storeStackTrace(String line) {
        writeLine(stacktraceWriter, line);
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

        writeLine(cpuAndMemoryWriter, String.join(",", line));
    }
}

