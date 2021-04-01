package com.arcadia.metricsprocessor;

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
import java.util.*;
import java.util.stream.*;
import java.lang.*;

public class MetricsProcessor {

    private static ArrayList<String> CPU_AND_MEM_SCHEMA = new ArrayList<String>();

    private static Gson g;
    private static String PARENT_DIR = "";

    public static void main(String... args) throws Exception{
        String path = args[0];
        PARENT_DIR = path;
        g = new Gson();
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
        CPU_AND_MEM_SCHEMA.add("virt_mem_hwm,mem_usage");

        Stream<Path> metricsFiles = Files.list(Paths.get(path));
        metricsFiles.forEach(p ->  {

            System.out.println("Processing Path: " + p);
            try {
                String[] tmp = p.getFileName().toString().split("\\.");
                String taskId = tmp[tmp.length-2];
                Stream<String> s = Files.lines(p);
                s.filter(l -> l.contains("CpuAndMemory") || l.contains("heapmax"))
                        .forEach(l -> processLine(l, taskId));
                s.close();
            } catch (IOException ex) {
                System.out.println("EXCEPTION: " + ex.getMessage());
                throw new UncheckedIOException(ex);
            }
            System.out.println("Finished processing Path: " + path);
        });
        metricsFiles.close();
        return;
    }

    private static void processLine(String line, String taskId) throws UncheckedIOException {

        try {
            String dest = PARENT_DIR + "processed_metrics/" ;


           if (line.contains("heapMemoryMax")) {
                storeCpuAndMemory(g.fromJson(line, Map.class), taskId);
               // Files.write(metricsPath, (line+"\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOptionCPU_AND_MEM_SCHEMA.add);
            } else if (line.contains("stacktrace")) {
               // Files.write(metricsPath, (line+"\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOptionCPU_AND_MEM_SCHEMA.add);
            }
        } catch (IOException ex) {
            System.out.println("EXCEPTION: " + ex.getMessage());
            throw new UncheckedIOException(ex);
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
        String virt_mem_hwm                     = metrics.get("vmHWM").toString();

        BigDecimal total_memory_in_use = new BigDecimal(heap_memory_in_use).add(new BigDecimal(nonheap_memory_in_use));
        BigDecimal total_memory_committed = new BigDecimal(heap_memory_committed_in_jvm).add(new BigDecimal(nonheap_memory_committed_in_jvm));
        BigDecimal total_memory_max = new BigDecimal(heapmax).add(new BigDecimal(nonheapmax));
        BigDecimal total_mem_usage = total_memory_in_use.divide(total_memory_max, 10, RoundingMode.HALF_UP);
        String[] line = new String[CPU_AND_MEM_SCHEMA.size()];

        line[CPU_AND_MEM_SCHEMA.indexOf("transform_name")] = transform_name;
        line[CPU_AND_MEM_SCHEMA.indexOf("timestamp_epoch")] = new BigDecimal(timestamp).divide(BigDecimal.valueOf(1000)).toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("timestamp_epoch_ms")] = new BigDecimal(timestamp).toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("host")] = host;
        line[CPU_AND_MEM_SCHEMA.indexOf("role")] = role;
        line[CPU_AND_MEM_SCHEMA.indexOf("executor_number")] = taskId.toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("process_uuid")] = processUUID.toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("infrastructure")] = "implementation2";
        line[CPU_AND_MEM_SCHEMA.indexOf("system_cpu_usage")] = new BigDecimal(system_cpu_usage).toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("process_cpu_usage")] = new BigDecimal(process_cpu_usage).toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("memory_in_use")] = total_memory_in_use.toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("memory_committed")] = total_memory_committed.toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("virtmemsize_incl_swap")] = new BigDecimal(vmsize_incl_swap).toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("max_memory_available")] = total_memory_max.toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("virt_mem_hwm")] = virt_mem_hwm.toString();
        line[CPU_AND_MEM_SCHEMA.indexOf("mem_usage")] = total_mem_usage.toString();








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