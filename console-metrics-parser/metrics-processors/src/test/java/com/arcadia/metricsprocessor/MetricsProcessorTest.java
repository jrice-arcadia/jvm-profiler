package com.arcadia.metricsprocessor;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import com.google.gson.*;
import java.util.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;



public class MetricsProcessorTest {
      Gson g = new GsonBuilder().create();

      @Test
      public void testCpuMemProcessing() throws Exception {
          FileWriterTest fw = new FileWriterTest();
          String cpuAndMemEx = 
                  "CpuAndMemory:{\"heapMemoryMax\":2.6724532224E10,\"role\":\"executor\",\"nonHeapMemoryTotalUsed\":1.56657064E8,\"bufferPools\":[{\"totalCapacity\":522087611,\"name\":\"direct\",\"count\":262,\"memoryUsed\":522087613},{\"totalCapacity\":0,\"name\":\"mapped\",\"count\":0,\"memoryUsed\":0}],\"executor_number\":\"089e7401-5e04-4768-af08-7ef48be10e76\",\"heapMemoryTotalUsed\":5.301252328E9,\"vmRSS\":15716831232,\"epochMillis\":1617669614055,\"nonHeapMemoryCommitted\":1.61939456E8,\"heapMemoryCommitted\":1.4618722304E10,\"memoryPools\":[{\"peakUsageMax\":251658240,\"usageMax\":251658240,\"peakUsageUsed\":59190272,\"name\":\"Code Cache\",\"peakUsageCommitted\":59965440,\"usageUsed\":58360512,\"type\":\"Non-heap memory\",\"usageCommitted\":59965440},{\"peakUsageMax\":-1,\"usageMax\":-1,\"peakUsageUsed\":86951080,\"name\":\"Metaspace\",\"peakUsageCommitted\":90046464,\"usageUsed\":86951080,\"type\":\"Non-heap memory\",\"usageCommitted\":90046464},{\"peakUsageMax\":1073741824,\"usageMax\":1073741824,\"peakUsageUsed\":11345472,\"name\":\"Compressed Class Space\",\"peakUsageCommitted\":11927552,\"usageUsed\":11345472,\"type\":\"Non-heap memory\",\"usageCommitted\":11927552},{\"peakUsageMax\":9934209024,\"usageMax\":9654763520,\"peakUsageUsed\":9900654592,\"name\":\"PS Eden Space\",\"peakUsageCommitted\":9900654592,\"usageUsed\":1284748632,\"type\":\"Heap memory\",\"usageCommitted\":9645850624},{\"peakUsageMax\":776470528,\"usageMax\":181403648,\"peakUsageUsed\":622339672,\"name\":\"PS Survivor Space\",\"peakUsageCommitted\":776470528,\"usageUsed\":28431504,\"type\":\"Heap memory\",\"usageCommitted\":181403648},{\"peakUsageMax\":20043530240,\"usageMax\":20043530240,\"peakUsageUsed\":4323917136,\"name\":\"PS Old Gen\",\"peakUsageCommitted\":4791468032,\"usageUsed\":3988072192,\"type\":\"Heap memory\",\"usageCommitted\":4791468032}],\"processCpuLoad\":0.02194747879432846,\"systemCpuLoad\":0.14869948981188771,\"processCpuTime\":13839140000000,\"vmHWM\":15732686848,\"appId\":\"5e046610-51ba-4624-95a2-359e359c85b3-0010-driver-20210405235559-48397\",\"vmPeak\":41635000320,\"name\":\"13@ip-10-115-106-209.arcadia.implementation2.local\",\"host\":\"ip-10-115-106-209.arcadia.implementation2.local\",\"processUuid\":\"0b0f081a-005f-4ac8-9faa-d976cfed9e04\",\"nonHeapMemoryMax\":-1.0,\"tag\":\"macigjr\",\"vmSize\":41621082112,\"gc\":[{\"collectionTime\":129675,\"name\":\"PS Scavenge\",\"collectionCount\":1184},{\"collectionTime\":9188,\"name\":\"PS MarkSweep\",\"collectionCount\":39}]}";
          MetricsProcessor.cpuAndMemoryWriter = fw;
          MetricsProcessor.initCpuMemSchema();
          MetricsProcessor.storeCpuAndMemory(g.fromJson(cpuAndMemEx.replace("CpuAndMemory:", ""), Map.class));
          String res = fw.buf.toString();
          Assert.assertEquals("macigjr,1617669614,1617669614055,13@ip-10-115-106-209.arcadia.implementation2.local,executor,089e7401-5e04-4768-af08-7ef48be10e76,0b0f081a-005f-4ac8-9faa-d976cfed9e04,implementation2,0.14869948981188771,0.02194747879432846,5457909392,14780661760,41621082112,26724532223,15732686848,0.2042284350\n", res);
      }

      @Test
      public void testIoProcessing() throws Exception {
          FileWriterTest fw = new FileWriterTest();
          String IOEx =
                  "IO:{\"stat\":[{\"system\":9419,\"idle\":1081217,\"cpu\":\"cpu\",\"iowait\":4793,\"user\":922953,\"nice\":0},{\"system\":1009,\"idle\":65695,\"cpu\":\"cpu0\",\"iowait\":2286,\"user\":56881,\"nice\":0},{\"system\":679,\"idle\":67686,\"cpu\":\"cpu1\",\"iowait\":66,\"user\":57612,\"nice\":0},{\"system\":729,\"idle\":67961,\"cpu\":\"cpu2\",\"iowait\":36,\"user\":57390,\"nice\":0},{\"system\":675,\"idle\":67279,\"cpu\":\"cpu3\",\"iowait\":40,\"user\":58067,\"nice\":0},{\"system\":851,\"idle\":65586,\"cpu\":\"cpu4\",\"iowait\":1987,\"user\":57706,\"nice\":0},{\"system\":693,\"idle\":67068,\"cpu\":\"cpu5\",\"iowait\":92,\"user\":58265,\"nice\":0},{\"system\":510,\"idle\":68034,\"cpu\":\"cpu6\",\"iowait\":37,\"user\":57571,\"nice\":0},{\"system\":744,\"idle\":66893,\"cpu\":\"cpu7\",\"iowait\":40,\"user\":58372,\"nice\":0},{\"system\":468,\"idle\":67906,\"cpu\":\"cpu8\",\"iowait\":29,\"user\":57798,\"nice\":0},{\"system\":475,\"idle\":66469,\"cpu\":\"cpu9\",\"iowait\":31,\"user\":59235,\"nice\":0},{\"system\":481,\"idle\":68321,\"cpu\":\"cpu10\",\"iowait\":61,\"user\":57352,\"nice\":0},{\"system\":480,\"idle\":68540,\"cpu\":\"cpu11\",\"iowait\":9,\"user\":57206,\"nice\":0},{\"system\":388,\"idle\":68636,\"cpu\":\"cpu12\",\"iowait\":29,\"user\":57196,\"nice\":0},{\"system\":437,\"idle\":68286,\"cpu\":\"cpu13\",\"iowait\":19,\"user\":57472,\"nice\":0},{\"system\":400,\"idle\":68391,\"cpu\":\"cpu14\",\"iowait\":7,\"user\":57437,\"nice\":0},{\"system\":393,\"idle\":68461,\"cpu\":\"cpu15\",\"iowait\":16,\"user\":57385,\"nice\":0}],\"role\":\"executor\",\"executor_number\":\"a4c2e9cf-6509-4b4e-896f-4df65aa5127e\",\"appId\":\"5e046610-51ba-4624-95a2-359e359c85b3-0010-driver-20210405235559-48397\",\"name\":\"13@ip-10-115-106-202.arcadia.implementation2.local\",\"host\":\"ip-10-115-106-202.arcadia.implementation2.local\",\"processUuid\":\"c87655fd-88eb-4b84-9907-e3a1a45be813\",\"self\":{\"io\":{\"wchar\":317763534,\"write_bytes\":188579840,\"rchar\":3974204830,\"read_bytes\":21450752}},\"epochMillis\":1617668320286,\"tag\":\"macigjr\"}";
          MetricsProcessor.ioWriter = fw;
          MetricsProcessor.initIoSchema();
          MetricsProcessor.storeIO(g.fromJson(IOEx.replace("IO:", ""), Map.class));
          String res = fw.buf.toString();
          Assert.assertEquals("macigjr,1617668320,1617668320286,13@ip-10-115-106-202.arcadia.implementation2.local,executor,a4c2e9cf-6509-4b4e-896f-4df65aa5127e,c87655fd-88eb-4b84-9907-e3a1a45be813,implementation2,9419,1081217,4793,922953,0,317763534,188579840,3974204830,21450752\n", res);
      }


      public class FileWriterTest extends Writer {

            public StringBuffer buf = new StringBuffer();

            public FileWriterTest() { super(); }

            public void close() {}

            @Override
            public void write(String s) {
                  buf.append(s);
            }

            public void flush() {

            }
            public void write(char[] cbuf, int off, int len) {}
      }

}