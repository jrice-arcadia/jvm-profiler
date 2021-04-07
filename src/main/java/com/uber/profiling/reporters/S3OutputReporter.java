package com.uber.profiling.reporters;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.uber.profiling.Reporter;
import com.uber.profiling.util.JsonUtils;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.UUID;

public class S3OutputReporter implements Reporter {
    private static AmazonS3 s3Client = new AmazonS3Client();
    private static UUID EXECUTOR_IDENTIFIER = UUID.randomUUID();
    private static final int MAX_BUF_SIZE_BYTES = 1000000; // 1 mb -- make it much smaller for debug
    //private static final int MAX_BUF_SIZE_BYTES = 500000; // 500kb //5kb - this happened SO fast. but it worked.
    private static StringBuffer buffer = new StringBuffer(MAX_BUF_SIZE_BYTES); // sb is threadsafe

    public void report(String profilerName, Map<String, Object> metrics) {
        synchronized (this) {
            metrics.put("executor_number", EXECUTOR_IDENTIFIER.toString());
            String json = JsonUtils.serialize(metrics);
            buffer.append(profilerName + ":" + json + "\n");
            int length = buffer.length();
            if (length < MAX_BUF_SIZE_BYTES) {
                return;
            }
            try {
                System.out.println("S3OutputReporter report() for " + profilerName + ". " +
                        "Flushing metrics buffer to S3. Buffer length: " + length);
                ObjectMetadata m = new ObjectMetadata();
                String content = buffer.toString();
                m.setContentLength(content.length());
                m.setServerSideEncryption(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                System.out.println("Encryption set. Writing to S3.");
                PutObjectResult res = s3Client.putObject("cigna.data.implementation2.arcadia",
                        "cna004/sources/macig/instances/cna004/events/jvm_metrics_output/" + System.currentTimeMillis() + "_" + metrics.get("host") + ".txt",
                        new ByteArrayInputStream(content.getBytes()),
                        m);
                System.out.println("S3 client returned from putObject(). Emptying buffer.");
                /** Emptying the buffer is more performant than re-allocating, probably. **/
                buffer.delete(0, buffer.length());
            } catch (AmazonS3Exception e) {
                System.out.println("AmazonS3Exception thrown.");
                System.out.println(e.getErrorCode());
                System.out.println(e.getMessage());
            } catch (Exception e) {
                System.out.println("Exception thrown. Type: " + e.getClass());
                System.out.println("message: " + e.getMessage());
            }
        }
    }

    public void close() {
    }
}
