package com.uber.profiling.reporters;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.uber.profiling.Reporter;
import com.uber.profiling.util.JsonUtils;
import com.uber.profiling.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.util.Map;

public class S3OutputReporter implements Reporter {
    private static AmazonS3 s3Client = new AmazonS3Client();
   // private static final int MAX_BUF_SIZE_BYTES = 5000000; // 5 mb -- make it much smaller for debug
    private static final int MAX_BUF_SIZE_BYTES = 5000; //5kb - this happened SO fast. but it worked.
    private static StringBuffer buffer = new StringBuffer(MAX_BUF_SIZE_BYTES); // sb is threadsafe

    public void report(String profilerName, Map<String, Object> metrics) {
        String json = JsonUtils.serialize(metrics);
        buffer.append(profilerName + ":"+ json + "\n");
        int length = buffer.length();
        System.out.println("S3OutputReporter report() for " + profilerName + ". Buffer length: " + length);
        if (length < MAX_BUF_SIZE_BYTES) {
            return;
        }
        try {
            ObjectMetadata m = new ObjectMetadata();
            String content = buffer.toString();
            m.setContentLength(content.length());
            m.setServerSideEncryption(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            System.out.println("Encryption set. Writing to S3.");
            PutObjectResult res = s3Client.putObject("cigna.data.implementation2.arcadia",
                    "bugathon/sources/macig/instances/bugathon/events/jvm_metrics_output/" + System.currentTimeMillis() + "_" + metrics.get("host") + ".txt",
                    new ByteArrayInputStream(content.getBytes()),
                    m);
            System.out.println("S3 client returned from putObject(). Emptying buffer.");
            buffer = new StringBuffer(MAX_BUF_SIZE_BYTES);
        }  catch (AmazonS3Exception e) {
            System.out.println("AmazonS3Exception thrown.");
            System.out.println(e.getErrorCode());
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println("Exception thrown. Type: " + e.getClass());
            System.out.println("message: " + e.getMessage());
        }


    }

    public void close() {

    }
}
