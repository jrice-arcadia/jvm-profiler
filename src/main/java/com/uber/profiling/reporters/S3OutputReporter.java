package com.uber.profiling.reporters;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.util.StringInputStream;
import com.uber.profiling.Reporter;
import com.uber.profiling.util.JsonUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

public class S3OutputReporter implements Reporter {
    private static AmazonS3Client s3Client = new AmazonS3Client();
    private  static ObjectMetadata objectMetadata = new ObjectMetadata();

    public void report(String profilerName, Map<String, Object> metrics) {
        System.out.println("S3OutputReporter report()");
        String json = JsonUtils.serialize(metrics);
        String path =  "s3a://cigna.data.implementation2.arcadia/bugathon/";
        String key = "metrics.txt";
        objectMetadata.setServerSideEncryption("AES256");
        try {
            System.out.println("Writing to S3: " + json);
            PutObjectResult res = s3Client.putObject(path, key, new ByteArrayInputStream(json.getBytes()), objectMetadata);
            System.out.println("S3 client returned from putObject().");
        } catch (Exception e) {
            System.out.println("Exception thrown. Type: " + e.getCause().getClass());
            System.out.println("message: " + e.getMessage());
        }

    }

    public void close() {

    }
}
