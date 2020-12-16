package com.uber.profiling.reporters;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.uber.profiling.Reporter;
import com.uber.profiling.util.JsonUtils;

import java.util.Map;

public class S3OutputReporter implements Reporter {
    private static AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

    public void report(String profilerName, Map<String, Object> metrics) {
        System.out.println("S3OutputReporter report()");
        String json = JsonUtils.serialize(metrics);
        String path =  "cigna.data.implementation2.arcadia/bugathon/";
        String key = "metrics.txt";

        try {
            System.out.println("Writing to S3: " + json);
            PutObjectResult res = s3Client.putObject(path, key, json);
            System.out.println("S3 client returned from putObject().");
        }  catch (AmazonS3Exception e) {
            System.out.println("AmazonS3Exception thrown.");
            System.out.println(e.getErrorResponseXml());
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println("Exception thrown. Type: " + e.getClass());
            System.out.println("message: " + e.getMessage());
        }


    }

    public void close() {

    }
}
