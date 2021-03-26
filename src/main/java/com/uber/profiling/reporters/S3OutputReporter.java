package com.uber.profiling.reporters;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.uber.profiling.Reporter;
import com.uber.profiling.util.JsonUtils;

import java.util.List;
import java.util.Map;

public class S3OutputReporter implements Reporter {
    private static AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

    public void report(String profilerName, Map<String, Object> metrics) {
        System.out.println("S3OutputReporter report()");
        String json = JsonUtils.serialize(metrics);
        String path = "cigna.data.implementation2.arcadia/bugathon/sources/macig/instances/bugathon/events/jvm_metrics_output/";
        //String path =  "cigna.data.implementation2.arcadia/bugathon/";
        String key = "metrics.txt";

        try {
            System.out.println("Doing a LIST on events.");
            List<S3ObjectSummary> o = s3Client.listObjects("cigna.data.implementation2.arcadia/bugathon/sources/macig/instances/bugathon/events/").getObjectSummaries();
            System.out.println("number of objects found: " + o.size());
            for(S3ObjectSummary s : o) {
                System.out.println("summary: " + s.getKey());
            }
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
