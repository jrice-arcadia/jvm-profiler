Fork of uber's jvm-profiler for use at 
arcadia. This just a proof of concept version.

There is another gradle project in this project under "console-metrics-parser".
This provides Java classes for manipulating metric data. Right now it contains a tool to convert metric JSONs from the profiler into a CSV format.

spark-submit-create-requests contains some raw payloads we can use to directly submit Spark jobs to spark-submit via curl. This is the actual spark-submit, not the proxy we put in front of it.


To build:

`./gradlew clean build`

To publish to local maven cache using maven plugin:

`./gradlew clean install`

Then build aa-common-notebook locally against the version of the jvm-profiler you published locally.