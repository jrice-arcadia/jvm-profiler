Fork of uber's jvm-profiler for use at 
arcadia. This just a proof of concept version.

To build:

`./gradlew clean build`

To publish to local maven cache using maven plugin:

`./gradlew clean install`

Then build aa-common-notebook locally against the version of the jvm-profiler you published locally.