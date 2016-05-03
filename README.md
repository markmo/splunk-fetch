# splunk-fetch

A little something to fetch test data off a Splunk Server overnight.

## Build Fat JAR

    ./gradlew fatJar

## Usage

    cp src/main/resources/config.properties.default src/main/resources/config.properties

Fill in connection details.

    java -jar build/libs/splunk-fetch-assembly-1.0-SNAPSHOT.jar StartTime IntervalInMinutes RepeatCount > output.raw
