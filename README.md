# Datonis Edge SDK - Java
This repository contains a Java language version of SDK for developing an Agent program capable of sending data to the [**Datonis Cloud Plaform**](https://www.datonis.io).

It can be used for developing Agents on Android as well. It contains a sample program that simulates Temperature and Humidity parameters for a Living Room modeled on Datonis. You can use this as a starting point and write your own application

## Pre-requisites
* Java 1.7 or later
* Android 2.2 or later
* Apache Maven (mvn)
* Git for checking out this source code from Github

## Downloading the repository

On your computer, execute following command:

git clone https://github.com/Altizon/datonis-edge-sdk-java.git

## Configuring the Agent

The agent needs to be configured first before you build the package. There are a few things that you need to do:

1. Signup on the [Datonis Platform](https://www.datonis.io) if you have not already
2. Create a Thing Template called **Room** with 2 metrics **humidity** and **temperature**
3. Create a Thing called **LivingRoom** using the **Room** as your template. Make a note of the Thing key that is generated for this thing
4. Download the key pair with associated role as **Agent** from the KeyPairs section
5. Edit the datonis-edge.properties file in examples/src/main/resources and replace the access key/secret key pair obtained from the downloaded file (from step 4)
6. Edit the SampleAgent.java file in examples/src/main/java/io/datonis/examples and replace the Thing key (obtained in step 3) in **startGateway()** function

## Building the package

mvn clean install eclipse:eclipse

## Importing projects into eclipse

The above command should automatically generate classpath and project files for the **sdk** and **examples** projects.

You can simply import these two as existing projects into your eclipse workspace

## Running your sample agent

cd examples

java -Dlog4j.properties=src/main/resources/log4j.properties -Ddatonis-edge.properties=src/main/resources/datonis-edge.properties -jar target/datonis-edge-examples-5.0.0-jar-with-dependencies.jar

## Viewing data on the UI

1. Login to Datonis using your credentials.
2. Click on the Things workspace
3. Select the Living Room thing and click the 'Data' button associated with it in the 'Actions' column

## Some useful configuration options in datonis-edge.properties

| Parameter   | Description | Possible Values  | Default Value  |
|---|---|---|---|
| protocol  | Protocol to use for connecting to Datonis  | http, https, mqtt, mqtts  | https  |
| bulk_transmit | Whether to batch and bulk transmit packets | true, false  | true  |
| bulk_transmit_interval  | Interval between two successive bulk requests in milliseconds | > 5000  | 60000 (1 minute)  |
| bulk_max_elements  | Maximum number of elements to put into a bulk packet | > 10  | 25 |
| concurrency  | Number of concurrent requests | >= 1  | 5 |
| request_timeout  | Timeout for socket connection | > 60000  | 180000 (3 minutes) |

## Need Help?

Please feel free to raise a ticket at our [Support Portal](https://support.datonis.io)
