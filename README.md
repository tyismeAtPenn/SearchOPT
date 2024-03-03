# SearchOPT

A distributed search engine that crawls the web, indexes the crawled pages, and provides a user interface for searching the index.

## Project Outcome

Please use the following link to access our search engine: [http://langqin.cis5550.net:8080/](http://langqin.cis5550.net:8080/).

### List of VMs

|VM Nickname|Service|Private IP|Public IP|
|-----------|--------|----------|---------|
|Controller |KVS Coordinator & User-Interface|172.31.22.89|18.191.48.219|
|flame-worker-1|Flame Coordinator & Worker 1|172.31.26.25|3.138.195.3|
|flame-worker-2|Flame Worker 2 & 3|172.31.24.32|52.14.200.166|
|flame-worker-3|Flame Worker 4 & 5|172.31.16.87|3.145.214.223|
|kvs-workers-1|KVS Worker 1 & 2|172.31.21.29|18.216.187.126|
|kvs-workers-2|KVS Worker 3 & 4|172.31.18.194|18.118.199.192|

### Data Retrieved

We will demonstrate the data retrieved by our crawler per kvs worker. Since storage is partitioned based on the kvs worker id (name), we also include the names of the kvs workers in the table below.

|KVS Worker|pt-crawl Size|
|:----------:|:-------------:|
|W1 - daa| 55959 |
|W2 - gab| 42573 |
|W3 - maa| 21322 |
|W4 - pab| 67130 |
|Total| 186984 |

## How to Run

### Prerequisites

- Java 20 or above
- libs included in the project:
  - jsoup-1.17.1.jar
  - json-20231013.jar
  - lucene-core-9.9.0.jar
  - lucene-suggest-9.9.0.jar

### Run with JAR for deployment

1. **Clone the project to your local machine.**

    ``` bash
    git clone https://github.com/CIS5550/23fa-CIS5550-SearchOPT.git
    ```

2. **Navigate to the project root directory.**

    ``` bash
    cd 23fa-CIS5550-SearchOPT
    ```

3. **Compile and package the project.**

   Before compiling each part of the project, ensure to remove existing .class files to avoid conflicts. This can be done using the command:

    1. Compile and package kvs, flame with webserver.

        ``` bash
        javac -cp lib/jsoup-1.17.1.jar:lib/json-20231013.jar:lib/lucene-core-9.9.0.jar:lib/lucene-suggest-9.9.0.jar -d bin src/cis5550/kvs/*.java src/cis5550/tools/*.java src/cis5550/utils/*.java src/cis5550/webserver/*.java src/cis5550/generic/*.java src/cis5550/external/*.java src/cis5550/flame/*.java

        jar cvfm backend.jar src/META-INF/MANIFEST.MF -C bin .
        ```

    2. Compile and package frontend.

        ``` bash
        javac -cp lib/jsoup-1.17.1.jar:lib/json-20231013.jar:lib/lucene-core-9.9.0.jar:lib/lucene-suggest-9.9.0.jar -d bin src/frontend/*.java src/cis5550/jobs/*.java

        jar cvfm frontend.jar src/META-INF/MANIFEST.MF -C bin .
        ```

    3. Compile and package jobs for crawler, indexer jobs.

        ``` bash
        javac -cp lib/jsoup-1.17.1.jar:lib/json-20231013.jar:lib/lucene-core-9.9.0.jar:lib/lucene-suggest-9.9.0.jar -d bin src/cis5550/jobs/*.java

        jar cvfm jobs.jar src/META-INF/MANIFEST.MF -C bin .
        ```

4. **Deploy Backend.**

    Here we demo the deployment of backend on a single machine with single worker. To deploy on multiple machines and scale the workers, simply replace the localhost with the corresponding IP address.

    Deploy kvs service using the following command:

    ```bash
    java -cp backend.jar:lib/jsoup-1.17.1.jar:lib/json-20231013.jar:lib/lucene-core-9.9.0.jar:lib/lucene-suggest-9.9.0.jar cis5550.kvs.Coordinator 8000

    java -cp backend.jar:lib/jsoup-1.17.1.jar:lib/json-20231013.jar:lib/lucene-core-9.9.0.jar:lib/lucene-suggest-9.9.0.jar cis5550.kvs.Worker 8001 localhost:8000
    ```

    Deploy flame service using the following command:

    ```bash
    java -cp backend.jar:lib/jsoup-1.17.1.jar:lib/json-20231013.jar:lib/lucene-core-9.9.0.jar:lib/lucene-suggest-9.9.0.jar cis5550.flame.Coordinator 9000 localhost:8000

    java -cp backend.jar:lib/jsoup-1.17.1.jar:lib/json-20231013.jar:lib/lucene-core-9.9.0.jar:lib/lucene-suggest-9.9.0.jar cis5550.flame.Worker 9001 localhost:9000
    ```

5. **Submit Crawler Job**

    ```bash
    java -cp lib/jsoup-1.17.1.jar:lib/json-20231013.jar:lib/backend.jar cis5550.flame.FlameSubmit localhost:9000 jobs.jar cis5550.jobs.Crawler https://en.wikipedia.org/wiki/Main_Page\
    ```

6. **Deploy Front-end**

   ```bash
   java -cp frontend.jar:lib/jsoup-1.17.1.jar:lib/json-20231013.jar:lib/lucene-core-9.9.0.jar:lib/lucene-suggest-9.9.0.jar frontend.Server 8080 localhost:8000
   ```

### Project Structure

``` markdown
📦 SearchOPT
├─ README.md
├─ final_report.pdf
├─ keystore.jks
├─ keystore.p12
├─ lib
│  ├─ jsoup-1.17.1.jar
│  ├─ lucene-core-9.9.0.jar
│  ├─ json-20231013.jar
│  ├─ lucene-suggest-9.9.0.jar
└─ src
   ├─ cis5550
   │  ├─ external
   │  │  └─ PorterStemmer.java
   │  ├─ flame
   │  │  ├─ Coordinator.java
   │  │  ├─ FlameContext.java
   │  │  ├─ FlameContextImpl.java
   │  │  ├─ FlamePair.java
   │  │  ├─ FlamePairRDD.java
   │  │  ├─ FlamePairRDDImpl.java
   │  │  ├─ FlameRDD.java
   │  │  ├─ FlameRDDImpl.java
   │  │  ├─ FlameSubmit.java
   │  │  └─ Worker.java
   │  ├─ generic
   │  │  ├─ Coordinator.java
   │  │  └─ Worker.java
   │  ├─ jobs
   │  │  ├─ Coordinator.java
   │  │  ├─ IrRank.java
   │  │  ├─ PageRank.java
   │  │  ├─ Query.java
   │  │  ├─ Crawler.java
   │  │  ├─ RuleManager.java
   │  │  └─ URLManager.java
   │  ├─ kvs
   │  │  ├─ Coordinator.java
   │  │  ├─ KVS.java
   │  │  ├─ KVSClient.java
   │  │  ├─ Row.java
   │  │  └─ Worker.java
   │  ├─ tools
   │  │  ├─ HTTP.java
   │  │  ├─ Hasher.java
   │  │  ├─ KeyEncoder.java
   │  │  ├─ Loader.java
   │  │  ├─ Logger.java
   │  │  ├─ Partitioner.java
   │  │  ├─ SNIInspector.java
   │  │  ├─ Serializer.java
   │  │  └─ URLParser.java
   │  ├─ utils
   │  │  └─ WorkerEntry.java
   │  └─ webserver
   │     ├─ Request.java
   │     ├─ RequestHandler.java
   │     ├─ RequestImpl.java
   │     ├─ RequestParser.java
   │     ├─ Response.java
   │     ├─ ResponseImpl.java
   │     ├─ ResponseParser.java
   │     ├─ Route.java
   │     ├─ Server.java
   │     ├─ Session.java
   │     └─ SessionImpl.java
   └─ frontend
      ├─ Sample.html
      └─ Search.js
```

