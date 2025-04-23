# OLake MongoDB to Apache Iceberg Integration Guide

This guide explains how to export data from MongoDB to Apache Iceberg using OLake's MongoDB driver, run discover and sync commands, and query the data with Apache Spark.

## Prerequisites

* Docker and Docker Compose installed
* MongoDB version 8.0 Docker image
* Git Bash (for running shell commands on Windows)
* OLake source code downloaded (assumed path: `C:/Users/deept/Downloads/olake-master/`)
* Java installed (required for OLake build scripts)
* Apache Spark installed for querying Iceberg data

## Required Files

Make sure the following files exist inside the `config/` directory:

* `config.json` – MongoDB connection details
* `catalog.json` – Generated from the discover step
* `writer.json` – Destination Iceberg/Hive settings

### Example Configuration Files

#### config.json
```json
{
  "connection": {
    "uri": "mongodb://admin:password@primary_mongo:27017/?replicaSet=rs0&authSource=admin",
    "database": "reddit",
    "collection": "posts"
  },
  "read_preference": "primary",
  "batch_size": 1000
}
```

#### writer.json
```json
{
  "type": "iceberg",
  "mode": "append",
  "catalog": {
    "name": "hive_prod",
    "type": "hive",
    "uri": "thrift://hive-metastore:9083",
    "warehouse": "s3a://warehouse/"
  },
  "storage": {
    "type": "s3",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "endpoint": "http://minio:9000",
    "path_style_access": true
  },
  "hive_clients": "5"
}
```

## Environment Setup

Set up the Java environment for OLake:

```bash
# Make sure JAVA_HOME is set
export JAVA_HOME=/path/to/java
export PATH=$JAVA_HOME/bin:$PATH

# Verify Java installation
java -version
```

## Step 1: Setup MongoDB Replica Set with Authentication

Use the provided `docker-compose.yml` file to start MongoDB with a replica set and keyfile authentication.

* The `init-keyfile` service generates the keyfile.
* `primary_mongo` initializes the replica set, creates an admin user, and restarts MongoDB with authentication enabled.
* `data-loader` imports sample Reddit JSON data into MongoDB after the primary is healthy.

Run:
```bash
docker-compose up -d
```

The replica set includes:
- Primary node: Handles write operations and initial reads
- Secondary nodes: Provide read replicas and failover capability
- Arbiter (optional): Participates in elections but doesn't store data

## Step 2: Run OLake Discover Command

Use Git Bash to generate the catalog file describing MongoDB collections:

```bash
./build.sh driver-mongodb discover --config /c/Users/deept/Downloads/olake-master/drivers/mongodb/config/config.json
```

This command creates `catalog.json` describing the schema of your MongoDB data. The catalog will map MongoDB document structures to Iceberg tables:
- MongoDB nested documents become structs in Iceberg
- MongoDB arrays become array types in Iceberg
- ObjectId fields become strings
- MongoDB's flexible schema is converted to a fixed schema in Iceberg

## Step 3: Run OLake Sync Command

Sync data from MongoDB into Apache Iceberg using OLake's Hive integration:

```bash
OLAKE_BASE_PATH="/c/Users/deept/Downloads/olake-master/drivers/mongodb/config/" && \
./build.sh driver-mongodb sync \
--config "$OLAKE_BASE_PATH/config.json" \
--catalog "$OLAKE_BASE_PATH/catalog.json" \
--destination "$OLAKE_BASE_PATH/writer.json"
```

For better performance, consider adding the `--parallelism=4` flag to enable concurrent processing.

## Step 4: Query Data Using Apache Spark

After syncing, use Apache Spark to query the data stored in Iceberg tables and display it in tabular format:

```scala
// Initialize Spark with Iceberg
spark.sql("CREATE DATABASE IF NOT EXISTS reddit")

// Query the synced data
spark.sql("SELECT * FROM reddit.posts LIMIT 10").show()

// Run complex queries
spark.sql("""
  SELECT 
    author, 
    COUNT(*) as post_count, 
    AVG(score) as avg_score 
  FROM reddit.posts 
  GROUP BY author 
  ORDER BY post_count DESC 
  LIMIT 10
""").show()
```

## Data Validation

To ensure data integrity, perform these validation steps:

1. Count documents in MongoDB:
   ```
   db.posts.count()
   ```

2. Count rows in Iceberg using Spark:
   ```
   spark.sql("SELECT COUNT(*) FROM reddit.posts").show()
   ```

3. Compare sample documents to verify field mappings:
   ```
   # MongoDB sample
   db.posts.findOne()
   
   # Iceberg/Spark sample
   spark.sql("SELECT * FROM reddit.posts LIMIT 1").show(truncate=false)
   ```

## Performance Tuning

- Adjust `batch_size` in config.json based on document size (smaller for large documents)
- Set appropriate `hive_clients` value based on available resources
- Use MongoDB indexes to optimize reading when filtering data
- Consider sharding large MongoDB collections for parallel extraction

## Known Issues and Solutions

1. **`hive_clients` Must Be a String**
   
   In `writer.json`, the property `hive_clients` must be a string, not an integer:
   ```json
   "hive_clients": "5"
   ```
   Passing it as a number causes errors during sync.

2. **Copy the `writers` Folder to MongoDB Driver Directory**
   
   The sync command requires JAR files inside the `writers` folder to be present under the MongoDB driver path. Copy the entire `writers` directory to:
   ```
   C:/Users/deept/Downloads/olake-master/drivers/mongodb/
   ```
   Without this, the sync fails to find essential files like:
   ```
   debezium-server-iceberg-sink-0.0.1-SNAPSHOT.jar
   ```

3. **Hive Server Missing in Local Docker Compose**
   
   The local docker-compose setup in `writers/iceberg/local-test/docker-compose.yml` does not start a Hive server. This causes connection errors when OLake tries to connect to Hive for metadata operations.
   
   **Solution:** Add a Hive metastore and Hive server service in the docker-compose file or connect OLake to an external Hive service.

## Common Error Scenarios

1. **MongoDB Connection Issues**:
   ```
   Error: MongoSocketOpenException: Exception opening socket
   Solution: Verify MongoDB is running and accessible from the host running OLake
   ```

2. **Schema Detection Problems**:
   ```
   Error: Failed to infer schema for collection
   Solution: Run the discover command with --verbose flag and ensure the collection has documents
   ```

3. **S3/MinIO Access Issues**:
   ```
   Error: com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied
   Solution: Verify S3 credentials and bucket permissions
   ```

## Additional Notes

* The example uses a local MinIO S3-compatible service for Iceberg's S3 storage. Adjust S3 endpoint and credentials as needed.
* Ensure your Hadoop and Hive configurations match your environment for smooth Iceberg integration.
* For large datasets, consider a phased migration approach using MongoDB filters to migrate subsets of data.
