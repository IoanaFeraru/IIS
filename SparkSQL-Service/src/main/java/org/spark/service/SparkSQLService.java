package org.spark.service;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

/**
 * Core Spark service — creates a SparkSession in local mode and starts
 * the Hive Thrift Server so DBeaver/JDBC clients can connect on port 10000.
 *
 * SparkSession is set as the default session so that java_method() calls
 * from SQL can reach it via SparkSession.getDefaultSession().get().
 *
 * Large-dataset tuning:
 *  - spark.sql.shuffle.partitions reduced from 200 → 8 (local mode has no cluster)
 *  - Off-heap memory enabled to avoid OOM on the 1M-row Postgres tables
 *  - Adaptive Query Execution (AQE) enabled — Spark 3.x feature that
 *    automatically coalesces shuffle partitions and handles skew
 *  - Result-set page size limited via the REST layer (see AutoRESTViewService)
 */
@Service
public class SparkSQLService {

    private static final Logger log = Logger.getLogger(SparkSQLService.class.getName());

    @Value("${spark.thrift.port:10000}")
    private String thriftPort;

    @Value("${spark.driver.memory:2g}")
    private String driverMemory;

    @Value("${spark.executor.memory:2g}")
    private String executorMemory;

    private SparkSession spark;

    public SparkSQLService(
            @Value("${spark.thrift.port:10000}") String thriftPort,
            @Value("${spark.driver.memory:2g}") String driverMemory,
            @Value("${spark.executor.memory:2g}") String executorMemory) {
        this.thriftPort = thriftPort;
        this.driverMemory = driverMemory;
        this.executorMemory = executorMemory;
        startThriftServer();
    }

    private void startThriftServer() {
        log.info(">>> Initialising SparkSession (local mode) ...");

        this.spark = SparkSession.builder()
                .master("local[*]")
                .appName("IIS-SparkSQL-Service")
                .enableHiveSupport()

                // ── Thrift Server ──────────────────────────────────────
                .config("hive.server2.thrift.port", thriftPort)

                // ── Memory ─────────────────────────────────────────────
                .config("spark.driver.memory", driverMemory)
                .config("spark.executor.memory", executorMemory)
                // Off-heap: helps with very large JSON datasets (1M rows)
                .config("spark.memory.offHeap.enabled", "true")
                .config("spark.memory.offHeap.size", "1g")

                // ── Shuffle / AQE ──────────────────────────────────────
                // Local mode has no cluster; 200 shuffle partitions is wasteful
                .config("spark.sql.shuffle.partitions", "8")
                // Adaptive Query Execution auto-optimises joins and partitions
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

                // ── JSON / REST view safety ────────────────────────────
                // Allow java_method() to call our classes
                .config("spark.sql.allowMultiStatements", "true")

                // ── Logging ────────────────────────────────────────────
                .config("spark.ui.enabled", "true")   // Spark UI on :4040
                .config("spark.eventLog.enabled", "false")

                .getOrCreate();

        // Make the session reachable from static java_method() calls
        SparkSession.setDefaultSession(this.spark);

        log.info(">>> Starting HiveThriftServer2 on port " + thriftPort + " ...");
        HiveThriftServer2.startWithContext(spark.sqlContext());
        log.info(">>> HiveThriftServer2 started — DBeaver can connect on port " + thriftPort);
    }

    public SparkSession getSpark() {
        return spark;
    }
}
