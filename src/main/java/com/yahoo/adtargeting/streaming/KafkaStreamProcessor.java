package com.yahoo.adtargeting.streaming;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.api.java.function.MapFunction;

import java.util.function.Function;
import java.util.concurrent.TimeoutException;

public class KafkaStreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamProcessor.class);
    private StreamingQuery query;

    public void startStreaming(SparkSession spark, Function<String, String> processor) {
        try {
            // Define schema for incoming Kafka messages
            StructType schema = new StructType()
                .add("url", DataTypes.StringType)
                .add("content", DataTypes.StringType)
                .add("timestamp", DataTypes.TimestampType);

            // Read from Kafka
            Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "web-pages")
                .option("startingOffsets", "latest")
                .load();

            // Process the stream
            Dataset<Row> processedDf = df
                .selectExpr("CAST(value AS STRING) as json")
                .selectExpr("from_json(json, 'url string, content string, timestamp timestamp') as data")
                .select("data.*")
                .map(new MapFunction<Row, Row>() {
                    @Override
                    public Row call(Row row) {
                        String content = row.getAs("content");
                        String processedContent = processor.apply(content);
                        return RowFactory.create(
                            row.getAs("url"),
                            processedContent,
                            row.getAs("timestamp")
                        );
                    }
                }, Encoders.bean(Row.class));

            // Write to Kafka
            query = processedDf
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "classified-pages")
                .option("checkpointLocation", "/tmp/kafka-checkpoint")
                .start();

            logger.info("Kafka stream processing started successfully");
        } catch (TimeoutException e) {
            logger.error("Timeout while starting Kafka stream processing", e);
            throw new RuntimeException("Timeout while starting Kafka stream processing", e);
        } catch (Exception e) {
            logger.error("Error starting Kafka stream processing", e);
            throw new RuntimeException("Failed to start Kafka stream processing", e);
        }
    }

    public void stop() {
        try {
            if (query != null) {
                query.stop();
                logger.info("Kafka stream processing stopped");
            }
        } catch (TimeoutException e) {
            logger.error("Timeout while stopping Kafka stream processing", e);
        } catch (Exception e) {
            logger.error("Error stopping Kafka stream processing", e);
        }
    }
} 