package com.yahoo.adtargeting.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ContentProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ContentProcessor.class);
    private final SparkSession spark;
    private final KeywordExtractor keywordExtractor;
    private final Map<String, String> esConfig;

    public ContentProcessor(SparkSession spark) {
        this.spark = spark;
        this.keywordExtractor = new KeywordExtractor();
        this.esConfig = new HashMap<>();
        esConfig.put("es.nodes", "localhost");
        esConfig.put("es.port", "9200");
        esConfig.put("es.nodes.wan.only", "true");
    }

    public void processAndIndex(Dataset<Row> documents) {
        try {
            // Extract keywords using OpenNLP
            Dataset<Row> processedDocs = keywordExtractor.extractKeywords(documents);

            // Index documents with keywords
            JavaEsSparkSQL.saveToEs(processedDocs, "ad-categories", esConfig);
            logger.info("Successfully indexed {} documents with keywords", processedDocs.count());
        } catch (Exception e) {
            logger.error("Error processing and indexing documents", e);
            throw new RuntimeException("Failed to process and index documents", e);
        }
    }

    public Dataset<Row> getDocumentsByCategory(String category) {
        String query = String.format(
            "{" +
                "\"query\": {" +
                    "\"bool\": {" +
                        "\"must\": [" +
                            "{ \"term\": { \"category\": \"%s\" } }" +
                        "]" +
                    "}" +
                "}" +
            "}",
            category
        );

        return JavaEsSparkSQL.esDF(spark, "ad-categories", query, esConfig);
    }

    public Dataset<Row> getDocumentsByKeywords(List<String> keywords) {
        String keywordsJson = String.join("\", \"", keywords);
        String query = String.format(
            "{" +
                "\"query\": {" +
                    "\"bool\": {" +
                        "\"should\": [" +
                            "{ \"terms\": { \"keywords\": [\"%s\"] } }" +
                        "]," +
                        "\"minimum_should_match\": 1" +
                    "}" +
                "}" +
            "}",
            keywordsJson
        );

        return JavaEsSparkSQL.esDF(spark, "ad-categories", query, esConfig);
    }
} 