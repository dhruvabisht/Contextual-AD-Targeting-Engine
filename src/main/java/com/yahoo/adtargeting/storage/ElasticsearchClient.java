package com.yahoo.adtargeting.storage;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import java.util.ArrayList;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchClient {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchClient.class);
    private final RestHighLevelClient client;
    private final String indexName;
    private final String typeName;

    public ElasticsearchClient() {
        this.client = createClient();
        this.indexName = "ad-categories";
        this.typeName = "page-classification";
        initializeIndex();
    }

    private RestHighLevelClient createClient() {
        RestClientBuilder builder = RestClient.builder(
            new HttpHost("localhost", 9200, "http")
        );
        return new RestHighLevelClient(builder);
    }

    private void initializeIndex() {
        try {
            // Check if index exists
            GetIndexRequest request = new GetIndexRequest(indexName);
            boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);

            if (!exists) {
                // Create index with mappings
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                
                // Configure index settings
                createIndexRequest.settings(Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                );

                // Define mappings
                Map<String, Object> properties = new HashMap<>();
                properties.put("url", Map.of("type", "keyword"));
                properties.put("content", Map.of("type", "text"));
                properties.put("category", Map.of("type", "keyword"));
                properties.put("timestamp", Map.of("type", "date"));

                Map<String, Object> mappings = new HashMap<>();
                mappings.put("properties", properties);

                createIndexRequest.mapping(mappings);
                
                // Create the index
                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("Created index: {}", indexName);
            }
        } catch (IOException e) {
            logger.error("Error initializing Elasticsearch index", e);
            throw new RuntimeException("Failed to initialize Elasticsearch index", e);
        }
    }

    public void indexDocument(String content, String category) {
        try {
            // Create document
            Map<String, Object> document = new HashMap<>();
            document.put("content", content);
            document.put("category", category);
            document.put("timestamp", System.currentTimeMillis());

            // Create index request
            IndexRequest request = new IndexRequest(indexName)
                .source(document, XContentType.JSON);

            // Index the document
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            
            logger.debug("Indexed document with ID: {}", response.getId());
        } catch (IOException e) {
            logger.error("Error indexing document", e);
            throw new RuntimeException("Failed to index document", e);
        }
    }

    public void close() {
        try {
            client.close();
            logger.info("Elasticsearch client closed");
        } catch (IOException e) {
            logger.error("Error closing Elasticsearch client", e);
        }
    }

    // Analytics: Total articles per category
    public Map<String, Long> getArticlesPerCategory() {
        Map<String, Long> result = new HashMap<>();
        try {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.size(0);
            sourceBuilder.aggregation(AggregationBuilders.terms("categories").field("category"));
            SearchRequest searchRequest = new SearchRequest(indexName).source(sourceBuilder);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            Terms terms = response.getAggregations().get("categories");
            for (Terms.Bucket bucket : terms.getBuckets()) {
                result.put(bucket.getKeyAsString(), bucket.getDocCount());
            }
        } catch (Exception e) {
            logger.error("Error fetching articles per category", e);
        }
        return result;
    }

    // Analytics: Top 5 keywords per category
    public Map<String, List<String>> getTopKeywordsPerCategory() {
        Map<String, List<String>> result = new HashMap<>();
        try {
            // If 'keywords' field is not present, return stub
            // TODO: Replace with real aggregation if 'keywords' is indexed
        } catch (Exception e) {
            logger.error("Error fetching top keywords per category", e);
        }
        return result;
    }

    // Analytics: Documents per hour/day
    public Map<String, Long> getDocumentsPerPeriod(String period) {
        Map<String, Long> result = new HashMap<>();
        try {
            DateHistogramInterval interval = "day".equalsIgnoreCase(period) ? DateHistogramInterval.DAY : DateHistogramInterval.HOUR;
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.size(0);
            DateHistogramAggregationBuilder agg = AggregationBuilders.dateHistogram("docs_per_period")
                .field("timestamp")
                .calendarInterval(interval);
            sourceBuilder.aggregation(agg);
            SearchRequest searchRequest = new SearchRequest(indexName).source(sourceBuilder);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            Histogram hist = response.getAggregations().get("docs_per_period");
            for (Histogram.Bucket bucket : hist.getBuckets()) {
                result.put(bucket.getKeyAsString(), bucket.getDocCount());
            }
        } catch (Exception e) {
            logger.error("Error fetching documents per period", e);
        }
        return result;
    }
} 