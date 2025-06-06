package com.yahoo.adtargeting.api;

import com.yahoo.adtargeting.pipeline.ContentProcessor;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/keywords")
public class KeywordSearchController {
    private final ContentProcessor contentProcessor;

    @Autowired
    public KeywordSearchController(SparkSession spark) {
        this.contentProcessor = new ContentProcessor(spark);
    }

    @GetMapping("/search")
    public ResponseEntity<Map<String, Object>> searchByKeywords(
            @RequestParam List<String> keywords,
            @RequestParam(required = false) String category) {
        try {
            Dataset<Row> results;
            if (category != null && !category.isEmpty()) {
                // Search by both keywords and category
                Dataset<Row> categoryDocs = contentProcessor.getDocumentsByCategory(category);
                results = categoryDocs.filter((FilterFunction<Row>) row -> {
                    List<String> docKeywords = row.getList(row.fieldIndex("keywords"));
                    return keywords.stream().anyMatch(docKeywords::contains);
                });
            } else {
                // Search by keywords only
                results = contentProcessor.getDocumentsByKeywords(keywords);
            }

            List<Map<String, Object>> documents = results.collectAsList().stream()
                .map(row -> Map.of(
                    "url", row.getAs("url"),
                    "title", row.getAs("title"),
                    "category", row.getAs("category"),
                    "author", row.getAs("author"),
                    "publishedAt", row.getAs("publishedAt"),
                    "keywords", row.getAs("keywords")
                ))
                .collect(Collectors.toList());

            return ResponseEntity.ok(Map.of(
                "total", documents.size(),
                "documents", documents
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/categories/{category}")
    public ResponseEntity<Map<String, Object>> getDocumentsByCategory(
            @PathVariable String category) {
        try {
            Dataset<Row> results = contentProcessor.getDocumentsByCategory(category);
            
            List<Map<String, Object>> documents = results.collectAsList().stream()
                .map(row -> Map.of(
                    "url", row.getAs("url"),
                    "title", row.getAs("title"),
                    "category", row.getAs("category"),
                    "author", row.getAs("author"),
                    "publishedAt", row.getAs("publishedAt"),
                    "keywords", row.getAs("keywords")
                ))
                .collect(Collectors.toList());

            return ResponseEntity.ok(Map.of(
                "total", documents.size(),
                "documents", documents
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body(Map.of("error", e.getMessage()));
        }
    }
} 