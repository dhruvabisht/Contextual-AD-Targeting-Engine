package com.yahoo.adtargeting.pipeline;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Encoders;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class KeywordExtractor {
    private static final Logger logger = LoggerFactory.getLogger(KeywordExtractor.class);
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
        "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by", "about",
        "as", "like", "through", "over", "before", "between", "after", "since", "without", "under",
        "within", "along", "following", "across", "behind", "beyond", "plus", "except", "but", "up",
        "down", "from", "into", "out", "off", "above", "below", "near", "far", "this", "that", "these",
        "those", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "do", "does",
        "did", "will", "would", "shall", "should", "may", "might", "must", "can", "could"
    ));

    private static final Set<String> RELEVANT_POS_TAGS = new HashSet<>(Arrays.asList(
        "NN", "NNS", "NNP", "NNPS",  // Nouns
        "JJ", "JJR", "JJS",          // Adjectives
        "VB", "VBD", "VBG", "VBN", "VBP", "VBZ"  // Verbs
    ));

    private final POSTaggerME posTagger;
    private final SimpleTokenizer tokenizer;

    public KeywordExtractor() {
        try {
            // Load POS model
            InputStream modelIn = getClass().getResourceAsStream("/models/en-pos-maxent.bin");
            if (modelIn == null) {
                throw new IOException("Could not find POS model file");
            }
            POSModel model = new POSModel(modelIn);
            this.posTagger = new POSTaggerME(model);
            this.tokenizer = SimpleTokenizer.INSTANCE;
        } catch (IOException e) {
            logger.error("Failed to initialize POS tagger", e);
            throw new RuntimeException("Failed to initialize POS tagger", e);
        }
    }

    public Dataset<Row> extractKeywords(Dataset<Row> documents) {
        return documents.map(
            (org.apache.spark.api.java.function.MapFunction<Row, Row>) row -> {
                String content = row.getAs("content");
                String category = row.getAs("category");
                String url = row.getAs("url");
                String title = row.getAs("title");
                String author = row.getAs("author");
                String publishedAt = row.getAs("publishedAt");

                // Tokenize and POS tag
                String[] tokens = tokenizer.tokenize(content);
                String[] tags = posTagger.tag(tokens);

                // Extract keywords based on POS tags
                List<String> keywords = new ArrayList<>();
                for (int i = 0; i < tokens.length; i++) {
                    String token = tokens[i].toLowerCase();
                    String tag = tags[i];
                    
                    if (RELEVANT_POS_TAGS.contains(tag) && !STOP_WORDS.contains(token)) {
                        keywords.add(token);
                    }
                }

                // Get top keywords by frequency
                Map<String, Long> keywordFreq = keywords.stream()
                    .collect(Collectors.groupingBy(k -> k, Collectors.counting()));
                
                List<String> topKeywords = keywordFreq.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(10)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

                return RowFactory.create(
                    url,
                    title,
                    content,
                    category,
                    author,
                    publishedAt,
                    topKeywords
                );
            },
            Encoders.kryo(Row.class)
        ).toDF("url", "title", "content", "category", "author", "publishedAt", "keywords");
    }
} 