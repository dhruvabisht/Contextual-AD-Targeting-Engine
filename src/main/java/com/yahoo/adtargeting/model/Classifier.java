package com.yahoo.adtargeting.model;

import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.BasicLineIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

public class Classifier {
    private static final Logger logger = LoggerFactory.getLogger(Classifier.class);
    private Word2Vec word2Vec;
    private TokenizerFactory tokenizerFactory;
    private Map<String, List<String>> categoryKeywords;

    public Classifier() {
        initializeModel();
        initializeCategories();
    }

    private void initializeModel() {
        try {
            // Initialize tokenizer
            tokenizerFactory = new DefaultTokenizerFactory();
            tokenizerFactory.setTokenPreProcessor(new CommonPreprocessor());

            // Load or train Word2Vec model
            File modelFile = new File("models/word2vec.model");
            if (modelFile.exists()) {
                word2Vec = WordVectorSerializer.readWord2VecModel(modelFile);
            } else {
                trainWord2VecModel();
            }
        } catch (Exception e) {
            logger.error("Error initializing model", e);
            throw new RuntimeException("Failed to initialize model", e);
        }
    }

    private void initializeCategories() {
        categoryKeywords = new HashMap<>();
        // Define keywords for each ad category
        categoryKeywords.put("technology", Arrays.asList("computer", "software", "hardware", "digital", "tech"));
        categoryKeywords.put("finance", Arrays.asList("money", "bank", "investment", "stock", "finance"));
        categoryKeywords.put("health", Arrays.asList("medical", "health", "fitness", "wellness", "diet"));
        categoryKeywords.put("travel", Arrays.asList("travel", "vacation", "tourism", "hotel", "flight"));
        // Add more categories as needed
    }

    private void trainWord2VecModel() {
        try {
            // Initialize Word2Vec
            word2Vec = new Word2Vec.Builder()
                    .minWordFrequency(5)
                    .iterations(5)
                    .layerSize(100)
                    .seed(42)
                    .windowSize(5)
                    .iterate(new BasicLineIterator("data/training.txt"))
                    .tokenizerFactory(tokenizerFactory)
                    .build();

            // Train the model
            word2Vec.fit();

            // Save the model
            WordVectorSerializer.writeWord2VecModel(word2Vec, new File("models/word2vec.model"));
        } catch (Exception e) {
            logger.error("Error training Word2Vec model", e);
            throw new RuntimeException("Failed to train model", e);
        }
    }

    public String classify(String content) {
        try {
            // Tokenize content
            List<String> tokens = tokenizerFactory.create(content).getTokens();
            
            // Get word vectors for tokens
            INDArray contentVector = getDocumentVector(tokens);
            
            // Calculate similarity with each category
            String bestCategory = null;
            double maxSimilarity = -1.0;
            
            for (Map.Entry<String, List<String>> category : categoryKeywords.entrySet()) {
                double similarity = calculateCategorySimilarity(contentVector, category.getValue());
                if (similarity > maxSimilarity) {
                    maxSimilarity = similarity;
                    bestCategory = category.getKey();
                }
            }
            
            return bestCategory != null ? bestCategory : "unknown";
        } catch (Exception e) {
            logger.error("Error classifying content", e);
            throw new RuntimeException("Failed to classify content", e);
        }
    }

    private INDArray getDocumentVector(List<String> tokens) {
        INDArray documentVector = Nd4j.zeros(word2Vec.getLayerSize());
        int wordCount = 0;
        
        for (String token : tokens) {
            if (word2Vec.hasWord(token)) {
                documentVector.addi(word2Vec.getWordVectorMatrix(token));
                wordCount++;
            }
        }
        
        if (wordCount > 0) {
            documentVector.divi(wordCount);
        }
        
        return documentVector;
    }

    private double calculateCategorySimilarity(INDArray contentVector, List<String> categoryKeywords) {
        INDArray categoryVector = Nd4j.zeros(word2Vec.getLayerSize());
        int wordCount = 0;
        
        for (String keyword : categoryKeywords) {
            if (word2Vec.hasWord(keyword)) {
                categoryVector.addi(word2Vec.getWordVectorMatrix(keyword));
                wordCount++;
            }
        }
        
        if (wordCount > 0) {
            categoryVector.divi(wordCount);
        }
        
        // Calculate cosine similarity
        double dotProduct = contentVector.mul(categoryVector).sumNumber().doubleValue();
        double norm1 = Math.sqrt(contentVector.mul(contentVector).sumNumber().doubleValue());
        double norm2 = Math.sqrt(categoryVector.mul(categoryVector).sumNumber().doubleValue());
        
        return dotProduct / (norm1 * norm2);
    }
} 