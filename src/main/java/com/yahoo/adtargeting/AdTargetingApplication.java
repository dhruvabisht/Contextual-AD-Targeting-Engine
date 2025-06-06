package com.yahoo.adtargeting;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class AdTargetingApplication {
    public static void main(String[] args) {
        SpringApplication.run(AdTargetingApplication.class, args);
    }

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName("AdTargetingEngine")
                .master("local[*]")
                .config("spark.es.nodes", "localhost")
                .config("spark.es.port", "9200")
                .config("spark.es.nodes.wan.only", "true")
                .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
                .getOrCreate();
    }
} 