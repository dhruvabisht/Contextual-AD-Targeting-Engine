# Contextual Ad Targeting Engine

A real-time pipeline that uses web page content and NLP models to classify pages into ad categories for contextual advertising.

## Features

- Real-time web page content processing
- NLP-based content classification
- Integration with Apache Kafka for streaming
- Elasticsearch for efficient storage and retrieval
- Apache Spark for distributed processing
- Deep Learning models for accurate classification

## Tech Stack

- Java 11
- Apache Spark 3.4.0
- Apache Kafka 3.4.0
- Elasticsearch 7.17.0
- DeepLearning4J
- Maven
- React (Dashboard)
- Node.js (Dashboard)

## Project Structure

```
src/
├── main/
│   ├── java/
│   │   └── com/
│   │       └── yahoo/
│   │           └── adtargeting/
│   │               ├── model/         # ML models and classification
│   │               ├── pipeline/      # Data processing pipeline
│   │               ├── storage/       # Elasticsearch integration
│   │               ├── streaming/     # Kafka streaming
│   │               └── utils/         # Utility classes
│   └── resources/
│       └── config/    # Configuration files
└── test/
    └── java/         # Test classes
```

## Setup and Installation

1. Prerequisites:
   - Java 11 or higher
   - Maven 3.6 or higher
   - Apache Spark
   - Apache Kafka
   - Elasticsearch
   - Node.js 14+ (for dashboard)
   - npm or yarn (for dashboard)

2. Build the backend:
   ```bash
   mvn clean package
   ```

3. Build the dashboard:
   ```bash
   cd dashboard
   npm install
   npm run build
   ```

4. Run the backend application:
   ```bash
   java -jar target/contextual-ad-targeting-1.0-SNAPSHOT-jar-with-dependencies.jar
   ```

5. Run the dashboard (development mode):
   ```bash
   cd dashboard
   npm start
   ```

## Configuration

Configuration files are located in `