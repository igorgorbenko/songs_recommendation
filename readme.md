# Songs Recommendation System

## Overview
This project is a music recommendation system that suggests songs to users based on their preferences and listening history. It uses a combination of machine learning algorithms and data from Kaggle to create personalized song recommendations.

## Features
- Personalized song recommendations based on user preferences.
- Clustering of songs and artists to enhance recommendation accuracy.
- Real-time recommendations using Kafka for event processing.

## Technologies
- Python for the backend and data processing.
- Milvus for vector database storage and retrieval.
- Kafka for real-time event streaming and processing.
- Docker for containerization and deployment.

## Installation
1. Clone the repository:
`git clone https://github.com/igorgorbenko/songs_recommendation.git`
2. Navigate to the project directory:
`cd songs_recommendation`
3. Install the required dependencies:
`pip install -r requirements.txt`
4. Set up your Milvis (Zilliz cloud) credentials and other necessary environment variables in a `.env` file.
An example of the .env file see below:

```
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_EVENTS_TOPIC_NAME=events
KAFKA_CONSUMER_GROUP_NAME=events_consumer

MILVUS_USERS_COLLECTION=users_collection
MILVUS_SONGS_COLLECTION=songs_collection
MILVUS_ARTISTS_COLLECTION=artists_collection
MILVUS_USERS_ARTISTS_COLLECTION=users_artists_collection

MILVUS_HOST = "https://123456.aws-us-west-2.vectordb.zillizcloud.com:19535"
MILVUS_TOKEN = "qazwsxedc"
```

## Usage
1. Start the Kafka and Milvus services (instructions may vary depending on your setup).
2. Run the recommendation system:
``python src/app.py``
3. Access the recommendations through the provided User interface (http://127.0.0.1:5000/)

## Contributing
Contributions to the Songs Recommendation project are welcome! Please feel free to submit pull requests or open issues to suggest improvements or report bugs.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.