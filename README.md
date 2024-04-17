# RSSE: Real-Time Sentiment Evaluation

## Project Overview

RSSE (Real-Time Social Media Sentiment Analysis Engine) is the final project for CSYE 7200. It is a sophisticated tool designed to decode public sentiment in real time. By analyzing sentiment data, RSSE provides users with data-driven insights to enhance decision-making processes.

## Core Features

- **Real-Time Analysis**: RSSE processes sentiment data as it arrives, ensuring up-to-date insights.
- **Data-Driven Advice**: The tool interprets sentiment data to offer actionable advice tailored to user needs based on GPT3.5-turbine-tutorial.

## Technical Architecture

RSSE leverages the powerful capabilities of Apache Spark, particularly using the MLlib library to perform scalable and efficient machine learning tasks. The main entry point of the program is `SparkApplication`, which serves as the heart of the backend server. By integrating Spark MLlib, RSSE efficiently processes large volumes of data to perform sentiment analysis, enabling real-time insights and predictions.

## Dataset

The sentiment analysis is powered by the "140-sentiment" dataset available from Kaggle. This dataset can be found here: [140-sentiment Kaggle Dataset](https://www.kaggle.com/datasets/kazanova/sentiment140).

## Dependencies

Ensure that you have the following installed(as we are not sure whether other versions can work or not,it's a good idea to follow the sbt file):
- JDK 11
- Spark 3.4
- Scala 2.13.8


## Getting Started

```sh
# clone the project
git clone git@github.com:LelouchCcCC/sentiment-analysis.git

# enter the project directory
cd sentimen-analysis

# run the SparkApplication file
```


## Contributions

Contributions to RSSE are welcome!

## Authors

- **Yuhan Zhang**
- **Yuxuan Qin**
- **Zhihan Zheng**

