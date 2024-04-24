# Advanced-NLP-Based-Amazon-Reviews-Analytics
Machine Learning Operations (MLOps) project on Sentiment Analysis and Aspect Based Sentiment Analysis (ABSA) on Amazon Reviews

# Introduction
The goal of this project is to design and implement an advanced analytics engine based on Natural 
Language Processing methodologies. The system is intended to automatically parse, dissect, and 
assimilate customer feedback from Amazon reviews. Its primary goal is to distill sentiment trajectories, 
uncover actionable insights, and group feedback into thematic buckets for each category of products, 
enabling businesses to improve product offerings, raise customer service standards, and increase overall 
consumer satisfaction.
Our goal is to create a dependable, scalable system that employs a sophisticated merger of NLP 
techniques, including tokenization, lemmatization, sentiment scoring, and thematic modeling, as well as 
advanced technologies like TF-IDF, word embeddings, LSTM networks, and BERT models. This 
combination intends to extract nuanced insights and identify significant sentiment trends and thematic 
categorizations from unstructured text, providing businesses with granular, actionable data.
Furthermore, our project includes an Aspect-based Sentiment Analysis module to further enhance its 
utility and impact. This module delves deeper than the general sentiment by analyzing the sentiment 
towards particular features e.g., the battery life of electronics, that people discuss about products in 
each category. Businesses can use this to determine which features are popular, valued, and in need of 
improvement.

# Data Information
## Dataset Introduction:
The Multi-Domain Sentiment Dataset serves as the source of the primary training dataset for the NLPbased Amazon Review Analytics System. This dataset is curated to facilitate deep Natural Language 
Processing and sentiment analysis studies, and it includes a large number of product reviews from 
various categories. Its primary purpose is to enable the extraction of sentimental trends from customer 
feedback, serving as the foundational training material for our NLP engine. Additionally, we will be using 
web scraping of Amazon reviews to augment the data for Aspect-based Sentiment Analysis by first 
feeding it through the already training NLP model to label it and then use the data to uncover consumer 
preferences and popular features for each category.
The data covers four major consumer categories: electronics, books, DVDs, and kitchen and housewares. 
Each category is represented by a separate folder containing two files containing customer reviews 
organized by sentiment: positive and negative. The data is structured in XML format, and each review 
entry includes several fields, including.
Data spans four key consumer categories: Electronics, Books, DVDs, and Kitchen & Housewares. Each 
category is represented by a dedicated folder containing two to three files that encapsulate customer 
reviews, delineated by sentiment: positive, negative, and unlabeled reviews. Data is structured in XML 
format and includes fields such as a unique identifier with an ASIN, a review title, review text, the 
reviewer's username and product name

## Data Card 
![image](https://github.com/Saravanan-Arumugam3/Advanced-NLP-Based-Amazon-Reviews-Analytics/assets/46652703/5c646a4b-a3d8-4691-ad21-625f6d8f6695)

## Data Scorce
The data is taken from [UCSD repository](https://cseweb.ucsd.edu/~jmcauley/datasets.html#amazon_reviews)

# Installation
This project uses `Python >= 3.8`. Please ensure that the correct version is installed on your device. This project also works on Windows, Linux and Mac.

# Prerequisities
1. git
2. python>=3.8
3. docker daemon/desktop is running

## User Installation
The steps for User installation are as follows:

1. Clone repository onto the local machine
```
git clone https://github.com/Saravanan-Arumugam3/Advanced-NLP-Based-Amazon-Reviews-Analytics.git
```
2. Check python version  >= 3.8
```python
python --version
```
3. Check if you have enough memory
```docker
docker run --rm "debian:bullseye-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'
```

<hr>

**FOR WINDOWS: Create a file called .env in the same folder as `docker-compose.yaml` and set the user as follows:**
```
AIRFLOW_UID=50000
```
**If you get the following error**
```
ValueError: Unable to configure handler 'processor'
```
**Setting the user manually like above fixes it**

<hr>

4. With Docker running, initialize the database. This step only has to be done once.
```docker
docker compose up airflow-init
```
5. Run airflow
```docker
docker-compose up
```
Wait until terminal outputs something similar to

`app-airflow-webserver-1  | 127.0.0.1 - - [17/Feb/2023:09:34:29 +0000] "GET /health HTTP/1.1" 200 141 "-" "curl/7.74.0"`

6. Visit localhost:8080 login with credentials

```
user:airflow2
password:airflow2
```
7. Run the DAG by clicking on the play button on the right side of the window

8. Stop docker containers
```docker
docker compose down
```
# Tools and Libraries

Data Scraping and Parsing: BeautifulSoup and Selenium for dynamic web scraping.
Data Processing: Apache Airflow for real-time data processing.
Machine Learning: TensorFlow or PyTorch for model development, and MLflow for lifecycle management.
Monitoring: Use of ELK Stack for logging and visualization of system performance.
  
# Development and Deployment

Repository and Version Control: Hosted on GitHub with structured directories for data, models, scripts, and documentation.
CI/CD: Continuous Integration and Deployment pipelines for automated building, testing, and deployment.
Monitoring and Logging: Systematic monitoring of infrastructure health, application performance, and model accuracy.

![image](https://github.com/Saravanan-Arumugam3/Advanced-NLP-Based-Amazon-Reviews-Analytics/assets/46652703/289729bb-cdd1-4ebf-ba87-d034e3ac2928)


This project represents a comprehensive effort to integrate NLP and machine learning for practical business applications, with a strong emphasis on real-time data processing, scalability, and actionable insights. It’s designed to be an end-to-end solution from data acquisition to processing, analysis, and visualization, ensuring that businesses can swiftly respond to consumer sentiment and market trends.


# Data Pipeline Airflow

Apache Airflow is an open-source tool for orchestrating complex computational workflows and data processing pipelines. For our Amazon Review Analytics project, Airflow allows us to schedule and monitor workflows, ensuring each step of the data processing from ingestion to analysis is executed in the correct order and at the correct time.

![image](https://github.com/Saravanan-Arumugam3/Advanced-NLP-Based-Amazon-Reviews-Analytics/assets/46652703/8cb0e9de-b158-4888-ac27-b39d17ad368e)

Using Airflow, we create a Directed Acyclic Graph (DAG) that defines the sequence of tasks required for the data pipeline. Each task is an instance of an Airflow operator, and the relationships between them determine the pipeline’s flow. This DAG includes tasks for scraping reviews from Amazon, preprocessing the data, running sentiment analysis, feature engineering, and regularly updating the models with new data to ensure that insights are current and relevant.

![image](https://github.com/Saravanan-Arumugam3/Advanced-NLP-Based-Amazon-Reviews-Analytics/assets/46652703/045306bb-29b5-4f19-a1c8-ab248cdc3cad)

To ensure the reliability of our data pipeline, we also incorporate error handling and retry logic within our Airflow tasks. This guarantees that our data processing is robust against failures and that any issues can be quickly identified and addressed.

Additionally, using Airflow's UI, we can visualize our pipeline's performance, monitor task completion, and diagnose any processing bottlenecks. It is a powerful tool for maintaining an overview of our data processing steps and for debugging when necessary.

![image](https://github.com/Saravanan-Arumugam3/Advanced-NLP-Based-Amazon-Reviews-Analytics/assets/46652703/1d251b6f-36e5-40dc-a102-5ce9e2f412be)

# TensorFlow Data Validation (TFDV)

TFDV is utilized to analyze the dataset and produce descriptive statistics that help understand its distribution, as well as to detect anomalies and missing values. This validation step is crucial to maintaining the quality of our NLP model's input data and ensuring that the model's predictions are reliable.

 ![image](https://github.com/Saravanan-Arumugam3/Advanced-NLP-Based-Amazon-Reviews-Analytics/assets/46652703/443829eb-5f3c-4734-b27d-887189c6d483)

![image](https://github.com/Saravanan-Arumugam3/Advanced-NLP-Based-Amazon-Reviews-Analytics/assets/46652703/c128cbef-c213-473b-9837-ceacc4f50b18)

As part of the data validation process, TFDV also checks for inconsistencies between the data's schema and the data served to the model during training and serving. This helps to avoid training/serving skew, a common issue where the model performs well on training data but poorly on production data due to differences between the datasets.
The Airflow and TFDV integrations are both parts of our commitment to building a robust and scalable analytics system. They allow us to handle large volumes of review data efficiently and to maintain the quality and performance of our predictive models over time.

![image](https://github.com/Saravanan-Arumugam3/Advanced-NLP-Based-Amazon-Reviews-Analytics/assets/46652703/aec012a0-b5ea-4729-a8d7-6b763b266f54)
![image](https://github.com/Saravanan-Arumugam3/Advanced-NLP-Based-Amazon-Reviews-Analytics/assets/46652703/c358bce1-ffa6-4084-b264-c7edfbd647e6)

# Data Version Control (DVC)

In our Amazon Review Analytics project, Data Version Control (DVC) plays a pivotal role in managing and versioning the datasets that feed into our NLP models. DVC is an open-source version control system designed specifically for machine learning projects, enabling us to handle large data files and models alongside code. It integrates seamlessly with Git, allowing for easy tracking of data and model versions in conjunction with our source code.

![image](https://github.com/Saravanan-Arumugam3/Advanced-NLP-Based-Amazon-Reviews-Analytics/assets/46652703/e78b0a5a-d207-48a3-b19c-8c59e0b9e81f)

## Implementation :

Storage Configuration: We utilize GCP's Cloud Storage as a remote repository for DVC. This allows us to store large datasets and models externally, while DVC maintains references to these files in the project's Git repository.

Pipeline Definition: DVC pipelines are defined to automate data preprocessing, model training, evaluation, and other stages of our machine learning workflow.

Data Transfer: DVC commands facilitate the transfer of datasets between local environments and cloud storage, ensuring that team members work with the correct version of the data.
Version Management: We leverage DVC's versioning capabilities to track and switch between different versions of datasets and models, corresponding to the various experiment iterations conducted during the project.

Performance Monitoring: DVC provides tools to monitor the performance of different data and model versions, helping us to identify the most effective configurations.

![image](https://github.com/Saravanan-Arumugam3/Advanced-NLP-Based-Amazon-Reviews-Analytics/assets/46652703/c812ebb5-19b4-49e5-9afe-ce5dfc2bfab3)

By incorporating DVC into our workflow, we ensure a robust mechanism for data and model versioning, which enhances our project's integrity and reproducibility. DVC serves as an essential tool in our data pipeline, bringing high levels of efficiency and collaboration to our data science practice.

# GCP Data Storage

The project leverages Google Cloud Platform's (GCP) robust data storage solutions to manage large and growing datasets. GCP provides highly scalable, secure, and flexible storage options that cater to the requirements of NLP data processing and analytics workloads.

![image](https://github.com/Saravanan-Arumugam3/Advanced-NLP-Based-Amazon-Reviews-Analytics/assets/46652703/9ae785af-5a6c-40f2-9e79-2138c8e27472)

Google Cloud Storage (GCS):

We use Google Cloud Storage for storing source files and CSV files. GCS used to store large volumes of raw Amazon reviews. GCS offers object storage with global edge-caching for fast access to data. The reviews are stored in organized buckets, with versioning enabled to keep track of changes and maintain data integrity. We are also using GCS to store versions of the processed data.

## Deployment Strategy
Our deployment strategy for the Amazon Reviews Analytics system is built around robust MLOps practices to ensure seamless transition from model training to production. We employ tools such as MLflow for model lifecycle management, TensorFlow Extended (TFX) for end-to-end machine learning pipelines, Vertex AI for model training and serving, and Docker for containerization.

### MLflow Integration
MLflow is utilized for tracking experiments, packaging code into reproducible runs, and managing and storing models. It aids in the systematic tracking of parameters, code versions, metrics, and artifacts across our experiments, leading to a structured approach in model development.

### TensorFlow Extended (TFX)
TensorFlow Extended (TFX) is our choice for a production-grade machine learning platform, which provides a framework to deploy robust, scalable, and production-ready ML pipelines. It includes TensorFlow Data Validation, TensorFlow Transform, and TensorFlow Model Analysis, providing a suite of tools from data preprocessing to post-training model evaluation.

### Vertex AI and Model Serving
We use Google Cloud's Vertex AI to train our machine learning models at scale. Vertex AI offers a managed environment that simplifies the process of training and serving models. For serving, Vertex AI provides a scalable solution that automatically adjusts resources to match demand, ensuring efficient and reliable model predictions.

### Docker and Containerization
The deployment is streamlined using Docker, where each service of our application is containerized, allowing for isolated environments and easy scalability. Docker ensures that our application can run consistently across different environments and simplifies dependencies management.

## Model Training and Selection
During the model training phase, two models were developed:

1. BERT-based Sentiment Analysis Model (`bert_sentiment_model.pth`)
2. LSTM-based Sentiment Analysis Model (`model.h5`)

The LSTM-based model showed superior performance in capturing the context and nuances of customer sentiment and was thus chosen for production deployment.

### Deployment Details
The deployed LSTM model (`model.h5`) is packaged into a Docker container along with a Flask web application that acts as a REST API endpoint. This API facilitates interaction with the model by external services and applications.

The deployment process includes:

1. Building a Docker image with the Flask application and LSTM model.
2. Pushing the Docker image to a container registry (GCP Container Registry).
3. Deploying the containerized application onto Google Cloud Run, which is a serverless platform that automatically scales up or down based on the incoming request load.

### Continuous Integration and Deployment (CI/CD)
CI/CD pipelines are configured to automate the testing, building, and deployment of our application. Upon a new commit to the main branch in the GitHub repository, the CI pipeline runs tests to validate the integrity of the code. Post successful tests, the CD pipeline automates the deployment of the application to the production environment.

### Monitoring and Observability
The production environment is closely monitored using Google Cloud Operations (formerly Stackdriver) which provides powerful logging, monitoring, and diagnostics. This enables us to track the model's performance, understand usage patterns, and quickly identify and rectify any potential issues.

## Conclusion
The integration of MLflow, TensorFlow, Vertex AI, and Docker encapsulates our commitment to robust MLOps practices. With a single LSTM-based model deployed out of the two trained, we maintain a focus on quality and performance, ensuring that the Advanced NLP-Based Amazon Reviews Analytics system remains at the forefront of sentiment analysis applications.
