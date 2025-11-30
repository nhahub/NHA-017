<div align="center">

# 🇪🇬 Egyptian Real Estate Intelligence Market Platform

## Market Analysis • Recommendation • AI Advisor

*A unified data platform for transparency & decision-making in Egyptian real estate through big data & AI.*

![Docker](https://img.shields.io/badge/Docker-Containerized-blue?logo=docker)
![Spark](https://img.shields.io/badge/Apache_Spark-Distributed-orange?logo=apachespark)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration-green?logo=apacheairflow)
![Postgres](https://img.shields.io/badge/PostgreSQL-DWH-blue?logo=postgresql)
![AI](https://img.shields.io/badge/AI-RAG_LLM-red)

</div>


## ✨ Project Overview

This project delivers a **production-ready, end-to-end data platform** designed to revolutionize the Egyptian real estate market. By implementing a robust data engineering pipeline and advanced machine learning models, we transform raw, fragmented property listings into **actionable market intelligence**, culminating in a personalized property recommendation engine and an intelligent AI assistant.

**Key Features:**

- **Medallion Architecture:** Ensures data quality and reliability across Bronze, Silver, and Gold layers.

- **Scalable Processing:** Leverages Apache Spark and Hadoop for distributed data transformation.

- **AI-Powered Insights:** Includes a K-Nearest Neighbors (KNN) recommender and an **Ollama RAG Chatbot** for expert advice.

- **Full Automation:** Orchestrated by Apache Airflow and containerized with Docker.

## 📈 Business Value & Impact

This platform addresses key challenges in the Egyptian real estate market by enabling:

- 📊 Transparent market analysis for buyers and renters
- 🔍 Accurate region-based price discovery
- 🧭 AI-powered recommendations for property matching
- 💼 Investor-ready intelligence reports
- 🏗 Planning insights for developers and policy makers
- 🤖 Natural language access to data using AI

---

## 🚀 Pipeline Architecture

The entire system is built around a scalable, automated data pipeline, orchestrated by **Apache Airflow** and containerized with **Docker**. The architecture follows the **Medallion Architecture** (Bronze, Silver, Gold layers) to ensure data quality and reliability at every stage.

The pipeline can be visualized as follows:

Pipeline Diagram<img width="2415" height="1113" alt="Screenshot 2025-11-30 012420" src="https://github.com/user-attachments/assets/0d884238-f718-435b-ac27-508cd318c2dd" />

---

## 🎯 Market Problem & Target Audience

### Problem Statement

The Egyptian real estate sector, despite its rapid growth, suffers from a significant **lack of market transparency**. Key challenges include:

- **Limited Price Visibility:** Difficulty for buyers and investors to determine fair market value due to significant regional price variations and a lack of structured data.

- **Absence of Structured Insights:** Stakeholders lack reliable, analytical insights to inform strategic decisions.

- **Inefficient Decision-Making:** Buyers struggle to find properties matching their specific needs and budget, while investors lack the market intelligence required for optimal portfolio management.

### Target Audience

The insights and tools developed by this project are designed to serve three primary groups:

| Audience | Benefit |
| --- | --- |
| **Home Buyers** | Receive personalized property recommendations and market context to make informed purchasing decisions. |
| **Real Estate Investors** | Gain reliable market intelligence, identify "hotspot" and "coldspot" regions, and optimize investment strategies. |
| **Policymakers & Developers** | Access structured data and analytical dashboards to inform housing policies and development strategies. |

---
## ⚙️ Data Orchestration with Airflow

Apache Airflow manages:

- Scraping schedules
- Data processing jobs
- Spark transformations
- Warehouse refresh cycles
- Automated recovery

![WhatsApp Image 2025-11-27 at 20 38 07_ea7b0321](https://github.com/user-attachments/assets/2f95ae5c-a0e5-4f5b-a4ee-fd0356635060)

## ⚙️ Technical Deep Dive: The Medallion Architecture

The data flows through three distinct layers, each adding value and refinement.

### 1. 🥉 Bronze Layer: Raw Ingestion

| Stage | Tools | Description |
| --- | --- | --- |
| **Data Sources** | dubizzle, Property Finder, bayut, Fazwaz | Key real estate listing platforms in the Egyptian market. |
| **Web Scraper** | BeautifulSoup, Playwright, Python | Automated scripts to navigate and extract property details, handling both static and dynamic content. |
| **Raw Ingestion** | **Hadoop Data Lake (Parquet)** | Raw, unprocessed data is ingested directly into the **Bronze Layer**. This layer serves as the immutable historical record, stored in the efficient Parquet format. |

### 2. 🥈 Silver Layer: Cleaning & Transformation

| Stage | Tools | Description |
| --- | --- | --- |
| **Processing Engine** | **Apache Spark, Python** | Used for distributed processing to handle large volumes of data efficiently. |
| **Transformation Steps** | Cleaning, Aggregation, Merging | **Cleaning** (null handling, duplicate removal), **Standardization** (unifying Location and deriving region and city to ensure accuracy of results), **Feature Engineering** (generating `price_per_sqm`), and **Merging** data from all sources into a unified schema. |
| **Output** | Silver Layer | Contains clean, standardized, and ready-to-be-analyzed datasets. |

### 3. 🥇 Gold Layer: Data Warehouse & Analytics

| Stage | Tools | Description |
| --- | --- | --- |
| **Data Warehouse (DWH)** | **PostgreSQL** | A robust, open-source relational database used to store the final, high-value data. |
| **Structure** | **One Big Table (OBT)** | All curated listings are loaded into a single, unified table to simplify querying and provide a single source of truth. |
| **Data Marts** | Analysis Data Mart, ML Data Mart | Two specialized marts are created from the OBT: one optimized for **Power BI/Superset** reporting and one for the **Machine Learning Model**. |
| **ML & BI** | KNN, Flask, Power BI, Superset | The final stage deploys the ML Recommendation Model via Flask and visualizes market trends through BI tools. |

---
### One Big Table DWH:
<img width="2559" height="1396" alt="Screenshot 2025-11-30 025514" src="https://github.com/user-attachments/assets/bb81652b-d4b1-4b7f-963c-218900c2b5fa" />




### Power BI Report
<img width="2129" height="1199" alt="Screenshot 2025-11-25 203437" src="https://github.com/user-attachments/assets/bccdb3d3-e558-4f4e-a2a2-bd65dd9ad459" />
.
<img width="2444" height="1310" alt="Screenshot 2025-11-25 203456" src="https://github.com/user-attachments/assets/34493a31-caff-45b4-9b83-19d8a4bb7ceb" />
.
<img width="2436" height="1314" alt="Screenshot 2025-11-25 203518" src="https://github.com/user-attachments/assets/834b0d62-8cf0-4c3e-be1d-a984446a5011" />

### Machine Learning Recommendation System
<img width="975" height="467" alt="image" src="https://github.com/user-attachments/assets/ee9ef158-b689-4ed0-bd2b-72fc4ed2c9de" />
.
<img width="975" height="463" alt="image" src="https://github.com/user-attachments/assets/9816909e-de00-4893-9654-5cce9501be89" />
.
<img width="975" height="467" alt="image" src="https://github.com/user-attachments/assets/0f561118-639f-4432-92ca-b7d4403d53c1" />
.
<img width="975" height="416" alt="image" src="https://github.com/user-attachments/assets/443550b6-32c2-4cdc-9171-80b655d997da" />

### Superset visualization

<img width="975" height="363" alt="image" src="https://github.com/user-attachments/assets/397e7f93-2de6-41f9-bb04-f14b1f0a7a3e" />
.
<img width="975" height="359" alt="image" src="https://github.com/user-attachments/assets/0366bc28-7e01-4377-8ad1-bd4829d1543e" />
.
<img width="975" height="324" alt="image" src="https://github.com/user-attachments/assets/c2c5b935-0367-48d0-a9db-a8eebd0fd7ed" />





## 🧠 AI Assistant (RAG) Implementation

The project features an advanced **AI Assistant Chatbot** built on a **Retrieval-Augmented Generation (RAG)** framework to provide expert, data-driven advice.

- **Framework:** Flask application with a RAG pipeline.

- **LLM:** **OLLAMA** (using `llama3`) for intent detection, SQL generation, and final response formatting.

- **Retrieval:** **FAISS** index and **Sentence Transformers** (all-MiniLM-L6-v2) are used to retrieve the most relevant property listings based on the user's natural language query.

- **Functionality:** The assistant can handle two main intents:
    1. **Recommendation:** Uses RAG to suggest properties based on semantic similarity.
    2. **Analytical:** Generates and executes a safe **PostgreSQL SELECT query** against the OBT to answer complex analytical questions (e.g., "What is the average price per square meter in Giza?").

---
### Logic
<img width="1784" height="911" alt="Screenshot 2025-11-23 164837" src="https://github.com/user-attachments/assets/3271e3cc-3400-4aec-8ace-92009a00eda5" />
### Ai assistant Chatbot
### What Makes It Powerful

- Answers with **your private data**
- SQL-safe query generation
- Intent classification
- Hallucination prevention logic
- Recommendation + reporting fusion

Unlike generic chatbots, this AI assistant understands **your warehouse schema**, not just language.

<img width="2559" height="429" alt="Screenshot 2025-11-26 004636" src="https://github.com/user-attachments/assets/d657bf34-2cc9-4c4d-92d5-3304ad213b03" />
.
<img width="2559" height="858" alt="Screenshot 2025-11-26 004616" src="https://github.com/user-attachments/assets/53f69c6a-fa4e-46c5-9f43-ae8d29a6a2c0" />
.
<img width="2559" height="1461" alt="Screenshot 2025-11-26 004558" src="https://github.com/user-attachments/assets/b74ef525-5fca-42be-823a-27af98bc1578" />

## 🛠️ Technology Stack & System Components

This project is built on a modern, open-source data stack.

| Component | Technology | Purpose |
| --- | --- | --- |
| **Orchestration** | **Apache Airflow** | Manages the End-to-End ETL process via a Directed Acyclic Graph (DAG). |
| **Distributed Processing** | **Apache Spark** | High-performance data cleaning and transformation in the Silver Layer. |
| **Data Lake** | **Hadoop (HDFS)** | Stores raw (Bronze) and processed (Silver) data in Parquet format. |
| **Data Warehouse** | **PostgreSQL** | Stores the final, curated Gold Layer data (OBT and Data Marts). |
| **Web Scraping** | **Python, Playwright, BeautifulSoup** | Automated data extraction from real estate portals. |
| **AI/RAG** | **Flask, Ollama, FAISS, Sentence Transformers** | Deploys the AI Assistant and Recommendation Model. |
| **BI & Reporting** | **Power BI, Apache Superset** | Interactive dashboards for market trend visualization. |
| **Containerization** | **Docker, Docker Compose** | Ensures consistent, reproducible environments for all services. |

---

## 📁 Project Structure

The repository is organized to clearly separate the data pipeline stages, models, and infrastructure components.

![Project Structure](pasted_file_LE0im4_image.png)

``` DEPI_GITHUB_REPO/
├── Cleaning_Layer_Pyspark/ # Spark scripts for Silver Layer
├── dags/# Airflow DAGs for orchestration
├── datasets_analysis_reports/# Reports and notebooks
├── logs/
├── Merging&Loading_Layer/ # Scripts for Gold Layer (OBT & Data Marts)
├── ML_model/ # KNN model code
├── postgres_data/# Persistent volume for PostgreSQL
├── RAG/ #AI Assistant components (faiss_index.idx, app.py) │
    ├── app.py # Flask application for AI Assistant │
    ├── faiss_index.idx # FAISS vector index │
    └── ingest_embeddings.py # Script to build the FAISS index
├── shared/ # Shared volume for data transfer (HDFS/Spark/Jupyter)
├── Transformation_Layer_Pyspark/ # Spark scripts for feature engineering
├── airflow.Dockerfile # Custom Dockerfile for Airflow
├── datalake-hdfs-commands.ipynb # Jupyter notebook for HDFS setup
├── docker-compose.yaml # Defines all services (Airflow, Spark, Postgres, etc.)
├── jupyter.Dockerfile # Custom Dockerfile for PySpark/Jupyter environment
└── PBI_Analysis_Dashboard.pbix # Power BI Report file ```

---



