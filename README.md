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

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Business Value & Impact](#-business-value--impact)
- [Market Problem & Target Audience](#-market-problem--target-audience)
- [Pipeline Architecture](#-pipeline-architecture)
- [Technical Deep Dive: Medallion Architecture](#-technical-deep-dive-the-medallion-architecture)
- [AI Assistant (RAG) Implementation](#-ai-assistant-rag-implementation)
- [Technology Stack](#-technology-stack--system-components)
- [Project Structure](#-project-structure)
- [Getting Started](#-getting-started)
- [Usage Guide](#-usage-guide)
- [Visualizations & Dashboards](#-visualizations--dashboards)

---

## ✨ Project Overview

This project delivers a **production-ready, end-to-end data platform** designed to revolutionize the Egyptian real estate market. By implementing a robust data engineering pipeline and advanced machine learning models, we transform raw, fragmented property listings into **actionable market intelligence**, culminating in a personalized property recommendation engine and an intelligent AI assistant.

**Key Features:**

- **Medallion Architecture:** Ensures data quality and reliability across Bronze, Silver, and Gold layers
- **Scalable Processing:** Leverages Apache Spark and Hadoop for distributed data transformation
- **AI-Powered Insights:** Includes a K-Nearest Neighbors (KNN) recommender and an **Ollama RAG Chatbot** for expert advice
- **Full Automation:** Orchestrated by Apache Airflow and containerized with Docker

---

## 📈 Business Value & Impact

This platform addresses key challenges in the Egyptian real estate market by enabling:

- 📊 Transparent market analysis for buyers and renters
- 🔍 Accurate region-based price discovery
- 🧭 AI-powered recommendations for property matching
- 💼 Investor-ready intelligence reports
- 🏗 Planning insights for developers and policy makers
- 🤖 Natural language access to data using AI

---

## 🎯 Market Problem & Target Audience

### Problem Statement

The Egyptian real estate sector, despite its rapid growth, suffers from a significant **lack of market transparency**. Key challenges include:

- **Limited Price Visibility:** Difficulty for buyers and investors to determine fair market value due to significant regional price variations and a lack of structured data
- **Absence of Structured Insights:** Stakeholders lack reliable, analytical insights to inform strategic decisions
- **Inefficient Decision-Making:** Buyers struggle to find properties matching their specific needs and budget, while investors lack the market intelligence required for optimal portfolio management

### Target Audience

The insights and tools developed by this project are designed to serve three primary groups:

| Audience | Benefit |
|----------|---------|
| **Home Buyers** | Receive personalized property recommendations and market context to make informed purchasing decisions |
| **Real Estate Investors** | Gain reliable market intelligence, identify "hotspot" and "coldspot" regions, and optimize investment strategies |
| **Policymakers & Developers** | Access structured data and analytical dashboards to inform housing policies and development strategies |

---

## 🚀 Pipeline Architecture

The entire system is built around a scalable, automated data pipeline, orchestrated by **Apache Airflow** and containerized with **Docker**. The architecture follows the **Medallion Architecture** (Bronze, Silver, Gold layers) to ensure data quality and reliability at every stage.

<img width="2415" height="1113" alt="Pipeline Diagram" src="https://github.com/user-attachments/assets/0d884238-f718-435b-ac27-508cd318c2dd" />

### Data Orchestration with Airflow

Apache Airflow manages:

- Scraping schedules
- Data processing jobs
- Spark transformations
- Warehouse refresh cycles
- Automated recovery

![Airflow DAG](https://github.com/user-attachments/assets/2f95ae5c-a0e5-4f5b-a4ee-fd0356635060)

---

## ⚙️ Technical Deep Dive: The Medallion Architecture

The data flows through three distinct layers, each adding value and refinement.

### 1. 🥉 Bronze Layer: Raw Ingestion

| Stage | Tools | Description |
|-------|-------|-------------|
| **Data Sources** | dubizzle, Property Finder, bayut, Fazwaz | Key real estate listing platforms in the Egyptian market |
| **Web Scraper** | BeautifulSoup, Playwright, Python | Automated scripts to navigate and extract property details, handling both static and dynamic content |
| **Raw Ingestion** | **Hadoop Data Lake (Parquet)** | Raw, unprocessed data is ingested directly into the **Bronze Layer**. This layer serves as the immutable historical record, stored in the efficient Parquet format |

### 2. 🥈 Silver Layer: Cleaning & Transformation

| Stage | Tools | Description |
|-------|-------|-------------|
| **Processing Engine** | **Apache Spark, Python** | Used for distributed processing to handle large volumes of data efficiently |
| **Transformation Steps** | Cleaning, Aggregation, Merging | **Cleaning** (null handling, duplicate removal), **Standardization** (unifying Location and deriving region and city to ensure accuracy of results), **Feature Engineering** (generating `price_per_sqm`), and **Merging** data from all sources into a unified schema |
| **Output** | Silver Layer | Contains clean, standardized, and ready-to-be-analyzed datasets |

### 3. 🥇 Gold Layer: Data Warehouse & Analytics

| Stage | Tools | Description |
|-------|-------|-------------|
| **Data Warehouse (DWH)** | **PostgreSQL** | A robust, open-source relational database used to store the final, high-value data |
| **Structure** | **One Big Table (OBT)** | All curated listings are loaded into a single, unified table to simplify querying and provide a single source of truth |
| **Data Marts** | Analysis Data Mart, ML Data Mart | Two specialized marts are created from the OBT: one optimized for **Power BI/Superset** reporting and one for the **Machine Learning Model** |
| **ML & BI** | KNN, Flask, Power BI, Superset | The final stage deploys the ML Recommendation Model via Flask and visualizes market trends through BI tools |

### One Big Table DWH:

<img width="2559" height="1396" alt="OBT Schema" src="https://github.com/user-attachments/assets/bb81652b-d4b1-4b7f-963c-218900c2b5fa" />

---

## 🧠 AI Assistant (RAG) Implementation

The project features an advanced **AI Assistant Chatbot** built on a **Retrieval-Augmented Generation (RAG)** framework to provide expert, data-driven advice.

### Architecture

- **Framework:** Flask application with a RAG pipeline
- **LLM:** **OLLAMA** (using `llama3`) for intent detection, SQL generation, and final response formatting
- **Retrieval:** **FAISS** index and **Sentence Transformers** (all-MiniLM-L6-v2) are used to retrieve the most relevant property listings based on the user's natural language query
- **Functionality:** The assistant can handle two main intents:
    1. **Recommendation:** Uses RAG to suggest properties based on semantic similarity
    2. **Analytical:** Generates and executes a safe **PostgreSQL SELECT query** against the OBT to answer complex analytical questions (e.g., "What is the average price per square meter in Giza?")

### Logic Flow

<img width="1784" height="911" alt="RAG Logic" src="https://github.com/user-attachments/assets/3271e3cc-3400-4aec-8ace-92009a00eda5" />

### What Makes It Powerful

- Answers with **your private data**
- SQL-safe query generation
- Intent classification
- Hallucination prevention logic
- Recommendation + reporting fusion

Unlike generic chatbots, this AI assistant understands **your warehouse schema**, not just language.

---

## 🛠️ Technology Stack & System Components

This project is built on a modern, open-source data stack.

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Orchestration** | **Apache Airflow** | Manages the End-to-End ETL process via a Directed Acyclic Graph (DAG) |
| **Distributed Processing** | **Apache Spark** | High-performance data cleaning and transformation in the Silver Layer |
| **Data Lake** | **Hadoop (HDFS)** | Stores raw (Bronze) and processed (Silver) data in Parquet format |
| **Data Warehouse** | **PostgreSQL** | Stores the final, curated Gold Layer data (OBT and Data Marts) |
| **Web Scraping** | **Python, Playwright, BeautifulSoup** | Automated data extraction from real estate portals |
| **AI/RAG** | **Flask, Ollama, FAISS, Sentence Transformers** | Deploys the AI Assistant and Recommendation Model |
| **BI & Reporting** | **Power BI, Apache Superset** | Interactive dashboards for market trend visualization |
| **Containerization** | **Docker, Docker Compose** | Ensures consistent, reproducible environments for all services |

---

## 📁 Project Structure

```
NHA-017/
│
├── dags/                               # Airflow DAGs for pipeline orchestration
│   └── main_pipeline_dag.py            # Complete ETL workflow orchestration
│
├── shared/                             # Shared volume - All processing scripts
│   │
│   ├── scraping_scripts/               # 🔵 Stage 1: Web Scraping (Bronze Layer)
│   │   ├── scrape_propertyfinder.py    # PropertyFinder scraper
│   │   ├── scrape_bayut.py             # Bayut scraper
│   │   ├── scrape_dubizzle.py          # Dubizzle scraper
│   │   └── scrape_fazwaz.py            # Fazwaz scraper
│   │
│   ├── Cleaning_Layer_Pyspark/         # 🟢 Stage 2: Data Cleaning (Silver Layer)
│   │   ├── clean_propertyfinder.py     # Clean PropertyFinder data
│   │   ├── clean_bayut.py              # Clean Bayut data
│   │   ├── clean_dubizzle.py           # Clean Dubizzle data
│   │   └── clean_fazwaz.py             # Clean Fazwaz data
│   │
│   ├── Transformation_Layer_Pyspark/   # 🟡 Stage 3: Feature Engineering (Silver Layer)
│   │   ├── transform_propertyfinder.py # Transform PropertyFinder data
│   │   ├── transform_bayut.py          # Transform Bayut data
│   │   ├── transform_dubizzle.py       # Transform Dubizzle data
│   │   └── transform_fazwaz.py         # Transform Fazwaz data
│   │
│   ├── ingest_to_bronze.py             # Ingest scraped data to HDFS Bronze layer
│   ├── load_to_dwh.py                  # Load processed data to PostgreSQL
│   ├── create_dwh_table.py             # Create One Big Table (OBT) in DWH
│   ├── create_datamarts.py             # Create specialized data marts
│   ├── populate_datamarts.py           # Populate data marts from OBT
│   │
│   ├── data_csv_files/                 # Intermediate CSV files
│   ├── __pycache__/                    # Python cache files
│   └── datalake-hdfs-commands.ipynb    # HDFS setup and management
│
├── postgres_data/                      # Persistent PostgreSQL data volume
│
├── logs/                               # Airflow logs and execution history
│
├── datasets_analysis_reports/          # EDA notebooks and analysis reports
│
├── ML_model/                           # Machine Learning components (Optional)
│   ├── knn_recommender.py              # KNN recommendation model
│   └── train_model.py                  # Model training script
│
├── RAG/                                # AI Assistant components (Optional)
│   ├── app.py                          # Flask application for AI Assistant
│   ├── ingest_embeddings.py            # Build FAISS index
│   └── faiss_index.idx                 # FAISS vector index
│
├── docker-compose.yaml                 # Multi-service orchestration
├── airflow.Dockerfile                  # Custom Airflow image
├── jupyter.Dockerfile                  # Custom PySpark/Jupyter image
├── PBI_Analysis_Dashboard.pbix         # Power BI Report file
└── README.md                           # This file
```

### Pipeline Execution Flow

The Airflow DAG executes tasks in the following order:

1. **Scraping Scripts** → Extract data from 4 real estate platforms
2. **Ingest to Bronze** → Store raw data in HDFS Bronze layer
3. **Cleaning Layer** → Clean and standardize data (Silver layer)
4. **Transformation Layer** → Feature engineering and enrichment
5. **Create DWH Table** → Set up One Big Table schema
6. **Load to DWH** → Load processed data to PostgreSQL
7. **Create & Populate Data Marts** → Build specialized marts for BI and ML

---

## 🚀 Getting Started

### Prerequisites

Ensure you have the following installed on your system:

- **Docker** (version 20.10+)
- **Docker Compose** (version 1.29+)
- **Git**
- At least **16GB RAM** and **50GB free disk space** for optimal performance

### Installation Steps

#### 1. Clone the Repository

```bash
git clone https://github.com/nhahub/NHA-017.git
cd NHA-017
```

#### 2. Build Custom Docker Images

The project uses custom Docker images for the environment.

**Build Jupyter Image (PySpark + Data Science tools):**

```bash
docker compose build jupyter
```

**Build Airflow and all other services:**

```bash
docker compose build
```

#### 3. Start All Services

Use Docker Compose to spin up the entire stack:

```bash
docker compose up -d
```

This command will start:
- Apache Airflow (Webserver, Scheduler, Workers)
- Apache Spark (Master + Workers)
- Hadoop HDFS (NameNode + DataNode)
- PostgreSQL (Data Warehouse)
- Jupyter Notebook (PySpark environment)

**Wait 2-3 minutes** for all services to initialize.

#### 4. Verify Services

Check that all containers are running:

```bash
docker compose ps
```

You should see all services in the "Up" state.

#### 5. Access Airflow

Navigate to the Airflow UI:

```
http://localhost:8082
```

**Login credentials:**
- Username: `airflow`
- Password: `airflow`

#### 6. Run the Pipeline

Once logged into Airflow:
1. Locate the real_estate_etl pipeline DAG in the DAGs list
2. Enable the DAG by toggling the switch on the left
3. Click the "Play" button to trigger the pipeline manually
4. Monitor execution in the Graph or Grid view

The pipeline will automatically:
- Scrape data from 4 real estate platforms
- Process through Bronze → Silver → Gold layers
- Load the final data into PostgreSQL Data Warehouse

---

## 📊 Usage Guide

### Running the Data Pipeline

The entire pipeline is automated through Airflow. Simply:

1. **Access Airflow UI:** Navigate to `http://localhost:8082`
2. **Login:** Use username `airflow` and password `airflow`
3. **Enable DAG:** Toggle the switch next to real_estate_etl pipeline DAG
4. **Trigger Pipeline:** Click the "Play" button to start execution
5. **Monitor Progress:** Watch tasks execute in real-time through the Graph or Grid view

The pipeline automatically handles:
- Web scraping from 4 platforms (PropertyFinder, Bayut, Dubizzle, Fazwaz)
- Data ingestion to Bronze layer (HDFS)
- Cleaning and transformation (Silver layer)
- Loading to PostgreSQL Data Warehouse (Gold layer)
- Creating and populating data marts

### Building Dashboards

**Power BI:**
1. Open `PBI_Analysis_Dashboard.pbix`
2. Update the data source connection to your PostgreSQL instance
3. Refresh data to see live insights from your data warehouse

**Apache Superset (if configured):**
1. Connect to PostgreSQL warehouse
2. Use the `analysis_mart` table for optimized BI queries
3. Create custom dashboards and visualizations

---

## 📈 Visualizations & Dashboards

### Power BI Reports

<img width="2129" height="1199" alt="Power BI Dashboard 1" src="https://github.com/user-attachments/assets/bccdb3d3-e558-4f4e-a2a2-bd65dd9ad459" />

<img width="2444" height="1310" alt="Power BI Dashboard 2" src="https://github.com/user-attachments/assets/34493a31-caff-45b4-9b83-19d8a4bb7ceb" />

<img width="2436" height="1314" alt="Power BI Dashboard 3" src="https://github.com/user-attachments/assets/834b0d62-8cf0-4c3e-be1d-a984446a5011" />

### Machine Learning Recommendation System

<img width="975" height="467" alt="ML Recommender 1" src="https://github.com/user-attachments/assets/ee9ef158-b689-4ed0-bd2b-72fc4ed2c9de" />

<img width="975" height="463" alt="ML Recommender 2" src="https://github.com/user-attachments/assets/9816909e-de00-4893-9654-5cce9501be89" />

<img width="975" height="467" alt="ML Recommender 3" src="https://github.com/user-attachments/assets/0f561118-639f-4432-92ca-b7d4403d53c1" />

<img width="975" height="416" alt="ML Recommender 4" src="https://github.com/user-attachments/assets/443550b6-32c2-4cdc-9171-80b655d997da" />

### Superset Visualizations

<img width="975" height="363" alt="Superset Dashboard 1" src="https://github.com/user-attachments/assets/397e7f93-2de6-41f9-bb04-f14b1f0a7a3e" />

<img width="975" height="359" alt="Superset Dashboard 2" src="https://github.com/user-attachments/assets/0366bc28-7e01-4377-8ad1-bd4829d1543e" />

<img width="975" height="324" alt="Superset Dashboard 3" src="https://github.com/user-attachments/assets/c2c5b935-0367-48d0-a9db-a8eebd0fd7ed" />

### AI Assistant Chatbot

<img width="2559" height="429" alt="AI Chatbot 1" src="https://github.com/user-attachments/assets/d657bf34-2cc9-4c4d-92d5-3304ad213b03" />

<img width="2559" height="858" alt="AI Chatbot 2" src="https://github.com/user-attachments/assets/53f69c6a-fa4e-46c5-9f43-ae8d29a6a2c0" />

<img width="2559" height="1461" alt="AI Chatbot 3" src="https://github.com/user-attachments/assets/b74ef525-5fca-42be-823a-27af98bc1578" />

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request
---

<div align="center">

**⭐ Star this repo if you find it useful!**

Made with ❤️ for the Egyptian real estate market

</div>
