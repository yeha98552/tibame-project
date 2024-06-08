# Taiwan Travel Attractions Analysis Data Pipeline

## Overview
This project develops a comprehensive data pipeline to analyze and visualize key metrics about travel attractions in Taiwan. Utilizing data processing technologies and cloud infrastructure, it incorporates data extracted from Google Maps reviews. This integration aims to provide valuable insights for tourists and businesses interested in the Taiwanese tourism sector.

## Prerequisites
Before running this project, you must have the following installed:

- Terraform (v1.8.5 or later)
- Docker (v26.1.4 or later)

## Installation
### Setup Terraform
Refer to the detailed instructions in [Terraform README](./terraform/README.md) for setting up Terraform.

### Setup Airflow
For setting up Airflow, follow the steps provided in [Airflow README](./airflow/README.md).

## Tech Stack
Technologies used in this project

- **Google Cloud**: Provides the computing and storage resources, specifically using Google Cloud Storage, BigQuery and Cloud Functions.
- **Terraform**: Manages the infrastructure as code.
- **Airflow**: Orchestrates and schedules the data pipeline workflows.
- **Python**: Used for scripting and data manipulation tasks, with key libraries including:
  - **Pandas**: For data manipulation and analysis.
  - **PyArrow**: For efficient data storage and retrieval.
  - **SQLAlchemy**: For database interaction.
  - **Psycopg2-binary**: For PostgreSQL database connectivity.
  - **jieba**: For Chinese text segmentation.
  - **SnowNLP**: For sentiment analysis of Chinese text.

## Project Structure

```javascript
.
├── .git                         // Folder for Git version control system
├── .vscode                      // Visual Studio Code configuration folder
├── airflow
|   ├── config                   // Configuration files for Airflow settings and environment variables
|   ├── dags                     // Contains Directed Acyclic Graphs (DAGs) for Airflow to schedule and run tasks
|   ├── plugins                  // Custom plugins for extending Airflow's built-in functionalities (automatically generated)
|   ├── logs                     // Logs generated by Airflow during DAG execution (automatically generated)
|   ├── utils
|   |   ├── common.py            // Contains common utility functions used across different modules
|   |   ├── gcp.py               // Google Cloud Platform specific utilities, including BigQuery and GCS operations
|   |   ├── email_callback.py    // Functions to handle email notifications on task failures or retries
|   |   └── config.yml           // Configuration file that stores environment and service settings
|   ├── variables                // Stores variables and configurations used across different Airflow tasks
|   ├── gcp_keyfile.json         // Google Cloud Platform service account key for Airflow
|   ├── crawler_gcp_keyfile.json // Additional GCP service account key for specific crawling tasks
|   ├── .env                     // Environment variables for Airflow setup
|   ├── .env.example             // .env template file, storing sample environment variables
|   ├── airflow.Dockerfile       // Dockerfile to build Airflow container image
|   ├── .dockerignore            // Specifies files to ignore during Docker build process
|   ├── docker-compose.yml       // Docker compose file to run Airflow in containers
|   ├── .gitignore               // Git ignore configuration, specifying files that don't need version control
|   └── README.md                // Airflow description file
├── terraform
|   ├── src                      // Contains the source code for Cloud Functions
|   ├── generated                
|   ├── main.tf                  // Main Terraform configuration file
|   ├── output.tf                // Terraform output configuration
|   ├── variables.tf             // Terraform variables definition
|   ├── .terraform.lock.hcl      // Terraform lock file for dependencies version locking
|   ├── .gitignore               // Git ignore configuration, specifying files that don't need version control
|   └── README.md                // Terraform description file
├── .gitignore                   // Git ignore configuration, specifying files that don't need version control
└── README.md                    // Project description file
```
