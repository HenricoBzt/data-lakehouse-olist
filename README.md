# 🛒 Olist Data Lakehouse: End-to-End Modern Data Stack

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![AWS S3](https://img.shields.io/badge/Amazon%20S3-569A31?style=for-the-badge&logo=Amazon%20S3&logoColor=white)
![Apache Spark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=Apache%20Spark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00AEEF?style=for-the-badge&logo=Databricks&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)

## 📌 Sobre o Projeto
Este projeto constrói um **Data Lakehouse corporativo de ponta a ponta** utilizando o dataset público de e-commerce da Olist (disponível no Kaggle). O objetivo é demonstrar a extração, processamento, governança e modelagem dimensional de dados reais utilizando as melhores práticas de Engenharia de Dados na nuvem.

Toda a infraestrutura computacional foi desenvolvida em **Databricks (Serverless Compute)**, com armazenamento físico distribuído no **AWS S3** gerenciado pelo **Unity Catalog** via *External Tables*.

## 🏗️ Arquitetura Macro

O pipeline segue a **Arquitetura Medallion** (Bronze, Silver e Gold), garantindo qualidade, idempotência e escalabilidade.

```mermaid
flowchart TD
    Origem[Kaggle API] -->|Python Request| Ingestao(Databricks: Ingest Notebook)
    Ingestao -->|JSON/CSV| S3_Raw[(AWS S3: Raw)]

    S3_Raw --> Bronze_Compute(Databricks: Raw to Bronze)
    Bronze_Compute -->|Delta Table| S3_Bronze[(AWS S3: Bronze)]

    S3_Bronze --> Silver_Compute(Databricks: Bronze to Silver)
    Silver_Compute -->|Delta Table Upsert/MERGE| S3_Silver[(AWS S3: Silver)]

    S3_Silver --> Gold_Compute(Databricks: Silver to Gold)
    Gold_Compute -->|Star Schema| S3_Gold[(AWS S3: Gold)]

    S3_Gold -->|Databricks SQL / Unity Catalog| PBI[Power BI]
