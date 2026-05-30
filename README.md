# realtime-clickstream-lakehouse-pipeline
Built a real-time retail clickstream lakehouse pipeline using Kafka, Spark Structured Streaming, and PySpark, implementing Bronze/Silver/Gold architecture, data quality validations, deduplication, partitioning, and product-level analytics metrics.

# Real-Time Retail Clickstream Lakehouse Pipeline

## Project Overview

This project demonstrates a production-style real-time data engineering pipeline for retail clickstream analytics.

The pipeline ingests clickstream events from Kafka, processes them using Spark Structured Streaming, stores raw data in a Bronze layer, cleans and validates data in a Silver layer, and creates business-ready Gold aggregates for analytics and reporting.

## Architecture

Kafka Producer → Spark Structured Streaming → Bronze Layer → Silver Layer → Gold Layer → BI/Analytics

## Key Features

- Real-time Kafka event ingestion
- Spark Structured Streaming pipeline
- Bronze, Silver, Gold lakehouse architecture
- Duplicate handling
- Null validation
- Event type validation
- Partitioned Parquet storage
- Product-level revenue and engagement metrics
- Unit tests for data quality framework
- Production-style folder structure

## Tech Stack

- Python
- PySpark
- Spark Structured Streaming
- Kafka
- Parquet
- Data Quality Framework
- Docker
- Pytest

## Business Use Case

Retail companies need to understand customer behavior across digital channels. This project captures page views, product views, cart events, and purchases to generate product-level metrics such as revenue, unique users, sessions, and engagement.

## How to Run

### Start Kafka

```bash
docker-compose up -d
