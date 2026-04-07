Solar Flux Real-Time Monitoring Pipeline: Documentation

This document outlines the architecture and implementation of a real-time data engineering pipeline designed to monitor solar X-ray flux and classify solar flare events.
1. System Architecture

The pipeline follows a standard distributed data processing model:

    Data Ingestion: A Python-based Kafka Producer fetching data from the NOAA SWPC API.

    Stream Processing: PySpark Structured Streaming for schema enforcement and real-time classification.

    Persistence: InfluxDB for time-series data storage.

    Visualization: FastAPI backend serving data to a real-time monitoring dashboard.

2. Implementation Details
2.1. Kafka Configuration

To ensure stability in a local environment and handle potential network latency, the following Spark-Kafka parameters are utilized:

    startingOffsets: Set to latest to focus on real-time data.

    failOnDataLoss: Set to false to prevent query termination during intermittent Kafka connectivity issues.

    kafka.request.timeout.ms: Increased to 120000 to accommodate processing delays.

2.2. Flare Classification Logic

Data is categorized based on peak flux intensity (measured in W/m2):

    X-Class: ≥1.0×10−4 (Significant impact; potential for global radio blackouts).

    M-Class: ≥1.0×10−5

    C-Class: ≥1.0×10−6

    Normal (A/B): <1.0×10−6

3. Operational Troubleshooting
3.1. Data Collision Management

A primary challenge involved multiple records appearing at the same timestamp (e.g., baseline data and manual test data).

    Resolution: The retrieval logic in the API was updated from last() to max() within a 1-minute window. This ensures that if an X-Class reading and a Normal reading exist at the same timestamp, the high-priority alert is displayed.

3.2. Checkpoint Maintenance

PySpark utilizes a checkpoint directory (C:\spark_checkpoints) to maintain state. In the event of an OffsetOutOfRangeException or TimeoutException during development:

    Stop the Spark Consumer.

    Delete the checkpoint directory.

    Restart the consumer to re-establish the stream from the latest Kafka offset.

4. Setup Instructions

    Initialize Infrastructure: Ensure Kafka and InfluxDB services are active.

    Execute Producer: Run producer.py to begin data ingestion.

    Execute Consumer: Run consumer.py to process and store flux data.

    Execute API: Initialize the FastAPI server to provide the data source for the dashboard.

Would you like to proceed with implementing a logic branch in the Spark consumer to trigger an external alert system when X-Class thresholds are reached?
