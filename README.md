# Track man Interview Project

## Overview

This project demonstrates a simple concurrent data processing pipeline with three main stages:

1. **Data Acquisition**:  
   Retrieves live stock price data from Yahoo Finance using `yahoo_fin` and distributes work among multiple threads. The
   acquired data is then pushed into a shared queue for processing.

2. **Data Processing**:  
   Consumes raw data from the acquisition queue, processes it (e.g., converts prices from USD to DKK), and then sends
   the processed data into another queue for storage.

3. **Data Storage**:  
   Consumes processed data from the processing queue. For demonstration, the storage step simply prints the data. In a
   production environment, this could be replaced with logic to store data into a database, file, or another persistent
   store.

## Architecture

**Processes & Queues**:

- The pipeline is split into three **processes** (using `multiprocessing.Process`), one for each stage:
    - **Acquisition Process**
    - **Processing Process**
    - **Storage Process**

- Communication is handled by `multiprocessing.Queue` objects:
    - **Data Acquisition → Data Processing** via `data_process_queue`
    - **Data Processing → Data Storage** via `data_storage_queue`

**Concurrency**:

- **Data Acquisition** uses multiple **threads** to fetch multiple stock symbols concurrently.
- **Data Processing** uses a `ThreadPoolExecutor` to process items in parallel, adjusting `max_workers` to handle
  different workloads.
- **Data Storage** currently runs synchronously. as i couldn't run my docker demon to run it in a container env. 

## Data Flow

1. **Acquisition**:
    - Acquisition threads fetch stock data at regular intervals.
    - Data is pushed into `data_process_queue`.
    - After the defined runtime, a `None` value is inserted into the queue to signal the end of data.

2. **Processing**:
    - The processing process reads data from `data_process_queue`.
    - Each item is processed (e.g., adding a converted price) using a `ThreadPoolExecutor`.
    - When a `None` is received, no more raw data will arrive. Processing inserts `None` into `data_storage_queue` to
      signal completion.

3. **Storage**:
    - The storage process reads processed items from `data_storage_queue`.
    - Each received item is printed to stdout. Upon receiving `None`, it knows all data has been processed and the
      process can shut down.

## Logging

- Logging is configured to provide visibility into each stage.
- Logs help track data flow, the start/end of processes and threads, and handle exceptions.
- Ensure `LoggerConfig` sets up handlers (e.g., console and file) and that logs are initialized before processes start.

## Setup and Running

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
2. **Configuration**:

- Review and modify config.py to set symbol lists, or other parameters as needed.
- Ensure LoggerConfig points to your desired log file path and console output configuration.

3. **Run the Main App**:

```bash
   python main.py
```