# CSV Processor - Distributed CSV Processing System

A distributed CSV processing system built with **.NET 8.0**, featuring dual-instance WebAPI architecture, asynchronous processing, and event-driven communication using **RabbitMQ**, **MinIO S3**, and **PostgreSQL**.


## ✨ Key Features

### 📤 CSV Import (Asynchronous)
- Upload CSV file → Store in MinIO S3 with unique filename (timestamp + GUID)
- Publish message to **RabbitMQ Queue** (`csv-import-queue`)
- Background workers (both instances) compete to process queue messages
- Parse CSV → Store records as JSON in PostgreSQL
- Automatic duplicate filename prevention

### 📥 CSV Export
- **Export Single File**: Download specific CSV by filename
- **Export All Files**: Download all CSVs as ZIP archive
- **List All Files**: View metadata (filename, status, record count, upload time)
- Direct file streaming (no temp storage needed)

### 🔄 Background Processing
1. **Queue Listener** (`CsvImportQueueListenerService`)
   - Competing consumers pattern (only one instance processes each message)
   - Downloads CSV from MinIO → Parses → Saves to PostgreSQL
   - Auto-retry on failure (NACK + requeue)

2. **Change Detection** (`ChangeDetectionBackgroundService`)
   - Runs every **1 minute** (configurable)
   - Detects new/updated CSV records
   - Publishes notifications to **RabbitMQ Topic** (`csv-changes-topic`)
   - Logs changes to `Materials/Logs/csv-changes-log.txt`

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Backend** | ASP.NET Core Web API | 8.0 |
| **Database** | PostgreSQL | 15-alpine |
| **Message Broker** | RabbitMQ | 3.12-management |
| **Object Storage** | MinIO (S3-compatible) | latest |
| **ORM** | Entity Framework Core | 8.0 |
| **Containerization** | Docker + Docker Compose | - |
| **Architecture Pattern** | Repository + Unit of Work | - |

## 📋 Prerequisites

- **Docker Desktop** (Windows/Mac) or **Docker Engine** (Linux)
- **Docker Compose** v2.0+
- **.NET 8.0 SDK** (for local development only)
- **PowerShell** 5.1+ (Windows) or **Bash** (Linux/Mac)

## 🚀 Quick Start

### 1. Clone Repository
```bash
git clone <repository-url>
cd CSVProssessor.API
```

### 2. Start All Services
```bash
# Build and run in detached mode
docker-compose up -d --build

# View logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f api-1
docker-compose logs -f postgres
```

### 3. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **API Instance 1** | http://localhost:5001 | - |
| **API Instance 2** | http://localhost:5002 | - |
| **Swagger UI (api-1)** | http://localhost:5001/swagger | - |
| **Swagger UI (api-2)** | http://localhost:5002/swagger | - |
| **RabbitMQ Management** | http://localhost:15672 | `guest` / `guest` |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **PostgreSQL** | localhost:5433 | `postgres` / `postgres` |

## 📡 API Endpoints

### CSV Operations

| Method | Endpoint | Description | Request | Response |
|--------|----------|-------------|---------|----------|
| `POST` | `/api/csv/upload` | Upload CSV file (async) | `multipart/form-data` | `202 Accepted` + JobId |
| `GET` | `/api/csv/list` | List all uploaded CSVs | - | JSON (metadata) |
| `GET` | `/api/csv/export/{fileName}` | Download single CSV | `fileName` (path param) | CSV file |
| `GET` | `/api/csv/export` | Download all CSVs as ZIP | - | ZIP file |

### System Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| `DELETE` | `/api/system/database` | Clear all database records |

