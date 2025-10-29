# CSV Processor - WebAPI Service

A CSV processing system with microservices architecture using Docker. The system consists of 2 instances of the same WebAPI service, connected to RabbitMQ, MinIO S3, and PostgreSQL.


## Phân tích luồng sau khi thay đổi

1. **Actor** gửi yêu cầu export/import CSV tới **WebAPI** (2 instance, cùng source code).

2. **Import:**
  - WebAPI nhận file CSV, upload lên **minioS3** (giả lập S3).
  - WebAPI gửi message vào **RabbitMQ queue**.

3. **Export:**
  - WebAPI lấy dữ liệu từ **PostgreSQL**, xuất ra file CSV, upload lên **minioS3**, trả về SAS URL.

4. **RabbitMQ queue:**
  - 2 instance WebAPI (hoặc worker) lắng nghe queue, nhận message, tải file CSV từ minioS3, ghi dữ liệu vào **PostgreSQL**.

5. **Timer job** (chạy mỗi 5 phút):
  - Đọc các thay đổi mới trong PostgreSQL.
  - Gửi message vào **RabbitMQ topic**.

6. **RabbitMQ topic:**
  - 2 instance WebAPI lắng nghe, nhận message, ghi log thông báo.

### Tóm tắt

- WebAPI xử lý import/export CSV, giao tiếp với minioS3, PostgreSQL, RabbitMQ.
- Timer job thay cho CosmosChangeFeedTrigger, kiểm tra thay đổi định kỳ.
- 2 instance WebAPI chạy độc lập, cùng source code, chia sẻ queue/topic.

## Requirements

- Docker & Docker Compose
- .NET 8.0 SDK (if running without Docker)
- PowerShell 5.1+ (Windows) or Bash (Linux/Mac)

## Installation & Running

### 1. Run with Docker Compose (Recommended)

```bash
# Clone/navigate to source code
cd c:\Users\PhucTG1\Desktop\projects\CSVProssessor

# Build and start services
docker-compose up --build

# Or run in background
docker-compose up -d --build
```

### 2. Services will be started

| Service | URL | Information |
|---------|-----|-------------|
| WebAPI Instance 1 | http://localhost:5001 | Swagger: http://localhost:5001/swagger |
| WebAPI Instance 2 | http://localhost:5002 | Swagger: http://localhost:5002/swagger |
| RabbitMQ Management | http://localhost:15672 | user: `guest` / pass: `guest` |
| MinIO Console | http://localhost:9001 | user: `minioadmin` / pass: `minioadmin` |
| PostgreSQL | localhost:5432 | user: `postgres` / pass: `postgres` |

## API Endpoints

### Export CSV
```http
POST /api/csv-export
Content-Type: application/json

{
  "query": "SELECT * FROM todos WHERE status = 'pending'"
}

Response: 200 OK
{
  "fileUrl": "https://minio:9000/csv-processor/export-2024-10-29.csv",
  "status": "exported",
  "timestamp": "2024-10-29T10:30:00Z"
}
```

### Import CSV
```http
POST /api/csv-import
Content-Type: multipart/form-data

file: (select CSV file)

Response: 202 Accepted
{
  "jobId": "uuid-string",
  "status": "processing",
  "message": "CSV import job started"
}
```

### Health Check
```http
GET /api/health

Response: 200 OK
{
  "status": "healthy",
  "database": "connected",
  "rabbitmq": "connected",
  "minio": "connected"
}
```

## Configuration

### appsettings.json

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    }
  },
  "ConnectionStrings": {
    "DefaultConnection": "Host=postgres;Port=5432;Database=csvprocessor;Username=postgres;Password=postgres"
  },
  "RabbitMq": {
    "HostName": "rabbitmq",
    "Port": 5672,
    "UserName": "guest",
    "Password": "guest",
    "VirtualHost": "/"
  },
  "MinIO": {
    "Endpoint": "minio:9000",
    "AccessKey": "minioadmin",
    "SecretKey": "minioadmin",
    "BucketName": "csv-processor",
    "UseSSL": false
  },
  "InstanceName": "WebAPI-1"
}
```

## Docker Services

### docker-compose.yml

Main services:
- **rabbitmq**: Message broker (port 5672, management 15672)
- **postgres**: Database (port 5432)
- **minio**: S3-compatible storage (port 9000, console 9001)
- **webapi-1**: First instance (port 5001)
- **webapi-2**: Second instance (port 5002)

Both WebAPI instances run the same source code but differ in:
- Environment variable `INSTANCE_NAME`: to distinguish logs
- Different ports: 5001 and 5002
- Same PostgreSQL database
- Same RabbitMQ broker



## Development

### Run local without Docker

```bash
# 1. Install dependencies
cd CSVProssessor.WebApi
dotnet restore

# 2. Migrate database
dotnet ef database update

# 3. Run
dotnet run --launch-profile https
```

### Build Docker image manually

```bash
cd CSVProssessor.WebApi
docker build -t csvprocessor:latest .
```

### Logs

View service logs:
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f webapi-1
docker-compose logs -f rabbitmq
docker-compose logs -f postgres
docker-compose logs -f minio
```

## Troubleshooting

### PostgreSQL Connection Error
```
Solution: Ensure postgres container has fully started
docker-compose up postgres -d
# Wait 10 seconds
docker-compose up webapi-1 webapi-2
```

### RabbitMQ Connection Error
```
Solution: Check RabbitMQ logs
docker-compose logs rabbitmq
```

### MinIO Connection Error
```
Solution: Create bucket first
docker exec csvprocessor-minio-1 mc mb minio/csv-processor
```

### Port already in use
```
Solution: Change port in docker-compose.yml or stop other services
docker-compose down
```

## Environment Variables

Can be overridden via .env file or docker-compose.override.yml:

```env
DB_HOST=postgres
DB_PORT=5432
DB_NAME=csvprocessor
DB_USER=postgres
DB_PASSWORD=postgres

RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest

MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=csv-processor

INSTANCE_NAME=WebAPI-1
```

## Performance Tuning

### RabbitMQ Prefetch
```csharp
model.BasicQos(0, 10, false); // Prefetch 10 messages
```

### Connection Pooling
```
PostgreSQL: pgbouncer integrated in driver
MinIO: Connection pooling via HttpClient
RabbitMQ: Built-in connection pooling
```

### Database Indexing
```sql
CREATE INDEX idx_todo_status ON todos(status);
CREATE INDEX idx_todo_created_at ON todos(created_at);
```

## Monitoring

### Health Check Endpoint
```bash
curl http://localhost:5001/api/health
```

### Metrics (if needed)
Can add:
- Application Insights
- Prometheus
- Grafana

## Testing

### Unit Tests
```bash
dotnet test
```

### Integration Tests (requires running services)
```bash
docker-compose up -d
dotnet test --filter "Integration"
```

### Load Testing
```bash
# Using Apache JMeter or k6
k6 run load-test.js
```

## CI/CD

Can integrate with:
- GitHub Actions
- Azure DevOps
- Jenkins

## Security

- RabbitMQ: Change default credentials
- PostgreSQL: Use strong password
- MinIO: Change default access keys
- API: Implement authentication/authorization
- HTTPS: Enable TLS in production

## Maintenance

### Backup Database
```bash
docker exec csvprocessor-postgres-1 pg_dump -U postgres csvprocessor > backup.sql
```

### Restore Database
```bash
docker exec -i csvprocessor-postgres-1 psql -U postgres csvprocessor < backup.sql
```

### Cleanup
```bash
# Remove all containers
docker-compose down

# Remove all volumes (data)
docker-compose down -v

# Remove images
docker image prune
```

## Contributing

1. Fork repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request

## License

MIT

## Support

For support, contact:
- Email: support@example.com
- Issues: GitHub Issues
- Documentation: Wiki

---

**Note**: This is a template README for CSV Processor project. Adjust configuration and endpoints according to your actual needs.
