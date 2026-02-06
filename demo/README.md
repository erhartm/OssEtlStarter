# Demo: ETL Pipeline with Docker Compose

This demo setup runs all necessary components to demonstrate the ETL pipeline in action using Docker and Docker Compose.

## Components
- SQL Server (with sample data)
- MongoDB (with sample data)
- Apache Spark (runs ETL jobs)
- Python client (reads gold-layer view)

## Usage
1. Place your ETL scripts in the `etl` folder (or symlink/copy from project root).
2. Place sample data and environment files in `demo-data` and `demo-env` as needed.
3. Run:
   ```bash
   docker-compose up --build
   ```
4. The Spark container will run all ETL jobs in sequence:
   - SQL Server to Delta
   - MongoDB to Delta
   - Bronze to Silver
5. The client container will read from the gold-layer view and print results.

## Customization
- Edit `demo-env` for environment variables (e.g., table names, paths).
- Add sample data to `demo-data` for SQL Server and MongoDB initialization.


## Persistent Data Folder
All persistent files (SQL Server, MongoDB, Hive metastore, Spark, Parquet) are stored under the `demo-persist` folder, with clean separation for each service.

### Example Directory Structure
```
demo/
   docker-compose.yml
   README.md
   demo-data/
      init.sql
      init.js
      mysql/
   demo-env/
      etl.env
      client.env
      README.md
   demo-persist/
      sqlserver/      # SQL Server data files
      mongodb/        # MongoDB data files
      mysql/          # MySQL (Hive metastore) data files
      spark/          # Spark persistent files
      hive/           # Hive warehouse files
      parquet/        # Parquet/Delta files
```

## Notes
- This setup is for demonstration only. For production, use proper orchestration and persistent storage.
- You may need to adjust ETL scripts for local paths and Docker networking.
