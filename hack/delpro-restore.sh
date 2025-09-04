#!/bin/bash

# Script to create MSSQL Server 2019 container and restore DelPro.bak file

set -e

# Configuration
CONTAINER_NAME="delpro-sql"
SA_PASSWORD="DelPro123!" # temporary local-only password
BACKUP_FILE="DelPro.bak"
DATABASE_NAME="DDM"

echo "Creating MSSQL Server 2019 container for DelPro database..."

# Pull SQL Server 2019 container image
echo "Pulling SQL Server 2019 container image..."
docker pull mcr.microsoft.com/mssql/server:2019-latest

# Stop and remove existing container if it exists
echo "Removing existing container if present..."
docker stop $CONTAINER_NAME 2>/dev/null || true
docker rm $CONTAINER_NAME 2>/dev/null || true
docker volume rm delpro-data

# Run the container
echo "Starting SQL Server container..."
docker run -e 'ACCEPT_EULA=Y' -e "MSSQL_SA_PASSWORD=$SA_PASSWORD" \
   --name "$CONTAINER_NAME" -p 1433:1433 \
   -v delpro-data:/var/opt/mssql \
   -d mcr.microsoft.com/mssql/server:2019-latest

# Wait for SQL Server to start
echo "Waiting for SQL Server to start..."
sleep 10

# Create backup directory in container
echo "Creating backup directory..."
docker exec -it $CONTAINER_NAME mkdir -p /var/opt/mssql/backup

# Copy backup file to container (assumes DelPro.bak is in current directory)
if [ ! -f "$BACKUP_FILE" ]; then
    echo "Error: $BACKUP_FILE not found in current directory"
    exit 1
fi

echo "Copying backup file to container..."
docker cp "$BACKUP_FILE" "$CONTAINER_NAME:/var/opt/mssql/backup/"

# Get logical file names from backup
echo "Getting logical file names from backup..."
docker exec -it $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
   -S localhost -U sa -P "$SA_PASSWORD" -C \
   -Q "RESTORE FILELISTONLY FROM DISK = '/var/opt/mssql/backup/$BACKUP_FILE'"

# Restore database
echo "Restoring DelPro database..."
docker exec -it $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
   -S localhost -U sa -P "$SA_PASSWORD" -C \
   -Q "RESTORE DATABASE $DATABASE_NAME FROM DISK = '/var/opt/mssql/backup/$BACKUP_FILE' WITH REPLACE, 
       MOVE 'DDM' TO '/var/opt/mssql/data/DDM.mdf',
       MOVE 'DDM_Log' TO '/var/opt/mssql/data/DDM_Log.ldf'"

# Verify database restoration
echo "Verifying database restoration..."
docker exec -it $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
   -S localhost -U sa -P "$SA_PASSWORD" -C \
   -Q "SELECT name FROM sys.databases WHERE name = '$DATABASE_NAME'"

echo "DelPro database container setup complete!"
echo "Container name: $CONTAINER_NAME"
echo "SA password: $SA_PASSWORD"
echo "Port: 1433"
echo "Database: $DATABASE_NAME"
