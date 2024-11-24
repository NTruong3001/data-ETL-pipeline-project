from pyspark.sql import SparkSession

# Configuration for the database connection
SQL_USERNAME = 'sa'
SQL_PASSWORD = '123'
SQL_DBNAME = 'test'
SQL_SERVERNAME = 'localhost:1433'  # Default SQL Server port
TABLENAME = 'dbo.test'

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PySpark SQL Server Connection") \
    .config("spark.jars", "E:/de_thay LOng/lab/data ETL pipeline project/mssql-jdbc-12.8.1.jre11.jar") \
    .config("spark.local.dir", "E:/spark-temp") \
    .getOrCreate()

# JDBC URL
url = f"jdbc:sqlserver://{SQL_SERVERNAME};databaseName={SQL_DBNAME};encrypt=false"

# SQL Server properties
sqlserver_properties = {
    "user": SQL_USERNAME,
    "password": SQL_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Read data from SQL Server table
df = spark.read.jdbc(url=url, table=TABLENAME, properties=sqlserver_properties)

# Show the DataFrame
df.show()
