from pyspark.sql import SparkSession

# Create a SparkSesision
spark = SparkSession.builder \
    .config("spark.jars", "/usr/local/spark/resources/mysql-connector-java-8.0.22.jar") \
    .appName("final_project") \
    .getOrCreate()


mysql_url = "jdbc:mysql://host.docker.internal:3307/final_project"
mysql_driver = "com.mysql.cj.jdbc.Driver"
mysql_user = "root"
mysql_password = "password"

postgres_url = "jdbc:postgresql://host.docker.internal:5433/final_project"
postgres_driver = "org.postgresql.Driver"
postgres_user = "postgres"
postgres_password = "password"

listFile = ["application_test", "application_train"]
for file in listFile:
    # Read data from mysql
    df = spark.read.format("jdbc") \
        .option("url", mysql_url) \
        .option("driver", mysql_driver) \
        .option("dbtable", "home_credit_default_risk_" + file) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .load()

    # Import data to Postgres
    df.write.format("jdbc") \
        .option("url", postgres_url) \
        .option("driver", postgres_driver) \
        .option("dbtable", "home_credit_default_risk_" + file) \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .mode("overwrite") \
        .save()
