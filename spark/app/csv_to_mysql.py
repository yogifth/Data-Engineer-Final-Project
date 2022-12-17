from pyspark.sql import SparkSession

# Create a SparkSesision
spark = SparkSession.builder \
    .config("spark.jars", "/usr/local/spark/resources/mysql-connector-java-8.0.22.jar") \
    .appName("final_project") \
    .getOrCreate()


url = "jdbc:mysql://host.docker.internal:3307/final_project"
driver = "com.mysql.jdbc.Driver"
user = "root"
password = "password"

listFile = ["application_test", "application_train"]
for file in listFile:
    # Read file csv
    df = spark.read.format("csv") \
        .options(header="true", inferschema="true") \
        .load(f"/usr/local/spark/resources/{file}.csv")

    # Import data to MySQL
    df.write.format('jdbc') \
        .option('url', url) \
        .option('dbtable', "home_credit_default_risk_" + file) \
        .option('driver', driver) \
        .option('user', user) \
        .option('password', password) \
        .mode("overwrite") \
        .save()