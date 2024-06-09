import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pymongo import MongoClient

class IMDbDataPipeline:
    def __init__(self, imdb_tsv_path, hdfs_path, mongo_uri, mongo_db, mongo_collection):
        self.spark = SparkSession.builder \
            .appName("IMDb Data Pipeline") \
            .config("spark.executorEnv.JAVA_HOME", "/usr/lib/jvm/java-8-openjdk-amd64") \
            .config("spark.mongodb.input.uri", mongo_uri) \
            .config("spark.mongodb.output.uri", mongo_uri) \
            .config("spark.jars", "/home/mongo-spark-connector_2.13-10.3.0.jar") \
            .getOrCreate()

        self.imdb_tsv_path = imdb_tsv_path
        self.hdfs_path = hdfs_path
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection

    def read_data(self):
        return self.spark.read.option("header", "true").option("sep", "\t").csv(self.imdb_tsv_path)

    def clean_data(self, df):
        # Handle missing values
        df = df.na.fill("Unknown", subset=["primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"])
        
        # Handle inconsistencies (example: convert birthYear and deathYear to integers, replace "\\N" with null)
        df = df.withColumn("birthYear", when(col("birthYear") == "\\N", None).otherwise(col("birthYear").cast("int")))
        df = df.withColumn("deathYear", when(col("deathYear") == "\\N", None).otherwise(col("deathYear").cast("int")))
        
        return df

    def transform_data(self, df):
        # Example transformation: extract the year from birthYear
        df = df.withColumn("birthYear", col("birthYear").cast("int"))
        
        # Example aggregation: count number of knownForTitles per person
        df = df.withColumn("numKnownForTitles", col("knownForTitles").isNotNull().cast("int"))
        
        return df

    def save_to_hdfs(self, df):
        df.write.mode("overwrite").parquet(self.hdfs_path)
        print(f"Data saved to HDFS at {self.hdfs_path}")

    def save_to_mongodb(self, df):
        df.write.format("mongo").mode("overwrite").option("uri", self.mongo_uri).option("database", self.mongo_db).option("collection", self.mongo_collection).save()
        print("Data saved to MongoDB")

    def run(self):
        df = self.read_data()
        df = self.clean_data(df)
        df = self.transform_data(df)
        
        self.save_to_hdfs(df)
        self.save_to_mongodb(df)
