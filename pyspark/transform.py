import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pymongo import MongoClient
#mongo_uri = "mongodb+srv://medouse15:6TbzE9CqeQFsM26r@imdb.ixiodlp.mongodb.net/"


class IMDbDataPipeline:
    def __init__(self, imdb_tsv_path, hdfs_path, mongo_uri, mongo_db, mongo_collection):
        self.spark = SparkSession.builder \
        .appName("IMDbDataPipeline") \
        .config("spark.mongodb.input.uri", mongo_uri) \
        .config("spark.mongodb.output.uri", mongo_uri) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

        self.imdb_tsv_path = imdb_tsv_path
        self.hdfs_path = hdfs_path
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection

    def read_data(self):
        return self.spark.read.option("header", "true").option("sep", "\t").csv(self.imdb_tsv_path)

    def clean_data(self, df):
        # Missing values
        df = df.na.fill("Unknown", subset=["primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"])
        df = df.withColumn("birthYear", when(col("birthYear") == "\\N", None).otherwise(col("birthYear").cast("int")))
        df = df.withColumn("deathYear", when(col("deathYear") == "\\N", None).otherwise(col("deathYear").cast("int")))
        
        return df

    def transform_data(self, df):
        df = df.withColumn("birthYear", col("birthYear").cast("int"))
        
        df = df.withColumn("numKnownForTitles", col("knownForTitles").isNotNull().cast("int"))
        
        return df

    def save_to_hdfs(self, df):
        df.write.mode("overwrite").parquet(self.hdfs_path)
        df.write \
        .format("csv") \
        .option("header", "true") \
        .save("data/output.csv")
        print(f"Data saved to HDFS at {self.hdfs_path}")

    def save_to_mongodb(self, df):
        df.write.format("mongo").mode("overwrite").option("database", "imdb").option("collection","new").save()
        
        
        print("Data saved to MongoDB")

    def run(self):

        df = self.read_data()

        df = self.clean_data(df)

        df = self.transform_data(df)

        self.save_to_hdfs(df)

        self.save_to_mongodb(df)



