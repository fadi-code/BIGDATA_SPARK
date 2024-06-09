from pyspark.sql import SparkSession
from hdfs import InsecureClient

mongo_uri = "mongodb://mongodb:27017/mydb"
output_directory = "/user/root/imdb/split/"
hdfs_url = "http://hadoop-master:50070"
hdfs_user = "root"
hdfs_file_path = "/user/root/imdb/name.basics.tsv"

def split_hdfs_file(hdfs_url, hdfs_user, hdfs_file_path, output_directory, chunk_size):
    client = InsecureClient(hdfs_url, user=hdfs_user)
    with client.read(hdfs_file_path) as reader:
        # Lecture du fichier depuis HDFS
        content = reader.read()
        # Calcul du nombre de chunks nécessaires
        num_chunks = len(content) // chunk_size
        if len(content) % chunk_size != 0:
            num_chunks += 1
        # Écriture des chunks dans des fichiers sur HDFS
        for i in range(num_chunks):
            chunk_data = content[i * chunk_size: (i + 1) * chunk_size]
            with client.write(f"{output_directory}part_{i}", overwrite=True) as writer:
                writer.write(chunk_data)

def transform_and_load_data(spark, hdfs_path, mongo_uri):
    # Lire les parties du fichier TSV depuis HDFS
    df = spark.read.option("header", "true").option("sep", "\t").csv(hdfs_path + "*")
    
    # Transformation des données (exemple: sélectionner des colonnes spécifiques)
    # transformed_df = df.select("nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles")
    
    # Charger les données transformées dans MongoDB
    df.write.format("mongo").mode("append").option("uri", mongo_uri).option("database", "mydb").option("collection", "name_basics").save()

def main():
    # Initialiser SparkSession
    spark = SparkSession.builder \
        .appName("IMDbDataPipeline") \
        .config("spark.mongodb.input.uri", mongo_uri) \
        .config("spark.mongodb.output.uri", mongo_uri) \
        .getOrCreate()
    
    # Diviser le fichier HDFS en parties
    split_hdfs_file(hdfs_url, hdfs_user, hdfs_file_path, output_directory, 100000000)
    
    # Transformer et charger les données
    transform_and_load_data(spark, output_directory, mongo_uri)
    
    spark.stop()

if __name__ == "__main__":
    main()
