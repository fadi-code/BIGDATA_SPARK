from pyspark.sql import SparkSession
from hdfs import InsecureClient
from pyspark.sql import Row
import time


#mongo_uri = "mongodb://127.0.0.1:27017"
mongo_uri = "mongodb+srv://medouse15:6TbzE9CqeQFsM26r@imdb.ixiodlp.mongodb.net/"

output_directory = "/user/root/imdb/split/"
hdfs_url = "http://hadoop-master:50070"
hdfs_user = "root"
hdfs_file_path = "/root/hdfs/purchases.txt"

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
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()
    
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")

    print(f'The PySpark {spark.version} version is running...')

        

    # Diviser le fichier HDFS en parties
    #split_hdfs_file(hdfs_url, hdfs_user, hdfs_file_path, output_directory, 100000000)
    
    # Transformer et charger les données
    # transform_and_load_data(spark, output_directory, mongo_uri)

    # Créer une liste de données
    data = [
        Row(id=1, name="Alice", age=23),
        Row(id=2, name="Bob", age=34),
        Row(id=3, name="Cathy", age=29),
        Row(id=4, name="David", age=45),
        Row(id=5, name="Eva", age=31),
        Row(id=6, name="Frank", age=38),
        Row(id=7, name="Grace", age=26),
        Row(id=8, name="Henry", age=41),
        Row(id=9, name="Ivy", age=36),
        Row(id=10, name="Jack", age=28)
    ]

    # Convertir la liste en DataFrame
    df = spark.createDataFrame(data)

    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")

    time.sleep(3)



    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    dataFrame = spark.read.format("mongo").option("database", "imdb").option("collection", "film").load()

    dataFrame.printSchema()

    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")

    print("L'heure de verité")

    df.printSchema()

    df.write.format("mongo").mode("append").option("database", "imdb").option("collection","mart_data").save()

    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")
    print("---------OK-------------")
    print("-----------------------")
    print("-----------------------")
    print("-----------------------")

    #spark.stop()
    print("-----------------------")

    time.sleep(1000)


if __name__ == "__main__":
    main()
