from loader import IMDBDataLoader
from transform import IMDbDataPipeline


"""
#mongo_uri = "mongodb://127.0.0.1:27017"
mongo_uri = "mongodb+srv://medouse15:6TbzE9CqeQFsM26r@imdb.ixiodlp.mongodb.net/"

output_directory = "/user/root/imdb/split/"
hdfs_url = "http://hadoop-master:50070"
hdfs_user = "root"
hdfs_file_path = "/root/hdfs/purchases.txt"
"""


def main():
    imdb_dataset_url = "https://datasets.imdbws.com/name.basics.tsv.gz"
    imdb_gz_file_name = "name.basics.tsv.gz"
    imdb_tsv_file_name = "name.basics.tsv"
    hdfs_base_dir = "/imdb"
    hdfs_url = "http://hadoop-master:50070"
    hdfs_user = "root"

    # Chemin complet vers le fichier TSV dans HDFS
    imdb_tsv_path = "hdfs://hadoop-master:9000/imdb/name.basics.tsv"
    # Répertoire de sortie dans HDFS pour les données traitées
    hdfs_path = "/user/root/imdb/split/"
    # URI de connexion MongoDB
    mongo_uri = "mongodb://mongodb:27017"
    # Nom de la base de données MongoDB
    mongo_db = "mydb"
    # Nom de la collection MongoDB
    mongo_collection = "name_basics"



    print("-----------------------------------------")
    print("-----------------------------------------")
    print("-----------------------------------------")
    print("-----------------------------------------")
    print("-----------------------------------------")
    print("-----------------------------------------")
    #loader = IMDBDataLoader(imdb_dataset_url, imdb_gz_file_name, imdb_tsv_file_name, hdfs_url, hdfs_user, hdfs_base_dir)
    #loader.run()

    print("-----------------------------------------")
    print("-----------------------------------------")
    print("-----------------------------------------")
    print("-----------------------------------------")
    print("-----------------------------------------")
    print("-----------------------------------------")


    # Transformation et sauvegarde des données dans HDFS et MongoDB
    pipeline = IMDbDataPipeline(imdb_tsv_path, hdfs_path, mongo_uri, "myimdb", "name_basics")
    pipeline.run()


    """
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

    df.write.format("mongo").mode("append").option("database", "imdb").option("collection","fadiu").save()

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

    """
if __name__ == "__main__":
    main()
