from loader import IMDBDataLoader
from transform import IMDbDataPipeline


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


    loader = IMDBDataLoader(imdb_dataset_url, imdb_gz_file_name, imdb_tsv_file_name, hdfs_url, hdfs_user, hdfs_base_dir)
    loader.run()

    # Transformation et sauvegarde des données dans HDFS et MongoDB
    pipeline = IMDbDataPipeline(imdb_tsv_path, hdfs_path, mongo_uri, "myimdb", "name_basics")
    pipeline.run()


if __name__ == "__main__":
    main()
