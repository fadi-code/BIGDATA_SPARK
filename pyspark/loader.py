import requests
import gzip
import shutil
from hdfs import InsecureClient


class IMDBDataLoader:
    def __init__(self, imdb_url, imdb_gz_file_name, imdb_tsv_file_name, hdfs_url, hdfs_user, hdfs_base_dir):
        self.imdb_url = imdb_url
        self.imdb_gz_file_name = imdb_gz_file_name
        self.imdb_tsv_file_name = imdb_tsv_file_name
        self.hdfs_url = hdfs_url
        self.hdfs_user = hdfs_user
        self.hdfs_base_dir = hdfs_base_dir
        self.hdfs_imdb_path = f"{hdfs_base_dir}/{imdb_tsv_file_name}"
        self.client = InsecureClient(hdfs_url, user=hdfs_user)

    def download_file(self):
        response = requests.get(self.imdb_url, stream=True)
        with open(self.imdb_gz_file_name, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded {self.imdb_gz_file_name}")

    def decompress_file(self):
        with gzip.open(self.imdb_gz_file_name, 'rb') as f_in:
            with open(self.imdb_tsv_file_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Decompressed {self.imdb_gz_file_name} to {self.imdb_tsv_file_name}")

    def upload_to_hdfs(self):
        if not self.client.status(self.hdfs_base_dir, strict=False):
            self.client.makedirs(self.hdfs_base_dir)
            print(f"Directory {self.hdfs_base_dir} created on HDFS.")
        self.client.upload(self.hdfs_imdb_path, self.imdb_tsv_file_name, overwrite=True)
        print(f"Uploaded {self.imdb_tsv_file_name} to {self.hdfs_imdb_path} on HDFS.")

    def run(self):
        self.download_file()
        self.decompress_file()
        self.upload_to_hdfs()

# Utilisation de la classe IMDBDataLoader
if __name__ == "__main__":
    imdb_dataset_url = "https://datasets.imdbws.com/name.basics.tsv.gz"
    imdb_gz_file_name = "name.basics.tsv.gz"
    imdb_tsv_file_name = "name.basics.tsv"
    hdfs_base_dir = "/imdb"
    hdfs_url = "http://hadoop-master:50070"
    hdfs_user = "root"

    loader = IMDBDataLoader(imdb_dataset_url, imdb_gz_file_name, imdb_tsv_file_name, hdfs_url, hdfs_user, hdfs_base_dir)
    loader.run()
