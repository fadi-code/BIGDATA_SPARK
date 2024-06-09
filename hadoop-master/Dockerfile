FROM liliasfaxi/spark-hadoop:hv-2.7.2

# Installer curl et bash
USER root
RUN apt-get update && apt-get install -y curl bash

# Copier les fichiers de configuration
COPY hadoop-env.sh /usr/local/hadoop/etc/hadoop/hadoop-env.sh
COPY core-site.xml /usr/local/hadoop/etc/hadoop/core-site.xml
COPY hdfs-site.xml /usr/local/hadoop/etc/hadoop/hdfs-site.xml

# Copier le script d'entrée
COPY entrypoint.sh /entrypoint.sh

# Rendre le script d'entrée exécutable
RUN chmod +x /entrypoint.sh

# Utiliser le script d'entrée comme point d'entrée du conteneur
ENTRYPOINT ["/entrypoint.sh"]
