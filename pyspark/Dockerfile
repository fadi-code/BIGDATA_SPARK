# Utiliser l'image officielle de PySpark
FROM apache/spark-py:latest

# Ajouter manuellement le fichier spark-submit
#COPY spark-submit /opt/spark/bin/spark-submit

# Mettre à jour et installer les dépendances nécessaires en tant qu'utilisateur root
USER root
RUN apt-get update && apt-get install -y curl wget default-jdk

# Vérifier et modifier les autorisations du répertoire /var/lib/apt/lists/
RUN mkdir -p /var/lib/apt/lists && chmod 755 /var/lib/apt/lists

# Installer les bibliothèques Python nécessaires en tant qu'utilisateur root
RUN pip install requests hdfs pymongo findspark


# Rétablir l'utilisateur par défaut
USER spark

# Copier le fichier principal dans l'image Docker
COPY main.py /home/main.py

# Télécharger et installer le connecteur Spark-MongoDB en tant qu'utilisateur root
USER root
#COPY mongo-spark-connector_2.13-10.3.0.jar /opt/spark/jars/mongo-spark-connector_2.13-10.3.0.jar



# Définir le répertoire de travail
WORKDIR /home

# Définir le point d'entrée pour exécuter le script principal
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1", "/home/main.py"]
