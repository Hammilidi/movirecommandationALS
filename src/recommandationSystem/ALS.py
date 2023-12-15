from pyspark.streaming import StreamingContext
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from datetime import datetime
from shutil import rmtree
import json, logging, os
import pandas as pd
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Configuration des logs pour Spark
def setup_spark_logging():
    log_directory = "/home/StreamingmovieRecommandationBigDataIA/Logs/ML_Log_Files"
    os.makedirs(log_directory, exist_ok=True)
    log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
    log_filepath = os.path.join(log_directory, log_filename)
    logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("MovieRecommendationSystem")\
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.1") \
    .getOrCreate()

logger = setup_spark_logging()

u_data = pd.read_csv('/home/StreamingmovieRecommandationBigDataIA/data/u.data', sep='\t', names=['userId', 'movieId', 'rating', 'timestamp'])


# # Connexion à Elasticsearch
# def connect_elasticsearch():
#     try:
#         es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
#         if es.ping():
#             logger.info("Connecté à Elasticsearch !")
#             return es
#         else:
#             logger.error("Échec de la connexion à Elasticsearch.")
#             return None
#     except Exception as e:
#         logger.error(f"Erreur lors de la connexion à Elasticsearch : {str(e)}")
#         return None

# es = connect_elasticsearch()

# # Chargement des données depuis Elasticsearch pour l'entraînement du modèle
# def load_training_data(spark, es_read_conf):
#     try:
#         training_data = spark.read.format("org.elasticsearch.spark.sql").options(**es_read_conf).load()
#         return training_data
#     except Exception as e:
#         logger.error(f"Erreur lors du chargement des données depuis Elasticsearch : {str(e)}")
#         return None

# es_read_conf = {
#     "es.nodes.wan.only": "true",
#     "es.port": "9200",
#     "es.resource": "movierecommendation_index",
#     "es.read.field.as.array.include": "genres"
# }

# training_data = load_training_data(spark, es_read_conf)

if not u_data.empty:
    # Conversion des colonnes pour l'entraînement du modèle ALS
    # (training, test) = u_data.randomSplit([0.8, 0.2])
    spark_u_data = spark.createDataFrame(u_data)
    (training, test) = spark_u_data.randomSplit([0.8, 0.2])


    # Initialisation du modèle ALS
    als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")

    # Paramètres à tester pour la validation croisée
    param_grid = ParamGridBuilder() \
        .addGrid(als.rank, [30, 40, 50]) \
        .addGrid(als.maxIter, [5, 10]) \
        .addGrid(als.alpha, [1.0, 5.0, 10.0]) \
        .addGrid(als.regParam, [0.01, 0.1]) \
        .build()

    # Évaluateur pour la validation croisée
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

    # Validation croisée
    cross_val = CrossValidator(estimator=als,
                               estimatorParamMaps=param_grid,
                               evaluator=evaluator,
                               numFolds=5)  # Nombre de plis pour la validation croisée

    # Entraînement avec validation croisée
    cv_model = cross_val.fit(training)

    # Sélection du meilleur modèle
    best_model = cv_model.bestModel

    # Évaluation du modèle optimisé
    predictions_cv = best_model.transform(test)
    rmse_cv = evaluator.evaluate(predictions_cv)
    logger.info(f"Root-mean-square error (after cross-validation) = {rmse_cv}")
    
    # Calcul du nombre de prédictions correctes
    correct_predictions = predictions_cv.filter("abs(rating - prediction) <= 1").count()

    # Calcul du nombre total de prédictions
    total_predictions = predictions_cv.count()

    # Calcul de la précision du modèle
    precision = correct_predictions / total_predictions
    logger.info(f"Precision = {precision}")


    # Sauvegarde du meilleur modèle
    model_path = "/home/StreamingmovieRecommandationBigDataIA/src/recommandationSystem/model"
    if os.path.exists(model_path):
        rmtree(model_path)

    best_model.write().save(model_path)

else:
    logger.error("Impossible de charger les données depuis Elasticsearch. Arrêt du processus.")
