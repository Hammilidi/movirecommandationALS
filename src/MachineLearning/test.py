import logging

from pyspark.sql import SparkSession

# Initialisation de SparkSession
spark = SparkSession.builder \
    .appName("MovieRecommendationSystem") \
    .getOrCreate()

# Le reste de votre code (chargement du modèle, etc.) ici

# Création d'un logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Définition du niveau de journalisation

# Utilisation d'un gestionnaire de fichiers pour enregistrer les logs dans un fichier
file_handler = logging.FileHandler('recommendation_logs.log')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Ajout du gestionnaire au logger
logger.addHandler(file_handler)

# Reste du code pour charger le modèle et obtenir les recommandations
from pyspark.ml.recommendation import ALSModel

# Chargement du modèle sauvegardé
saved_model_path = "/home/StreamingmovieRecommandationBigDataIA/src/recommandationSystem/model"
try:
    loaded_model = ALSModel.load(saved_model_path)
    logger.info("Modèle chargé avec succès.")
except Exception as e:
    logger.error(f"Erreur lors du chargement du modèle : {str(e)}")
    raise SystemExit

def get_recommendations_for_user(user_id, num_recommendations=5):
    # Obtenir les recommandations pour tous les utilisateurs
    all_user_recs = loaded_model.recommendForAllUsers(num_recommendations)

    # Extraire les recommandations pour un utilisateur spécifique
    user_recs = all_user_recs.filter(all_user_recs.userId == user_id).select("recommendations").collect()

    return user_recs

print(get_recommendations_for_user(1))
