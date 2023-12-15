import logging
import os
import random
from flask import Flask, jsonify, request
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from pyspark.ml.recommendation import ALSModel
from datetime import datetime

app = Flask(__name__)

# Configuration du logging
log_directory = "/home/StreamingmovieRecommandationBigDataIA/Logs/FLASK_APP/"
os.makedirs(log_directory, exist_ok=True)

log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
log_filepath = os.path.join(log_directory, log_filename)

logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connexion à Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Initialisation de Spark
spark = SparkSession.builder \
    .appName("MovieRecommendationSystem") \
    .getOrCreate()

# Chargement du modèle sauvegardé
saved_model_path = "/home/StreamingmovieRecommandationBigDataIA/src/recommandationSystem/model"
try:
    loaded_model = ALSModel.load(saved_model_path)
    logger.info("Modèle chargé avec succès.")
except Exception as e:
    logger.error(f"Erreur lors du chargement du modèle : {str(e)}")
    raise SystemExit

# Fonction pour générer des URLs d'images aléatoires
def get_random_image_url(title, genre):
    keywords = title.lower().split() + genre.lower().split()
    seed = sum(ord(char) for char in ''.join(keywords))
    random.seed(seed)
    image_url = f'https://source.unsplash.com/300x400/?{"+".join(keywords)}'
    return image_url

# Fonction pour obtenir les recommandations pour un utilisateur
def get_recommendations_for_user(user_id, num_recommendations=5):
    all_user_recs = loaded_model.recommendForAllUsers(num_recommendations)
    user_recs = all_user_recs.filter(all_user_recs.userId == user_id).select("recommendations").collect()
    return user_recs

def get_movie_details_from_elasticsearch(movie_id):
    # Requête Elasticsearch pour récupérer les détails du film en fonction de movie_id
    es_query = {
        "query": {
            "match": {
                "movieId": movie_id
            }
        }
    }
    res = es.search(index="movierecommendation_index", body=es_query)
    
    if res['hits']['total']['value'] > 0:
        # Récupération des détails du film depuis Elasticsearch
        movie_details = res['hits']['hits'][0]['_source']
        return movie_details
    else:
        return None


@app.route('/', methods=['GET'])
def home():
    try:
        es_query = {
            "size": 4,
            "sort": [{"rating": {"order": "desc"}}]
        }

        res = es.search(index="movierecommendation_index", body=es_query)

        film_cards = ''
        for hit in res['hits']['hits']:
            movie = hit['_source']
            image_url = f'https://source.unsplash.com/300x400/?{movie["title"].replace(" ", "+")}'
            film_cards += f'''
                <div style="border: 1px solid #ccc; border-radius: 5px; padding: 10px; margin: 10px; display: inline-block; width: calc(33.33% - 20px);">
                    <img src="{image_url}" alt="{movie['title']}" style="width: 100%; height: 200px; object-fit: cover;">
                    <h3>{movie['title']}</h3>
                    <p><strong>Genre:</strong> {', '.join(movie['genres'])}</p>
                    <p><strong>Année:</strong> {movie['year']}</p>
                    <p><strong>Note:</strong> {movie['rating']}</p>
                </div>
            '''

        return f'''
            <!DOCTYPE html>
            <html>
            <head>
                <title>FidelisProd - Accueil</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 0;
                display: flex;
                flex-direction: column;
                height: 100vh;
            }}
            header {{
                background-color: #333;
                color: white;
                text-align: center;
                padding: 10px 0;
            }}
            header h1 {{
                margin: 0;
            }}
            nav {{
                background-color: #eee;
                padding: 10px 0;
                text-align: center;
            }}
            nav input[type="text"] {{
                padding: 5px;
                width: 200px;
            }}
            main {{
                padding: 20px;
                display: flex;
                flex-wrap: wrap;
                justify-content: space-around;
                flex-grow: 1;
            }}
            footer {{
                background-color: #333;
                color: white;
                text-align: center;
                padding: 10px 0;
                width: 100%;
                flex-shrink: 0;
            }}
        </style>
            </head>
            <body>
                <header>
                    <h1>FidelisProd</h1>
                </header>
                <nav>
                    <form action="/search_movie" method="get">
                        <input type="text" name="title" placeholder="Rechercher un film...">
                        <input type="submit" value="Rechercher">
                    </form>
                </nav>
                <main>
                    {film_cards}
                </main>
                <footer>
                    &copy; 2023 FidelisProd. Tous droits réservés.
                </footer>
            </body>
            </html>
        '''
    except Exception as e:
        logger.error(f"Erreur lors de l'affichage des films les plus populaires : {str(e)}")
        return jsonify({'message': 'Une erreur s\'est produite lors de l\'affichage des films les plus populaires'})




@app.route('/search_movie', methods=['GET'])
def search_movie():
    try:
        movie_title = request.args.get('title')

        es_query = {
            "query": {
                "match": {
                    "title": movie_title
                }
            }
        }
        res = es.search(index="movierecommendation_index", body=es_query)

        if res['hits']['total']['value'] == 0:
            return "<p>Film non disponible</p>"
        else:
            movie = res['hits']['hits'][0]['_source']
            user_id = 1  # Remplace avec l'ID de l'utilisateur concerné

            recommendations = get_recommendations_for_user(user_id, num_recommendations=6)
            
            film_details = f'''
                <h1>{movie['title']}</h1>
                <p><strong>Genre:</strong> {', '.join(movie['genres'])}</p>
                <p><strong>Année:</strong> {movie['year']}</p>
                <p><strong>Note:</strong> {movie['rating']}</p>
            '''

            recommendation_cards = ''
            for rec in recommendations:
                for recommendation in rec['recommendations']:
                    movie_id = recommendation['movieId']
                    rating = recommendation['rating']

                    # Récupération des détails du film depuis Elasticsearch ou une autre source de données
                    # Par exemple, en supposant que les détails du film sont stockés dans Elasticsearch
                    movie_details = get_movie_details_from_elasticsearch(movie_id)

                    # Génération d'une URL aléatoire pour l'image du film recommandé
                    image_url = get_random_image_url(movie_details['title'], ', '.join(movie_details['genres']))

                    # Création de la carte de recommandation
                    recommendation_card = f'''
                        <div style="border: 1px solid #ccc; border-radius: 5px; padding: 20px; margin: 20px; background-color: #f9f9f9;">
                            <img src="{image_url}" alt="{movie_details['title']}" style="width: 100%; height: 200px; object-fit: cover;">
                            <h3>{movie_details['title']}</h3>
                            <p><strong>Rating:</strong> {rating}</p>
                            <p><strong>Genre:</strong> {', '.join(movie_details['genres'])}</p>
                            <!-- Ajoutez d'autres détails si nécessaire -->
                        </div>
                    '''

                    recommendation_cards += recommendation_card

            return f'''
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Détails du Film</title>
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            margin: 0;
                            padding: 0;
                            display: flex;
                            flex-direction: column;
                            height: 100vh;
                        }}
                        header {{
                            background-color: #333;
                            color: white;
                            text-align: center;
                            padding: 10px 0;
                        }}
                        nav {{
                            background-color: #eee;
                            padding: 10px 0;
                            text-align: center;
                        }}
                        nav input[type="text"] {{
                            padding: 5px;
                            width: 200px;
                        }}

                        /* Ajoute ici tes modifications pour la page /search_movie */
                        main {{
                            display: flex;
                            flex-direction: column;
                            align-items: center;
                            padding: 20px;
                        }}
                        h1, h3, p {{
                            margin: 10px 0;
                        }}
                        .film-details {{
                            border-bottom: 1px solid #ccc;
                            width: 100%;
                            padding-bottom: 20px;
                        }}
                        .film-details p {{
                            margin: 5px 0;
                        }}
                        .films-container {{
                            display: flex;
                            flex-wrap: wrap;
                            justify-content: space-around;
                            width: 100%;
                        }}
                        .film-card {{
                            border: 1px solid #ccc;
                            border-radius: 5px;
                            padding: 20px;
                            margin: 20px;
                            background-color: #f9f9f9;
                            width: calc(33.33% - 40px);
                        }}
                        .film-card img {{
                            width: 100%;
                            height: 200px;
                            object-fit: cover;
                            border-radius: 5px;
                            margin-bottom: 10px;
                        }}
                        .film-card h3 {{
                            margin-bottom: 5px;
                        }}
                    </style>
                </head>
                <body>
                    <header>
                        <h1><a href="/">FidelisProd</a></h1>
                    </header>
                    <main>
                        <div class="film-details">
                            {film_details}
                        </div>
                        <h2>Recommandations:</h2>
                        <div class="films-container">
                            {recommendation_cards}
                        </div>
                    </main>
                    <footer>
                        &copy; 2023 FidelisProd. Tous droits réservés.
                    </footer>
                </body>
                </html>
            '''

    except Exception as e:
        logger.error(f"Erreur lors de la recherche du film : {str(e)}")
        return jsonify({'message': 'Une erreur s\'est produite lors de la recherche du film'})

if __name__ == '__main__':
    app.run(debug=True)