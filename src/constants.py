import os

# API params
PATH_LAST_PROCESSED = "./data/last_processed.json"
#MAX_LIMIT = 100
#MAX_OFFSET = 10000

# We have three parameters in the URL:
# 1. MAX_LIMIT: the maximum number of records to be returned by the API
# 2. date_de_publication: the date from which we want to get the data
# 3. offset: the index of the first result
URL_API = "https://fantasy.premierleague.com/api"
endpoints = [{'name': 'bootstrap-static', 'tables': ['elements', 'teams']}, {'name': 'fixtures'}]

API = "config.yaml"


yaml_data = {
    'env': 'dev',
    'api': {
        'baseurl': 'https://fantasy.premierleague.com/api',
        'endpoints': [
            {
                'name': 'bootstrap-static',
                'tables': [
                    'elements',
                    'teams'
                ]
            },
            {
                'name': 'fixtures'
            }
        ]
    }
}

#URL_API = URL_API.format(MAX_LIMIT, "{}", "{}")

# POSTGRES PARAMS
user_name = os.getenv("POSTGRES_DOCKER_USER", "localhost")
POSTGRES_URL = f"jdbc:postgresql://{user_name}:5432/postgres"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver",
}

# NEW_COLUMNS = [
#     "risques_pour_le_consommateur",
#     "recommandations_sante",
#     "date_debut_commercialisation",
#     "date_fin_commercialisation",
#     "informations_complementaires",
# ]

# COLUMNS_TO_NORMALIZE = [
#     "categorie_de_produit",
#     "sous_categorie_de_produit",
#     "nom_de_la_marque_du_produit",
#     "noms_des_modeles_ou_references",
#     "identification_des_produits",
#     "conditionnements",
#     "temperature_de_conservation",
#     "zone_geographique_de_vente",
#     "distributeurs",
#     "motif_du_rappel",
#     "numero_de_contact",
#     "modalites_de_compensation",
# ]

# COLUMNS_TO_KEEP = [
#     "reference_fiche",
#     "liens_vers_les_images",
#     "lien_vers_la_liste_des_produits",
#     "lien_vers_la_liste_des_distributeurs",
#     "lien_vers_affichette_pdf",
#     "lien_vers_la_fiche_rappel",
#     "date_de_publication",
#     "date_de_fin_de_la_procedure_de_rappel",
# ]
# DB_FIELDS = COLUMNS_TO_KEEP + COLUMNS_TO_NORMALIZE + NEW_COLUMNS



#TABLES AND COLUMNS  

ELEMENTS = ['chance_of_playing_next_round', 'chance_of_playing_this_round', 'code',
       'cost_change_event', 'cost_change_event_fall', 'cost_change_start',
       'cost_change_start_fall', 'dreamteam_count', 'element_type', 'ep_next',
       'ep_this', 'event_points', 'first_name', 'form', 'id', 'in_dreamteam',
       'news', 'news_added', 'now_cost', 'photo', 'points_per_game',
       'second_name', 'selected_by_percent', 'special', 'squad_number',
       'status', 'team', 'team_code', 'total_points', 'transfers_in',
       'transfers_in_event', 'transfers_out', 'transfers_out_event',
       'value_form', 'value_season', 'web_name', 'minutes', 'goals_scored',
       'assists', 'clean_sheets', 'goals_conceded', 'own_goals',
       'penalties_saved', 'penalties_missed', 'yellow_cards', 'red_cards',
       'saves', 'bonus', 'bps', 'influence', 'creativity', 'threat',
       'ict_index', 'starts', 'expected_goals', 'expected_assists',
       'expected_goal_involvements', 'expected_goals_conceded',
       'influence_rank', 'influence_rank_type', 'creativity_rank',
       'creativity_rank_type', 'threat_rank', 'threat_rank_type',
       'ict_index_rank', 'ict_index_rank_type',
       'corners_and_indirect_freekicks_order',
       'corners_and_indirect_freekicks_text', 'direct_freekicks_order',
       'direct_freekicks_text', 'penalties_order', 'penalties_text',
       'expected_goals_per_90', 'saves_per_90', 'expected_assists_per_90',
       'expected_goal_involvements_per_90', 'expected_goals_conceded_per_90',
       'goals_conceded_per_90', 'now_cost_rank', 'now_cost_rank_type',
       'form_rank', 'form_rank_type', 'points_per_game_rank',
       'points_per_game_rank_type', 'selected_rank', 'selected_rank_type',
       'starts_per_90', 'clean_sheets_per_90']



TEAMS = ['code', 'draw', 'form', 'id', 'loss', 'name', 'played', 'points',
       'position', 'short_name', 'strength', 'team_division', 'unavailable',
       'win', 'strength_overall_home', 'strength_overall_away',
       'strength_attack_home', 'strength_attack_away', 'strength_defence_home',
       'strength_defence_away', 'pulse_id']


FIXTURES = ['code', 'event', 'finished', 'finished_provisional', 'id',
       'kickoff_time', 'minutes', 'provisional_start_time', 'started',
       'team_a', 'team_a_score', 'team_h', 'team_h_score', 'stats',
       'team_h_difficulty', 'team_a_difficulty', 'pulse_id']



DB_FIELDS = {'elements': ELEMENTS, 'teams': TEAMS, 'fixtures': FIXTURES}