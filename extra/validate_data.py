# Attempt mysql connection
import requests
import json
from datetime import date, datetime
from pathlib import Path
from dotenv import load_dotenv
import mysql.connector
import os
import logging
import time
from pathlib import Path
import ast

load_dotenv()
ltd_api_key = os.getenv("ltd_api_key")
mysql_config = {
    'user': os.getenv("mysql_user"),
    'password': os.getenv("mysql_password"),
    'host': os.getenv("mysql_host"),
    'database': os.getenv("mysql_database"),
    'raise_on_warnings': True
    }

def connect_to_mysql(config, attempts=3, delay=2):
    attempt = 1

    # Set up logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Log to console
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Also log to a file
    file_handler = logging.FileHandler("cpy-errors.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler) 

    # Implement a reconnection routine
    while attempt < attempts + 1:
        try:
            return mysql.connector.connect(**config)
        except (mysql.connector.Error, IOError) as err:
            if (attempts is attempt):
              # Attempts to reconnect failed; returning None
                logger.info("Failed to connect, exiting without a connection: %s", err)
                return None
            logger.info(
                "Connection failed: %s. Retrying (%d/%d)...",
                err,
                attempt,
                attempts-1,
            )
            # progressive reconnect delay
            time.sleep(delay ** attempt)
            attempt += 1
    return None

def sql_query_to_list():
    cnx = connect_to_mysql(mysql_config)
    cursor = cnx.cursor()

    query = ("SELECT * FROM match_data ORDER BY BUILD_ID desc")

    cursor.execute(query)
    total_list = []

    for row in cursor:
        total_list.append((row[1], row[5])) #gameid, playername

    return total_list

def api_request_for_individual_game(game_id):
    """
    Makes the API request to Legion TD API
    Returns a dictionary containing the data from api call
    """

    api_url = "https://apiv2.legiontd2.com"
    headers = {'x-api-key': ltd_api_key, 'accept': 'application/json'}
    URL = f"""https://apiv2.legiontd2.com/games/byId/{game_id}?includeDetails=true"""

    try:
        r = requests.get(URL, headers=headers)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(e)
        return False

    print("Retrieved data for: ", game_id)

    return json.loads(r.text)

# find duplicates: 
# SELECT GAME_ID, COUNT(*) c FROM match_data GROUP BY GAME_ID HAVING C > 1; 


unit_dictionary = {
    "Snail" : 5,
    "Dragon Turtle" : 10,
    "Lizard" : 10,
    "Brute" : 12,
    "Fiend" : 15,
    "Hermit" : 16,
    "Dino" : 20,
    "Cannoneer" : 25,
    "Drake" : 30
}

def check_if_player_has_anomalies(player):
    incomePerWave = player["incomePerWave"]
    mercenariesSent = player["mercenariesSentPerWave"]
    kingUpgradesPerWave = player["kingUpgradesPerWave"]

    for i in range(0, 3):
        prevIncome, newIncome = incomePerWave[i], incomePerWave[i + 1]

        upgradeIncome = 6 * len(kingUpgradesPerWave[i])
        sentIncome = 0

        for unit in mercenariesSent[i]:
            sentIncome += unit_dictionary[unit]
        if (prevIncome + upgradeIncome + sentIncome) != newIncome:
            return True
    
    return False

def check_for_giga(game_data):

    if game_data["queueType"] == "Normal":
        return False

    anomalies = 0
    for player in game_data["playersData"]:
        if check_if_player_has_anomalies(player):
            anomalies += 1
    
    if anomalies > 3:
        print(anomalies)
        return True

    return False

def download_games_to_json():
    games_to_check = sql_query_to_list()
    print(len(games_to_check))
    data = []
    for i in range((len(games_to_check))):
        game_id = games_to_check[i][0]
        game_data = api_request_for_individual_game(game_id)
        data.append(game_data)

    with open('alldata.json', 'w') as f:
        json.dump(data, f)

def check_dual_build():

    f = open('alldata.json')
    data = json.load(f)

    print(len(data))
    games_to_check = sql_query_to_list()

    mapping = {}
    for id, name in games_to_check:
        mapping[id] = name

    for game in data:
        
        match_id = game["_id"]
        player_name = mapping[match_id]
        
        for player in game["playersData"]:
            if player["playerName"] == player_name:
                if player["cross"] == True:
                    print(match_id, player_name)


def fix_dates():

    f = open('alldata.json')
    data = json.load(f)
    date_format = "%Y-%m-%dT%H:%M:%S.%f%z"

    # get the proper date format from the javascript data
    match_to_date = {}

    for game in data:
        match_id = game["_id"]
        date = game["date"]

        match_to_date[match_id] = date
    
    cnx = connect_to_mysql(mysql_config)
    cursor = cnx.cursor()

    for game_id, date in match_to_date.items():

        date = datetime.strptime(date, date_format)
        formatted_date = date.strftime('%Y-%m-%d %H:%M:%S')
        
        sql = "UPDATE match_data SET datetime = %s WHERE GAME_ID = %s"
        val = (formatted_date, game_id)
        
        cursor.execute(sql, val)
        print("Updated", game_id, date)
    
    # Make sure data is committed to the database
    cnx.commit()

    cursor.close()
    cnx.close()



#download_games_to_json()
#main()
fix_dates()