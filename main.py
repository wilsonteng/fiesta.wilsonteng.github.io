import requests
import json
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import mysql.connector
import os
import logging
import time
from pathlib import Path

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

def one_api_request(limit: int, offset : int, queuetype : str):
    """
    Makes the API request to Legion TD API
    Returns a dictionary containing the data from api call
    """

    api_url = "https://apiv2.legiontd2.com"
    headers = {'x-api-key': ltd_api_key, 'accept': 'application/json'}
    
    dateBefore = datetime.strftime(datetime.utcnow() - timedelta(0), '%Y-%m-%d') # YYYY-MM-DD
    dateAfter = datetime.strftime(datetime.utcnow() - timedelta(1), '%Y-%m-%d') # One Day Ago
    URL = f"""{api_url}/games?limit={limit}&offset={offset}&sortBy=date&sortDirection=1&dateBefore={dateBefore}%2000%3A00%3A00&dateAfter={dateAfter}%2000%3A00%3A00&includeDetails=true&countResults=false&queueType={queuetype}"""

    try:
        r = requests.get(URL, headers=headers)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(e)
        return False

    #print(f"Retrieving data for {offset} to {offset + int(limit)} for queueType {queuetype}. Status Code: {r.status_code}")
    
    return json.loads(r.text)

def check_if_data_useful(player_data) -> bool:
    """
    Checks if this particular build is a pro leak
    Looking for groups of waves that leak less than 4, 4, 7 on waves 1, 2, 3
    """
    if "cross" in player_data and player_data["cross"] == True:
        return False

    wave_leak_data = player_data["leaksPerWave"]
    if len(wave_leak_data) < 3:
        return False

    if (0 < len(wave_leak_data[0]) < 4 and
        0 < len(wave_leak_data[1]) < 4 and
        0 < len(wave_leak_data[2]) < 7):
            return True

    return False

def filter_data(raw_data : str) -> list:
    """
    Takes in entire api json return in string format.
    Returns a dictionary containing only items we want to keep
    """

    number_of_waves_to_keep = 3
    new_list = []
    
    date_format = "%Y-%m-%dT%H:%M:%S.%f%z"

    for game in raw_data:
        for player in game["playersData"]:
            if "votedmode" in game and game["votedmode"] != "GigaMercs" and check_if_data_useful(player):
                
                date = game["date"]
                date = datetime.strptime(date, date_format)
                formatted_date = date.strftime('%Y-%m-%d %H:%M:%S')

                # creating a new dictionary with only the data we need
                player_dict = {}
                player_dict["game_id"] = game["_id"]
                player_dict["version"] = game["version"]
                player_dict["date"] = formatted_date
                player_dict["queueType"] = game["queueType"]
                player_dict["playerName"] = player["playerName"]
                player_dict["legion"] = player["legion"]
                player_dict["buildPerWave"] = str(player["buildPerWave"][:3])
                player_dict["mercenariesReceivedPerWave"] = str(player["mercenariesReceivedPerWave"][:3])
                player_dict["leaksPerWave"] = str(player["leaksPerWave"][:3])

                new_list.append(player_dict)

    return new_list

def write_sql_insert_statement(input_data):
    """
    Formats the data into an SQL insert statement.
    Takes in entire input data
    """

    add_match_data = ("INSERT INTO match_data "
                      "(GAME_ID, GAME_VERSION, datetime, queueType, PLAYER_NAME, PLAYER_LEGION, PLAYER_BUILDPERWAVE, PLAYER_MERCSRECEIVED, PLAYER_LEAKSPERWAVE )"
                      "VALUES (%(game_id)s, %(version)s, %(date)s, %(queueType)s, %(playerName)s, %(legion)s, %(buildPerWave)s, %(mercenariesReceivedPerWave)s, %(leaksPerWave)s)")
    
    cnx = connect_to_mysql(mysql_config)
    cursor = cnx.cursor()
    for row in input_data:
        print(row)
        cursor.execute(add_match_data, row)
        print("Inserted: ", row["game_id"])
        cnx.commit()
    
    return None

def api_call_loop(gamemode):

    start, end = 0, 1100
    limit = 20
    for i in range(start, end):
        offset = i * int(limit)
        data = one_api_request(limit, offset, gamemode)
        time.sleep(0.5) # To prevent hitting rate limit

        if not data:
            print("Return was empty or ran out of data")
            return

        filtered = filter_data(data)
        write_sql_insert_statement(filtered)
    
    return True

def main():

    print("Starting")
    api_call_loop("Normal")
    api_call_loop("Classic")

    # write date created to json file
    date_utc = datetime.strftime(datetime.utcnow(), '%Y-%m-%d at %H:%M:%S UTC')
    with open('assets/date_created.json', 'w') as f:
        json.dump(date_utc, f)

main()