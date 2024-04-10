import requests
import psycopg2
import traceback

connection = psycopg2.connect(
    dbname="SoccerDatabase",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)


def get_JSON(repository):
    url = 'https://raw.githubusercontent.com/statsbomb/open-data/master/'+repository
    returnValue = requests.get(url)
    # The .json() method automatically parses the response into JSON.
    content = returnValue.json()
    return content

def add_competitions(competitions_data):
    cursor = connection.cursor()
    for competition_data in competitions_data:
        # Insert seasons
        cursor.execute("INSERT INTO Seasons (season_id, season_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                       (competition_data['season_id'], competition_data['season_name']))
        # Insert competitions
        cursor.execute("""
                    INSERT INTO Competitions (competition_id, country_name,competition_name,season_id,competition_gender,competition_youth,competition_international,season_name,match_updated,match_updated_360,match_available_360,match_available)
                    Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """,(
            competition_data['competition_id'],
            competition_data['country_name'],
            competition_data['competition_name'],
            competition_data['season_id'],
            competition_data['competition_gender'],
            competition_data['competition_youth'],
            competition_data['competition_international'],
            competition_data['season_name'],
            competition_data['match_updated'],
            competition_data['match_updated_360'],
            competition_data['match_available_360'],
            competition_data['match_available']
        ))


    # Commit the transaction and close the cursor
    connection.commit()
    cursor.close()

def add_lineup(lineups_data, match_id):
    cursor = connection.cursor()
    try:
        for lineup_data in lineups_data:
            # Extract lineup information
            team_id = lineup_data['team_id']

            # Insert lineup into Lineup table
            cursor.execute("""INSERT INTO Lineup (team_id, match_id) VALUES(%s, %s) ON CONFLICT DO NOTHING RETURNING lineup_id;""", (team_id, match_id))
            lineup_id = cursor.fetchone()[0]

            # Insert players and their positions
            for player_data in lineup_data['lineup']:
                cursor.execute("INSERT INTO Country (country_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                               (player_data['country']['id'], player_data['country']['name']))

                # Insert player into Player table
                player_insert_query = """
                    INSERT INTO Player (player_id, player_name, player_nickname, jersey_number, country_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """
                cursor.execute(player_insert_query, (
                    player_data['player_id'],
                    player_data['player_name'],
                    player_data['player_nickname'],
                    player_data['jersey_number'],
                    player_data['country']['id']
                ))

                # Insert player positions into Player_Position table
                for position_data in player_data['positions']:
                    position_insert_query = """
                                        INSERT INTO Position (position_id,position)
                                        VALUES (%s, %s) ON CONFLICT DO NOTHING
                                    """
                    cursor.execute(position_insert_query, (
                        position_data['position_id'],
                        position_data['position'],
                    ))

                    player_position_insert_query = """
                        INSERT INTO Player_Position (player_id, position_id, from_time, to_time, from_period, to_period, start_reason, end_reason,lineup_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s) ON CONFLICT DO NOTHING
                    """
                    cursor.execute(player_position_insert_query, (
                        player_data['player_id'],
                        position_data['position_id'],
                        position_data['from'],
                        position_data['to'],
                        position_data['from_period'],
                        position_data['to_period'],
                        position_data['start_reason'],
                        position_data['end_reason'],
                        lineup_id
                    ))

        # Commit changes and close connection
        print("Lineup Data inserted successfully")
        connection.commit()
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        connection.rollback()
        print("Error while inserting lineup data:", error)
        traceback.print_exc()

def add_matches(matches_JSON):
    cursor = connection.cursor()
    try:
        for match in matches_JSON:
            # Insert countries
            cursor.execute("INSERT INTO Country (country_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                           (match['home_team']['country']['id'], match['home_team']['country']['name']))
            cursor.execute("INSERT INTO Country (country_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                           (match['away_team']['country']['id'], match['away_team']['country']['name']))
            cursor.execute(
                "INSERT INTO Country (country_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (match['away_team']['country']['id'], match['away_team']['country']['name'])
            )
            referee_data = match.get('referee')  # Use get method to avoid KeyError
            if referee_data:
                cursor.execute(
                    "INSERT INTO Country (country_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (match['referee']['country']['id'], match['referee']['country']['name'])
                )
            # Insert teams
            home_team_data = match.get('home_team')
            if 'managers' in home_team_data:
                for manager in home_team_data.get('managers'):
                    cursor.execute(
                        "INSERT INTO Country (country_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (manager['country']['id'], manager['country']['name'])
                )
            away_team_data = match.get('away_team')
            if 'managers' in away_team_data:
                for manager in away_team_data.get('managers'):
                    cursor.execute(
                        "INSERT INTO Country (country_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (manager['country']['id'], manager['country']['name'])
                )

            # Insert competitions
            cursor.execute(
                "INSERT INTO Competitions (competition_id, country_name, competition_name,season_id) VALUES (%s, %s,%s,%s) ON CONFLICT DO NOTHING",
                (match['competition']['competition_id'], match['competition']['country_name'],
                 match['competition']['competition_name'],match['season']['season_id']))

            # Insert seasons
            cursor.execute("INSERT INTO Seasons (season_id, season_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                           (match['season']['season_id'], match['season']['season_name']))

            # Insert stadiums
            if 'stadium' in match:
                cursor.execute(
                    "INSERT INTO Stadium (stadium_id, country_id, stadium_name) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    (match['stadium']['id'], match['stadium']['country']['id'], match['stadium']['name']))

            referee_data = match.get('referee')  # Use get method to avoid KeyError
            if referee_data:
                # Insert referees
                cursor.execute(
                    "INSERT INTO referee (referee_id, name, country_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    (match['referee']['id'], match['referee']['name'], match['referee']['country']['id']))

            # Insert teams
            cursor.execute(
                "INSERT INTO Team (team_id, team_name, team_gender, country_id) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (match['home_team']['home_team_id'], match['home_team']['home_team_name'],
                 match['home_team']['home_team_gender'], match['home_team']['country']['id']))
            cursor.execute(
                "INSERT INTO Team (team_id,team_name,team_gender,country_id) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (match['away_team']['away_team_id'], match['away_team']['away_team_name'],
                 match['away_team']['away_team_gender'], match['away_team']['country']['id']))

            # Insert matches
            if 'stadium' in match:
                match_statdium=match['stadium']['id']
            else:
                match_statdium=None
            cursor.execute("""
                    INSERT INTO Matches (match_id, match_date, kick_off, competition_id, season_id, home_team_id, away_team_id, home_score, away_score, match_status, match_status_360, last_updated, last_updated_360, match_week, competition_stage, stadium_id, referee_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """, (match['match_id'], match['match_date'], match['kick_off'], match['competition']['competition_id'], match['season']['season_id'], match['home_team']['home_team_id'],
                      match['away_team']['away_team_id'], match['home_score'],match['away_score'], match['match_status'], match['match_status_360'], match['last_updated'],
                      match['last_updated_360'], match['match_week'],match['competition_stage']['name'], match_statdium, match.get('referee', {}).get('id')))

            # Insert managers
            if 'managers' in match['home_team']:
                for manager in match['home_team']['managers']:
                    cursor.execute(
                        """INSERT INTO Manager (manager_id, name, nickname, dob, country_id,match_id) 
                        VALUES (%s, %s, %s, %s, %s,%s) ON CONFLICT DO NOTHING""",
                        (manager['id'], manager['name'], manager['nickname'], manager['dob'],
                         manager['country']['id'], match['match_id']))
            if 'managers' in match['away_team']:
                for manager in match['away_team']['managers']:
                    cursor.execute(
                        """INSERT INTO Manager (manager_id, name, nickname, dob, country_id,match_id) 
                        VALUES (%s, %s, %s, %s, %s,%s) ON CONFLICT DO NOTHING""",
                        (manager['id'], manager['name'], manager['nickname'], manager['dob'],
                         manager['country']['id'], match['match_id']))

            connection.commit()
        print("Matches inserted successfully!")
    except (Exception, psycopg2.Error) as error:
        connection.rollback()
        print("Error while inserting match data:", error)
        traceback.print_exc()

def get_all_match_ids():

    cursor = connection.cursor()

    # Fetch all match IDs from the Matches table
    cursor.execute("SELECT match_id FROM Matches")
    match_ids = [row[0] for row in cursor.fetchall()]
    return match_ids

def add_events(match_id):
    url=get_JSON("data/events/"+str(match_id)+".json")
    returnValue = requests.get(url)
    insert_events(returnValue,match_id)

def insert_events(events,match_id):
    try:
        cursor = connection.cursor()
        for event in events:
            event_type=event['type']['name']
            if 'player' in event:
                cursor.execute("INSERT INTO Player (player_id, player_name ) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                               (event['player']['id'], event['player']['name']))

            if 'play_pattern' in event:
                # Insert play pattern data
                cursor.execute("INSERT INTO Play_Pattern (play_pattern_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (event['play_pattern']['id'], event['play_pattern']['name']))

            # Insert event type data
            event_type_id = event['type']['id']
            event_type_name = event['type']['name']

            # Insert team data
            team_id = event['team']['id']
            team_name = event['team']['name']
            cursor.execute("INSERT INTO Team (team_id, team_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                           (team_id, team_name))
            # Insert possession team data
            possession_team_id = event['possession_team']['id']
            possession_team_name = event['possession_team']['name']
            cursor.execute(
                "INSERT INTO Team (team_id, team_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                           (possession_team_id, possession_team_name))

            cursor.execute("INSERT INTO Event_Type (event_type_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (event_type_id, event_type_name))

            # Insert position data
            if 'position' in event:
                position_id = event['position']['id']
                position_name = event['position']['name']

                cursor.execute("INSERT INTO Position (position_id, position) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (position_id, position_name))

            if 'location' in event and 'position' in event and 'duration' in event:
                # Insert into Event table
                cursor.execute("""
                            INSERT INTO Event (event_id,index, period, "timestamp", minute, second, possession, duration, location_x, location_y,under_pressure,match_id,play_pattern_id,possession_team_id,team_id,position_id,event_type_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING 
                        """, (
                event['id'], event['index'], event["period"], event['timestamp'], event['minute'], event['second'],
                event['possession'], event['duration'],
                event['location'][0], event['location'][1], False, match_id, event["play_pattern"]["id"],
                event["possession_team"]["id"], event["team"]["id"],
                event["position"]["id"], event["type"]["id"]))
            else:
                # Insert into Event table
                cursor.execute("""INSERT INTO Event (event_id,index, period, "timestamp", minute, second, possession, under_pressure, match_id, play_pattern_id, possession_team_id,team_id,event_type_id)
                                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
                                        """, (
                    event['id'], event['index'], event["period"], event['timestamp'], event['minute'], event['second'],
                    event['possession'],
                     False, match_id, event["play_pattern"]["id"],
                    event["possession_team"]["id"], event["team"]["id"],
                     event["type"]["id"]))

            # Insert into Shot table
            if event['type']['name'] == "Shot":
                shot_data = event['shot']
                if 'first_time' in shot_data:
                    cursor.execute("""INSERT INTO Shot(statsbomb_xg,start_location_x ,start_location_y ,end_location_x,end_location_y,end_location_z,outcome,first_time,technique,body_part,event_id,player_id) 
                                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT DO NOTHING
                                    """, (
                        shot_data['statsbomb_xg'], event['location'][0], event['location'][1],
                        shot_data['end_location'][0], shot_data['end_location'][1], shot_data['end_location'][2],
                        shot_data['outcome']['name'], shot_data['first_time'], shot_data['technique']['name'],
                        shot_data['body_part']['name'], event['id'], event['player']['id']))
                else:
                    cursor.execute("""INSERT INTO Shot(statsbomb_xg,start_location_x ,start_location_y ,end_location_x,end_location_y,outcome,first_time,technique,body_part,event_id,player_id) 
                                                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                                    ON CONFLICT DO NOTHING
                                                        """, (
                        shot_data['statsbomb_xg'], event['location'][0], event['location'][1],
                        shot_data['end_location'][0], shot_data['end_location'][1],
                        shot_data['outcome']['name'], str(False), shot_data['technique']['name'],
                        shot_data['body_part']['name'], event['id'], event['player']['id']))
            # If the event type is Fifty_Fifty, insert Fifty_Fifty data
            if event_type == 'Fifty_Fifty':
                fifty_fifty_data = event['fifty_fifty']
                player_id = fifty_fifty_data['player']['id']
                position_id = fifty_fifty_data['position']['id']
                outcome = fifty_fifty_data['outcome']
                counterpass = fifty_fifty_data['counterpass']
                cursor.execute("""INSERT INTO Fifty_Fifty (player_id, position_id, event_id, outcome, counterpass) 
                                    VALUES (%s, %s, %s, %s, %s) 
                                    ON CONFLICT DO NOTHING
                                                    """, (
                    player_id, position_id, event['id'], outcome, counterpass))

            # If the event type is StartingXI_LineUp, insert StartingXI_LineUp data
            if event_type == 'Starting XI':
                starting_xi_data = event['tactics']['lineup']
                for player_data in starting_xi_data:
                    player_id = player_data['player']['id']
                    position_id = player_data['position']['id']
                    player_insert_query = """
                                    INSERT INTO Player (player_id, player_name)
                                    VALUES (%s, %s)
                                    ON CONFLICT DO NOTHING
                                """
                    cursor.execute(player_insert_query, (
                        player_data['player']['id'],
                        player_data['player']['name']
                    ))
                    cursor.execute("""INSERT INTO StartingXI_LineUp (player_id, position_id, event_id) 
                                    VALUES (%s, %s, %s) 
                                    ON CONFLICT DO NOTHING
                                                                        """, (
                        player_id, position_id, event['id']))

            # If the event type is Ball_Receipt, insert Ball_Receipt data
            if event_type == 'Ball Receipt*':
                if 'ball_receipt' in event:
                    receipt_data = event['ball_receipt']['outcome']['name']
                else:
                    receipt_data = 'Complete'
                cursor.execute("""INSERT INTO Ball_Receipt (start_location_x, start_location_y, event_id, player_id, position_id, outcome) 
                                VALUES (%s, %s, %s, %s,%s,%s) 
                                ON CONFLICT DO NOTHING""", (
                    event['location'][0], event['location'][1], event['id'],
                    event['player']['id'], event['position']['id'], receipt_data))

            # If the event type is Pass, insert Pass data
            if event_type == 'Pass':
                pass_data = event['pass']
                start_location_x, start_location_y = event['location']
                end_location_x, end_location_y = pass_data['end_location']
                pass_length = pass_data['length']
                pass_angle = pass_data['angle']
                if 'body_part' in pass_data:
                    body_part = pass_data['body_part']['name']
                else:
                    body_part='unknown'
                pass_height = pass_data['height']['name']
                if 'through_ball' in pass_data:
                    through_ball = pass_data['through_ball']
                else:
                    through_ball = False

                if 'outcome' in pass_data:
                    outcome = pass_data['outcome']['name']
                else:
                    outcome = 'Complete'
                if 'recipient' in pass_data:
                    recipient_player_id = pass_data['recipient']['id']
                else:
                    recipient_player_id=None
                if 'shot assist' in pass_data:
                    shot_assist=pass_data['shot assist']
                else:
                    shot_assist=False
                if 'technique' in pass_data:
                    technique=pass_data['technique']['name']
                else:
                    technique=None
                cursor.execute(
                    "INSERT INTO Pass (start_location_x, start_location_y, end_location_x, end_location_y, pass_length, pass_angle, body_part, height, through_ball, outcome, event_id, player_id, player_recipient_id,shot_assist,technique) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s) "
                    "ON CONFLICT DO NOTHING",
                    (start_location_x, start_location_y, end_location_x, end_location_y,
                     pass_length, pass_angle, body_part, pass_height, through_ball,
                      outcome, event['id'], event['player']['id'], recipient_player_id,shot_assist,technique))

            # If the event type is Carry, insert Carry data
            if event_type == 'Carry':
                carry_data = event['carry']
                start_location_x, start_location_y = event['location']
                end_location_x, end_location_y = carry_data['end_location']

                cursor.execute("""INSERT INTO Carry (start_location_x, start_location_y, end_location_x, end_location_y, event_id) 
                                VALUES (%s, %s, %s, %s, %s) 
                                ON CONFLICT DO NOTHING""",
                    (start_location_x, start_location_y, end_location_x, end_location_y,
                     event['id']))

            # If the event type is Dribbled_Past, insert Dribbled_Past data
            if event_type == 'Dribbled Past':
                location_x, location_y = event['location']

                cursor.execute(
                    """INSERT INTO Dribbled_Past (location_x, location_y, event_id, player_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING""",
                    (location_x, location_y, event['id'], event['player']['id']))

            # If the event type is Dribble, insert Dribble data
            if event_type == 'Dribble':
                dribble_data = event['dribble']
                location_x, location_y = event['location']
                outcome = dribble_data['outcome']['name']

                cursor.execute(
                    """INSERT INTO Dribble (location_x, location_y, event_id, player_id, outcome) 
                        VALUES (%s, %s, %s, %s, %s) 
                        ON CONFLICT DO NOTHING""",
                    (location_x, location_y, event['id'], event['player']['id'],
                     outcome))

            # If the event type is Block, insert Block data
            if event_type == 'Block':
                location_x, location_y = event['location']

                cursor.execute(
                    """INSERT INTO Block (location_x, location_y, event_id, player_id) 
                            VALUES (%s, %s, %s, %s) 
                            ON CONFLICT DO NOTHING""",
                    (location_x, location_y, event['id'], event['player']['id']))
            # If the event type is Pressure, insert Pressure data
            if event_type == 'Pressure':
                location_x, location_y = event['location']

                cursor.execute(
                    """INSERT INTO Pressure (location_x, location_y, event_id, player_id) 
                        VALUES (%s, %s, %s, %s) 
                        ON CONFLICT DO NOTHING""",
                    (location_x, location_y, event['id'], event['player']['id']))

            # If the event type is Ball Recovery, insert Ball Recovery data
            if event_type == 'Ball Recovery':
                location_x, location_y = event['location']
                if 'ball_recovery' in event and 'recovery_failure' in event['ball_recovery']:
                    cursor.execute(
                        """INSERT INTO Ball_Recovery (location_x, location_y, event_id, player_id,recovery_failure) 
                        VALUES (%s, %s, %s, %s,%s) 
                        ON CONFLICT DO NOTHING""",
                        (location_x, location_y, event['id'], event['player']['id'],
                         event['ball_recovery']['recovery_failure']))
                else:
                    cursor.execute(
                        """INSERT INTO Ball_Recovery (location_x, location_y, event_id, player_id,recovery_failure) 
                        VALUES (%s, %s, %s, %s,%s) 
                        ON CONFLICT DO NOTHING""",
                        (location_x, location_y, event['id'], event['player']['id'],
                         False))
            # If the event type is Bad Behaviour, insert Bad Behaviour data
            if event_type == 'Bad Behaviour':
                card_id = event['bad_behaviour']['card']['id']
                cursor.execute(
                    """INSERT INTO Card (card_id,card_type) 
                    VALUES (%s,%s) 
                    ON CONFLICT DO NOTHING""",
                    (card_id, event['bad_behaviour']['card']['name']))

                # Insert data into Bad_Behaviour table
                cursor.execute(
                    """INSERT INTO Bad_Behaviour (event_id, card_id,player_id) 
                    VALUES (%s,%s,%s) 
                    ON CONFLICT DO NOTHING""",
                               (event['id'], card_id, event['player']['id']))

            # Insert Clearance data
            if event_type == 'Clearance':
                cursor.execute(
                    """INSERT INTO Clearance (event_id, location_x, location_y, under_pressure,body_part,player_id) 
                    VALUES (%s, %s, %s, %s,%s,%s) 
                    ON CONFLICT DO NOTHING""",
                    (event['id'], event['location'][0], event['location'][1], event['under_pressure'],
                     event['clearance']['body_part']['name'], event['player']['id']))

            # Insert Player_OFF data
            if event_type == 'Player Off':
                if 'player_off' in event:
                    permanent = event['player_off']['permanent']
                else:
                    permanent = False
                cursor.execute(
                    """INSERT INTO Player_Off (event_id,permanent,player_id) 
                    VALUES (%s, %s, %s) 
                    ON CONFLICT DO NOTHING""",
                    (event['id'], permanent, event['player']['id']))

            # Insert Duel data
            if event_type == 'Duel':
                cursor.execute(
                    """INSERT INTO Duel (event_id,player_id,duel_name,location_x ,location_y ) 
                    VALUES (%s, %s, %s,%s,%s) 
                    ON CONFLICT DO NOTHING""",
                    (
                    event['id'], event['player']['id'], event['duel']['type']['name'], event['location'][0],
                    event['location'][1]))

            # Insert Miscontrol( data
            if event_type == 'Miscontrol':
                cursor.execute(
                    """INSERT INTO Duel (event_id,player_id ,location_x ,location_y ) 
                    VALUES (%s, %s, %s,%s) 
                    ON CONFLICT DO NOTHING""",
                    (event['id'], event['player']['id'], event['location'][0], event['location'][1]))

            # Insert Interception( data
            if event_type == 'Interception':
                cursor.execute(
                    """INSERT INTO Interception (event_id,player_id ,location_x ,location_y,interception_name  ) 
                    VALUES (%s, %s, %s,%s,%s) 
                    ON CONFLICT DO NOTHING""",
                    (event['id'], event['player']['id'], event['location'][0], event['location'][1],
                     event['interception']['outcome']['name']))

            # Insert Foul_Committed( data
            if event_type == 'Foul Committed':
                cursor.execute(
                    """INSERT INTO Foul_Committed (event_id,player_id ,location_x ,location_y) 
                    VALUES (%s, %s,%s,%s) 
                    ON CONFLICT DO NOTHING""",
                    (event['id'], event['player']['id'], event['location'][0], event['location'][1]))
            # Insert Foul_Won( data
            if event_type == 'Foul Won':
                if 'under_pressure' in event:
                    under_pressure = event['under_pressure']
                else:
                    under_pressure = False
                cursor.execute(
                    """INSERT INTO Foul_Won (event_id,player_id ,location_x ,location_y,under_pressure) 
                    VALUES (%s, %s,%s,%s,%s) 
                    ON CONFLICT DO NOTHING""",
                    (event['id'], event['player']['id'], event['location'][0], event['location'][1],
                     under_pressure))

            # Insert Substitution( data
            if event_type == 'Substitution':
                player_insert_query = """
                            INSERT INTO Player (player_id, player_name)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                                                """
                cursor.execute(player_insert_query, (
                    event['substitution']['replacement']['id'],
                    event['substitution']['outcome']['name']
                ))
                cursor.execute(
                    """INSERT INTO Substitution (event_id,player_id ,player_replacement_id ,outcome) 
                    VALUES (%s, %s,%s,%s) 
                    ON CONFLICT DO NOTHING""",
                    (event['id'], event['player']['id'], event['substitution']['replacement']['id'],
                     event['substitution']['outcome']['name']))

        # Commit changes outside the loop after all events have been inserted
        connection.commit()
        print("Event Data inserted successfully.")

    except (Exception, psycopg2.Error) as error:
        # Rollback the transaction if an error occurs
        connection.rollback()
        print("Error while inserting event data", error)
        traceback.print_exc()


add_competitions(get_JSON('data/competitions.json'))

add_matches(get_JSON('data/matches/11/90.json'))
add_matches(get_JSON('data/matches/11/42.json'))
add_matches(get_JSON('data/matches/11/4.json'))
add_matches(get_JSON('data/matches/2/44.json'))

all_matches=get_all_match_ids()
for match in all_matches:
    add_lineup(get_JSON("data/lineups/"+str(match)+".json"),match)
    insert_events(get_JSON("data/events/"+str(match)+".json"),match);
