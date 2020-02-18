import requests
import pymysql
import json

session = requests.Session()
match_ids = []
match_id = 2078532827
for i in range(0, 20000):
    current_match = match_id
    match_ids.append(current_match)
    i+=1
    match_id+=1

# Open a connection to MySQL server
connection = pymysql.connect(host='host', user='user',password='password',db='mydb', charset='utf8')
cur = connection.cursor() # Create new cursor
cur.execute('USE mydb')

for match_id in match_ids:
    url = "https://api.opendota.com/api/matches/{}".format(match_id)
    response = session.get(url).content.decode("utf-8")
    match=json.loads(response)
    chat = match.get('chat')
    if chat:
        for message in chat:
            if message :
                try:
                    query = """INSERT INTO match_chat(match_id, player_slot, chat_key, slot, radiant_win)
                    VALUES (%s,%s,%s,%s, %s)"""      
                    match_id = match.get('match_id')
                    player_slot = message.get('player_slot')
                    key = message.get('key')
                    slot = message.get('slot')
                    radiant_win = match.get('radiant_win')
                    query_data = (match_id, player_slot, key, slot, radiant_win)
                    cur.execute(query, query_data)
                    connection.commit()                        
                except KeyError:
                    continue                    

# Close database connection
connection.close()