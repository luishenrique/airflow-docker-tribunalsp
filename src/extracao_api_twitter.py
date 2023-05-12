from datetime import datetime, timedelta
import os
import requests
import json


TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
query = "data science"

tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

url_raw = f"https://labdados.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}{start_time}{start_time}".format(query, tweet_fields, user_fields, start_time, end_time)

bearer_token = "AAAAAAAAAAAAAAAAAAAAABX9nAEAAAAAnwI3ll9MBtsTygfjLK%2FW5DZxTZ4%3DN9XHMdoSXwDC2QOpM1MV0uCT7aFOe6bzXRUZhMiVrpuHgFo5nx"

headers = {"Authorization": "Bearer {}".format(bearer_token)}
response = requests.Request("GET", url_raw, headers=headers)

json_response = response.json()

print(json.dumps(json_response, indent=4, sort_keys=True))

while "next_token" in json_response.get("meta",{}):
    next_token = json_response['meta']['next_token']
    url = f"{url_raw}&next_token={next_token}"
    response = requests.Request("GET", url, headers=headers)
    json_response = response.json()
    print(json.dumps(json_response, indent=4, sort_keys=True))
