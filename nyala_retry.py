import requests
import json
from datetime import datetime, timedelta
import pencilbox as pb
import pandas as pd
from pandas import DataFrame

LIMIT = 30
MINUTES = 30
x = 100
TOPICS = [
    "nyala.prod.lastmile-order_update.oms-delivered",
    "nyala.prod.lastmile-order_update.oms-delivered.retry",
]
today = datetime.utcnow() - timedelta(minutes=MINUTES)
date = today.strftime("%Y-%m-%dT%H:%M")
pd.options.display.max_colwidth = 100
b = []
c=[]
from_email = "k.harish@grofers.com"
to_email = [
    "k.harish@grofers.com",
]

def fetch_messages(LIMIT,OFFSET,date,topic):
    baseurl = "http://nyala.prod.grofer.io/api/v1/sidelined-messages/"
    params = {
        "retried": False,
        "error_code": 500,
        "limit": LIMIT,
        "offset": OFFSET,
        "created_at__gte": date,
        "topic": topic,
    }
    response = requests.get(baseurl, params)
    data = response.text
    data1 = json.loads(data)
    meta = data1['meta']
    meta1 = meta['next']
    total_count = meta['total_count']
    objects = data1['objects']

    return objects, meta1, total_count

def fetch_message_ids_from_response(response):
    message_ids = []
    for messages in response:
        message_ids.append(messages["id"])
    return message_ids

def retry_messages(message_ids):
    url = "http://nyala.prod.grofer.io/api/v1/sidelined-messages/retry/"
    response = requests.post(url, json={'ids': message_ids})
    return response

def send_mail(df):
    subject = "Nyala Retry Sidelined Messages"
    html_content = """<p>The total number of sidelined messages retried for each topic is<br></p>
                       {}""".format(
            df[
                ["NyalaTopic", "No_of_retried_messages"]
            ].to_html(justify="center", index=False)
        )
    pb.send_email(from_email, to_email, subject, html_content)

def retry_sidelined_messages_task():
    for topic in TOPICS:
        OFFSET = 0
        while True:
            response, meta_response, total_count = fetch_messages(LIMIT,OFFSET,date,topic)
            message_ids = fetch_message_ids_from_response(response)
            retry = retry_messages(message_ids)
            No_of_messages = len(message_ids)
            nyala_messages = [topic,No_of_messages]
            if len(message_ids)!= 0:
                c.append(len(message_ids))
                b.append(nyala_messages)
            if meta_response is None:
                break
            else:
                OFFSET = OFFSET + LIMIT
    print(sum(c))
    if x <= sum(c) :
        df = DataFrame (b,columns=['NyalaTopic','No_of_retried_messages'])
        mail = send_mail(df)
retry_sidelined_messages_task()