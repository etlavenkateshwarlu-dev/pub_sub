from google.cloud import pubsub_v1
import json
import csv
import time

project_id='vctbatch-45'
topic_name='employee-topic'
subscriber_name='employee-topic-sub'
csv_file='employee_1.csv'


# ---------------- PUBSUB CLIENT ----------------
publisher=pubsub_v1.PublisherClient()
topic_path=publisher.topic_path(project_id,topic_name)
count=0
boolflag=True
with open(csv_file,mode='r') as file:
    reader=csv.DictReader(file)
    for row in reader:
        count=count+1
        message=json.dumps(row).encode("utf-8")
        publisher.publish(topic_path,message)
        print(f'published ,message is  : {message}')
        time.sleep(1)

  #  if(count>2):
   #     boolflag=False
print(f'All order published ......total order are: {count}')



