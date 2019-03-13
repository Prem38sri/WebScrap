import pandas as pd
import requests as req
from kafka import KafkaConsumer
from kafka import KafkaProducer
import time

install_home = "/apps/tibco/WebScrap/"
kafka_home = install_home+"kafkaAPI/"
kafka_log = kafka_home+"Errorfiles.txt"
topic = 'topic.hawk.restart'
topicprd = 'topic.report.invoke'
connection = KafkaConsumer(topic,bootstrap_servers='kafka_server:9092')
produceradm = KafkaProducer(bootstrap_servers='kafka_server:9092')

def hawk_stop(hostname,domain):
        url = "http://jenkins_host:8080/job/HawkResolver/job/StopHawk/buildWithParameters?token=1938&HawkMachine="+str(hostname)+"&Domain="+str(domain)
        r = req.get(url)
        r = str(r)
        with open(kafka_home+'kafkaLoggerReq.txt',"a") as reqwriter:
                reqwriter.write(r)
        time.sleep(15)


def hawk_start(hostname,domain):
        url = "http://jenkins_host:8080/job/HawkResolver/job/StartHawk/buildWithParameters?token=1938&HawkMachine="+str(hostname)+"&Domain="+str(domain)
        r = req.get(url)
        r = str(r)
        with open(kafka_home+'kafkaLoggerReq.txt',"a") as reqwriter:
                reqwriter.write(r)

for msg in connection:
	errorfile = msg.value.split(",")[0]
	adminerrorfile = msg.value.split(",")[1]
	with open(kafka_log,'a') as writer:
		writer.write(errorfile+","+adminerrorfile)
	df_allerror=pd.read_csv(errorfile)
	df_adminerror=pd.read_csv(adminerrorfile)
	df_allerror_filter = df_allerror.query('OS=="Windows"')
	print df_adminerror.iterrows
	print "\n\n"
	for index,row in df_adminerror.iterrows():
		dom = row["Domain"]
		dom = dom.strip()
		url = row["URL"]
		url = url.strip()
		print dom+" and "+url
		df_allerror_filter = df_allerror_filter[(df_allerror_filter.Domain != dom) & (df_allerror_filter.URL != url)]
	
	df_allerror_filter = df_allerror_filter.drop_duplicates(subset='Machine', keep='last')
	
	for index,eachrow in df_allerror_filter.iterrows():
		hostname = eachrow["Machine"]
		hostname = hostname.strip()
		domain = eachrow["Domain"].strip()
		hawk_stop(hostname,domain)
		hawk_start(hostname,domain)	
	message=errorfile+","+adminerrorfile
	produceradm.send(topicprd,value=message).get(timeout=60)	
