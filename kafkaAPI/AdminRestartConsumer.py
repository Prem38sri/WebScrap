from kafka import KafkaConsumer
import requests as req
import time

install_home="/apps/tibco/WebScrap/"
kafka_home=install_home+"kafkaAPI/"

topic = 'topic.admin.restart'
connection = KafkaConsumer(topic,bootstrap_servers='kafka_server:9092')

def admin_stop(hostname,domain):
        url = "http://xsnw50f525a:8080/job/HawkResolver/job/AdminStop/buildWithParameters?token=1938&AdminMachine="+str(hostname)+"&Domain="+str(domain)
        r = req.get(url)
	r = str(r)
        with open(kafka_home+'kafkaLoggerReq.txt',"a") as reqwriter:
                reqwriter.write(r)
        time.sleep(30)


def admin_start(hostname,domain):
        url = "http://jenkins_host:8080/job/HawkResolver/job/AdminStart/buildWithParameters?token=1938&AdminMachine="+str(hostname)+"&Domain="+str(domain)
        r = req.get(url)
	r = str(r)
        with open(kafka_home+'kafkaLoggerReq.txt',"a") as reqwriter:
                reqwriter.write(r)

for msg in connection:
	domain = msg.value.split(",")[0]
	hostname = msg.value.split(",")[1]
	#Jenkins API Call for restarting Admin
	with open(kafka_home+'kafkaLogger.txt',"a") as writer:
		writer.write(domain+","+hostname+"\n")
	
	admin_stop(hostname,domain)
	admin_start(hostname,domain)
	
