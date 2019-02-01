import json
import time
import re
import datetime
import selenium
from selenium import webdriver
from pyvirtualdisplay import Display
from kafka import KafkaProducer

install_home='/apps/tibco/WebScrap/'
inputjson = install_home+'admin.json'

adminerrorfound = "False"

produceradm = KafkaProducer(bootstrap_servers='kafka_server:9092')
destinationadm = 'topic.admin.restart'
try:
        with open(inputjson,'r') as admin:
                admindata = json.load(admin)
except IOError as e:
        print "file not present"
        print e
# file open for output

timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

outputfile = install_home+"tmp/output.csv"+"-"+timestamp
outputerrorfile = install_home+"tmp/error.csv"+"-"+timestamp
adminerrorfile = install_home+"tmp/adminerror.csv"+"-"+timestamp
FH = open(outputfile,"w")
FHE =  open(outputerrorfile,"w")
FHAE = open(adminerrorfile,"w")

FH.write("Domain,URL,Machine,Status,OS\n")
FHE.write("Domain,URL,Machine,Status,OS\n")
FHAE.write("Domain,URL\n")

for domain in admindata:
        for url in admindata[domain]['admin']:
                try:
			print url
                        display = Display(visible=0, size=(800, 800))
                        display.start()
                        chrome_options = webdriver.ChromeOptions()
                        chrome_options.add_argument('--no-sandbox')
                        driver = webdriver.Chrome('/apps/tibco/Report/lib/chromedriver', chrome_options=chrome_options)
                        driver.get(url+"/administrator/servlet/tibco_administrator")
                        driver.find_element_by_name("UserName").send_keys("your_user_for_admin")
                        driver.find_element_by_name("Password").send_keys("your_password_for_admin")
                        driver.find_element_by_name("Login").click()
                        driver.get_screenshot_as_file("xyz.png")
                        result1 = driver.find_element_by_xpath("/html/frameset/frame[2]")
                        driver.switch_to_frame(result1)
                        result2 = driver.find_element_by_xpath("/html/frameset/frame[2]")
                        driver.switch_to_frame(result2)
                        result3 = driver.find_element_by_xpath("/html/body/form/table/tbody/tr[2]/td/table/tbody/tr/td/table/tbody/tr/td/table/tbody/tr[4]/td/table/tbody")
                        i=0
                        machine_count = 0
                        dead_count = 0
                        for row in result3.find_elements_by_xpath("./tr"):
                                i = i + 1
                                if i > 1:
                                        machine_count = machine_count + 1
                                        machine = row.find_element_by_xpath("./td[2]/table/tbody/tr/td[2]/a")
                                        status = row.find_element_by_xpath("./td[4]")
                                        status = status.find_element_by_tag_name('img')
                                        status = status.get_attribute('src')
                                        osname = row.find_element_by_xpath("./td[6]/span")
                                        osname = osname.get_attribute("innerHTML")
                                        osname = osname.split(" ")[0].rstrip()
                                        #print osname.split(" ")[0]
                                        FH.write(domain)
                                        FH.write(",")
                                        FH.write(url)
                                        FH.write(",")
                                        FH.write(machine.get_attribute("innerHTML").strip())
                                        FH.write(",")
                                        FH.write(status.rsplit('/')[-1])
                                        FH.write(",")
                                        FH.write(osname)
                                        FH.write("\n")
                                        if status.rsplit('/')[-1] == 'dead_status.gif':
                                                dead_count = dead_count + 1
                                                FHE.write(domain)
                                                FHE.write(",")
                                                FHE.write(url)
                                                FHE.write(",")
                                                FHE.write(machine.get_attribute("innerHTML").strip())
                                                FHE.write(",")
                                                FHE.write(status.rsplit('/')[-1])
                                                FHE.write(",")
                                                FHE.write(osname)
                                                FHE.write("\n")
                        driver.close()
                        driver.quit()
                        print "machine count is "+str(machine_count)+" and dead count is"+str(dead_count)
                        if machine_count == dead_count:
				adminerrorfound = "True"
				FHAE.write(domain+","+url)
                        	host = url.split("http://")[-1].split(':')[0]
				message = str(domain)+","+str(host)
				produceradm.send(destinationadm,value=message).get(timeout=60)
				print domain+url+"machine count is "+str(machine_count)+" and dead count is same"+str(dead_count)
                        
                except selenium.common.exceptions.NoSuchElementException as e:
                        adminerrorfound = "True"
			FHAE.write(domain+","+url)
			print domain
                        print "Error in -"+url+"check if url is up, user and access"
                        print e
			host = url.split("http://")[-1].split(':')[0]
			print host
			message = str(domain)+","+str(host)
			print type(message)
			produceradm.send(destinationadm,value=message).get(timeout=60)
                        driver.close()
			driver.quit()
			continue

		except selenium.common.exceptions.TimeoutException as e:
			adminerrorfound = "True"
                        FHAE.write(domain+","+url)
                        print domain
                        print "Error in -"+url+"check if url is up, user and access"
                        print e
                        host = url.split("http://")[-1].split(':')[0]
                        print host
                        message = str(domain)+","+str(host)
                        print type(message)
                        produceradm.send(destinationadm,value=message).get(timeout=60)
                        driver.close()
                        driver.quit()
                        continue
		except selenium.common.exceptions.StaleElementReferenceException as e:
			adminerrorfound = "True"
                        FHAE.write(domain+","+url)
                        print domain
                        print "Error in -"+url+"check if url is up, user and access"
                        print e
                        host = url.split("http://")[-1].split(':')[0]
                        print host
                        message = str(domain)+","+str(host)
                        print type(message)
                        produceradm.send(destinationadm,value=message).get(timeout=60)
                        driver.close()
                        driver.quit()
                        continue
FH.close()
FHE.close()
FHAE.close()


##send message to resolve hawk issues:

producerhawk = KafkaProducer(bootstrap_servers='kafka_server:9092')
destinationhawk = 'topic.hawk.restart'
message = str(outputerrorfile)+","+str(adminerrorfile)
producerhawk.send(destinationhawk,value=message).get(timeout=60)

