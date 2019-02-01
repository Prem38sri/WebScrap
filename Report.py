import os
import pandas as pd
import datetime
from kafka import KafkaConsumer
import selenium
from selenium import webdriver
import time
from pyvirtualdisplay import Display

topic="topic.report.invoke"
connection = KafkaConsumer(topic,bootstrap_servers='kafka_server:9092')
install_home="/apps/tibco/WebScrap/"
logdir=install_home
def callme(data_filter_final):
	with open(install_home+"exception.list","r") as filer:
		line = filer.readlines()
	for i in line:
		domain = i.split(",")[0].strip()
		machine = i.split(",")[1].strip()
		data_filter_final = data_filter_final[(data_filter_final.Domain != domain) & (data_filter_final.Machine != machine)]
	
	with open("email.html","w") as er:
		er.write("<HTML>\n")
		er.write("\n")
		er.write('<title>HAWK AGENT REPORT</title></head><style type="text/css" media="screen">@import url( style.css );</style>')
		er.write("\n")
		er.write('<BODY bgcolor="#CCCCCC" leftmargin="0" TOPMARGIn="40">')
		er.write("\n")
		er.write('<h1 Align=center><font size=25 color="#DC143C">HAWK AGENT REPORT</h1>')
		er.write("\n")
		er.write('<table style="width:100%" border="1">')
		er.write("\n")
		er.write('<tr><th>Domain</th><th>URL</th><th>Machine</th><th>Status</th><th>OS</th></tr>')
		er.write("\n")
		for index,rows in data_filter_final.iterrows():
			er.write("<tr><td>"+rows["Domain"]+"</td><td>"+rows["URL"]+"</td><td>"+rows["Machine"]+"</td><td>"+rows["Status"]+"</td><td>"+rows["OS"]+"</td></tr>")
			er.write("\n")
		er.write('</table>')
		er.write("\n")
		er.write('</BODY>')
		er.write("\n")
		er.write('</HTML>')
	
	os.system(install_home+"SendMail.sh")
		
		
	

def reportgen(listurl,data_filter_final):
    for j in listurl:
            domain = j.split(",")[0].strip()
            url = j.split(",")[1].strip()
            try:
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
                                    machine = machine.get_attribute("innerHTML").strip()
                                    status = status.rsplit('/')[-1]
                                    if status.rsplit('/')[-1] == 'dead_status.gif' and osname == 'Windows':
                                            df = pd.DataFrame({'Domain':[domain],'URL':[url],'Machine':[machine],'Status':[status],'OS':[osname]})
                                            data_filter_final = data_filter_final.append(df)
                    driver.close()
                    driver.quit()
            except selenium.common.exceptions.NoSuchElementException as e:
                    continue

    callme(data_filter_final)

for msg in connection:
    time.sleep(600)
    errorfile_msg=msg.value.split(",")[0]
    adminerror_msg=msg.value.split(",")[1]
    errorfile=errorfile_msg
    adminerror=adminerror_msg
    FHE = open("testfile.txt","w")

    data = pd.read_csv(errorfile)
    dataerror = pd.read_csv(adminerror)

    listurl = []

    data_filter_win = data.query('OS=="Windows"')
    data_filter_final = data.query('OS!="Windows"')

    for index,itr in data_filter_win.iterrows():
        FHE.write(itr["Domain"]+","+itr["URL"]+"\n")
        listurl.append(itr["Domain"]+","+itr["URL"])

    for index,itra in dataerror.iterrows():
        FHE.write(itra["Domain"]+","+itra["URL"]+"\n")
        listurl.append(itra["Domain"]+","+itra["URL"])

    FHE.close()
    reportgen(listurl,data_filter_final)

