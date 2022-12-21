import os
from google.cloud import bigquery
import urllib
from bs4 import BeautifulSoup
import yfinance as yf
import schedule
from datetime import datetime
import time

CREDS_BIGQUERY = '/creds/bigsurmining-14baacf42c48.json'
global last_run
last_run = round(time.time())
#######UPDATE BTC
def getBtcValue():
    BTC_Ticker = yf.Ticker("BTC-USD")
    BTC_Data = BTC_Ticker.history(period="1D")
    BTC_Value = BTC_Data['High'].loc[BTC_Data.index[0]]
    return BTC_Value


#UPDATE USD
def getUSDValue():
    url = "https://dolarhoy.com/cotizaciondolarblue"
    html = urllib.request.urlopen(url)
    soup = BeautifulSoup(html,"html.parser")
    tags = soup.find_all("div",class_="value")
    precios = list()
    for tag in tags:
        precios.append(tag.contents[0])
    buy = float(precios[0].replace("$",""))
    sell = float(precios[1].replace("$",""))
    return buy


def zabbix_push(puid, key, value):
    stream = os.popen(f"zabbix_sender -z '54.92.215.92'    -s {puid} -k application.{key} -o {str(value)}")
    output = stream.read()
    print(f"ID: {puid}, key: {key}, value: {value} {output[37:][:23]}")

def bigQueryRead(query):
    client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS_BIGQUERY)
    bq_response = client.query(query=f'{query}').to_dataframe()
    return bq_response

def job():
    usuariosDF = bigQueryRead(f"SELECT * FROM BD1.usuarios ORDER BY id DESC")
    for usuariosPool, inversionInicial,revShare,revShare_std, totalMined_mtd, actualHashrate, qtyAsics,activeWorkers,inactiveWorkers,totalMined_std,totalPayed_mtd,inmatureBalance,totalPayed_std,revShare_mtd,paidTodayEstimate,miningStartDay,baseTotalmined in zip(usuariosDF["usuariosPool"],usuariosDF["inversionInicial"],usuariosDF["revShare"], usuariosDF["revShare_std"], usuariosDF["totalMined_mtd"], usuariosDF["actualHashrate"], usuariosDF["qtyAsics"],usuariosDF["activeWorkers"],usuariosDF["inactiveWorkers"],usuariosDF["totalMined_std"],usuariosDF["totalPayed_mtd"],usuariosDF["inmatureBalance"],usuariosDF["totalPayed_std"],usuariosDF["revShare_mtd"],usuariosDF["paidTodayEstimate"],usuariosDF["miningStartDay"],usuariosDF["baseTotalmined"]):
        zabbix_push(usuariosPool, "dolar_blue", getUSDValue())
        zabbix_push(usuariosPool, "btc_price", getBtcValue())
        zabbix_push(usuariosPool, "valor_kwh", 11)
        zabbix_push(usuariosPool, "inversion_inicial", inversionInicial)
        zabbix_push(usuariosPool, "rev_share", revShare)
        zabbix_push(usuariosPool, "revShare_std", revShare_std)
        zabbix_push(usuariosPool, "totalMined_mtd", totalMined_mtd)
        zabbix_push(usuariosPool, "hashrate", actualHashrate)
        zabbix_push(usuariosPool, "qtyAsics", qtyAsics)
        zabbix_push(usuariosPool, "workers_active", activeWorkers)
        zabbix_push(usuariosPool, "workers_inactive", inactiveWorkers)
        zabbix_push(usuariosPool, "totalMined_std", totalMined_std)
        zabbix_push(usuariosPool, "totalPayed_mtd", totalPayed_mtd)
        zabbix_push(usuariosPool, "inmatureBalance", inmatureBalance)
        zabbix_push(usuariosPool, "totalPayed_std", totalPayed_std)
        zabbix_push(usuariosPool, "revShare_mtd", revShare_mtd)
        zabbix_push(usuariosPool, "paidTodayEstimate", paidTodayEstimate)
        zabbix_push(usuariosPool, "paidTodayEstimate", paidTodayEstimate)
        zabbix_push(usuariosPool, "baseTotalmined", baseTotalmined)
        zabbix_push(usuariosPool,"dayOfMonth", datetime.now().day)
        diasHastaHoy = int(int(((int(datetime.now().strftime('%s')) - int(miningStartDay.strftime('%s')))))/86400)
        zabbix_push(usuariosPool,"minedDaysToday", diasHastaHoy)
        print("\n")
    print("--FIN JOB--")
    global last_run
    last_run = round(time.time())

def monitor():
    global last_run
    zabbix_push("pushtozabbix001", "ping", 1)
    zabbix_push("pushtozabbix001", "last_run", last_run)
    print("--FIN MONITOR--")
job()
schedule.every(2).minutes.do(job)
schedule.every(1).minutes.do(monitor)
while True:
    schedule.run_pending()
