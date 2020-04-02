import threading
import time
import pandas as pd
import requests

dataset_url = 'https://raw.githubusercontent.com/PetrNovak96/ph_web_cl/master/dataset_stary.csv'
df = pd.read_csv(dataset_url)

def uses_https(domain):
    url = 'https://' + domain
    try:
        r = requests.get(url,timeout=3)
        r.raise_for_status()
    except requests.exceptions.RequestException as err:
        print("0:\t\t{}".format(domain[:80]))
        return 0
    print("1:\t\t{}".format(domain[:80])) 
    return 1

def apply_function(dataframe,column_name,new_column_name,function):
    dataframe[new_column_name] = dataframe[column_name].apply(function)
    return dataframe

def work(i):
    frames[i] = df.iloc[1000*i:1000*(i+1),:]
    apply_function(frames[i],'domain','usesHttps',uses_https)
    frames[i].to_csv('export_'+ str(i+1) + '.csv', sep=',', encoding='utf-8')
    
threads = []
frames = [0 for _ in range(100)]

for i in range(100):
    t = threading.Thread(target=work,args=[i])
    threads.append(t)
    t.start()

for thread in threads:
    thread.join()

df2 = pd.concat(frames)
df2.to_csv('final_export.csv', sep=',', encoding='utf-8', index = False)