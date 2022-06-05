import subprocess
from multiprocessing.pool import ThreadPool
import pandas as pd
import json
import os.path

num_processes = 5

def work(row_filtered):
	row_json = json.dumps(row_filtered)
	f = open("wrapper/%s.log"%row_filtered["code_name"], "w")
	process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=f)
	process.communicate(row_json.encode())
	print(row_filtered["senario"],row_filtered["code_name"], "finished")

cmd = ["python3","simulation.py"]

senarios = pd.read_csv("wrapper.csv",index_col=0)[:1]
senarios = senarios.fillna("None") 
senarios.insert(0,"senario",senarios.index)
senarios['code_name'] = senarios[senarios.columns].astype(str).apply(lambda x: '_'.join(x), axis = 1)

if os.path.isfile("wrapper/wrapper_finished.txt"):
	senarios_finished = pd.read_csv("wrapper/wrapper_finished.txt")
	senarios = senarios.drop(senarios_finished.senario, errors='ignore')

print(senarios)

tp = ThreadPool(num_processes)
for row in senarios.to_dict(orient="records"):
	row_filtered = {k:v for k,v in row.items() if "None"!=v}
	tp.apply_async(work,(row_filtered,))

tp.close()
tp.join()