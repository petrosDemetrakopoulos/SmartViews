import pandas as pd
import time
import sys
import json

data = pd.read_json(sys.argv[1])
#print(data.head)
#json_data=open(sys.argv[1]).read()
#j1 = json.loads(json_data)
start_time = time.time()
if sys.argv[4] == "COUNT":
    gbRes = data.groupby(sys.argv[2])[sys.argv[3]].count()
elif sys.argv[4] == "SUM":
     gbRes = data.groupby(sys.argv[2])[sys.argv[3]].sum()
elif sys.argv[4] == "AVERAGE":
     gbRes = data.groupby(sys.argv[2])[sys.argv[3]].mean()
elif sys.argv[4] == "MAX":
     gbRes = data.groupby(sys.argv[2])[sys.argv[3]].max()
elif sys.argv[4] == "MIN":
     gbRes = data.groupby(sys.argv[2])[sys.argv[3]].min()
#print(gbRes.to_json(orient='columns'))
#print("--- %s seconds ---" % (time.time() - start_time))
print(gbRes.to_json(orient='columns'))
