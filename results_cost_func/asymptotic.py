import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
df_cost_func_10 = pd.read_json('result_final_DefaultCostFunction_10.json', orient='records')
df_cost_func_10 = df_cost_func_10[['allTotal','bcTime','sqlTime','totalTime']]
df_cost_func_10['serverProcessingTime'] = df_cost_func_10['allTotal'] - df_cost_func_10['totalTime']
df_cost_func_50 = pd.read_json('result_final_DefaultCostFunction_50.json', orient='records')
df_cost_func_50 = df_cost_func_50[['allTotal','bcTime','sqlTime','totalTime']]
df_cost_func_50['serverProcessingTime'] = df_cost_func_50['allTotal'] - df_cost_func_50['totalTime']
df_cost_func_100 = pd.read_json(r'result_final_DefaultCostFunction_100.json', orient='records')
df_cost_func_100 = df_cost_func_100[['allTotal','bcTime','sqlTime','totalTime']]
df_cost_func_100['serverProcessingTime'] = df_cost_func_100['allTotal'] - df_cost_func_100['totalTime']
df_cost_func_150 = pd.read_json('result_final_DefaultCostFunction_150.json', orient='records')
df_cost_func_150 = df_cost_func_150[['allTotal','bcTime','sqlTime','totalTime']]
df_cost_func_150['serverProcessingTime'] = df_cost_func_150['allTotal'] - df_cost_func_150['totalTime']
df_cost_func_200 = pd.read_json(r'result_final_DefaultCostFunction_200.json', orient='records')
df_cost_func_200 = df_cost_func_200[['allTotal','bcTime','sqlTime','totalTime']]
df_cost_func_200['serverProcessingTime'] = df_cost_func_200['allTotal'] - df_cost_func_200['totalTime']
df_cost_func_250 = pd.read_json(r'result_final_DefaultCostFunction_250.json', orient='records')
df_cost_func_250 = df_cost_func_250[['allTotal','bcTime','sqlTime','totalTime']]
df_cost_func_250['serverProcessingTime'] = df_cost_func_250['allTotal'] - df_cost_func_250['totalTime']
df_cost_func_300 = pd.read_json(r'result_final_DefaultCostFunction_300.json', orient='records')
df_cost_func_300 = df_cost_func_300[['allTotal','bcTime','sqlTime','totalTime']]
df_cost_func_300['serverProcessingTime'] = df_cost_func_300['allTotal'] - df_cost_func_300['totalTime']
df_cost_func_350 = pd.read_json(r'result_final_DefaultCostFunction_350.json', orient='records')
df_cost_func_350 = df_cost_func_350[['allTotal','bcTime','sqlTime','totalTime']]
df_cost_func_350['serverProcessingTime'] = df_cost_func_350['allTotal'] - df_cost_func_350['totalTime']
df_cost_func_400 = pd.read_json(r'result_final_DefaultCostFunction_400.json', orient='records')
df_cost_func_400 = df_cost_func_400[['allTotal','bcTime','sqlTime','totalTime']]
df_cost_func_400['serverProcessingTime'] = df_cost_func_400['allTotal'] - df_cost_func_400['totalTime']
df_cost_func_450 = pd.read_json(r'result_final_DefaultCostFunction_450.json', orient='records')
df_cost_func_450 = df_cost_func_450[['allTotal','bcTime','sqlTime','totalTime']]
df_cost_func_450['serverProcessingTime'] = df_cost_func_450['allTotal'] - df_cost_func_450['totalTime']
df_cost_func_500 = pd.read_json(r'result_final_DefaultCostFunction_500.json', orient='records')
df_cost_func_500 = df_cost_func_500[['allTotal','bcTime','sqlTime','totalTime']]
df_cost_func_500['serverProcessingTime'] = df_cost_func_500['allTotal'] - df_cost_func_500['totalTime']
x = [10, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500]
y = [df_cost_func_10['allTotal'].mean(), 
df_cost_func_50['allTotal'].mean(),
df_cost_func_100['allTotal'].mean(),
df_cost_func_150['allTotal'].mean(),
df_cost_func_200['allTotal'].mean(),
df_cost_func_250['allTotal'].mean(),
df_cost_func_300['allTotal'].mean(),
df_cost_func_350['allTotal'].mean(),
df_cost_func_400['allTotal'].mean(),
df_cost_func_450['allTotal'].mean(),
df_cost_func_500['allTotal'].mean()]
resData = {'a=10': [df_cost_func_10['allTotal'].mean(), df_cost_func_10['bcTime'].mean(), df_cost_func_10['sqlTime'].mean(), df_cost_func_10['serverProcessingTime'].mean()],
			'a=50': [df_cost_func_50['allTotal'].mean(), df_cost_func_50['bcTime'].mean(), df_cost_func_50['sqlTime'].mean(), df_cost_func_50['serverProcessingTime'].mean()],
	       'a=100': [df_cost_func_100['allTotal'].mean(), df_cost_func_100['bcTime'].mean(), df_cost_func_100['sqlTime'].mean(), df_cost_func_100['serverProcessingTime'].mean()],
	       'a=150': [df_cost_func_150['allTotal'].mean(), df_cost_func_150['bcTime'].mean(), df_cost_func_150['sqlTime'].mean(), df_cost_func_150['serverProcessingTime'].mean()],
	       'a=200': [df_cost_func_200['allTotal'].mean(), df_cost_func_200['bcTime'].mean(), df_cost_func_200['sqlTime'].mean(), df_cost_func_200['serverProcessingTime'].mean()],
           'a=250': [df_cost_func_250['allTotal'].mean(), df_cost_func_250['bcTime'].mean(), df_cost_func_250['sqlTime'].mean(), df_cost_func_250['serverProcessingTime'].mean()],
           'a=300': [df_cost_func_300['allTotal'].mean(), df_cost_func_300['bcTime'].mean(), df_cost_func_300['sqlTime'].mean(), df_cost_func_300['serverProcessingTime'].mean()],
           'a=350': [df_cost_func_350['allTotal'].mean(), df_cost_func_350['bcTime'].mean(), df_cost_func_350['sqlTime'].mean(), df_cost_func_350['serverProcessingTime'].mean()],
           'a=400': [df_cost_func_400['allTotal'].mean(), df_cost_func_400['bcTime'].mean(), df_cost_func_400['sqlTime'].mean(), df_cost_func_400['serverProcessingTime'].mean()],
           'a=450': [df_cost_func_450['allTotal'].mean(), df_cost_func_450['bcTime'].mean(), df_cost_func_450['sqlTime'].mean(), df_cost_func_450['serverProcessingTime'].mean()],
           'a=500': [df_cost_func_500['allTotal'].mean(), df_cost_func_500['bcTime'].mean(), df_cost_func_500['sqlTime'].mean(), df_cost_func_500['serverProcessingTime'].mean()]
}

resTbl = pd.DataFrame(resData, columns=['a=10', 'a=50', 'a=100', 'a=150','a=200','a=250','a=300','a=350','a=400','a=450','a=500'], index=['allTotal','bcTime', 'sqlTime','serverProcessingTime'])
print(resTbl.T)
resTbl.to_csv('res.txt', header=None, index=None, sep='\t', mode='a')
blue_patch = mpatches.Patch(color='blue', label='a = 100')
plt.suptitle('Mean total time vs a-factor in Cost function', fontweight='bold')
plt.plot(x,y, color='blue')
plt.title("Total time", fontweight='bold')
plt.ylabel('Mean total time (s)')
plt.xlabel('a-factor (s)')
plt.show()