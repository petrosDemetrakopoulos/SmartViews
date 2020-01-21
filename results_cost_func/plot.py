import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
df_cost_func_10 = pd.read_json('result_final_DefaultCostFunction_10_new_delete_after.json', orient='records')
df_cost_func_100 = pd.read_json(r'result_final_DefaultCostFunction_100_new_delete_after.json', orient='records')
df_cost_func_250 = pd.read_json(r'result_final_DefaultCostFunction_250_new_delete_after.json', orient='records')
df_cost_func_500 = pd.read_json(r'result_final_DefaultCostFunction_500_new_delete_after.json', orient='records')

resData = {'CostFunc10': [df_cost_func_10['allTotal'].mean(), 
df_cost_func_10['bcTime'].mean(),
df_cost_func_10['sqlTime'].mean(),
(df_cost_func_10['allTotal'] - df_cost_func_10['totalTime']).mean()],
'CostFunc100': [df_cost_func_100['allTotal'].mean(), df_cost_func_100['bcTime'].mean(),
df_cost_func_100['sqlTime'].mean(),
(df_cost_func_100['allTotal'] - df_cost_func_100['totalTime']).mean()],
'CostFunc250': [df_cost_func_250['allTotal'].mean(), df_cost_func_250['bcTime'].mean(),
df_cost_func_250['sqlTime'].mean(),
(df_cost_func_250['allTotal'] - df_cost_func_250['totalTime']).mean()],
'CostFunc500': [df_cost_func_500['allTotal'].mean(), df_cost_func_500['bcTime'].mean(),
df_cost_func_500['sqlTime'].mean(),
(df_cost_func_500['allTotal'] - df_cost_func_500['totalTime']).mean()]
}

resTbl = pd.DataFrame(resData, columns=['CostFunc10', 'CostFunc100','CostFunc250','CostFunc500'], index=['allTotal(mean)','bcTime(mean)', 'sqlTime(mean)', 'serverProcessingTime(mean)'])
print(resTbl)
red_patch = mpatches.Patch(color='red', label='a = 10')
blue_patch = mpatches.Patch(color='blue', label='a = 100')
green_patch = mpatches.Patch(color='green', label='a = 250')
black_patch = mpatches.Patch(color='black', label='a = 500')
plt.suptitle('Materialization time vs # of view materialization request', fontweight='bold')
plt.subplot(221)
plt.plot(range(0,100),df_cost_func_10['allTotal'], color='red')
plt.plot(range(0,100),df_cost_func_100['allTotal'], color='blue')
plt.plot(range(0,100),df_cost_func_250['allTotal'], color='green')
plt.plot(range(0,100),df_cost_func_500['allTotal'], color='black')
plt.title("Total time", fontweight='bold')
plt.ylabel('Total time (s)')
plt.xlabel('#of view materialization request')

plt.subplot(222)
plt.plot(range(0,100),df_cost_func_10['bcTime'], color='red')
plt.plot(range(0,100),df_cost_func_100['bcTime'], color='blue')
plt.plot(range(0,100),df_cost_func_250['bcTime'], color='green')
plt.plot(range(0,100),df_cost_func_500['bcTime'], color='black')
plt.title("Blockchain time", fontweight='bold')
plt.ylabel('Blockchain time (s)')
plt.xlabel('#of view materialization request')

plt.subplot(223)
plt.plot(range(0,100),df_cost_func_10['sqlTime'], color='red')
plt.plot(range(0,100),df_cost_func_100['sqlTime'], color='blue')
plt.plot(range(0,100),df_cost_func_250['sqlTime'], color='green')
plt.plot(range(0,100),df_cost_func_500['sqlTime'], color='black')
plt.title("SQL time", fontweight='bold')
plt.ylabel('SQL time (s)')
plt.xlabel('#of view materialization request')

plt.subplot(224)
plt.plot(range(0,100),df_cost_func_10['allTotal'] - df_cost_func_10['totalTime'], color='red')
plt.plot(range(0,100),df_cost_func_100['allTotal'] - df_cost_func_100['totalTime'], color='blue')
plt.plot(range(0,100),df_cost_func_250['allTotal'] - df_cost_func_250['totalTime'], color='green')
plt.plot(range(0,100),df_cost_func_500['allTotal'] - df_cost_func_500['totalTime'], color='black')
plt.title("Server processing time", fontweight='bold')
plt.ylabel('Server processing time (s)')
plt.xlabel('#of view materialization request')
plt.legend(loc='lower left', bbox_to_anchor=(1.03, 0),   # Position of legend
           borderaxespad=0.1,
           handles=[red_patch, blue_patch, green_patch, black_patch])
plt.tight_layout()
plt.subplots_adjust(top=0.89)
plt.show()