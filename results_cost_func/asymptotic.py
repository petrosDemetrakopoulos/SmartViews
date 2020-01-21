import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
df_cost_func_10 = pd.read_json('result_final_DefaultCostFunction_10.json', orient='records')
df_cost_func_50 = pd.read_json('result_final_DefaultCostFunction_50.json', orient='records')
df_cost_func_100 = pd.read_json(r'result_final_DefaultCostFunction_100.json', orient='records')
df_cost_func_150 = pd.read_json('result_final_DefaultCostFunction_150.json', orient='records')
df_cost_func_200 = pd.read_json(r'result_final_DefaultCostFunction_200.json', orient='records')
df_cost_func_250 = pd.read_json(r'result_final_DefaultCostFunction_250.json', orient='records')
df_cost_func_300 = pd.read_json(r'result_final_DefaultCostFunction_300.json', orient='records')
df_cost_func_350 = pd.read_json(r'result_final_DefaultCostFunction_350.json', orient='records')
df_cost_func_400 = pd.read_json(r'result_final_DefaultCostFunction_400.json', orient='records')
df_cost_func_450 = pd.read_json(r'result_final_DefaultCostFunction_450.json', orient='records')
df_cost_func_500 = pd.read_json(r'result_final_DefaultCostFunction_500.json', orient='records')
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
resData = {'CostFunc10': df_cost_func_10['allTotal'].mean(),
			'CostFunc50': df_cost_func_50['allTotal'].mean(),
	       'CostFunc100': df_cost_func_100['allTotal'].mean(),
	       'CostFunc150': df_cost_func_150['allTotal'].mean(),
	       'CostFunc200': df_cost_func_200['allTotal'].mean(),
           'CostFunc250': df_cost_func_250['allTotal'].mean(),
           'CostFunc300': df_cost_func_300['allTotal'].mean(),
           'CostFunc350': df_cost_func_350['allTotal'].mean(),
           'CostFunc400': df_cost_func_400['allTotal'].mean(),
           'CostFunc450': df_cost_func_450['allTotal'].mean(),
           'CostFunc500': df_cost_func_500['allTotal'].mean()
}

resTbl = pd.DataFrame(resData, columns=['CostFunc10', 'CostFunc50', 'CostFunc100', 'CostFunc200','CostFunc250','CostFunc300','CostFunc350','CostFunc400','CostFunc450','CostFunc500'], index=['allTotal(mean)'])
print(resTbl)
blue_patch = mpatches.Patch(color='blue', label='a = 100')
plt.suptitle('Mean total time vs a-factor in Cost function', fontweight='bold')
plt.plot(x,y, color='blue')
plt.title("Total time", fontweight='bold')
plt.ylabel('Mean total time (s)')
plt.xlabel('a-factor (s)')
plt.show()