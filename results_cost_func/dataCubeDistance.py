import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
datacubeDistance = pd.read_json('result_final_DataCubeDistance_1.json', orient='records')
datacubeDistanceOptimized = pd.read_json(r'result_final_DataCubeDistance_optimized_1.json', orient='records')

resData = {'datacubeDistance': [datacubeDistance['allTotal'].mean(), 
datacubeDistance['bcTime'].mean(),
datacubeDistance['sqlTime'].mean(),
(datacubeDistance['allTotal'] - datacubeDistance['totalTime']).mean()],
'datacubeDistanceOptimized': [datacubeDistanceOptimized['allTotal'].mean(), datacubeDistanceOptimized['bcTime'].mean(),
datacubeDistanceOptimized['sqlTime'].mean(),
(datacubeDistanceOptimized['allTotal'] - datacubeDistanceOptimized['totalTime']).mean()]}

resTbl = pd.DataFrame(resData, columns=['datacubeDistance', 'datacubeDistanceOptimized'], index=['allTotal(mean)','bcTime(mean)', 'sqlTime(mean)', 'serverProcessingTime(mean)'])
print(resTbl)
red_patch = mpatches.Patch(color='red', label='Data cube distance')
blue_patch = mpatches.Patch(color='blue', label='Data cube distance optimized')
plt.suptitle('Materialization time vs # of view materialization request', fontweight='bold')
plt.subplot(221)
plt.plot(range(0,100),datacubeDistance['allTotal'], color='red')
plt.plot(range(0,100),datacubeDistanceOptimized['allTotal'], color='blue')
plt.title("Total time", fontweight='bold')
plt.ylabel('Total time (s)')
plt.xlabel('#of view materialization request')

plt.subplot(222)
plt.plot(range(0,100),datacubeDistance['bcTime'], color='red')
plt.plot(range(0,100),datacubeDistanceOptimized['bcTime'], color='blue')
plt.title("Blockchain time", fontweight='bold')
plt.ylabel('Blockchain time (s)')
plt.xlabel('#of view materialization request')

plt.subplot(223)
plt.plot(range(0,100),datacubeDistance['sqlTime'], color='red')
plt.plot(range(0,100),datacubeDistanceOptimized['sqlTime'], color='blue')
plt.title("SQL time", fontweight='bold')
plt.ylabel('SQL time (s)')
plt.xlabel('#of view materialization request')

plt.subplot(224)
plt.plot(range(0,100),datacubeDistance['allTotal'] - datacubeDistance['totalTime'], color='red')
plt.plot(range(0,100),datacubeDistanceOptimized['allTotal'] - datacubeDistanceOptimized['totalTime'], color='blue')
plt.title("Server processing time", fontweight='bold')
plt.ylabel('Server processing time (s)')
plt.xlabel('#of view materialization request')
plt.legend(loc='lower left', bbox_to_anchor=(1.03, 0),   # Position of legend
           borderaxespad=0.1,
           handles=[red_patch, blue_patch])
plt.tight_layout()
plt.subplots_adjust(top=0.89)
plt.show()