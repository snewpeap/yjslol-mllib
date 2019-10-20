from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

spark = SparkSession.builder.appName("readModel").getOrCreate()

mlDF = spark.read.parquet('../model/pca-kmeans-result')
data = mlDF.toPandas()
print(data)

color = ['black', 'red', 'blue', 'green', 'purple']     # 颜色
marker = ['8', 'o', '*', 'D', '^']                      # 图标
elev = 30                                               # 上下转
azim = 25                                               # 左右转

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
ax.view_init(elev=elev, azim=azim)                      # 视角转换

for group in range(0, 5):
    x = []
    y = []
    z = []

    for line in range(0, data.shape[0]):
        if data.prediction[line] == group:
            col = data.pcaFeatures[line]
            x.append(col[0])
            y.append(col[1])
            z.append(col[2])

    ax.scatter(x, y, z, color=color[group], marker=marker[group])

plt.show()
