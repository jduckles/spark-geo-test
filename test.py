# Run Spark client like:
# IPYTHON=1 ./bin/pyspark --master spark://hadoop10:7077 --executor-memory 10g --driver-memory 2g

from osgeo import gdal
import numpy as np
import os

DIR='/mnt/shared10/radar'
os.chdir(DIR)

def allfiles(DIR):
    return os.listdir(DIR)

def stack(files):
    out=[]
    for filen in files:
        out.append(np.array_split(gdal.Open(filen).ReadAsArray(),10))
    return out

mystack = stack( allfiles(DIR)[0:1] )

sparkstack = sc.parallelize(mystack, len(mystack)).cache()
.import centralamerica_records_table.csv central_america

gmax = sparkstack.map(lambda grid: grid.max()).reduce(lambda a, b: np.max( (a,b) ))
