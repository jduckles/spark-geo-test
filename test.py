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
    for filen in files:
        out=list()
        out.append(gdal.Open(filen).ReadAsArray())
    return np.dstack(out)

mystack = stack(allfiles(DIR)[1:1000])

sparkstack = sc.parallelize(mystack).cache()

sparkstack.map(lambda grid: grid.max()).reduce(lambda a, b: np.max((a,b)))
