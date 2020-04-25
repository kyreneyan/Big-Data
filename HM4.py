import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext

def createIndex(shapefile):

    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()

    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):

    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def processTrips(pid, records):

    import csv
    import pyproj
    import shapely.geometry as geom
    

    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('neighborhoods.geojson')    
    

    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    
    for row in reader:

        p = geom.Point(proj(float(row[9]), float(row[10])))
        
        zone = findZone(p, index, zones)
        if zone:
            counts[zone] = counts.get(zone, 0) + 1
    return counts.items()


if __name__=='__main__':

    sc = SparkContext.getOrCreate()
    input_file = sys.argv[1] 

    taxi = sc.textFile(input_file)

    counts = taxi.mapPartitionsWithIndex(processTrips) \
            .reduceByKey(lambda x,y: x+y) \
            .collect()

    countsPerNeighborhood = list(map(lambda x: (zones['neighborhood'][x[0]],zones['borough'][x[0]], x[1]), counts))
    df = pd.DataFrame(countsPerNeighborhood)
    
    res=df.sort_values([1,2], ascending=[True,False]).groupby(1).head(5)

    print((res))