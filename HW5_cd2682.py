from pyspark import SparkContext

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
        try:
            pickup_point = geom.Point(proj(float(row[5]), float(row[6])))
            dropoff_point = geom.Point(proj(float(row[9]), float(row[10])))
            pickup_zone_idx = findZone(pickup_point, index, zones)
            dropoff_zone_idx = findZone(dropoff_point, index, zones)
            dropoff_borough = zones.borough[dropoff_zone_idx]
            pickup_zone = zones.neighborhood[pickup_zone_idx]
        except:
            continue

        if pickup_zone:
            yield ((dropoff_borough, pickup_zone), 1)

if __name__ == '__main__':
    sc = SparkContext()
    rdd = sc.textFile('/tmp/bdm/yellow_tripdata_2011-05.csv.gz')
    counts = rdd.mapPartitionsWithIndex(processTrips) \
                .reduceByKey(lambda x,y: x+y) \
                .map(lambda x: (x[0][0], (x[1], x[0][1]))) \
                .groupByKey() \
                .map(lambda x : (x[0], sorted(x[1], reverse = True))) \
                .map(lambda x: [(x[0], x[1][i][1]) for i in range(3)]) \
                .collect()
    print(counts)
    
# saveas('')