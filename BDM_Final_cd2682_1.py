from pyspark import SparkContext
from pyspark.sql import SQLContext

# remove emoji in the txt
def deEmojify(inputString):
    return inputString.encode('ascii', 'ignore').decode('ascii')
# reference: https://stackoverflow.com/questions/33404752/removing-emojis-from-a-string-in-python

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

# read in drug-related words into a list
def process_drug_words(txtfile):
    import csv
    drug_words=[]
    with open(txtfile) as file:
        reader = csv.reader(file)
        for row in reader:
            drug_words.append(row)
    return drug_words

# find the census_tract for each drug_related tweet and yield ((census_tract, population),1)
def process_tweet_zone(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom
    import re
    
    # gather the two drug_related words lists into one list 'drug_words'
    drug_words = process_drug_words('drug_illegal.txt') + process_drug_words('drug_sched2.txt')
    # define the punctuations will be replaced by ' '
    punc = '[,.!?@#$%^&*-_+=\/|]'
    
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('500cities_tracts.geojson')    
    
    # the tweet.csv should be split into columns by '|'
    reader = csv.reader(records, delimiter='|')

    for row in reader:
        drug_related = set()
        try:
            tweet_point = geom.Point(proj(float(row[2]), float(row[1])))
            tweet_zone_idx = findZone(tweet_point, index, zones)
            tweet_zone = zones.plctract10[tweet_zone_idx] # census_tract
            zone_pop = zones.plctrpop10[tweet_zone_idx] # population
        except:
            continue

        # process the tweet txt
        processed_tweet = deEmojify(row[5]).lower() #remove emoji & lowercase words
        processed_tweet = re.sub(punc, ' ', processed_tweet) #remove punctuations
        processed_tweet = ' '+ processed_tweet +' ' # add ' ' at the beginning and end of one tweet
        
        # find drug_related words in the tweet
        for words in drug_words:
            for i in words:
                if ' '+ i +' ' in processed_tweet: 
                # add ' ' at the beginning and end of a drug_word to make sure it's in the middle of sentences instead of a word
                    drug_related.add(1)
     
        if tweet_zone:    
            if 1 in drug_related:
                yield ((tweet_zone, zone_pop), 1) # yield the census_tract of drug_related tweet and its population
                
if __name__ == '__main__':
    sc = SparkContext()
    rdd = sc.textFile('/tmp/bdm/tweets-100m.csv')
    # add up the same census_tract's tweets' number
    # nomalize the tweets' number by divided by tract's population
    # sort the result
    counts = rdd.mapPartitionsWithIndex(process_tweet_zone) \
                .reduceByKey(lambda x,y: x+y) \
                .map(lambda x: (x[0][0], x[1]/x[0][1])) \
                .sortByKey() \
                .collect()
   
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame([(str(tup[0]), float(tup[1])) for tup in counts], ['plctract10 ID', 'Nomalized number of tweets'])
    df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save('BDM_Final_cd2682_1_result')