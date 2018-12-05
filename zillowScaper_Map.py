#!bin/python3.6
#
# title             : Zillow map scraper
# description       : Scrapes by imitating a map request with a bounding
#                   : box at zoom level 17. This returns a json that
#                   : includes some basic building unit information.
# date              : 23-08-18
import csv
from os.path import exists
import zillow_library as zl
from shapely import geometry
from random import random
from tqdm import tqdm
from time import sleep
import pandas as pd
import re
import numpy as np

INFILE = './building_list/FME/bbox_FME_remain.csv'
OUTFILE = './building_list/FME/house_list_FME_2018.csv'
error_output = './building_list/FME/bbox_FME_error.csv'

# polygons of several area
boston = [(-71.25217972711778,42.283457946955764),(-70.97102961392963,42.283457946955764),
          (-70.97102961392963,42.4274577705672),(-71.25217972711778,42.4274577705672)]
new_york = [(-74.11880083532142,40.54020366007822),(-73.72237719317219,40.54020366007822),
            (-73.72237719317219,40.88561049161959),(-74.11880083532142,40.88561049161959)]
bay_area = [(-122.53480050948436,37.21884060634524),(-121.75001415877546,37.21884060634524),
            (-121.75001415877546,38.00058249529579),(-122.53480050948436,38.00058249529579)]
seatle = [(-122.6036016669864,47.28465348011332),(-122.0225608593298,47.28465348011332),
          (-122.0225608593298,48.02375034620229),(-122.6036016669864,48.02375034620229)]
washington = [(-77.34215672356156,38.74010416034949),(-76.80102843032667,38.74010416034949),
              (-76.80102843032667,39.11182467537361),(-77.34215672356156,39.11182467537361)]
chicago = [(-88.03704611472412,41.58233551938514),(-87.33962285508021,41.58233551938514),
           (-87.33962285508021,42.130312025831955),(-88.03704611472412,42.130312025831955)]
los_angeles = [(-118.50045575207021,33.604538403027064),(-117.71115527855116,33.604538403027064),
               (-117.71115527855116,34.17924862233099),(-118.50045575207021,34.17924862233099)]
austin = [(-97.92593800668382,30.13940262683782),(-97.55180611385572,30.13940262683782),
            (-97.55180611385572,30.578355017513672),(-97.92593800668382,30.578355017513672)]

# specified the polygon that you want to search
#test_polygon = geometry.Polygon(austin)
test_polygon = None
# Initiate the geohashlist going to search
if exists(INFILE):
    # spilt the search area to geohash plygons (# example: split to areas of geohash 5)
    #zl.splitSearchPolygon(test_polygon, 5, INFILE, inner = False)
    #house_list = pd.read_csv('./building_list/FME/missing_lat_lngs_fe1.csv')
    #center_list = list(zip(house_list['lat_raw'].tolist(),house_list['lng_raw'].tolist()))
    geo_list = pd.read_csv(INFILE)
    center_list = list(zip((0.5*geo_list['south']+0.5*geo_list['north']).tolist(),(0.5*geo_list['west']+0.5*geo_list['east']).tolist()))
    zl.expandSearchHash(center_list,6,INFILE,expand_neighbor = True)

# Initialize the webdriver. 
driver = zl.initDriver("./chromedriver")

# initiate output
if not exists(OUTFILE):
    # create the output csv if not exist 
    with open(OUTFILE, 'w') as wb: 
        csv.writer(wb, delimiter=',', quotechar='"').writerow(['zpid', 'hash','lat', 'lng', 'price', 'beds', 'baths', 'sqft', 'rent'])

# read the list of geohash for scraping
bbox_remain = pd.read_csv(INFILE,dtype= str,index_col = 'geohash')

# retrieve map data with support for selenium workers
for idx,row in bbox_remain.iterrows():
    # search term 
    search = [idx] + list(row)
    # start scraping
    # status code:  100000 (on list), 000010(for rent), 001000(recently sold)
    # status code can be combined: 101000 (on list + recently sold)
    to_split = zl.QueryWorker(OUTFILE, search, use_proxy = False, driver = driver,
                              status = '101000',split = True,cutoff_hash=8, thres = 1, error_output=error_output)
    # if a geohash area contain more than 500 properties, it need to split to get all result
    if to_split: 
        # split the current geohash to new geohash list
        geohash_list = zl.splitGeohash(search[0],test_polygon)
        new_hashes = pd.DataFrame.from_dict(geohash_list,orient = 'index',columns = bbox_remain.columns)
        # attach the new geohashes to the end of list
        bbox_remain = bbox_remain.append(new_hashes)
    # drop scraped
    bbox_remain.drop(idx,inplace = True)
    # save the updated the remaining geohash list
    bbox_remain.to_csv(INFILE,index= True,index_label = 'geohash')

# clean table (only for rental)
house_list = pd.read_csv(OUTFILE)

# get price digit
def translate(x):
    if 'K' in str(x):
        return 1e3*float(re.findall(r'\$(.*)K',str(x))[0].replace(',',''))
    elif 'M' in str(x):
        return 1e6*float(re.findall(r'\$(.*)M',str(x))[0].replace(',',''))
    else:
        return x

house_list['price'] = house_list['price'].map(translate)

# drop duplicates
house_list.drop_duplicates(inplace = True)

# save result
house_list.to_csv(OUTFILE, index = False)

