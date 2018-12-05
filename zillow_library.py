import copy
import csv
import json
import os
import random
from multiprocessing import Pool
from time import sleep
import re

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from geohash import bbox, encode,expand,neighbors
from osgeo import gdal, osr
from polygon_geohasher.polygon_geohasher import polygon_to_geohashes
from pyproj import Proj, transform
from requests import ConnectionError
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from shapely.geometry import Point, Polygon

#decls
NUMBER_OF_WORKERS = 4
USER_AGENT_FILE = 'user_agent.txt'
PROXY_FILE = 'proxy.txt'
PROXY_PORTS = ['1080', '1085', '1090']
PROXY_USERNAME = ''
PROXY_PASSWORD = ''

# DECIMAL of geohash level
DECIMAL = ['0','1','2','3','4','5','6','7','8','9','b','c','d','e','f','g',
           'h','j','k','m','n','p','q','r','s','t','u','v','w','x','y','z']

# dynamic base strings (mordified)
BASE_URL = 'http://www.zillow.com/search/GetResults.htm?spt=homes&status=%s&lt=111101&ht=111111&pr=,&mp=,&bd=0%%2C&ba=0%%2C&sf=,&lot=0%%2C&yr=,&singlestory=0&hoa=0%%2C&pho=0&pets=0&parking=0&laundry=0&income-restricted=0&pnd=0&red=0&zso=0&days=any&ds=all&pmf=1&pf=1&sch=100111&zoom=17&rect=%s,%s,%s,%s&p=%s&sort=globalrelevanceex&search=maplist&listright=true&isMapSearch=true&zoom=17'
BASE_REQUEST_HEADER = {
    'Host': 'www.zillow.com',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'X-Requested-With': 'XMLHttpRequest',
    'Referer': 'http://www.zillow.com/homes/for_sale/globalrelevanceex_sort/%s,%s,%s,%s_rect/11_zm/',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0'}

""" This block is for Geographical Manipulation """

# parse and transfer zipcode store in int
def cleanZipcode(zipcode):
    if zipcode > 100000 or zipcode < 1000:
        return None
    elif zipcode < 10000:
        return '0' + str(zipcode)
    else:
        return str(zipcode)

# split the search areas into geohash aras
def splitSearchPolygon(test_polygon, level, file_name, inner = False):
    # get the geohash areas of a polygon
    geohash_list = list(polygon_to_geohashes(test_polygon, level, inner = inner))

    # get the bbox of geohash areas:
    bbox_list = [bbox(gh) for gh in geohash_list]

    # write the map data
    if file_name is not None:
        with open(file_name, 'w') as wb:
            writer = csv.writer(wb,delimiter=',')
            # rite the header
            writer.writerow(['geohash','west','south','east','north','west_refer','south_refer','east_refer','north_refer'])
            # write the body
            for i, b in enumerate(bbox_list):
                w = format(b['w'],'.6f')
                s = format(b['s'],'.6f')
                e = format(b['e'],'.6f')
                n = format(b['n'],'.6f')
                writer.writerow([geohash_list[i],w,s,e,n,w,s,e,n])
    return geohash_list, bbox_list

# expand geohash from center
def expandSearchHash(center, level,file_name, expand_neighbor = True):
    with open(file_name, 'w') as wb:
        writer = csv.writer(wb,delimiter=',')
        # rite the header
        writer.writerow(['geohash','west','south','east','north','west_refer','south_refer','east_refer','north_refer'])
        # write the body
        for lat, lng in center:
            ghash = encode(lat,lng,precision=level)
            if expand_neighbor:
                ghash_list = neighbors(ghash)
            else:
                ghash_list = [ghash]
            for gh in ghash_list:
                b = bbox(gh)
                w = format(b['w'],'.6f')
                s = format(b['s'],'.6f')
                e = format(b['e'],'.6f')
                n = format(b['n'],'.6f')
                writer.writerow([gh,w,s,e,n,w,s,e,n])
    # clean table
    geo_list = pd.read_csv(file_name,dtype = str).drop_duplicates(subset=['geohash'])
    # save the new file
    geo_list.to_csv(file_name,index = False)
    return None
    
# save the score_map to GTIFF
def saveScore(score_map, dst_filename, x_min,x_max,y_min,y_max):
    x_pixels = score_map.shape[1]  # number of pixels in x
    y_pixels = score_map.shape[0]  # number of pixels in y
    driver = gdal.GetDriverByName('GTiff')
    outds = driver.Create(dst_filename,x_pixels, y_pixels, 1,gdal.GDT_Float32)
    outds.GetRasterBand(1).WriteArray(score_map)
    # get the reference raster profile to data0
    #data0 = gdal.Open(ref_file)
    #  get GeoTranform from existed 'data0'
    x_res= (x_max - x_min)/x_pixels
    y_res= (y_max - y_min)/y_pixels
    geotrans= (x_min,x_res, 0, y_min,0, y_res)
    # get projection
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(32618)
    # save to destination
    outds.SetGeoTransform(geotrans)
    outds.SetProjection(srs.ExportToWkt())
    # Cache the profile
    outds.FlushCache()
    outds=None

    return

# split the geohash polygon
def splitGeohash(geohash, boundary_polygon):
    sub_list = {}
    # get the sub geohash
    subhashes = [geohash + d for d in DECIMAL]
    for subhash in subhashes:
        b = bbox(subhash)
        w,s,e,n = b['w'],b['s'],b['e'],b['n']
        w = format(w,'.6f')
        s = format(s,'.6f')
        e = format(e,'.6f')
        n = format(n,'.6f')
        if boundary_polygon is None:
            sub_list[subhash] = [w,s,e,n,w,s,e,n]
        # if not completed irrelavent, the append the gohash
        elif not boundary_polygon.disjoint(Polygon([(b['w'],b['s']),(b['e'],b['s']),(b['e'],b['n']),(b['w'],b['n'])])):
            sub_list[subhash] = [w,s,e,n,w,s,e,n]

    return sub_list

# transform coordinate
def transformCoordinate(lat,lng, in_proj = 'epsg:4326',out_proj = 'epsg:3857'):
    # make projection and transformation
    outProj = Proj(init= out_proj)
    inProj = Proj(init= in_proj)
    return transform(inProj,outProj,np.array(lng),np.array(lat))

""" This block is for related functions of QueryWorker """

# initiate driver
def initDriver(file_path):
    # Starting maximized fixes https://github.com/ChrisMuir/Zillow/issues/1
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(executable_path=file_path, 
                            chrome_options=options)
    driver.wait = WebDriverWait(driver, 10)
    return(driver)

# Helper function for checking for the presence of a web element.
def _isElementDisplayed(driver, elem_text, elem_type):
    if elem_type == "class":
        try:
            out = driver.find_element_by_class_name(elem_text).is_displayed()
        except (NoSuchElementException, TimeoutException):
            out = False
    elif elem_type == "css":
        try:
            out = driver.find_element_by_css_selector(elem_text).is_displayed()
        except (NoSuchElementException, TimeoutException):
            out = False
    else:
        raise ValueError("arg 'elem_type' must be either 'class' or 'css'")
    return(out)

# If captcha page is displayed, this function will run indefinitely until the 
# captcha page is no longer displayed (checks for it every 100 seconds).
# Purpose of the function is to "pause" execution of the scraper until the 
# user has manually completed the captcha requirements.
def _pauseForCaptcha(driver):
    while True:
        sleep(30)
        if not _isElementDisplayed(driver, "captcha-container", "class"):
            break

# Check to see if the page is currently stuck on a captcha page. If so, pause 
# the scraper until user has manually completed the captcha requirements.
def checkForCaptcha(driver):
    if _isElementDisplayed(driver, "captcha-container", "class"):
        print("\nCAPTCHA!\n"\
            "Manually complete the captcha requirements.\n"\
            "Once that's done, if the program was in the middle of scraping "\
            "(and is still running), it should resume scraping after ~30 seconds.")
        _pauseForCaptcha(driver)
        return True
    else:
        return False

# open the website on selenium driver
def navigateToWebsite(driver, site):
    driver.get(site)
    # Check to make sure a captcha page is not displayed.
    after_captcha = checkForCaptcha(driver)
    while after_captcha:
        # after captcha re-submit the request
        driver.get(site)
        after_captcha = checkForCaptcha(driver)

# retrive file contents
def retrieveFileContents(user_agent, proxy):
    if ('USER_AGENT_FILE' in globals()):
        with open(USER_AGENT_FILE, 'r') as lines: [user_agent.append(line.rstrip('\n')) for line in lines]

    if ('PROXY_FILE' in globals()):
        with open(PROXY_FILE, 'r') as lines: [proxy.append(line.rstrip('\n')) for line in lines]

# generate list of proxies
def generateProxyDict(proxy):
    socks5 = 'socks5://%s:%s@%s:%s' % (PROXY_USERNAME, PROXY_PASSWORD, random.choice(proxy), random.choice(PROXY_PORTS))

    return dict(http=socks5, https=socks5)

# generate list of request hearder
def generateRequestHeader(user_agent, ref_bbox):
    header = copy.deepcopy(BASE_REQUEST_HEADER)
    header['User-Agent'] = random.choice(user_agent)
    header['Referer'] = header['Referer'] % ref_bbox

    return header

# send map request
def sendMapRequest(url, user_agent, proxy, ref_bbox, driver = None):
    for attempts in range(10):
        sleep(attempts)
        # None Proxy version
        if proxy is None:
            # Go to www.zillow.com/homes
            navigateToWebsite(driver, url)
            # get the raw_data
            return driver.find_element_by_tag_name("pre").text
            #return requests.get(url, headers=generateRequestHeader(user_agent, ref_bbox)).content
        # Proxy version
        else:
            try:
                proxy_dict = generateProxyDict(proxy) if 'PROXY_FILE' in globals() else {}
                return requests.get(url, headers=generateRequestHeader(user_agent, ref_bbox), proxies=proxy_dict).content
            except ConnectionError as e:
                print('Failed to connect with error %s (ATTEMPT %s with %s)' % (e , attempts, proxy_dict))

    return None

# query in an area
def QueryWorker(out_file, search, use_proxy = True,driver = None,status = '100000',
                split = True,cutoff_hash = 8, for_rent = False, thres = 0, error_output = None):
    user_agent, proxy = [], []
    retrieveFileContents(user_agent, proxy)

    # place the bounding boxes in tuples
    search_bbox = [status]+[a.replace('.', '') for a in search[1:5]]+['0']
    ref_bbox = (search[5], search[6], search[7], search[8])

    # set up a page search
    total_page = 20
    p = 0
    # send a map request for the current bounding box
    while p < total_page:
        # turn the page
        p += 1
        # request the page url
        search_bbox[-1] = str(p)
        url = BASE_URL % tuple(search_bbox)
        # set up proxy
        if use_proxy:
            # for multiproxy use, keep requests
            results = sendMapRequest(url, user_agent, proxy, ref_bbox)
        else:
            results = None
            # get results until page is valid
            while results is None:
                try:
                    # for single machine without proxy use selenium
                    results = sendMapRequest(url, user_agent, None, ref_bbox, driver = driver)
                except:
                    driver.refresh()
        # load returned jsons
        result_json = json.loads(results)

        # parse the pages
        if not result_json['map']['properties']:
            # if it is empty 
            total_property = 0

        if p == 1:
            if result_json['list']['numPages'] > 20 and len(search[0])<cutoff_hash:
                if split:
                    print('GeoHash: {0}, Split this geohash, scrape later'.format(search[0]))
                    return True
                else:
                    print('GeoHash: {0}, Warning: Only 500 properties can be scraped'.format(search[0]))
            # update total pages:
            total_page = min([20,result_json['list']['numPages']])
            # upsrw total properties:
            total_property = result_json['list']['binCounts']['totalResultCount']
        
        # if smaller than thes-hold, then keep
        if total_property <= thres:
            if error_output is None:
                print('GeoHash: {0}, No Enough Properties, skip'.format(search[0]))
            else:
                print('GeoHash: {0}, No Enough Properties, save for later'.format(search[0]))
                with open(error_output,'a') as wb:
                    csvwriter = csv.writer(wb, delimiter=',', quotechar='"')
                    csvwriter.writerow(search)
            return False
            
        # scraped data
        print('GeoHash: {0}, Number of properties: {1:d}/{2:d}'.format(search[0],min(25*p,total_property),total_property))
        # save to outfile
        with open(out_file, 'a') as wb:
            csvwriter = csv.writer(wb, delimiter=',', quotechar='"')
            # scrape different places
            if for_rent:
                handler = 'properties'
            else:
                handler = 'nearbyProperties'
            # get information
            for i in result_json['map'][handler]:
                csvwriter.writerow([i[0], search[0], i[1], i[2], i[8][0], i[8][1], i[8][2], i[8][3], i[8][4]])
    return False

""" This block is for Data Manipulation """

def cleanSqft(detail_list, idx, row):
    # justify sqft
    # fill the finished size
    if pd.isna(row['lotSizeSqFt']) and pd.notna(row['finishedSqFt']):
        if pd.isna(row['sqft']) and row['sqft'] != row['finishedSqFt']:
            detail_list.at[idx, 'lotSizeSqFt'] = row['sqft']
    # fill the lot size
    elif pd.isna(row['finishedSqFt']) and pd.notna(row['lotSizeSqFt']):
        if row['sqft'] != row['lotSizeSqFt']:
            detail_list.at[idx, 'finishedSqFt'] = row['sqft']
    # if both side is null
    elif pd.isna(row['finishedSqFt']) and pd.isna(row['lotSizeSqFt']):
        # usually the sqft refers to finished sqft
        detail_list.at[idx, 'finishedSqFt'] = row['sqft']

def inferYearUpdated(detail_list, idx, row):
    # justify year update
    if pd.notna(row['yearBuilt']) and pd.isna(row['yearUpdated']):
        detail_list.at[idx, 'yearUpdated'] = row['yearBuilt']

def houseType(detail_list,idx,row):
    if row['useCode'] in ['MultiFamily5Plus','Apartment','Condominium','Cooperative']:
        detail_list.at[idx,'typeCode'] = 'HighDense'
    elif row['useCode'] in ['MultiFamily2To4','Duplex','Triplex','Quadruplex']:
        detail_list.at[idx,'typeCode'] = 'MidDense'
        # assign number of units
        if row['useCode'] == 'Duplex':
            detail_list.at[idx,'numUnits'] = 2.0
        elif row['useCode'] == 'Triplex':
            detail_list.at[idx,'numUnits'] = 3.0
        elif row['useCode'] == 'Quaduplex':
            detail_list.at[idx,'numUnits'] = 4.0
    elif row['useCode'] in ['SingleFamily','TownHouse']:
        detail_list.at[idx,'typeCode'] = 'LowDense'
        detail_list.at[idx,'numUnits'] = 1.0
    else:
        detail_list.at[idx,'typeCode'] = 'Other'
        detail_list.at[idx,'numUnits'] = 1.0
        detail_list.at[idx,'floorNumber'] = 1.0

def getFloorDigit(digits):
    if len(digits) == 3:
        return float(digits[0])
    elif len(digits) == 4:
        return float(digits[0:2])
    else:
        return np.nan

# simple digit extraction
def simpleAptDigit(digits,word_before):
    # case 1: pure digit or letter + digit
    # example? '3721' -> ['', '3721',''], 'APT302'->['APT', '302','']
    if digits[2] == '':
        # usually apartment digits is 1 floor digit+ 2 apt digit or 2 floor digit+ 2 apt digit
        if len(digits[1])>2:
            return getFloorDigit(digits[1])
        # for 1 digit or 2 digit usually it writes: 'Floor2','2FL', etc....
        elif 'fl' in digits[0].lower():
            return float(digits[1])
        # also there is possibility to have: 'Floor 2', 'FL 2N'
        elif 'fl' in word_before.lower():
            return float(digits[1])
    # case 2: digit + letter
    # example: '21C'-> ['', '21', 'C'],'408W'-> ['', '408', 'W']
    elif len(digits) == 3:
        return getFloorDigit(digits[1])
    elif len(digits) > 3:
        # case 3: Complex structure: digit + letter + digit +....
        # example: '1A203'-> ['', '1', 'A','203'], '1E2B'-> ['', '1', 'E','2','B'] 
        # do it recursively
        return simpleAptDigit(digits[2:],''.join(digits[:1]))
    # if miss all the cases
    return np.nan

#infer floor number
def inferFloorNumber(detail_list, idx, row):
    # if floor number is given and valid or street number is invalid skip
    if pd.isna(row['street']) or float(row['floorNumber']) < 100:
        return
    # split street string
    street = row['street'].split(' ')
    # get the digit segmentation of last two words
    digits = re.split(r'(\d+)', street[-1])   
    # bad street string, but still parsible
    if len(street)<2 and len(digits) > 3:
        detail_list.at[idx,'floorNumber'] = simpleAptDigit(digits,'') 
    # if there is a number in it, then it is the first 3 cases
    elif len(street) > 2:
        if len(digits)>1:
            detail_list.at[idx,'floorNumber'] = simpleAptDigit(digits,street[-2]) 
        # case 4: floor number is not in the last word, example: '2nd Floor', '4th FL'
        else:
            digits_before = re.split(r'(\d+)', street[-2])
            if len(digits_before) > 1 and 'fl' in street[-1].lower() and len(digits_before[1])<3:
                detail_list.at[idx,'floorNumber'] = float(digits_before[1])

# construct value dictionary of features
def constructDict(feature_list,detail_list):
    # initiarize
    feature_dict = {}
    for feature in feature_list:
        feature_dict[feature] = []
    for _,row in detail_list.iterrows():
        for feature in feature_list:
            if pd.isna(row[feature]):
                continue
            value_list = row[feature].split(',')
            for value in value_list:
                text = value.replace(' ', '').lower()
                if text not in feature_dict[feature]:
                    feature_dict[feature].append(text)
                    
    return feature_dict