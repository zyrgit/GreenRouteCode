#!/usr/bin/env python

import os, sys, getpass
import subprocess
import random, time
import inspect
import json
import pprint
import requests

mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
iprint = 2


def find_timezone(lat,lng):
	url="http://api.geonames.org/timezoneJSON?lat=%f&lng=%f&username=zhao97"%(lat,lng)
	if iprint: print(url)
	ret = requests.get(url).json()
	if iprint>=2: pprint.pprint(ret)
	return ret

def find_elevation(lat,lng):
	#from raster, digital elevation model (DEM)
	url="http://api.geonames.org/gtopo30?lat=%f&lng=%f&username=zhao97"%(lat,lng)
	if iprint: print(url)
	ret = requests.get(url).json()
	if iprint: print ret
	return float(ret)

def find_nearest_intersection(lat,lng):
	#if outside USA: http://api.geonames.org/findNearestIntersectionOSMJSON?
	url="http://api.geonames.org/findNearestIntersectionJSON?lat=%f&lng=%f&username=zhao97"%(lat,lng)
	if iprint: print(url)
	ret = requests.get(url).json()["intersection"]
	if iprint>=2: pprint.pprint(ret)
	return ret

def find_weather_latlng(lat,lng):
	url="http://api.geonames.org/findNearByWeatherJSON?lat=%f&lng=%f&username=zhao97"%(lat,lng)
	if iprint: print(url)
	ret = requests.get(url).json()["weatherObservation"]
	if iprint>=2: pprint.pprint(ret)
	return ret

def find_nearby_streets(lat,lng):
	url="http://api.geonames.org/findNearbyStreetsJSON?lat=%f&lng=%f&username=zhao97"%(lat,lng)
	if iprint: print(url)
	ret = requests.get(url).json()["streetSegment"]
	if iprint>=2: pprint.pprint(ret)
	return ret


if __name__ == "__main__":
	# http://www.geonames.org/export/ws-overview.html
	if 0: find_timezone(40.113548,-88.223992)
	if 0: find_elevation(40.113548,-88.223992)
	if 0: find_nearest_intersection(40.113548,-88.223992)
	if 0: find_weather_latlng(40.113548,-88.223992)
	if 0: find_nearby_streets(40.113548,-88.223992)
	

