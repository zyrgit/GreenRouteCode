#!/usr/bin/env python

import os, sys, getpass
import random, time
import inspect
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
import googlemaps
from datetime import datetime
import json
import pprint
iprint =2
API_Restricted='AIzaSyAJ*********Your API Key***********' 
api_keys=[
"AIzaSyCG9tcBio*********Your API Key***********",
]

class GoogleMaps:
	def __init__(self,**kwargs): # timeout is 10s. 
		self.gmaps = []
		self.globe_cnt=0
		self.modulo = max(1,len(api_keys))
		for k in api_keys:
			self.gmaps.append(googlemaps.Client(key=k))

	def _inc_globe_cnt(self,):
		self.globe_cnt+=1
		if self.globe_cnt>2147483647: self.globe_cnt=0
	def _select_cnt(self,):
		return self.globe_cnt% self.modulo
	def _select_gmap(self,):
		self._inc_globe_cnt()
		return self.gmaps[self._select_cnt()]


	def get_route_given_2latlng(self,latlng1,latlng2):
		res=  googlemaps.directions.directions(self._select_gmap(),latlng1,latlng2,mode="driving",units="metric")
		if iprint>=3: pprint.pprint(res)
		route=res[0]
		steps=route["legs"][0]["steps"]
		ret=[ [latlng1,0,0] ] # latlng, dist, time
		for s in steps:
			dist=s["distance"]["value"]
			dura=s["duration"]["value"]
			ret.append([ [s["end_location"]["lat"],s["end_location"]["lng"]],dist,dura ])
		return ret
		''' [ {u'bounds': {u'northeast': {u'lat': 40.1147939, u'lng': -88.2102632},
              				u'southwest': {u'lat': 40.106979, u'lng': -88.2223511}},
			  u'copyrights': u'Map data \xa92018 Google',
			  u'legs': [ { u'distance': {u'text': u'1.9 km', u'value': 1904},
			             u'duration': {u'text': u'5 mins', u'value': 310},
			             u'end_address': u'751-799 S Cedar St, Urbana, IL 61801, USA',
			             u'end_location': {u'lat': 40.106979, u'lng': -88.2102632},
			             u'start_address': u'300-398 N Harvey St, Urbana, IL 61801, USA',
			             u'start_location': {u'lat': 40.1147939, u'lng': -88.2223511},
			             u'steps': [{u'distance': {u'text': u'33 m', u'value': 33},
			                         u'duration': {u'text': u'1 min', u'value': 7},
			                         u'end_location': {u'lat': 40.114494,
			                                           u'lng': -88.22235049999999},
			                         u'html_instructions': u'Head <b>south</b> on <b>N Harvey St</b> toward <b>W Main St</b>',
			                         u'polyline': {u'points': u'm|ysFt|myOz@?'},
			                         u'start_location': {u'lat': 40.1147939,
			                                             u'lng': -88.2223511},
			                         u'travel_mode': u'DRIVING'}, ...
			            			 ],
			             u'traffic_speed_entry': [],
			             u'via_waypoint': [] } ],
			  u'overview_polyline': {u'points': u'm|ysFt|myOz@?AkICiGbHAfLGzOIbDACwHAsII{PI{PpBA'},
			  u'summary': u'N Lincoln Ave and W Oregon St',
			  u'warnings': [],
			  u'waypoint_order': [] } ]
        ret: [[[40.114794, -88.222276], 0, 0], [[40.114494, -88.22235049999999], 33, 7], ...]                 '''
	def address2bbox(self,addr):
		result = googlemaps.geocoding.geocode(self._select_gmap(),addr)[0]
		latlng = result["geometry"]['viewport']
		return latlng
		#{u'northeast': {u'lat': 40.1670741, u'lng': -88.2210401}, u'southwest': {u'lat': 40.062188, u'lng': -88.333179}}
	def address2latlng(self,addr):
		result = googlemaps.geocoding.geocode(self._select_gmap(),addr)[0]
		latlng = result["geometry"]['location']
		return latlng
		#{u'lat': 40.11380279999999, u'lng': -88.2249052}
	def latlng2address(self,latlng):
		if isinstance(latlng, list):
			latlng={"lat":latlng[0],"lng":latlng[1]}
		result = googlemaps.geocoding.reverse_geocode(self._select_gmap(),latlng)[0]
		addr = result["formatted_address"]
		return str(addr)
		#'Thomas M. Siebel Center for Computer Science, 201 N Goodwin Ave, Urbana, IL 61801, USA'
	def elevation(self, locationList):
		#URLs must be properly encoded to be valid and are limited to 8192 characters
		#https://maps.googleapis.com/maps/api/elevation/json?locations=39.7391536,-104.9847034|36.455556,-116.866667&key=
		list_of_dict = googlemaps.elevation.elevation(self._select_gmap(),locationList)
		list_of_float = [x['elevation'] for x in list_of_dict]
		return list_of_float
		'''list_of_dict:[{u'resolution': 4.771975994110107, u'elevation': 220.0458984375, u'location': {u'lat': 40.1138, u'lng': -88.22491}}, {u'resolution': 4.771975994110107, u'elevation': 219.8804779052734, u'location': {u'lat': 40.1138, u'lng': -88.22489999999999}}]'''


if __name__ == "__main__":
	obj=GoogleMaps()

	if 0: print(obj.get_route_given_2latlng([40.114794, -88.222276],[40.106979, -88.210283]))
	if 0: print( obj.address2bbox("champaign, il") )
	if 0: print( obj.elevation([[40.11380279999999,  -88.2249052],[40.11380,  -88.22490]]) )
	if 0: pprint.pprint( obj.latlng2address({u'lat': 40.11380279999999, u'lng': -88.2249052}) )
	if 0: pprint.pprint( obj.address2latlng('Thomas M. Siebel Center for Computer Science') )
