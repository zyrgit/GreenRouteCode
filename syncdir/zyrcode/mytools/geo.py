#!/usr/bin/env python

import os, sys, glob,getpass
import subprocess,threading
import random, time
import inspect
import cPickle as pickle
from math import fabs, cos, sin, sqrt, atan2, pi
import math
try:
	from geopy.distance import great_circle
	from geopy.geocoders import Nominatim
except: 
	print("RUN $HOME/anaconda2/bin/pip install geopy")
try:
	import reverse_geocoder as rg
except:
	print("RUN $HOME/anaconda2/bin/pip install --upgrade reverse_geocoder")

from mem import Mem
from namehostip import get_platform
My_Platform = get_platform()
On_Cluster = False
if My_Platform=='centos': On_Cluster = True

HomeDir = os.path.expanduser("~")
Geo_File_Dir = HomeDir+os.sep+"greendrive"+os.sep+"geo"+os.sep 

try:
	dic_mm_geo_addr_mapping= pickle.load(open(Geo_File_Dir+"mm_geo_addr_mapping","rb"))
except: dic_mm_geo_addr_mapping={}
try:
	dic_mm_addr_bbox_snwe= pickle.load(open(Geo_File_Dir+"mm_addr_bbox_snwe","rb"))
except: dic_mm_addr_bbox_snwe={}


if On_Cluster:
	mm_geo_addr_mapping = Mem({ "num":5, "prefix":"geo~map~", "expire": 90*86400 })
	mm_addr_bbox_snwe = Mem({ "num":5, "prefix":"bbox~snwe~", "expire": 90*86400 })
else:
	mm_geo_addr_mapping = Mem({ "use_ips":['localhost'], "prefix":"geo~map~", "expire": 90*86400, 'overwrite_servers':True })
	mm_addr_bbox_snwe = Mem({ "use_ips":['localhost'], "prefix":"bbox~snwe~", "expire": 90*86400, 'overwrite_servers':True })


def load_geo_addr_mapping_and_bbox(write_to_file=True, extern_addr_map_dic=None):
	''' For addr->addr/bbox. map only those covered in .osm file! '''
	# bbox: [s,n,w,e]
	dic={
		"Illinois,US":[36.941741, 42.562473, -91.547092, -87.430626],
		"Washington,US":[ 45.616939, 49.016200, -124.879617, -116.805817],
		"California,US":[ 32.524124, 42.003200, -124.607388, -113.969842],
		"NewYork,US":[ 40.509925, 40.968345, -74.244953, -73.668072],
		"Indiana,US":[ 37.936420, 41.761474, -87.525282,  -84.793035],
	}
	if write_to_file:
		pickle.dump(dic, open(Geo_File_Dir+"mm_addr_bbox_snwe","wb"))
	for key,val in dic.items():
		mm_addr_bbox_snwe.set(key,val)
		print(key," --> ",val)
	# change addr
	dic={}
	if extern_addr_map_dic:
		dic.update(extern_addr_map_dic) 
	if write_to_file:
		pickle.dump(dic, open(Geo_File_Dir+"mm_geo_addr_mapping","wb"))
	for key,val in dic.items():
		mm_geo_addr_mapping.set(key,val)
		print("%s --> %s"%(key,val))


def get_turn_angle(hd1,hd2):
	''' heading 1 turn into heading 2, calc turn angle -180 -> 180'''
	dhead=hd2-hd1
	if dhead>180:
		return dhead-360.0
	if dhead<=-180:
		return dhead+360.0
	return dhead

def dist_point_to_line_of_2pts(latlng, pt1, pt2):
	''' Get meters of <latlng> to line formed by <pt1,pt2>'''
	area = get_area_given_3latlng(latlng, pt1, pt2)
	edge = get_dist_meters_latlng2(pt1, pt2)
	if area<1e-6 or edge<1e-6:
		return get_dist_meters_latlng2(pt1, latlng)
	return 2.0*area/edge

def get_area_given_3latlng(p1,p2,p3):
	a=get_dist_meters_latlng2(p1,p2)
	b=get_dist_meters_latlng2(p1,p3)
	c=get_dist_meters_latlng2(p3,p2)
	return get_area_given_3edges(a,b,c)

def get_area_given_3edges(a,b,c):
	s = (a + b + c) / 2.0
	if (s-a)<0 or (s-b)<0 or (s-c)<0: 
		print("triangle area <0 !! edges: ",a,b,c)
		return 0.0
	return (s*(s-a)*(s-b)*(s-c)) ** 0.5


''' Using geopy, or pip install reverse_geocoder.'''
def latlng_to_name_admin1_admin2_country(lat,lng):# not in use.
	results = rg.search([(lat,lng)])[0]
	ret=results["name"]+","+results["admin1"]+","+results["admin2"]+","+results["cc"]
	ret.replace(",,",",")
	return ret



''' Using geopy, or pip install reverse_geocoder.'''
def latlng_to_city_state_country(lat,lng,no_space=True,use_my_mapping=True, print_str=False, lookup_file=True):
	results = rg.search([(lat,lng)])[0]
	if results["admin2"]=="":
		ret=results["name"]+","+results["admin1"]+","+results["cc"]
	else:
		ret=results["admin2"]+","+results["admin1"]+","+results["cc"]
	'''[{'name': 'Urbana', 'cc': 'US', 'lon': '-88.20727', 'admin1': 'Illinois', 'admin2': 'Champaign County', 'lat': '40.11059'}]
	[{'name': 'Chengdu', 'cc': 'CN', 'lon': '104.06667', 'admin1': 'Sichuan', 'admin2': '', 'lat': '30.66667'}]'''
	if print_str: print(ret)
	addr = ret
	if no_space: 
		addr = addr.replace(" ","")
	if use_my_mapping:
		tmp = mm_geo_addr_mapping.get(addr)
		if tmp is not None:
			addr=tmp
		elif lookup_file: # not in use, over-written by state level addr later.
			if addr in dic_mm_geo_addr_mapping: 
				addr = dic_mm_geo_addr_mapping[addr]
		if print_str: print("mm mapping",addr)
		'''-------- my further logic --------'''
		if addr.endswith("Illinois,US"):
			addr= "Illinois,US" # IL state
		elif addr.endswith("Washington,US"):
			addr= "Washington,US" # Seattle etc. all to this.
		elif addr.endswith("California,US"):
			addr= "California,US" # Bay area etc. 
		elif addr.endswith("NewYork,US"):
			addr= "NewYork,US" 
		elif addr.endswith("Indiana,US"):
			addr= "Indiana,US" 
	return addr
	

def get_all_addr_given_snwe(slat,nlat,wlng,elng, granularity=0.2, write_file_name=None):
	dlat = (nlat-slat)*granularity
	dlng = (elng-wlng)*granularity
	x=int(1.0/granularity)
	if write_file_name is not None:
		dic={}
	for i in range(x+1):
		for j in range(x+1):
			addr=latlng_to_city_state_country(slat+dlat*i, wlng+dlng*j, use_my_mapping=False)
			print(addr)
			if write_file_name:
				if addr not in dic: 
					dic[addr]=0
				dic[addr]+=1
	if write_file_name:
		pickle.dump(dic, open(write_file_name,'wb'))


def gen_add_mapping_dic(write_file_name, target_addr):
	dic = pickle.load(open(write_file_name,"rb"))
	res={}
	for k in dic.keys():
		res[k]=target_addr
	return res


Highway_Routable_types=["motorway","trunk","primary","secondary","tertiary","unclassified","residential","motorway_link","trunk_link","primary_link","secondary_link","tertiary_link","service"] 


def yield_obj_from_osm_file(obj, fpath, apply_filter=True, filter_tag_list=[], print_str=False):
	assert fpath.startswith(os.sep)
	assert obj in ["way"] 
	QUOTE=get_osm_file_quote_given_file(fpath) 
	f=open(fpath,"r")
	if print_str: print(fpath,QUOTE)
	kInit=1
	kWithin=2
	state=kInit
	cnt=0
	pthresh=1
	if obj=="way":
		while True:
			l= f.readline()
			if not l: break
			cnt+=1
			if cnt>pthresh:
				if print_str: print("yield cnt",cnt)
				pthresh*=2
			l= l.strip()
			if state==kInit:
				if l.startswith("<way "):
					data=[l]
					state=kWithin
					valid=False
			elif state==kWithin:
				if apply_filter and l.startswith('<tag k=%shighway%s'%(QUOTE,QUOTE)):
					v=l.split(" v=")[-1].split(QUOTE)[1]
					if len(filter_tag_list)>0:
						if v in filter_tag_list:
							valid=True
					elif v in Highway_Routable_types:
						valid=True
					data.append(l)
				elif l.startswith("</way>"):
					state=kInit
					if apply_filter and not valid: continue
					yield data
				else:
					data.append(l)
	f.close()


def get_osm_file_quote_given_addr(addr):
	fn= HomeDir+"/greendrive/osmdata/%s/%s.osm"%(addr,addr)
	return get_osm_file_quote_given_file(fn)

def get_osm_file_quote_given_file(fn): # some ugly osm file first has single ' then double "
	fn=fn.strip()
	vote={"'":0,'"':0}
	with open(fn,"r") as f:
		cnt=10
		for l in f:
			if "='" in l:
				vote["'"]+=1
				cnt-=1
			if '="' in l:
				vote['"']+=1
				cnt-=1
			if cnt<=0: break
	if vote['"']>=vote["'"]: return '"'
	return "'"


KeyToType={
"SYSTEMMILLIS":int, "GPSTime":int, "GPSLatitude":float, "GPSLongitude":float, "GPSAccuracy":float, "GPSSpeed":float, "GPSBearing":float, "GPSAltitude":float, "OBDThrottlePosition":float, "OBDEngineRPM":float, "OBDCommandEqRatio":float, "OBDSpeed":float, "OBDMassAirFlow":float, "Gas":float, "OriSysMs":int,
}

def convert_line_to_dic(line, CUT="~|", EQU="="):
	st = line.strip().split(CUT)
	res={}
	for x in st:
		k,v = x.split(EQU)
		v=KeyToType[k](v)
		res[k]=v
	return res


def get_dist_meters_latlng2(latlon1, latlon2):
	return get_dist_meters_latlng(latlon1[0] , latlon1[1] , latlon2[0] , latlon2[1])

def get_dist_meters_latlng(lat1, lon1, lat2, lon2):
	return great_circle((lat1, lon1), (lat2, lon2)).meters


""" Returns the distance in miles between two points described by their 
		longitudes and lattitudes. Not in use."""
def get_dist_meters_math(lat1, lon1, lat2, lon2):
	a = 6378137;
	b = 6356752.3142;
	avgLat = fabs(lat1 + lat2) / 2.0;
	complement = a*a*(cos(avgLat * pi / 180) * cos(avgLat * pi / 180)) + b*b*(sin(avgLat*pi/180)*sin(avgLat*pi/180));
	f_lon = (a*a/sqrt(complement) + 200) * cos(avgLat * pi/180) * pi / 180;
	f_lat = (a*a*b*b/(complement * sqrt(complement)) + 200) * pi / 180;
	x = f_lat * (lat2 - lat1);
	y = f_lon * (lon2 - lon1);
	return sqrt(x*x + y*y);


def get_bearing_given_nid12(n1,n2,mm_latlng):
	return get_bearing_latlng2(mm_latlng.get(n1), mm_latlng.get(n2))

""" Returns the direction bearing in degress given start and end 
		longitude and lattitude  """
def get_bearing_latlng2(latlng1, latlng2):
	return get_bearing_given_lat_lng(latlng1[0], latlng1[1], latlng2[0], latlng2[1])

def get_bearing_given_lat_lng(lat1, lon1, lat2, lon2):# another way, same
	startLat = math.radians(lat1) 
	startLong = math.radians(lon1)
	endLat = math.radians(lat2)
	endLong = math.radians(lon2)
	dLong = endLong - startLong
	dPhi = math.log(math.tan(endLat/2.0+math.pi/4.0)/math.tan(startLat/2.0+math.pi/4.0))
	if abs(dLong) > math.pi:
		if dLong>0.0:
			dLong=-(2.0*math.pi-dLong)
		else:
			dLong=(2.0*math.pi+dLong)
	bearing = (math.degrees(math.atan2(dLong, dPhi)) + 360.0) % 360.0
	return bearing

def min_angle_diff(an1,an2):
	dif1 = abs(an1-an2)
	dif2 = abs(an1+360.0-an2)
	dif3 = abs(an1-360.0-an2)
	return min(dif1,min(dif2,dif3))

def headings_all_close(lst, thresh=20):
	good=True
	for i in range(len(lst)-1):
		for j in range(i+1,len(lst)):
			if min_angle_diff(lst[i],lst[j])>thresh:
				good=False
	return good

if __name__ == "__main__":

	arglist=sys.argv[1:]

	if "map" in arglist:
		load_geo_addr_mapping_and_bbox()

	if "latlng" in arglist:
		print latlng_to_city_state_country(39.838334, -86.144976)

