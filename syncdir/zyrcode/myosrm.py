#!/usr/bin/env python

import os, sys, getpass, glob
import subprocess
import random, time
import inspect
import json
import pprint
import requests
import cPickle as pickle
import numpy as np
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
if mypydir not in sys.path: sys.path.append(mypydir)
from readconf import get_conf,get_conf_int,get_conf_float
from namehostip import get_platform
from executor import DelayRetryExecutor,ErrorShouldDelay
from myexception import ErrorNotInCache,ErrorTaskGiveUp
from mem import Mem, MemLock, Semaphore, AccessRestrictionContext
from mygmaps import GoogleMaps
from CacheManager import CacheManager
from geo import convert_line_to_dic, get_dist_meters_latlng, latlng_to_city_state_country, get_bearing_latlng2, min_angle_diff, dist_point_to_line_of_2pts, headings_all_close, get_dist_meters_latlng2, yield_obj_from_osm_file,get_turn_angle, get_osm_file_quote_given_file
from myoverpy import query_obj_given_id_list
from util import py_fname

iprint = 2 
configfile="conf.txt"
My_Platform = get_platform()
On_Cluster = False
if My_Platform=='centos': On_Cluster = True

mm = CacheManager(overwrite_prefix=True)
# overwrite ./code/constants
OSRM_MATCH = "http://router.project-osrm.org/match/v1/driving/%s?annotations=true" 
OSRM_ROUTE = "http://router.project-osrm.org/route/v1/driving/%s?steps=true&annotations=true" #?steps=true
URL_Route = "http://{Backend}/route/v1/driving/{Loc}?steps=true&annotations=true" 
OSRM_Backend="router.project-osrm.org"
Fuel_Backend=get_conf(configfile,"Fuel_Backend")

EXT = get_conf(configfile,"EXT") # .gz 
CUT = get_conf(configfile,"CUT") # ~| 
EQU = get_conf(configfile,"EQU",delimiter=":")
KeyUserEmail = get_conf(configfile,"KeyUserEmail") 
KeyUserName = get_conf(configfile,"KeyUserName") 
UnknownUserEmail = get_conf(configfile,"UnknownUserEmail") # Anonymous 
KeySysMs=get_conf(configfile,"KeySysMs")
KeyGPSTime=get_conf(configfile,"KeyGPSTime")
KeyGPSLat=get_conf(configfile,"KeyGPSLat")
KeyGPSLng=get_conf(configfile,"KeyGPSLng")
KeyGPSAccuracy=get_conf(configfile,"KeyGPSAccuracy")
KeyGPSSpeed=get_conf(configfile,"KeyGPSSpeed")
KeyGPSBearing=get_conf(configfile,"KeyGPSBearing")

New_Nid_thresh=5000000000 # node id > this, may be new nodes.

def match(addr=""):
	'''NOT IN USE. match by reading gps from local trace.txt file'''
	lstr = "new google.maps.LatLng("
	rstr = "), "
	LngFirst = 1 # osrm url/ret is lng,lat ?  
	coord=""
	radiuses="&radiuses="
	timestamp="&timestamps="
	paths=[]
	trace=[]
	with open("trace.txt","r") as f:
		for l in f:
			l=l.strip()
			if len(l)<=0: continue
			if lstr in l:
				st= l.lstrip(lstr).rstrip(rstr).split(rstr+lstr)
				st = [x.split(",")[LngFirst]+","+x.split(",")[1-LngFirst] for x in st]
				coord+= ";".join(st)
			else:
				st = l.split(",")
				coord += st[LngFirst]+","+st[1-LngFirst]+";"
				trace.append([float(st[0]),float(st[1])])
				radiuses+="20;"
				timestamp+="%d;"%(1500000000+len(trace))
		coord = coord.rstrip(";")
		radiuses = radiuses.rstrip(";")
		timestamp = timestamp.rstrip(";")
		paths.append(trace)

	url = OSRM_MATCH%coord
	url += radiuses +timestamp
	print(url)
	ret = requests.get(url).json()
	pprint.pprint(ret) #['tracepoints']
	if "matchings" in ret:
		tracepoints=ret['tracepoints']
		matchpoints = ret["matchings"][0]["legs"]
		if iprint>=2: print("matchpoints",len(matchpoints),". tracepoints",len(tracepoints))

		traceLatLng = []
		for pt in tracepoints:
			if pt:
				lnglat = pt['location'] #[-88.232058, 40.110284]
				traceLatLng.append([lnglat[LngFirst],lnglat[1-LngFirst]])
		paths.append(traceLatLng)

		for i in range(len(ret["matchings"])):
			matchpoints=ret["matchings"][i]["legs"]
			nodeids= refine_matched_leg_points(matchpoints,addr=addr)
			paths.append(convert_nids_to_list_of_latlng(nodeids, disturb=True, addr=addr,suppress_cache_err=True))
	gen_map_html_from_path_list(paths,mypydir+"/map.html",addr=addr)


def query_node_elevation_from_osm_file(fpath, fout, addr="", ignore_mc=False, load_previous=True, lock_sfx=""):
	'''Get (on a way) node altitude from gmaps.
	Run on a cluster. 
	It's idempotent before cache expiration. 
	--- fpath = File-In:
	<way id="5324735" version="1">
		<nd ref="37948105"/>
		<nd ref="37948104"/>
	--- fout:
	4252247176 912.59
	4252247177 913.41
	1101337426 914.81 ...
	'''
	gmaps=GoogleMaps()
	if On_Cluster:
		mc = Mem({
			"expire":60*86400, 
			"num":34, # on cluster
			"prefix":"~n2e-",})
	else:
		mc = Mem({
			"expire":60*86400, #  1 PC
			"use_ips":['localhost'],
			"prefix":"~n2e-",})
	semaphore=Semaphore(prefix=py_fname(__file__,True)+"~ne~"+fout.split(os.sep)[-1], count=1,no_restriction= not On_Cluster,)
	QUOTE=get_osm_file_quote_given_file(fpath)
	def _write(st):
		with semaphore:
			with open(fout,"a") as f:
				f.write(st)
	lock=AccessRestrictionContext(prefix=py_fname(__file__,False)+"~ACNE~"+fout.split(os.sep)[-1]+lock_sfx,
		persistent_restriction=True,
		persist_seconds=600,print_str=False,no_restriction= not On_Cluster)

	exe=DelayRetryExecutor({ # delay-retry after error.
		"name":"exec_query_node_elevation", 
		"init_sleep":0.5,
		"sleep_thresh":0.1,
		"retryAllException":True,
		"max_sleep":86400/4, })

	if load_previous:
		with lock:
			lock.Access_Or_Wait_And_Skip('load previous.')
			print('reading '+fout)
			cnt=0
			try:
				with open(fout,'r') as f:
					for l in f:
						try:
							st=l.split()
							nid = int(st[0])
							ele = float(st[1])
							mc.set(nid,ele)
							cnt+=1
							if cnt%5000==1: print(nid,ele)
						except: pass
			except:
				print("[ ERR ] "+fout)

	query_latlng_list=[]
	query_nid_list=[]
	Max_Size = 500 #  about 2000+ char. Timeout if larger.
	cnt=0
	totalnids=0
	pthresh=1
	print("reading "+fpath)
	for da in yield_obj_from_osm_file("way",fpath,print_str=False):
		with lock:
			lock.Access_Or_Skip(da[0])
			cnt+=1
			if cnt>=pthresh:
				print("elv-way cnt",cnt)
				pthresh*=2
			nlist=set() # nids [int]
			for e in da:
				if e.startswith("<nd "):
					nid = int(e.split(' ref=%s'%QUOTE)[-1].split(QUOTE)[0])
					if (ignore_mc or mc.get(nid) is None ): # if re-run this task 
						nlist.add(nid)
			if len(nlist)==0: 
				continue
			nlist = list(nlist)
			latlnglist= convert_nids_to_list_of_latlng(nlist, disturb=False, addr=addr, suppress_cache_err=True)
			while len(query_latlng_list)<Max_Size and len(latlnglist)>0:
				query_latlng_list.append(latlnglist.pop(0))
				query_nid_list.append(nlist.pop(0))
			if len(query_latlng_list)==Max_Size: # about 30 ways' nids.
				# list_of_float = gmaps.elevation(query_latlng_list) # time out 
				list_of_float= exe.execute(gmaps.elevation, query_latlng_list )
				assert len(list_of_float) == len(query_nid_list)
				for i in range(len(query_nid_list)):
					nid = query_nid_list[i]
					ele = list_of_float[i]
					_write("%d %.2f\n"%(nid,ele))
					mc.set(nid,ele)
					totalnids+=1
				query_latlng_list = latlnglist
				query_nid_list = nlist
				if iprint>=2 and cnt%10==1: print("way cnt",cnt,"my total nids",totalnids)
	# the rest of the nodes last batch.
	if len(query_latlng_list)>0:
		list_of_float= exe.execute(gmaps.elevation, query_latlng_list )
		assert len(list_of_float) == len(query_nid_list)
		for i in range(len(query_nid_list)):
			nid = query_nid_list[i]
			ele = list_of_float[i]
			_write("%d %.2f\n"%(nid,ele))
			mc.set(nid,ele)
			totalnids+=1
	print("-- My Num nodes with elevation:",totalnids)


def query_way_speed_from_osm_file(fpath, fout, addr, exec_init_delay=0.1,lock_sfx='', load_previous=True):
	'''Get way avg speed btw nodes from OSRM.
	Run on cluster.
	It's idempotent before cache expiration. But cost time.
	<way id="5324735" version="1">
		<nd ref="37948105"/>
		<nd ref="37948104"/>
		<tag k="highway" v="service"/>
		<tag k="oneway" v="yes"/> /no /-1 '''
	if On_Cluster:
		mc = Mem({
			"expire":60*86400, # 60 days
			"num":34,
			"prefix":"~qws-" ,})
	else:
		mc = Mem({
			"expire":60*86400, #  1 PC
			"use_ips":['localhost'],
			"prefix":"~qws-",})
	semaphore = Semaphore(prefix=py_fname(__file__,True)+"~ws~"+fout.split(os.sep)[-1], count=1,no_restriction= not On_Cluster)
	QUOTE=get_osm_file_quote_given_file(fpath)
	def _make_key(n1,n2):
		return "%dn%d"%(n1,n2)
	def _write(st):
		with semaphore:
			with open(fout,"a") as f:
				f.write(st)
			time.sleep(0.001)
	bugNoSpeedFn=mypydir+"/cache/nospeed-%s.txt"%addr 
	semaphore2= Semaphore(prefix=py_fname(__file__,True)+"~ws2~"+fout.split(os.sep)[-1], count=1,no_restriction= not On_Cluster)

	with semaphore2:
		if not os.path.exists(mypydir+"/cache"): os.makedirs(mypydir+"/cache")
	def _write_no_speed(st):
		with semaphore2:
			with open(bugNoSpeedFn,"a") as f:
				f.write(st)

	lock=AccessRestrictionContext(prefix=py_fname(__file__,False)+"~ACWS~"+fout.split(os.sep)[-1]+lock_sfx,
		persistent_restriction=True,
		persist_seconds=600,print_str=False,no_restriction= not On_Cluster)

	exe=DelayRetryExecutor({
		"name":"query_way_speed_from_osm_file",
		"init_sleep":exec_init_delay,
		"sleep_thresh":0.005,
		"retryAllException":True,
		"max_sleep":86400/4,})

	if load_previous:
		with lock:
			lock.Access_Or_Wait_And_Skip('load previous.')
			print('reading '+fout)
			cnt=0
			try:
				with open(fout,'r') as f:
					for l in f:
						try:
							st=l.split(',')
							n1 = int(st[0])
							n2 = int(st[1])
							spd = float(st[2])
							mc.set(_make_key(n1,n2) , spd)
							cnt+=1
							if cnt%5000==1: print((n1,n2) , spd)
						except: pass
			except:
				print("[ ERR ] "+fout)
	cnt=0
	cthresh=1
	for da in yield_obj_from_osm_file("way",fpath):
		cnt+=1
		if cnt>=cthresh:
			cthresh*=2
			print('spd-way cnt',cnt)
		#da = ['<way id="538433053" version="1">', '<nd ref="5211677821"/>', '<nd ref="5211677822"/>', '<tag k="name" v="Declaration Dr"/>', '<tag k="oneway" v="yes"/>']
		with lock:
			lock.Access_Or_Skip(da[0])
			nlist=[]
			oneway="no"
			busOnly="no"
			firstNid = None
			for e in da:
				if e.startswith("<way "):
					widstr = e.split(' id=%s'%QUOTE)[-1].split(QUOTE)[0]
				elif e.startswith("<nd "):
					nid = int(e.split(' ref=%s'%QUOTE)[-1].split(QUOTE)[0])
					nlist.append(nid)
					if firstNid is None:
						firstNid=nid
				elif e.startswith('<tag k=%soneway%s'%(QUOTE,QUOTE)):
					oneway= e.split(' v=%s'%QUOTE)[-1].split(QUOTE)[0]
				# special: <tag k="bus" v="yes"/> means bus only...
				elif e.startswith('<tag k=%sbus%s'%(QUOTE,QUOTE)) or e.startswith('<tag k=%staxi%s'%(QUOTE,QUOTE)) or e.startswith('<tag k=%sshare_taxi%s'%(QUOTE,QUOTE)) or e.startswith('<tag k=%sminibus%s'%(QUOTE,QUOTE)) or e.startswith('<tag k=%stourist_bus%s'%(QUOTE,QUOTE)):
					busOnly= e.split(' v=%s'%QUOTE)[-1].split(QUOTE)[0]
					if iprint: print(widstr,"Find special tag: ",e)
			if firstNid==nid: 
				for i in range(len(nlist)-1):
					n1=nlist[i]
					n2=nlist[i+1]
					if mc.get(_make_key(n1,n2)) is None: 
						_write_no_speed("%d,%d\n"%(nlist[i],nlist[i+1]))
						mc.set(_make_key(n1,n2), -1)
				continue
			for i in range(len(nlist)-1):
				n1=nlist[i]
				n2=nlist[i+1]
				if mc.get(_make_key(n1,n2)) is None:
					if iprint>=2: print("Task   w"+widstr+" at %d,n%d,n%d"%(i,n1,n2))
					tmpurl=[]
					try:
						rt = exe.execute(route_from_nid_to_nid, n1,n2, tmpurl,addr=addr)
						spd= get_speed_given_route_legs0_nids(rt,n1,n2, url=tmpurl[0])
						if spd is None: 
							_write_no_speed("%d,%d\n"%(n1,n2))
							if iprint>=3: 
								print(tmpurl[0],n1,n2)
								pprint.pprint(rt)
							mc.set(_make_key(n1,n2), -1)
						if busOnly=="no" and spd is not None:
							if mc.get(_make_key(n1,n2)) is None: 
								_write("%d,%d,%.2f\n"%(n1,n2, spd))
								mc.set(_make_key(n1,n2) , spd)
					except:
						_write_no_speed("%d,%d\n"%(n1,n2))
						mc.set(_make_key(n1,n2), -1)
				if oneway!="yes":
					if mc.get(_make_key(n2,n1)) is None:
						if iprint>=2: print("Task r "+widstr+" at %d,n%d,n%d"%(i,n2,n1))
						tmpurl=[]
						try:
							rt = exe.execute(route_from_nid_to_nid, n2,n1,tmpurl,addr=addr)
							spd= get_speed_given_route_legs0_nids(rt,n2,n1, url=tmpurl[0])
							if spd is not None: # both-ways
								if mc.get(_make_key(n2,n1)) is None: 
									_write("%d,%d,%.2f\n"%(n2,n1, spd))
									mc.set(_make_key(n2,n1) , spd)
							else: # 1-way
								if iprint>=2: print([n2,n1],"not oneway but no speed.")
								_write_no_speed("%d,%d\n"%(n2,n1))
								mc.set(_make_key(n2,n1), -1)
						except:
							_write_no_speed("%d,%d\n"%(n2,n1))
							mc.set(_make_key(n2,n1), -1)


def get_speed_given_route_legs0_nids(rt, n1,n2, url=None):
	'''Input: {u'annotation': {   u'datasources': [5],
		     u'distance': [53.055217831039634],
		     u'duration': [8.2],
		     u'nodes': [38158213, 38099611],
		     u'weight': [8.2]   },
	    u'distance': 53.1,
	    u'duration': 8, }'''
	if not isinstance(rt, dict):
		return None
	if 'annotation' not in rt or not isinstance(rt['annotation'],dict):
		if "distance" in rt and "duration" in rt:
			return rt['distance']/rt['duration']
		else:
			return None
	nodes = rt['annotation']['nodes']
	pos=0
	while pos<len(nodes)-1:
		if nodes[pos]==n1 and nodes[pos+1]==n2:
			dist= rt['annotation']['distance'][pos]
			dura= rt['annotation']['duration'][pos]
			return float(dist)/dura
		pos+=1
	if iprint>=2: 
		print("\nget_speed_given_route_legs0_nids()  weird cases...")
		if iprint>=3: pprint.pprint(rt)
		print("From",n1,"to",n2)
	''' the case when the chosen longer route has two ends being n1,n2:'''
	pos1=0
	while pos1<len(nodes)-1:
		if nodes[pos1]==n1 : break
		pos1+=1
	pos2=pos1+1
	while pos2<len(nodes):
		if nodes[pos2]==n2 : break
		pos2+=1
	if nodes[pos1]==n1 and pos2<len(nodes) and nodes[pos2]==n2 :
		dist=0.0
		dura=0.0
		for i in range(pos1,pos2): # dist entry 1 less than nids.
			dist+= rt['annotation']['distance'][i]
			dura+= rt['annotation']['duration'][i]
		return float(dist)/dura # assume speed on weird route to be the same.
	''' the case when two pts too close and cannot route correctly, or err upon update.'''
	if len(nodes)==2:
		if nodes[0] in [n1,n2] or nodes[1] in [n1,n2]:
			if rt['duration']>0:
				return rt['distance']/rt['duration']
	''' the case of update out of sync, wrong nodes. But just use speed.'''
	if len(nodes)==2 and rt['duration']>0: 
		return rt['distance']/rt['duration']
	print("Wrong nodes ... return dist/dura",n1,n2)
	if iprint>=3: print(url)
	if "distance" in rt and "duration" in rt:
		if rt['duration']>0:
			return rt['distance']/rt['duration']
		else:
			print("rt['duration']==0")
			return None
	return None


def check_if_nodes_are_connected(nids, brokenPosList=[], addr="", mm_external=None, not_in_cache_list=[]):
	'''check if the sequence of nids are neighbors. brokenPosList[i] to [i+1] disconnected.'''
	if len(nids)==0: return True
	if mm_external is None: 
		mm_use=mm
		if mm_use.get_id()!="osm/cache-%s-nodeid-to-neighbor-nid.txt"%addr:
			mm_use.use_cache( meta_file_name="osm/cache-%s-nodeid-to-neighbor-nid.txt"%addr, overwrite_prefix=True)
	else: 
		mm_use=mm_external
	connected=True
	not_in_cache=0
	for i in range(0,len(nids)-1):
		res=mm_use.get(nids[i])
		if res is None: 
			not_in_cache+=1
			not_in_cache_list.append(nids[i])
		elif nids[i+1] not in res:
			connected= False
			brokenPosList.append(i)
	res=mm_use.get(nids[-1])
	if res is None: 
		not_in_cache+=1
		not_in_cache_list.append(nids[-1])
	if not_in_cache>0:
		st=[str(x) for x in not_in_cache_list]
		st=",".join(st)
		raise ErrorNotInCache("%s not in cache!"%st)
	return connected


def convert_latlng_seq_into_nodeids_route(latlnglist,backend=OSRM_Backend,addr=None,print_res=False):# not in use
	LngFirst = 1 # osrm url/ret is lng,lat format?  
	locs=""
	for stt in latlnglist:
		locs+= "%.6f,%.6f;"%(stt[LngFirst],stt[1-LngFirst]) 
		if addr is None:
			addr =  latlng_to_city_state_country(stt[0],stt[1])
	locs=locs.rstrip(";")
	routeUrl = URL_Route.format(Backend=backend,Loc=locs)
	ret = requests.get(routeUrl).json()
	if 'routes' in ret:
		legs= ret['routes'][0]['legs']
		nodeslists=[]
		for rt in legs:
			nodes = rt['annotation']['nodes']
			nodeslists.append(nodes)
		nodeids = []
		for nlst in nodeslists:
			connect_dots(nodeids, nlst, allow_duplicate=0, mm_nid2neighbor=None,addr=addr)
		return nodeids
	else:
		return None

def convert_latlng_seq_into_nodeids_match(latlngdisttime,addr=None,speedup_ratio=1.0,print_res=False):
	''' input is [ [ latlng, dist, duration ], ...]  , default using osrm match backend, if want to reduce returned duration for each seg dist, use larger speedup_ratio, so when osrm matching, less likely to have detour.'''
	LngFirst = 1 # osrm url/ret is lng,lat ?  
	listOfDict=[]
	if addr is None:
		addr = latlng_to_city_state_country(latlngdisttime[0][0][0],latlngdisttime[0][0][1])
	for stt in latlngdisttime:
		dic={}
		dic[KeyGPSAccuracy]=6.0
		dic[KeyGPSLat]=stt[0][0]
		dic[KeyGPSLng]=stt[0][1]
		dic[KeyGPSSpeed]=speedup_ratio* stt[1]/max(1,stt[2])
		listOfDict.append(dic)
	try:
		ret=convert_path_to_nids(listOfDict,addr)
	except ErrorTaskGiveUp:
		return None
	if print_res: print(ret)
	nlists=ret[0]
	if len(nlists)==0:
		return None
	return nlists[0]


'''url: http://172.22.68.74:5000/route/v1/driving/-88.219134,40.098083;-88.218833,40.0806?steps=true'''

def route_from_latlng1_to_latlng2(latlng1, latlng2,lst=None,backend=OSRM_Backend,print_res=False):
	LngFirst = 1 # osrm url/ret is lng,lat ?  
	url =URL_Route.format(Backend=backend,Loc=str(latlng1[LngFirst]) +","+str(latlng1[1-LngFirst]) +";"+str(latlng2[LngFirst]) +","+str(latlng2[1-LngFirst]))
	if lst is not None: lst.append(url)
	if iprint>=3 or print_res: print(url)
	ret = requests.get(url).json()
	if iprint>=3 or print_res: pprint.pprint(ret)
	if 'routes' in ret:
		return ret['routes'][0]['legs'][0] 
	if 'code' in ret:
		if ret['code']=='NoRoute':
			return None
	raise ErrorShouldDelay("route_from_latlng1_to_latlng2") # osrm rejects if too many requests.


def route_from_nid_to_nid(fnid, tnid, lst=None,addr=""):
	latlng1,latlng2 = convert_nids_to_list_of_latlng([fnid,tnid], disturb=False,addr=addr)
	return route_from_latlng1_to_latlng2(latlng1,latlng2,lst)


def convert_nids_to_list_of_latlng(nodeids, addr, disturb=False, not_in_cache_list=[], suppress_cache_err=False, disturb_shrink=1.0, crawl_if_mem_fail=False):
	''' node id is int, disturb only for display html.
	- crawl_if_mem_fail: use online API to query node->latlng if mm not loaded.
	'''
	if mm.get_id()!="osm/cache-%s-nodeid-to-lat-lng.txt"%addr:
		mm.use_cache( meta_file_name="osm/cache-%s-nodeid-to-lat-lng.txt"%addr, ignore_invalid_mem=crawl_if_mem_fail , overwrite_prefix=True) 
	trace=[]
	not_in_cache=0
	for n in nodeids:
		latlng=mm.get(n)
		if latlng is None:
			print(n, "does not exist in cache!")
			if crawl_if_mem_fail:
				try:
					latlng=crawl_nid_to_latlng(n)
					mm.set(n,latlng)
				except: print('Exception',n)
		if latlng is None:
			not_in_cache+=1
			not_in_cache_list.append(n)
		else:
			if disturb: # for display only 
				latlng[0]+=disturb_shrink*(0.000015*pow(-1,random.randint(0,1))+0.000007*(random.random()-0.5))
				latlng[1]+=disturb_shrink*(0.000015*pow(-1,random.randint(0,1))+0.000007*(random.random()-0.5))
			trace.append(latlng)
	if not_in_cache>0:
		should_suppress=1
		for n in not_in_cache_list:
			if n<New_Nid_thresh:
				should_suppress=0 # not a new node?
				break
		if should_suppress==0 or not suppress_cache_err:
			raise ErrorNotInCache("%d etc Not in cache!"%not_in_cache_list[0])
	return trace


def crawl_nid_to_latlng(nid, print_str=False, silent=False):
	if not silent: print('[ crawl_nid_to_latlng ] %d'%nid)
	res = query_obj_given_id_list('node',[nid])
	if print_str: pprint.pprint(res)
	try:
		lnglat = res["features"][0]["geometry"]["coordinates"]
	except:
		print("[myosrm] crawl_nid_to_latlng() err: query_obj_given_id_list return",res)
		print(nid," node not found by overpy!!! ")
	return [lnglat[1],lnglat[0]]


def gen_map_html_from_path_list(paths, outFileName, addr, disturb=False,disturb_shrink=1.0, right_col_disp_list=["----"], heats=[] ,print_str=False, comment=None, deltaBaseColor=0.0, crawl_if_mem_fail=False):
	''' paths is list of [[lat,lng], ] or list of [ node-ids ].
	    heats is list of [ [[lat,lng],val], ]: gas dots.
	'''
	lstr = "new google.maps.LatLng("
	rstr = "), "
	for i in range(len(paths)):
		path = paths[i]
		if iprint>=2 or print_str: print(" [ gen_map_html ] path%d len %d"%(i,len(path)))
		if iprint>=3 and i>0 and print_str: print(path)
		if isinstance(path, list) and len(path)>0 and isinstance(path[0], int):#[ node-ids ]
			tmplst=[]
			try:
				paths[i] = convert_nids_to_list_of_latlng(path, disturb=disturb,addr=addr, not_in_cache_list=tmplst,disturb_shrink=disturb_shrink, crawl_if_mem_fail=crawl_if_mem_fail)
			except ErrorNotInCache:
				print("not in cache list",tmplst)
				for tmp in tmplst:
					path.remove(tmp)
				paths[i] = convert_nids_to_list_of_latlng(path, disturb=disturb,addr=addr,disturb_shrink=disturb_shrink,crawl_if_mem_fail=crawl_if_mem_fail)
	kInit=0
	kInsert1=1
	kInsert2=2
	kInsert3=3
	kInsert4=4
	kInsert5=5
	kInsert6=6
	state = kInit
	with open(outFileName,"w") as of:
		with open(mypydir+"/template.html","r") as f:
			for l in f:
				if state==kInit:
					if l.startswith("////startinsertlatlng1"):
						state=kInsert1
						for p in paths:
							of.write("[")
							for latlng in p:
								of.write(lstr+str(latlng[0])+","+str(latlng[1])+rstr)
							of.write("],\n")
					elif l.startswith("////startinsertlatlng2"):
						state=kInsert2
						for i in range(len(paths)):
							of.write("function check%d() {\n"%i)
							of.write("if (myform.chk%d.checked == true) {\n"%i)
							of.write("set_checked_index(%d,true); }else{\n"%i)
							of.write("set_checked_index(%d,false);}}\n"%i)
					elif l.startswith("<!-- ////startinsertlatlng3 -->"):
						state=kInsert3
						for i in range(len(paths)):
							of.write('<input name="chk%d" type=checkbox checked onClick="check%d()"> <b><span style="color:blue">%s</span></b> <br/>\n'%(i,i,"Path")) # use path
					elif l.startswith("<!-- ////startinsertlist -->"):
						state=kInsert4
						for i in range(len(right_col_disp_list)):
							of.write("<p>")
							of.write(str(right_col_disp_list[i]) )
							of.write("</p>\n")
					elif l.startswith("////insertheatstart1"):
						state=kInsert5
						for i in range(len(heats)): # 
							hp=heats[i]# [ [ [lat,lng],val ], ... ]
							of.write("[\n")
							for latlngval in hp: #[{lat: 40.1137, lng: -88.2246},0.5],
								lat=latlngval[0][0]
								lng=latlngval[0][1]
								val=min(max(0.0,latlngval[1]+deltaBaseColor),1.0)
								of.write("[{lat:%.6f,lng:%.6f},%.6f],\n"%(lat,lng,val))
							of.write("],\n")
					elif l.startswith("<!--startcomment-->"):
						state=kInsert6
						if comment is not None:
							of.write("<!--\n")
							of.write(str(comment)+"\n")
							of.write("-->\n")

					else:
						of.write(l)

				elif state == kInsert1:
					if l.startswith("////endinsertlatlng1"):
						state=kInit
				elif state == kInsert2:
					if l.startswith("////endinsertlatlng2"):
						state=kInit
				elif state == kInsert3:
					if l.startswith("<!-- ////endinsertlatlng3 -->"):
						state=kInit
				elif state == kInsert4:
					if l.startswith("<!-- ////endinsertlist -->"):
						state=kInit
				elif state == kInsert5:
					if l.startswith("////insertheatend1"):
						state=kInit
				elif state == kInsert6:
					if l.startswith("<!--endcomment-->"):
						state=kInit


def match_trace_listOfDict(listOfDict,genHTML=False,addr="",printUrl=False):
	'''listOfDict: [{SYSTEMMILLIS:1510716136000,GPSTime:1510716136000,GPSLongitude:-88.259},]'''
	kPtsNum=100 # match # pts per batch/url.
	path_pos=0
	TotalLen=len(listOfDict)
	if iprint>=2: 
		print("[ Enter match_trace_listOfDict() ] len(listOfDict) %d"%TotalLen)
	matched_osm_nodes=[]
	paths=[]
	oriPath=[]
	paths.append(oriPath)
	start_time=1500000000
	lastlat=1.0
	lastlng=1.0
	first_batch=True
	last_batch=False
	write_file=0
	if write_file: of=open("./trace-match.txt","w")

	while path_pos<TotalLen:
		rough_angle_dic={} # check if has u-turn...
		no_matching=False

		while True:
			coord=""
			radiuses="&radiuses="
			timestamp="&timestamps="
			batch_pos=path_pos
			batch_cnt=0
			while batch_pos<TotalLen and batch_cnt<kPtsNum: 
				dic=listOfDict[batch_pos] 
				oriPath.append([dic[KeyGPSLat],dic[KeyGPSLng]])
				''' only add if truely moved'''
				moved_dist=get_dist_meters_latlng(lastlat,lastlng,dic[KeyGPSLat],dic[KeyGPSLng])
				if KeyGPSSpeed in dic: moving_speed=dic[KeyGPSSpeed]
				else: moving_speed=0

				if (moved_dist<10) or moving_speed==0:
					batch_pos+=1
					continue
				batch_cnt+=1
				start_time+= max(1, int( moved_dist/max(2,moving_speed)))
				lastlat=dic[KeyGPSLat]
				lastlng=dic[KeyGPSLng]

				lngstr="%.6f"%dic[KeyGPSLng]
				latstr="%.6f"%dic[KeyGPSLat]
				coord += lngstr+","+latstr+";"
				if write_file: of.write(latstr+","+lngstr+"\n")
				
				if KeyGPSBearing  in dic:
					head = int(dic[KeyGPSBearing])/10*10
					rough_angle_dic[head]=1

				if KeyGPSAccuracy in dic:
					if isinstance(dic[KeyGPSAccuracy],str):
						accu=min(50,float(dic[KeyGPSAccuracy])+3)
					else: accu=min(50,dic[KeyGPSAccuracy]+3)
					radiuses+= "%d;"%accu
				else: radiuses+="20;"

				timestamp+= "%d;"%start_time
				batch_pos+=1

			if batch_pos==TotalLen:
				last_batch=True
				if batch_cnt<2:
					# invalid input, too few points at end of path. 
					print("Too few pts for url, TotalLen=%d, batch_cnt=%d, mv dist=%.2f"%(TotalLen,batch_cnt,moved_dist))
					dic=listOfDict[-1]
					lngstr="%.6f"%dic[KeyGPSLng]
					latstr="%.6f"%dic[KeyGPSLat]
					coord += lngstr+","+latstr+";"
					if KeyGPSAccuracy in dic:
						if isinstance(dic[KeyGPSAccuracy],str):
							accu=min(50,float(dic[KeyGPSAccuracy])+3)
						else: accu=min(50,dic[KeyGPSAccuracy]+3)
						radiuses+= "%d;"%accu
					else: radiuses+="20;"
					timestamp+= "%d;"%(1+start_time)

			coord = coord.rstrip(";")
			radiuses = radiuses.rstrip(";")
			timestamp = timestamp.rstrip(";")
			url = OSRM_MATCH%coord
			url += radiuses + timestamp
			if iprint>=2 and printUrl: print(url)
			try:
				ret = requests.get(url).json()
				if "matchings" not in ret:
					print(ret)
					print("no match for %d pts!"%batch_cnt)
					no_matching=True
					path_pos=batch_pos
					break
				if 'code' in ret and ret['code'].lower()=='ok': 
					path_pos=batch_pos
					break
			except: 
				print("\nException at kPtsNum %d"%kPtsNum)
				time.sleep(1)
			kPtsNum-=10
			if kPtsNum<1: break

		if no_matching: 
			continue
		''' see if this batch trace contains u-turn'''
		if iprint>=2: print("first_batch?",first_batch,"last_batch?",last_batch)
		anlist=rough_angle_dic.keys()
		has_u_turn=0
		for i in range(len(anlist)-1):
			for j in range(i,len(anlist)):
				if min_angle_diff(anlist[i],anlist[j]) > 160:
					has_u_turn+=1
		if has_u_turn>0 and iprint>=2: print("[ match_trace_ ] has_u_turn=%d !"%has_u_turn)
		if iprint>=4 and printUrl: 
			print("pprint(ret):")
			pprint.pprint(ret) 

		numMatch=len(ret["matchings"])
		if numMatch>1:
			nodeids=[]
			for i in range(numMatch):
				legnodeids= refine_matched_leg_points(ret["matchings"][i]["legs"],addr=addr)
				if iprint>=3:
					print("-- ret[matchings][%d][legs], U-turn %d"%(i,has_u_turn))
				if iprint>=2:
					print("-- %d matching attempt to add"%i,legnodeids)
				if len(legnodeids)>2 and has_u_turn==0:
					while legnodeids[-1] in legnodeids[0:-1]:
						tmp=legnodeids.pop()
						if iprint>=2: print("-- %d clean dup, pop legnodeids %d"%(i, tmp))
				try:
					connect_dots(nodeids, legnodeids, allow_duplicate=has_u_turn,addr=addr,print_str="too many matchings")
				except ErrorTaskGiveUp:
					if last_batch: # re-use [nodeids]
						print("\nlast loop in path give up !")
					elif first_batch:
						print("\n1st loop in path give up ...")
						nodeids=legnodeids # give up previous [nodes]
					else:
						raise ErrorTaskGiveUp("not last loop in path cannot give up")

				if iprint>=2: print("-- %d matching nodeids end of numMatch"%i,nodeids)
		else:
			nodeids= refine_matched_leg_points(ret["matchings"][0]["legs"],addr=addr)
		
		''' clean nodeids for duplicate: '''
		if has_u_turn==0:
			''' should not contain duplicated if no u-turn!'''
			while nodeids[0] in nodeids[1:]:
				nodeids.pop(0)
				if iprint>=2:print("[ u-turn ] after pop head:",nodeids)
			while nodeids[-1] in nodeids[0:-1]:
				nodeids.pop()
				if iprint>=2:print("[ u-turn ] after pop tail:",nodeids)
		if iprint>=2: 
			print("--- nodeids this Loop",nodeids)
		connect_dots(matched_osm_nodes, nodeids,addr=addr,print_str="end of this loop")
		paths.append(nodeids)
		first_batch=False

		if iprint>=2:
			print("So far matched %d pts into %d nodes"%(path_pos,len(matched_osm_nodes)))
	try:
		broken=[]
		no_cache=[]
		if not check_if_nodes_are_connected(matched_osm_nodes,broken,addr=addr,not_in_cache_list=no_cache):
			fixnids=[]
			for i in range(len(broken)): 
				tmp=broken[i]
				print("Out loop. try fix broken at %d:"%tmp,matched_osm_nodes[tmp],matched_osm_nodes[tmp+1])
				if i==0:
					connect_dots(fixnids,matched_osm_nodes[0:tmp+1],addr=addr,print_str="try fix")
				if i==len(broken)-1:
					connect_dots(fixnids,matched_osm_nodes[tmp+1:],addr=addr,print_str="  try fix")
				if i>0 and i<len(broken)-1:
					connect_dots(fixnids,matched_osm_nodes[tmp+1:broken[i+1]+1],addr=addr,print_str=" try fix")
			if check_if_nodes_are_connected(fixnids,addr=addr):
				print("before fix, matched_osm_nodes:",matched_osm_nodes)
				matched_osm_nodes=fixnids
				print("Fixed broken. matched_osm_nodes <= fixnids:",matched_osm_nodes)
			else:
				raise Exception("[ match_trace_listOfDict ] nodes_are_connected False, fix fail !")
	except ErrorNotInCache as e:
		print(e)

	if iprint>=2: print("[ End match_trace_listOfDict() ] Total osm nd Num: %d\n"%len(matched_osm_nodes))
	if write_file: of.close()
	return matched_osm_nodes



def refine_matched_leg_points(matchpoints,addr="",duplicated_list=[]):
	'''osrm match annotation, filter duplicated nodes and check sanity.'''
	nodeids = []
	duplicated = 0
	NumAnnotations=len(matchpoints)
	for i in range(NumAnnotations):
		n=matchpoints[i]
		nlist=n["annotation"]["nodes"]
		connect_dots(nodeids, nlist,addr=addr,print_str="refine_matched_leg_points")

	try:
		broken=[]
		no_cache=[]
		if not check_if_nodes_are_connected(nodeids,brokenPosList=broken,addr=addr,not_in_cache_list=no_cache): 
			print("refine_matched_leg_points() not connected: ",broken)
			for i in range(len(broken)):
				print(nodeids[broken[i]],nodeids[broken[i]+1])
			raise Exception("refine_matched_leg_points() connected False!")
	except ErrorNotInCache as e:
		print("no_cache",no_cache)
		for n in no_cache:
			nodeids.remove(n)
		broken=[]
		if not check_if_nodes_are_connected(nodeids,addr=addr,brokenPosList=broken): 
			print("nodeids",nodeids)
			print("broken",broken)
			raise Exception("refine_matched_leg_points() connected False, cache does not fix it !")

	for i in range(len(nodeids)-1):
		for j in range(i+1,len(nodeids)):
			if nodeids[i]==nodeids[j]:
				duplicated+=1
				duplicated_list.append(nodeids[i])
	assert len(nodeids)>1
	if iprint>=2: print("refined %d Legs into %d nodes"%(len(matchpoints),len(nodeids)))
	return nodeids


def match_from_CUT_EQU_file(fpath,genHTML=False,addr="",printUrl=False):
	kCutTraceTimeGap = 4 # timestamp between two lines > this then cut.
	kCutTraceDistGap = min(150,kCutTraceTimeGap* 50) # dist between two lines > this then cut.
	lastlat=None
	lastlng=None
	lasttime=None
	segs=[]
	path=[]
	with open(fpath,"r") as f:
		for l in f:
			dic=    convert_line_to_dic(l)
			if lastlat is not None:
				lastlat=lat
				lastlng=lng
				lasttime=gti
			lat = dic[KeyGPSLat] 
			lng = dic[KeyGPSLng]
			gti = dic[KeyGPSTime]
			if lastlat is None:
				lastlat=lat
				lastlng=lng
				lasttime=gti
			ddif=    get_dist_meters_latlng(lastlat,lastlng, lat, lng)
			dtime=  abs(lasttime-gti)
			if ddif>kCutTraceDistGap or dtime/1000.0 >kCutTraceTimeGap:
				if ddif>kCutTraceDistGap: 
					if iprint>=2: print("weird inside cut? dist %.2f "%ddif)
				if dtime/1000.0 >kCutTraceTimeGap:
					if iprint>=2: print("weird inside cut? time %d "%dtime)
				if iprint>=2: print("fragmented path len %d"%len(path))
				segs.append(path)
				path=[]
			path.append(dic)
		if iprint>=2: print("last path len %d"%len(path))
		segs.append(path)

	if iprint>=2: print("Seg Num %d"%len(segs))
	for path in segs:
		if iprint>=2: 
			print("-- Path len %d"%len(path))
		if len(path)<10: 
			print("path too short, skip")
			continue
		dic=path[0]
		splat = dic[KeyGPSLat]
		splng = dic[KeyGPSLng]
		dic=path[-1]
		splat2 = dic[KeyGPSLat]
		splng2 = dic[KeyGPSLng]
		dist=get_dist_meters_latlng(splat,splng,splat2,splng2)
		if dist<50:
			print("path dist too small, skip. %.1f"%dist)
			continue
		res= match_trace_listOfDict(path,genHTML,addr=addr,printUrl=printUrl)
		try:
			no_cache=[]
			connected=check_if_nodes_are_connected(res,addr=addr,not_in_cache_list=no_cache)
		except ErrorNotInCache:
			for n in no_cache:
				res.remove(n)
			connected=check_if_nodes_are_connected(res,addr=addr,not_in_cache_list=no_cache)



def connect_dots(nids, next, allow_duplicate=1, mm_nid2neighbor=None,addr="",print_str="",bfs_brute_force=True): 
	# append [next] to existing [nids], retain pointer to nids.
	if len(nids)==0: 
		nids.extend(next)
		return
	if len(next)==0: 
		return
	''' 1-2 + 2-3-4 '''
	if nids[-1]==next[0]: 
		if allow_duplicate: 
			nids.extend(next[1:])
		else:
			for n in next[1:]: 
				if n not in nids: nids.append(n)
		return
	''' 1-2-3 + 2-3-4  need to remove next 2-3'''
	if len(nids)>1 and len(next)>1 and nids[-2]==next[0] and nids[-1]==next[1]:
		if allow_duplicate: 
			nids.extend(next[2:])
		else:
			for n in next[2:]: 
				if n not in nids: nids.append(n)
		return
	''' 1-2-3 + 2-4-5  need to remove nids 3 and next 2'''
	if len(nids)>1 and len(next)>1 and nids[-2]==next[0] and nids[-1]!=next[1]:
		nids.pop()
		if allow_duplicate: 
			nids.extend(next[1:])
		else:
			for n in next[1:]: 
				if n not in nids: nids.append(n)
		return
	''' 1-2-3 + 4-3-5  need to remove next 4-3'''
	if len(nids)>1 and len(next)>1 and nids[-1]==next[1] and nids[-2]!=next[0]:
		if allow_duplicate: 
			nids.extend(next[2:])
		else:
			for n in next[2:]: 
				if n not in nids: nids.append(n)
		return
	''' 1-2-3 + 4-2-5  need to remove nid 3 and next 4-2'''
	if len(nids)>1 and len(next)>1 and nids[-1]!=next[0] and nids[-2]==next[1]:
		nids.pop()
		if allow_duplicate: 
			nids.extend(next[2:])
		else:
			for n in next[2:]: 
				if n not in nids: nids.append(n)
		return
	'''------- The following does not have nid overlap.'''
	if len(nids)>1 and len(next)>1 and not (nids[-1]!=next[0] and nids[-1]!=next[1] and nids[-2]!=next[0] and nids[-2]!=next[1]):
		raise Exception("Corner case condition wrong!")
	if mm_nid2neighbor is None: 
		mm_use=mm
		if addr=="":
			raise Exception("[connect_dots] input either mm_nid2neighbor or addr !")
	else: 
		mm_use=mm_nid2neighbor
	if addr and mm_use.get_id()!="osm/cache-%s-nodeid-to-neighbor-nid.txt"%addr:
		mm_use.use_cache(meta_file_name="osm/cache-%s-nodeid-to-neighbor-nid.txt"%addr, overwrite_prefix=True)
	
	''' 1-4-5  +  3-6  OR  1-4  +  3-6   need to connect 4-3 and remove 5.'''
	nb2=mm_use.get(next[0])
	found=None
	l1=len(nids)
	if nb2 is not None:
		for i in range(l1-1,-1,-1): # check backwards
			if nids[i] in nb2:
				found=i
				break
	if found is not None:
		for i in range(l1-1-found):
			nids.pop()
		if allow_duplicate: 
			nids.extend(next)
		else:
			for n in next: 
				if n not in nids: nids.append(n)
		return
	''' 1-2  +  4-3-5  OR  1-2  +  3-5   need to connect 2-3 and remove 4.'''
	nb1=mm_use.get(nids[-1])
	found=None
	l2=len(next)
	if nb1 is not None:
		for i in range(l2): # check forwards
			if next[i] in nb1:
				found=i
				break
	if found is not None:
		for i in range(found):
			next.pop(0)
		if allow_duplicate: 
			nids.extend(next)
		else:
			for n in next: 
				if n not in nids: nids.append(n)
		return
	'''------- The following contains missing 1 mid node.'''
	''' 1-2  (3)  6-4-5  OR  1-2  (3)  4-5  need to missing mid 3, remove 6'''
	nb1=mm_use.get(nids[-1])
	l2=len(next)
	foundn=None
	ind2=None
	if nb1 is not None:
		for i in range(l2):
			nb2=mm_use.get(next[i])
			if nb2 is not None:
				for n in nb2:
					if n in nb1:
						foundn=n
						ind2=i
						break
			if foundn is not None: break
	if foundn is not None:
		nids.append(foundn)
		if allow_duplicate: 
			nids.extend(next[ind2:])
		else:
			for n in next[ind2:]: 
				if n not in nids: nids.append(n)
		return
	''' 1-2-5  (3)  4-6  OR  1-2  (3)  4-5  need to missing mid 3, remove 6'''
	nb2=mm_use.get(next[0])
	l1=len(nids)
	foundn=None
	ind=None
	if nb2 is not None:
		for i in range(l1-1,-1,-1):
			nb1=mm_use.get(nids[i])
			if nb1 is not None:
				for n in nb1:
					if n in nb2:
						foundn=n
						ind=l1-1-i
						break
			if foundn is not None: break
	if foundn is not None:
		for x in range(ind):
			nids.pop()
		nids.append(foundn)
		if allow_duplicate: 
			nids.extend(next)
		else:
			for n in next: 
				if n not in nids: nids.append(n)
		return
	'''------- The following contains missing 2 mid nodes.'''
	''' 1-2  (3) (4)  5-6 '''
	nb1=mm_use.get(nids[-1])
	nb2=mm_use.get(next[0])
	foundn1=None
	foundnn1=None
	if nb1 is not None and nb2 is not None:
		for n1 in nb1:
			nbb1=mm_use.get(n1)
			for nn1 in nbb1:
				if nn1 in nb2:
					foundn1=n1
					foundnn1=nn1
					break
	if foundn1 is not None:
		nids.append(foundn1)
		nids.append(foundnn1)
		if allow_duplicate: 
			nids.extend(next)
		else:
			for n in next: 
				if n not in nids: nids.append(n)
		return
	''' 1-2  (3) (4)  7-5-6  need removal 7 as well '''
	if len(next)>1:
		nb1=mm_use.get(nids[-1])
		nb2=mm_use.get(next[1])
		foundn1=None
		foundnn1=None
		if nb1 is not None and nb2 is not None:
			for n1 in nb1:
				nbb1=mm_use.get(n1)
				for nn1 in nbb1:
					if nn1 in nb2:
						foundn1=n1
						foundnn1=nn1
						break
		if foundn1 is not None:
			nids.append(foundn1)
			nids.append(foundnn1)
			if allow_duplicate: 
				nids.extend(next[1:])
			else:
				for n in next[1:]: 
					if n not in nids: nids.append(n)
			return
	''' 1-2-7  (3) (4)  5-6  need removal 7 as well '''
	if len(nids)>1:
		nb1=mm_use.get(nids[-2])
		nb2=mm_use.get(next[0])
		foundn1=None
		foundnn1=None
		if nb1 is not None and nb2 is not None:
			for n1 in nb1:
				nbb1=mm_use.get(n1)
				for nn1 in nbb1:
					if nn1 in nb2:
						foundn1=n1
						foundnn1=nn1
						break
		if foundn1 is not None:
			nids.pop()
			nids.append(foundn1)
			nids.append(foundnn1)
			if allow_duplicate: 
				nids.extend(next)
			else:
				for n in next: 
					if n not in nids: nids.append(n)
			return

	if bfs_brute_force:
		trace={nids[-1]:None,} # node->parent node
		que = [nids[-1]]
		foundNidInNext=None
		while que:
			nid = que.pop(0)
			nb = mm_use.get(nid)
			if nb:
				for newnd in nb:
					if newnd not in trace:
						trace[newnd] = nid
						if newnd in next:
							foundNidInNext = newnd
							print(" bfs found: ", newnd)
							break
						que.append(newnd)
			if foundNidInNext:
				break
			if len(que)>100:
				# print("bfs_brute_force reach max...")
				break
		if foundNidInNext:
			reverselist  = [foundNidInNext,]
			while 1:
				prev= trace[reverselist[-1]] # back tracking 
				if prev not in nids:
					reverselist.append(prev)
				else:
					pind = nids.index(prev) # already connected nids and next. stop
					nids[pind+1:]=[] # delete nodes after joint point
					while reverselist:
						nids.append(reverselist.pop())
					break
			print(" bfs track back:", nids)
			nind = next.index(foundNidInNext)
			for x in range(nind+1,len(next)):
				if next[x] not in nids:
					nids.append(next[x])
			print(" bfs after adding next ", nids)
			return # modify nids in place

	''' Give up '''
	print("[ connect_dots Fail ] "+print_str,nids,next)
	print("nb1",nb1)
	print("nb2",nb2)
	raise ErrorTaskGiveUp("connect_dots() fail! "+print_str)


def take_some_time_to_connect(nidlast,nidnext,headlast,headnext, mm_nid2neighbor, mm_nid2latlng, print_str=False):
	''' try longer time to fix  nidlast- ? -nidnext, given headings '''
	if print_str: print("[ take_some_time_to_connect ]",nidlast,nidnext,headlast,headnext)
	hdthresh=150
	latlnglast = mm_nid2latlng.get(nidlast)
	candidate1=[]
	traceDic={}
	nid1=nidlast
	lcnt=0
	while len(candidate1)<=1:
		lcnt+=1
		nb1 = mm_nid2neighbor.get(nid1)
		latlng1 = mm_nid2latlng.get(nid1)
		for nbn in nb1:
			if nbn not in traceDic: traceDic[nbn]=nid1
			if nbn == nidnext:
				retlist=[nidnext]
				while retlist[0]!=nidlast:
					retlist.insert(0,traceDic[retlist[0]])
				if print_str: 
					print("Fix Found1 ",retlist)
				return retlist
			latlngnb= mm_nid2latlng.get(nbn)
			hd= get_bearing_latlng2( latlng1, latlngnb )
			if min_angle_diff(hd, headlast)<hdthresh:
				candidate1.append(nbn)
		if len(candidate1)==1:
			nid1= candidate1.pop()
		if lcnt>100:
			raise ErrorTaskGiveUp("take_some_time_to_connect loop1 %d"%lcnt)
	if print_str: print("Fix At %d"%nid1, traceDic)
	''' now nidlast -> a cross , same proc for nidnext '''
	latlngnext = mm_nid2latlng.get(nidnext)
	candidate2=[]
	traceDic2={}
	nid2=nidnext
	lcnt=0
	while len(candidate2)<=1:
		lcnt+=1
		nb = mm_nid2neighbor.get(nid2)
		latlng2 = mm_nid2latlng.get(nid2)
		for nbn in nb:
			if nbn not in traceDic2: traceDic2[nbn]=nid2
			if nbn in candidate1:
				retlist=[nbn]
				while retlist[-1]!=nidnext:
					retlist.append( traceDic2[retlist[-1]] )
				while retlist[0]!=nidlast:
					retlist.insert(0,traceDic[retlist[0]])
				if print_str: 
					print("Fix Found2 ",retlist)
				return retlist
			latlngnb= mm_nid2latlng.get(nbn)
			hd= get_bearing_latlng2(latlngnb, latlng2)
			if min_angle_diff(hd, headnext)<hdthresh:
				candidate2.append(nbn)
		if len(candidate2)==1:
			nid2= candidate2.pop()
		if lcnt>100:
			raise ErrorTaskGiveUp("take_some_time_to_connect loop2 %d"%lcnt)
	if print_str: print("Fix To %d"%nid2, traceDic2)
	print(" try connect two candidate Lst ",candidate1,candidate2)
	for n1 in candidate1:
		nids=[n1]
		sucess=0
		for n2 in candidate2:
			try:
				connect_dots(nids, [n2], allow_duplicate=0, mm_nid2neighbor=mm_nid2neighbor)
			except ErrorTaskGiveUp:
				continue
			sucess=1
			break
		if sucess: break
	if sucess:
		retlist=nids
		while retlist[-1]!=nidnext:
			retlist.append( traceDic2[retlist[-1]] )
		while retlist[0]!=nidlast:
			retlist.insert(0,traceDic[retlist[0]])
		if print_str: 
			print("Fix Found3 ",retlist)
		return retlist
	print("\n[ take_some_time_to_connect ] Fail\n")
	return None



def yield_matched_nids_and_path_pos(listOfDict, printUrl=False):
	'''listOfDict: [{SYSTEMMILLIS:1510716136000,GPSTime:1510716136000,GPSLongitude:-88.259},]'''
	kPtsNum=100 # match # pts per batch/url.
	path_pos=0
	last_path_pos=0
	TotalLen=len(listOfDict)
	start_time=1500000000
	lastlat=1.0
	lastlng=1.0

	while path_pos<TotalLen:
		no_matching=False

		while True:
			coord=""
			radiuses="&radiuses="
			timestamp="&timestamps="
			batch_pos=path_pos
			batch_cnt=0
			while batch_pos<TotalLen and batch_cnt<kPtsNum: 
				dic=listOfDict[batch_pos] # don't change dic in place!
				''' only add if truely moved'''
				moved_dist=get_dist_meters_latlng(lastlat,lastlng,dic[KeyGPSLat],dic[KeyGPSLng])
				if KeyGPSSpeed in dic: moving_speed=dic[KeyGPSSpeed]
				else: moving_speed=0

				if (moved_dist<10) or moving_speed==0:
					batch_pos+=1
					continue
				batch_cnt+=1
				start_time+= max(1, int( moved_dist/max(2,moving_speed)))
				lastlat=dic[KeyGPSLat]
				lastlng=dic[KeyGPSLng]

				lngstr="%.6f"%dic[KeyGPSLng]
				latstr="%.6f"%dic[KeyGPSLat]
				coord += lngstr+","+latstr+";"
				
				if KeyGPSAccuracy in dic:
					if isinstance(dic[KeyGPSAccuracy],str):
						accu=min(50,float(dic[KeyGPSAccuracy])+3)
					else: accu=min(50,dic[KeyGPSAccuracy]+3)
					radiuses+= "%d;"%accu
				else: radiuses+="20;"

				timestamp+= "%d;"%start_time
				batch_pos+=1

			if batch_pos==TotalLen:
				if batch_cnt<2:
					# invalid input, too few points at end of path. 
					dic=listOfDict[-1]
					lngstr="%.6f"%dic[KeyGPSLng]
					latstr="%.6f"%dic[KeyGPSLat]
					coord += lngstr+","+latstr+";"
					if KeyGPSAccuracy in dic:
						if isinstance(dic[KeyGPSAccuracy],str):
							accu=min(50,float(dic[KeyGPSAccuracy])+3)
						else: accu=min(50,dic[KeyGPSAccuracy]+3)
						radiuses+= "%d;"%accu
					else: radiuses+="20;"
					timestamp+= "%d;"%(1+start_time)

			coord = coord.rstrip(";")
			radiuses = radiuses.rstrip(";")
			timestamp = timestamp.rstrip(";")
			url = OSRM_MATCH%coord
			url += radiuses + timestamp
			if iprint>=2 and printUrl: print(url)
			try:
				ret = requests.get(url).json()
				if "matchings" not in ret:
					print(ret)
					print("no match for %d pts!"%batch_cnt)
					no_matching=True
					last_path_pos=path_pos
					path_pos=batch_pos
					break
				if 'code' in ret and ret['code'].lower()=='ok': # success, break loop.
					last_path_pos=path_pos
					path_pos=batch_pos
					break
			except: 
				print("\nException at kPtsNum %d"%kPtsNum)
				time.sleep(1)
			kPtsNum-=10 # if not successful, decrease batch size, if helps.
			if kPtsNum<1: break

		if no_matching: 
			continue
		if iprint>=4 and printUrl: 
			print("pprint(ret):")
			pprint.pprint(ret) 

		numMatch=len(ret["matchings"])
		for m in range(numMatch):
			matchpoints=ret["matchings"][m]["legs"]
			NumAnnotations=len(matchpoints)
			for i in range(NumAnnotations):
				nlist=matchpoints[i]["annotation"]["nodes"]
				if iprint>=3: print("yielding [%d , %d]"%(last_path_pos, path_pos))
				yield [ nlist, [last_path_pos, path_pos] ]




def convert_path_to_nids(listOfDict,addr):
	res=[]
	respos=[]
	nodeids=[]
	if mm.get_id()!="osm/cache-%s-nodeid-to-neighbor-nid.txt"%addr:
		mm.use_cache(meta_file_name="osm/cache-%s-nodeid-to-neighbor-nid.txt"%addr, overwrite_prefix=True)
	startpos=None
	for next in yield_matched_nids_and_path_pos(listOfDict):
		nd=next[0]
		pos=next[1]
		if startpos is None: startpos=pos[0]
		newnids=[]
		for n in nd:
			if mm.get(n) is not None:
				newnids.append(n)
		try:
			connect_dots(nodeids,newnids,mm_nid2neighbor=mm)
		except ErrorTaskGiveUp as e:
			print(e)
			if len(nodeids)>0:
				res.append(nodeids)
				respos.append([startpos, pos[1]])
				startpos=pos[0]
			nodeids=newnids

		broken=[]
		no_cache=[]
		if not check_if_nodes_are_connected(nodeids,brokenPosList=broken,addr=addr,not_in_cache_list=no_cache): 
			if iprint:
				print("[ not connected ] broken pos:",broken)
				print("before fix, nodeids:", nodeids, "len %d"%len(nodeids))
			fixnids=[]
			giveUp_1_=0
			for i in range(len(broken)): 
				tmp=broken[i]
				try:
					if iprint:
						print("Try fix broken at %d:"%tmp, nodeids[tmp],nodeids[tmp+1])
					if i==0: # just copy to empty init fixnids.
						connect_dots(fixnids,nodeids[0:tmp+1],addr=addr,print_str="0 try fix")
					if i==len(broken)-1: # last seg
						connect_dots(fixnids,nodeids[tmp+1:],addr=addr,print_str="1 try fix")
					else: # connect middle
						connect_dots(fixnids,nodeids[tmp+1:broken[i+1]+1],addr=addr,print_str="2 try fix")
					if iprint:
						print("After fix %d'th, fixnids:"%i, fixnids)
					
				except ErrorTaskGiveUp as e:
					print("[convert_path_to_nids] ErrorTaskGiveUp _1_")
					print(e)
					res.append(nodeids[0:tmp+1])
					respos.append([startpos, pos[1]])
					startpos=None
					giveUp_1_=1
					nodeids=[]
					break

			if giveUp_1_==0 and check_if_nodes_are_connected(fixnids,addr=addr):
				nodeids=fixnids
			elif giveUp_1_==0:
				raise Exception("[convert_path_to_nids] check_if_nodes_are_connected False, fix fail!")

	if len(nodeids)>0: 
		res.append(nodeids)
		respos.append([startpos, pos[1]])
	duplicated_list=[]
	for nodeids in res:
		for i in range(len(nodeids)-1):
			for j in range(i+1,len(nodeids)):
				if nodeids[i]==nodeids[j]:
					duplicated_list.append(nodeids[i])
	return [res,respos] # if listofdict is cut, res contains multi segs, res is list of nid list.



if __name__ == "__main__":

	arglist=sys.argv[1:]

	addr='Illinois,US'

	if "html" in arglist and My_Platform=='mac' : # gen html to see node lists
		gen_map_html_from_path_list([
			[38049068, 38049067]
			],mypydir+"/mapnids.html", addr="Illinois,US", disturb=True,disturb_shrink=0.1,print_str=True,crawl_if_mem_fail=True)

	if 'crawl_nid_to_latlng' in arglist:
		print(crawl_nid_to_latlng(80375169, print_str=True))

