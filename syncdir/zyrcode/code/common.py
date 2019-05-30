#!/usr/bin/env python
# gen_sample() etc.
import os, sys, getpass, glob
import subprocess
import random, time
import inspect
import collections
import math
from shutil import copy2, move as movefile
import numpy as np
import pandas as pd
import gzip
from sklearn.externals import joblib
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from scipy.stats import multivariate_normal
from numpy.linalg import matrix_rank
from sklearn.metrics.pairwise import cosine_similarity
import cPickle as pickle
import pprint
if os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))+"/../mytools" not in sys.path: sys.path.append(os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))+"/../mytools") 
from readconf import get_conf,get_conf_int,get_conf_float,get_list_startswith,get_dic_startswith
from logger import Logger,SimpleAppendLogger,ErrorLogger
from util import read_lines_as_list,read_lines_as_dic,read_gzip,strip_illegal_char,strip_newline,unix2datetime,get_file_size_bytes,make_choice,sort_return_list_val_ind,sort_dic_by_value_return_list_val_key,replace_user_home
from myexception import ErrorNotInCache
from myosrm import connect_dots,check_if_nodes_are_connected,convert_path_to_nids,convert_latlng_seq_into_nodeids_match,convert_latlng_seq_into_nodeids_route,gen_map_html_from_path_list,take_some_time_to_connect,crawl_nid_to_latlng
from CacheManager import CacheManager
from mem import Mem,AccessRestrictionContext,Semaphore
from geo import convert_line_to_dic, get_dist_meters_latlng, latlng_to_city_state_country, get_bearing_latlng2, min_angle_diff, dist_point_to_line_of_2pts, headings_all_close, get_dist_meters_latlng2, yield_obj_from_osm_file,get_turn_angle, get_bearing_given_nid12
from ScoreCalculator import ScoreCalculator
from namehostip import get_platform,get_my_ip
from stats import plot_x_y_bar,plot_x_y_points,PlotDistributionBins,plot_x_y_group_scatter
from func import * 
from cache_action import action_if_get_none 
from configure import * # IPs for mm train

My_Platform = get_platform() 
On_Cluster = False
if My_Platform=='centos': On_Cluster = True
HomeDir = os.path.expanduser("~")

configfile = "conf.txt"
DirData = get_conf(configfile,"DirData") #~/greendrive/proc
DirOSM = get_conf(configfile,"DirOSM") #~/greendrive/osmdata
DirData = replace_user_home(DirData)
DirOSM = replace_user_home(DirOSM)
StatsDir="stats"
gpsfolder = "gps"
obdfolder = "obd"
combinefolder ="combine"
SegFileOutdir = "data" 
matchfolder="match"
userFolder="user"
mapfolder="map"
accfolder="acc"
gyrfolder="gyr"
linfolder="lin"
magfolder="mag"
TestStartsWithInMapGz="Testcode" 
DirHTML="html"

iprint = 1   

tinyURL=True
RTranking=True # for cluster.

mm_nid2latlng= CacheManager(overwrite_prefix=tinyURL,rt_servers=RTranking, )
mm_nid2elevation=CacheManager(overwrite_prefix=tinyURL,rt_servers=RTranking, )
mm_nids2speed= CacheManager(overwrite_prefix=tinyURL,rt_servers=RTranking, )
mm_nid2neighbor=CacheManager(overwrite_prefix=tinyURL,rt_servers=RTranking, )
mm_nid2waytag=CacheManager(overwrite_prefix=tinyURL,rt_servers=RTranking, )

mm_nid2elevation.func_if_get_none = action_if_get_none


Feat_Mass_area_air=[KMmd,KMmtime,KMmelev,KMmdv2,KMav2d,KMdragv2d,KMvd,KMmleft,KMmright,KMmstraight] # greengps original
Infocom_features = [Kdist,Ktime,Kelevation,KelevDecNeg,TPspd2inc,TPleft,TPright,TPstraight] # elev inc/dec, stop prob.  TPspd2inc, RealSegSpeedinc
CMEM_features = [CMEMv0,CMEMv1,CMEMv2,CMEMv3,CMEMv4] # CMEM model 
VTCPFEM_features = [VTCPFEMv0,VTCPFEMv1,VTCPFEMv2,VTCPFEMv3,VTCPFEMv4,VTCPFEMv5,VTCPFEMv6,VTCPFEMav1,VTCPFEMav2,VTCPFEMav3,VTCPFEMav4,VTCPFEMa2v2] # VT-CPFEM model 

plot_corr_features = [Ktime,Kelevation,KincSpeed2,Kv2d,Kvd,TstopLeft,TstopRight,TstopStraight]  
cov_dims=["lv0","v0","lv1","v1"]# used for calc sp dist/similarity

''' -------- 6.py output model: '''
try:
	Global_model_pickle = joblib.load("%s/model_lr"%StatsDir)
	Global_model= Global_model_pickle["model"]
	Global_model_coef= Global_model.coef_
	Global_model_features = Global_model_pickle["features"]
	Global_feat2coef = {}
	for i in range(len(Global_model_features)):
		Global_feat2coef[Global_model_features[i]]= Global_model_coef[i]
	Global_lb2model= joblib.load("%s/lb2model"%StatsDir)
	tmp=pickle.load(open(StatsDir+"/datares","rb"))
	Global_lb2center=tmp["lb2center"]
	Global_datamean=pickle.load( open(StatsDir+"/datamean","rb"))
except:
	print("\nException loading "+"%s/model_lr !!\n"%StatsDir)

Car2ScaleFile= get_conf(configfile,"Car2ScaleFile")
Car2ScaleFile= Car2ScaleFile%StatsDir
try: 
	carkey2scale=pickle.load(open(Car2ScaleFile,"rb")) 
	for carkey in carkey2scale.keys():
		if carkey in G_adjust_gas_scale_ratio:
			carkey2scale[carkey]*=G_adjust_gas_scale_ratio[carkey]
except: pass

# ---------- Main Mem ----------
debugPrefix="" # cache key prefix sfx
defaultNum=1 # num redis on cluster.
time10days=3 # 30 days expire
overwrite_servers_train = True # not using previous ips
rt_servers_train = False # use hostrank-rt config file ?
use_ips = [Train_Samples_mm_IP] # overwrites 'num'
print('MM train using IPs:',use_ips)

mm_train = Mem({ "num":defaultNum, "prefix":"tr~0416~"+debugPrefix, "expire": 864000*time10days , "overwrite_servers":overwrite_servers_train, "rt_servers":rt_servers_train , "use_ips":use_ips }) 

mm_train_turn = Mem({ "num":defaultNum, "prefix":"tn~0416~"+debugPrefix, "expire": 864000*time10days , "overwrite_servers":overwrite_servers_train, "rt_servers":rt_servers_train , "use_ips":use_ips }) 

mm_train_valid = Mem({ "num":defaultNum, "prefix":"~trvld~", "expire": 864000*time10days , "overwrite_servers":overwrite_servers_train, "rt_servers":rt_servers_train , "use_ips":use_ips }) 

mm_analyze = Mem({ "num":defaultNum, "prefix":"mm_analyze~", "expire": 864000*time10days , "overwrite_servers":overwrite_servers_train, "rt_servers":rt_servers_train , "use_ips":use_ips }) 

# not in use:
mm_cost = Mem({ "num":defaultNum, "prefix":"~cost~", "expire": 864000 , "overwrite_servers":overwrite_servers_train, "rt_servers":rt_servers_train, "use_ips":use_ips}) 
mm_seg_spd = Mem({ "num":4, "prefix":"~sts~", "expire": 86400, 'highLow':'low', "overwrite_servers":overwrite_servers_train, "rt_servers":rt_servers_train , "use_ips":use_ips}) 
mm_special_ids = Mem({ "num":4, "prefix":"spec~tag~"+debugPrefix, "expire": 864000, "overwrite_servers":overwrite_servers_train, "use_ips":use_ips })

try:
	cov_mat=pickle.load(open("%s/cov_mat"%StatsDir,"rb"))
	covvar = multivariate_normal( cov=cov_mat )
except: 
	print("\ncov_mat Not Found! Run 6train.py ana\n")

''' ----- speed decrease distrib at crossings: '''
try:
	distrib_frtag_totag_turn_vdiff_dec=pickle.load(open("%s/distrib_frtag_totag_turn_vdiff_dec"%StatsDir,"rb"))
	distrib_slftag_vtag_T_vhl_dec=pickle.load(open("%s/distrib_frtag_totag_turn_vdiff_dec_T"%StatsDir,"rb"))
except:
	print("RUN  get_info.py gen_turn_stats/gen_wait_time_stats first !!")


GenRealSegSpeed=0 # use mm to store real seg speed, for old greengps only.


'''-----------   used by 5.py costModule.py etc. ____ '''

def gen_sample(**kwargs):

	global GenRealSegSpeed
	path = kwargs.get("path",None) # list of GPS dict.
	train_nid_pos = kwargs.get("train_nid_pos",None)
	carkey = kwargs.get("carkey",None)
	mm_nid2elevation = kwargs.get("mm_nid2elevation",None)
	mm_nid2latlng= kwargs.get("mm_nid2latlng",None)
	mm_nid2neighbor= kwargs.get("mm_nid2neighbor",None)
	mm_train_turn= kwargs.get("mm_train_turn",None)
	mm_train= kwargs.get("mm_train",None)
	semaphore= kwargs.get("semaphore",None)
	testcnt = kwargs.get("testcnt",-1) 
	print_str=kwargs.get("print_str",False)
	isTest=kwargs.get("isTest",0)
	returnSampleStr=kwargs.get("returnSampleStr",False)
	email=kwargs.get("email",None)
	addr=kwargs.get("addr",None)
	car2meta = kwargs.get("car2meta",None) # mass, area, etc.
	gasScale=kwargs.get("gasScale",None) 
	mfn = kwargs.get("mfn",None)  # /match file name.
	tag_type = kwargs.get("tag_type",None)  # road priority.
	bugTimes=kwargs.get("bugTimes",None) # for debug.
	is_end_to_end = kwargs.get("is_end_to_end",True) # true if gen train samples
	iprint= 2 if print_str else 1
	realPath = True if path else False # is this GPS path or just OSM nodes?


	''' ---- check (train_nid_pos) before use: --'''
	if train_nid_pos and realPath:
		badInd=None
		for i in range(len(train_nid_pos)/2,0,-1): # cut away u-turn part.
			if train_nid_pos[i][0]==train_nid_pos[i-1][0] or (i>=2 and train_nid_pos[i][0]==train_nid_pos[i-2][0]):
				badInd=i
				break
		if badInd is not None:
			print(train_nid_pos,badInd)
			train_nid_pos=train_nid_pos[badInd:]
		badInd=None
		for i in range(len(train_nid_pos)/2,len(train_nid_pos)-1):
			if train_nid_pos[i][0]==train_nid_pos[i+1][0] or (i<len(train_nid_pos)-2 and train_nid_pos[i][0]==train_nid_pos[i+2][0]):
				badInd=i
				break
		if badInd is not None:
			print(train_nid_pos,badInd)
			train_nid_pos=train_nid_pos[0:badInd+1]

		for i in range(len(train_nid_pos)-1):
			if train_nid_pos[i][1]>train_nid_pos[i+1][1]:
				train_nid_pos[i+1][1]=train_nid_pos[i][1]

	sample=dict()
	'''------------ valid sp, feat for whole trip: ------ '''
	if realPath:
		sample["addr"]=addr
		sample['email']=email
		sample[KisTest]=isTest
		try:
			clst=car2meta[carkey]
		except: 
			print(__file__, carkey+" not in car2meta !")
			return
		sample[KMmass]=clst[indMass]
		sample[KMair]=clst[indDrag]
		sample[KMarea]=clst[indArea]
		sample["gscale"]=gasScale
		sample["carkey"]=carkey
		sample["mfn"]=mfn
	sample[KtagType]=tag_type

	'''---- calc speed^2*dist using crawled spd --------  '''
	kv2d=0.0
	kvd=0.0
	triptime=0.0 # crawled OSRM time. 
	tripwaittime=0.0 # not in use
	totaldist=0.0
	lastspd=SpeedTagLow
	for i in range(len(train_nid_pos)-1):
		nid0 = train_nid_pos[i][0]
		nid1 = train_nid_pos[i+1][0]
		spd= get_speed_nid2(nid0,nid1)
		if spd<=0.: spd=lastspd 
		latlng0=mm_nid2latlng.get(nid0)
		latlng1=mm_nid2latlng.get(nid1)
		dist = get_dist_meters_latlng2(latlng0,latlng1)
		kv2d+= spd**2 * dist
		kvd+= spd * dist
		triptime+= dist/spd
		totaldist+=dist
		lastspd=spd
	if realPath and totaldist < Trip_sample_min_dist: 
		print('totaldist < Trip_sample_min_dist... quit', totaldist)
		return
	sample[Kv2d]=kv2d
	sample[Kvd]=kvd
	sample[Ktime]=triptime # OSRM estimated time
	sample[Kdist]=totaldist
	mile = totaldist/MetersPerMile

	''' ---------- gas '''
	if realPath:
		gas=0.0
		for i in range(train_nid_pos[0][1],train_nid_pos[-1][1]+1):
			gas+= path[i][KeyGas] 
		assert gas>0, mfn
		sample[Kgas]=gas * gasScale
		sample[Krealgas]=gas
		gallon= sample[Kgas]/GramPerGallon
		sample["mpg"]=mile/gallon

	''' ---------- elevations from Gmaps --- '''
	cumuElevation =0.0 # or use nid2elev to calc
	cumuElevDec = 0.0 
	nodes=[]
	for i in range(len(train_nid_pos)-1):
		nid0elev = mm_nid2elevation.get(train_nid_pos[i][0])
		nid1elev = mm_nid2elevation.get(train_nid_pos[i+1][0])
		if nid1elev is not None and nid0elev is not None:
			cumuElevation += max(0,nid1elev-nid0elev)
			cumuElevDec += max(0,nid0elev-nid1elev)
		else:
			print("\n\nElevation Cache got None !! %s"%mfn)
			print(train_nid_pos[i][0],nid0elev)
			print(train_nid_pos[1+i][0],nid1elev)
		nodes.append(train_nid_pos[i][0])
	nodes.append(train_nid_pos[-1][0])
	sample["nodes"]=nodes
	sample[Kelevation]=cumuElevation # pure increase
	sample[KelevDecNeg]= -1.0* cumuElevDec # pure dec, negative, so coef is positive.

	''' fix net elev dec problem if not using downhill, use KelevInc '''
	if is_end_to_end:
		nid0elev = mm_nid2elevation.get(train_nid_pos[0][0])
		nid1elev = mm_nid2elevation.get(train_nid_pos[-1][0])
		if nid1elev is not None and nid0elev is not None:
			sample[KelevInc] =sample[Kelevation]- max(0,nid0elev-nid1elev) #not in use

	if not realPath: # for ../costModule.py 
		return sample

	''' ---- calc v^0-3 sec-by-sec for CMEM, assume path is in seconds --- '''
	v1,v2,v3,v4,v0=0.,0.,0.,0.,0.
	lastv=3.
	for ppos in range(train_nid_pos[0][1],train_nid_pos[-1][1]+1):
		if KeyOBDSpeed in path[ppos]:
			v=path[ppos][KeyOBDSpeed]/3.6
		elif KeyGPSSpeed in path[ppos]:
			v=path[ppos][KeyGPSSpeed]
		else:
			v=lastv
		v1+=v
		v2+=v*v
		v3+=v*v*v
		v4+=v*v*v*v
		lastv = v
		v0+=1. # const value, since CMEM is fuel rate = c*Power, so *time
	sample[CMEMv0]= v0
	sample[CMEMv1]= v1
	sample[CMEMv2]= v2
	sample[CMEMv3]= v3
	sample[CMEMv4]= v4

	''' ---- f=c+cP+cP^2, sec-by-sec for VT-CPFEM: --- '''
	vsize = 7 # v^0-6
	vs = [0. for _ in range(vsize)]
	lastv=None
	avsize=4 # acc*v^1-4
	avs = [0. for _ in range(avsize)]
	aavv = 0. # a^2*v^2
	for ppos in range(train_nid_pos[0][1],train_nid_pos[-1][1]+1):
		if KeyOBDSpeed in path[ppos]:
			v=path[ppos][KeyOBDSpeed]/3.6
		elif KeyGPSSpeed in path[ppos]:
			v=path[ppos][KeyGPSSpeed]
		else:
			v=lastv if lastv is not None else 3.
		tmp=1.
		for i in range(len(vs)):
			vs[i]+=tmp
			tmp*=v
		acc = max(0., v-lastv) if (lastv is not None and v is not None) else 0.
		aavv +=  acc*acc*v*v
		tmp= acc*v
		for i in range(len(avs)):
			avs[i]+=tmp
			tmp*=v
		lastv = v
	sample[VTCPFEMv0]=vs[0]
	sample[VTCPFEMv1]=vs[1]
	sample[VTCPFEMv2]=vs[2]
	sample[VTCPFEMv3]=vs[3]
	sample[VTCPFEMv4]=vs[4]
	sample[VTCPFEMv5]=vs[5]
	sample[VTCPFEMv6]=vs[6]
	sample[VTCPFEMa2v2]=aavv
	sample[VTCPFEMav1]=avs[0]
	sample[VTCPFEMav2]=avs[1]
	sample[VTCPFEMav3]=avs[2]
	sample[VTCPFEMav4]=avs[3]

	''' ---- calc Inc speed^2 according to turn min speed: ---  '''
	dspd2inc=0.0 # incV2 got from ground truth (min speed at cross).
	numleft=0 # just count num
	numright=0
	numstraight=0
	stopleft=0 # truely stopped ones
	stopright=0
	stopstraight=0
	''' cumu prob for stop at turn '''
	probleft=0
	probright=0
	probstraight=0
	inferspd2inc=0.0 # inferred from cross speed dec distri, not ground truth.
	inferwaittime=0.0 # inferred wait time, not in use
	''' get true trip time '''
	dic1=path[train_nid_pos[0][1]]
	dic2=path[train_nid_pos[-1][1]]
	truetriptime = (dic2[KeySysMs]-dic1[KeySysMs])/1000.0
	sample[KtripTime]=truetriptime

	lastavgspd=None
	rspeedinc = 0. #get real seg/block speed, as in greengps 
	''' ----- for genTrain only ------- '''

	for i in range(1, len(train_nid_pos)-1):
		nid0 = train_nid_pos[i-1][0]
		nid1 = train_nid_pos[i][0]
		nid2 = train_nid_pos[i+1][0]
		ppos0= train_nid_pos[i-1][1]
		ppos1= train_nid_pos[i][1]
		latlng0 = mm_nid2latlng.get(nid0)
		latlng1 = mm_nid2latlng.get(nid1)
		dist = get_dist_meters_latlng2(latlng0,latlng1)
		try:
			exnid0,exlatlng0= extend_from_cross_center(nid1,nid0,mm_nid2latlng,mm_nid2neighbor)
		except:
			print("\n\n\n bad extend_from_c... should exit !!\n\n")
			if realPath: return
		hd= get_bearing_latlng2(exlatlng0,latlng1)
		spd= get_speed_nid2(nid0,nid1)
		spd01= spd
		spd12= get_speed_nid2(nid1,nid2)
		minspd=None # min speed in cross, if real path.

		''' get real seg speed inc '''
		slst1 = mm_seg_spd.get((nid0,nid1))
		if slst1 is None: GenRealSegSpeed=1 
		rspd1 = np.median(slst1) if slst1 else 0.
		slst2 = mm_seg_spd.get((nid1,nid2))
		rspd2 = np.median(slst2)  if slst2 else 0.
		rspeedinc+= max(0., rspd2**2 - rspd1**2)
		''' get avg/median speed of seg '''
		avgspd=None
		posv=ppos0-1
		spdlist=[]
		spdsIfEmpty=[]
		while posv<ppos1:
			posv+=1
			if KeyOBDSpeed in path[posv]:obdspd=path[posv][KeyOBDSpeed]/3.6
			else: obdspd=-1
			if KeyGPSSpeed in path[posv]:
				gpsspd=path[posv][KeyGPSSpeed]
				if abs(obdspd-gpsspd)>8 or obdspd<=0.01: # trust gps
					v0=gpsspd
				else:
					v0=obdspd
			else: v0=obdspd
			if v0>=0: spdsIfEmpty.append(v0)
			if v0>0: spdlist.append(v0) # don't add zero speed in median.

		if ppos0==ppos1: # nodes too close.
			''' maybe at bus stop node, check previous '''
			if i>=2 and lastavgspd is not None:
				avgspd=lastavgspd
		if avgspd is None and len(spdlist)>0:
			avgspd= np.median(spdlist)
		if avgspd is None and len(spdsIfEmpty)>0:
			avgspd= np.median(spdsIfEmpty)

		if len(spdlist)==0 and avgspd<=0:
			spdlist=[path[ppos1][KeyGPSSpeed],path[ppos1-1][KeyGPSSpeed],path[ppos1-2][KeyGPSSpeed]]
			avgspd= sum(spdlist)/len(spdlist)
		if avgspd<=0: 
			avgspd=spd01

		''' ------- put seg avg speed into mem, for rspeedinc next run '''
		if GenRealSegSpeed: 
			pair = (nid0,nid1)
			slst = mm_seg_spd.get(pair)
			if slst is None: slst=[]
			slst.append(avgspd)
			mm_seg_spd.set(pair,slst)
		assert avgspd>0, mfn
		minspd=avgspd # min speed in cross.

		speedDecPercent=0
		turntype=0
		nblist=mm_nid2neighbor.get(nid1)
		if nblist is None:
			print("\n\n\n bad mm_nid2neighbor... should exit !!\n\n")
			if realPath: return

		''' ------------- check if it is cross -------------- '''
		validCrossWithSpeedDecDistrib = 0 

		if len(nblist)>2 and i<=len(train_nid_pos)-2: # at crossing
			nextNid=nid2
			if iprint>=2: print("AT len(nblist)>2 --- n0,n1,n2,nb,lat1:",nid0,nid1,nextNid,nblist,latlng1)
			spd0 = spd01 # self Uturn
			spd1 = -1e-6 # left . neg mark as invalid 
			spd2 = -1e-6 # right
			spd3 = -1e-6 # straight
			nidleft=-1
			nidstraight=-1 # node id of this turn
			nidright=-1

			if nid0 in nblist: nblist.remove(nid0) 
			hdnidlist=[]
			for nbn in nblist:
				exnbn,latlngnb= extend_from_cross_center(nid1,nbn,mm_nid2latlng,mm_nid2neighbor)
				hdn = get_bearing_latlng2(latlng1,latlngnb)
				turnAngle=get_turn_angle(hd,hdn)
				hdnidlist.append([turnAngle,nbn])
			turn2hdnid =      analyze_crossing_given_turn_angle_nid(hdnidlist)

			if    is_valid_cross(turn2hdnid):
				if iprint>=2: 
					print(turn2hdnid,"turn2hdnid")
				Print_Here= False
				resdict =     get_turn_lv_spd_dict(turn2hdnid,nid1,nextNid, print_str = not Print_Here)
				if "spd2" in resdict: 
					spd2= resdict["spd2"]
					nidstraight=resdict["nidstraight"]
					if (iprint>=3 or print_str) and Print_Here: 
						print("  straight to",nbn,"spd2",spd2,"?=",spd0)
				if "spd3" in resdict:
					spd3= resdict["spd3"]
					vlevel3=resdict["vlv3"]
					nidright=resdict["nidright"]
					if (iprint>=3 or print_str) and Print_Here: 
						print("  right turn to",nbn,"spd3",spd3)
				if "spd1" in resdict: 
					spd1= resdict["spd1"]
					vlevel1=resdict["vlv1"]
					nidleft=resdict["nidleft"]
					if (iprint>=3 or print_str) and Print_Here: 
						print("  left turn to",nbn,"spd1",spd1)
				turntype=resdict["turntype"]
				if turntype<=0:
					print("\n\n\n bad turntype... should exit !!\n\n")
					return

				if turntype==Turn_Straight:
					numstraight+=1
				elif turntype==Turn_Right:
					numright+=1
				elif turntype==Turn_Left:
					numleft+=1

			validspdcnt=0
			if spd1>0: validspdcnt+=1 # some just straight into two split parallel roads, does not count.
			if spd2>0: validspdcnt+=1 # only leg with >45 deg angle valid.
			if spd3>0: validspdcnt+=1
			waittime=0.0
			if validspdcnt>=2 and turntype!=0: # don't allow unknown/invalid turn 
				crossPos=train_nid_pos[i][1]
				pp=crossPos 
				while pp>=0:
					latlng2=[path[pp][KeyGPSLat],path[pp][KeyGPSLng]]
					if KeyOBDSpeed in path[pp]:
						obdspd=path[pp][KeyOBDSpeed]/3.6
					else: obdspd=-1
					if KeyGPSSpeed in path[pp]:
						gpsspd=path[pp][KeyGPSSpeed]
					if obdspd<0: tmpspd=gpsspd
					else:
						if abs(obdspd-gpsspd)>8 and gpsspd<1: 
							tmpspd=gpsspd
						else: tmpspd=obdspd
					assert tmpspd>=0,path[pp]
					if minspd>tmpspd: # find min speed around cross.
						minspd=tmpspd 
					if tmpspd<1:
						waittime+=1
					if get_dist_meters_latlng2(latlng1,latlng2)>50:
						break # may have a queue waiting.
					if pp<=train_nid_pos[i-1][1]: break
					pp-=1
				pp=crossPos+1 # look at further trace
				while pp<len(path):
					latlng2=[path[pp][KeyGPSLat],path[pp][KeyGPSLng]]
					if KeyOBDSpeed in path[pp]:
						obdspd=path[pp][KeyOBDSpeed]/3.6
					else: obdspd=-1
					if KeyGPSSpeed in path[pp]:
						gpsspd=path[pp][KeyGPSSpeed]
					if obdspd<0: tmpspd=gpsspd
					else:
						if abs(obdspd-gpsspd)>8 and gpsspd<1: 
							tmpspd=gpsspd
						else: tmpspd=obdspd
					assert tmpspd>=0,path[pp]
					if minspd>tmpspd: # find min speed around cross.
						minspd=tmpspd
					if tmpspd<1:
						waittime+=1
					if get_dist_meters_latlng2(latlng1,latlng2)>10:
						break # beyond cross center 10
					if i<=len(train_nid_pos)-2 and pp>=train_nid_pos[i+1][1]: break
					pp+=1
				turn_sample={}
				turn_sample[TSelfV]=spd0
				turn_sample[TLeftV]=spd1
				turn_sample[TStraightV]=spd2
				turn_sample[TRightV]=spd3
				turn_sample[TminV]=minspd
				turn_sample[Ttype]=turntype
				speedDecPercent = max(0.0, avgspd-minspd)/avgspd
				turn_sample[TspdDec]=speedDecPercent
				turn_sample["mj?"]=None # not in use
				turn_sample[TnidStraight]=nidstraight
				turn_sample[TnidRight]=nidright
				turn_sample[TnidLeft]=nidleft
				turn_sample[TnidAt]=nid1
				turn_sample[TnidFrom]=nid0
				turn_sample[Ttime]=waittime
				turn_sample["addr"]=addr

				if realPath and len(bugTimes)==0:
					with semaphore: # turn features
						mm_turn_cnt=mm_train_turn.get("max_key_turn")
						if mm_turn_cnt is None: mm_turn_cnt=0
						mm_train_turn.set(mm_turn_cnt,turn_sample)
						mm_turn_cnt+=1
						mm_train_turn.set("max_key_turn",mm_turn_cnt)
						if iprint and (mm_turn_cnt%2000==10 or len(bugTimes)>0 or testcnt>0): 
							print(mm_turn_cnt,"turn_sample",turn_sample)
						
				'''  Truely stopped crossings: ''' 
				if minspd<SpeedAsStop:
					if turntype==1: stopleft+=1
					elif turntype==2: stopstraight+=1
					elif turntype==3: stopright+=1

				try: # test and print
					tmp = get_turn_cost(nid0,nid1,nextNid,print_str=False)
				except:
					print("\n\n\n bad ...2 should exit !!\n\n")
					return

				inferspd2inc+= tmp[0] # cumu speed^2 inc across stop prob
				probleft+= tmp[1]
				probright+= tmp[2]
				probstraight+= tmp[3]
				validCrossWithSpeedDecDistrib=1

			tripwaittime+=waittime
			
		if validCrossWithSpeedDecDistrib==0:
			'''--- not a cross, spd2 inc should be calc here: '''
			inferspd2inc+= max(0.0, spd12**2-spd01**2)
		else:
			''' is cross and may dec speed, get_turn_cost will calc part of it '''
			if speedDecPercent>0: 
				if iprint>=2: print(spd01,"speedDecPercent>0, v down to",spd01*(1-speedDecPercent))
				spd01= spd01*(1-speedDecPercent)
		
		dspd2inc+= max(0.0, spd12**2-spd01**2)
		lastavgspd = avgspd

	sample[KincSpeed2]=dspd2inc # true speed inc
	sample[TNleft]=numleft
	sample[TNright]=numright
	sample[TNstraight]=numstraight
	sample[TstopLeft]=stopleft
	sample[TstopRight]=stopright
	sample[TstopStraight]=stopstraight
	sample[TPleft]=probleft
	sample[TPright]=probright
	sample[TPstraight]=probstraight
	sample[TPspd2inc]=inferspd2inc # speed inc from turn stop distrib + not cross inc.
	sample[KaddWaitTime]=sample[Ktime]+tripwaittime # not in use
	sample[TPwtime]=sample[Ktime]+inferwaittime # not in use
	sample[RealSegSpeedinc]= rspeedinc

	''' fix net spd dec problem.'''
	spd0= get_speed_nid2(train_nid_pos[0][0],train_nid_pos[1][0])
	spd1= get_speed_nid2(train_nid_pos[-2][0],train_nid_pos[-1][0])
	sample[KincSpeed2] -= max(0, spd0**2 - spd1**2)
	sample[TPspd2inc] -= max(0, spd0**2 - spd1**2)

	sample["asp"]=     get_lv01_v01_given_nlist(nodes)
	if realPath and len(bugTimes)==0:
		with semaphore:
			mm_train_cnt=mm_train.get("max_key")
			if mm_train_cnt is None: mm_train_cnt=0
			mm_train.set(mm_train_cnt,sample)
			mm_train_cnt+=1
			mm_train.set("max_key",mm_train_cnt)

	if returnSampleStr:
		return pprint.pformat(sample, indent=2)


def get_turn_cost(nid0,nid1,nid2,print_str=False, country="US"):
	if iprint>=3 or print_str: 
		print("[ get_turn_cost ] Enter ---- %d, %d, %d"%(nid0,nid1,nid2)+ " "+country)
	spd= get_speed_nid2(nid0,nid1)
	if nid0==nid2:
		print("\n\n\n U-turn !!!\nget_turn_cost\n\n")
		print(nid0,nid1,nid2)
		return [spd**2, 1.0, 1.0, 1.0] # TODO
	if country=="US":
		distrib=distrib_frtag_totag_turn_vdiff_dec
		distrib_T=distrib_slftag_vtag_T_vhl_dec
	dspd2inc=0.0
	stopleft=0.0 # cumu prob 
	stopright=0.0
	stopstraight=0.0
	stopleft_T=0.0 # cumu prob for T-junc
	stopright_T=0.0
	stopstraight_T=0.0
	latlng0 = mm_nid2latlng.get(nid0)
	latlng1 = mm_nid2latlng.get(nid1)
	dist = get_dist_meters_latlng2(latlng0,latlng1)
	exnid0,exlatlng0= extend_from_cross_center(nid1,nid0,mm_nid2latlng,mm_nid2neighbor)
	hd= get_bearing_latlng2(exlatlng0,latlng1)
	nblist=mm_nid2neighbor.get(nid1)
	proc_cross=False # determined by is_valid_cross(turn2hdnid)

	if len(nblist)>2: # at crossing
		avgspd=spd
		if iprint>=3 or print_str: print("  self speed","spd0",spd)
		if avgspd<=0: 
			avgspd=SpeedTagLow
		turntype=0
		nextNid=nid2
		if iprint>=3 or print_str: print(nid0,nid1,nblist,latlng1,nextNid)
		spd0 = spd # self Uturn
		spd1 = -1e-6 # left
		spd2 = -1e-6 # straight
		spd3 = -1e-6 # right
		vlevel0 = get_spd_type_given_nid2(nid0,nid1) # priority, service road or etc.
		vlevel1 = -1 # priority on verti left leg
		vlevel3 = -1 # right arm

		if nid0 in nblist: 
			nblist.remove(nid0) # remove u-turn leg
		hdnidlist=[]
		for nbn in nblist:
			exnbn,latlngnb=   extend_from_cross_center(nid1,nbn,mm_nid2latlng,mm_nid2neighbor)
			hdn = get_bearing_latlng2(latlng1,latlngnb)
			turnAngle=get_turn_angle(hd,hdn)
			hdnidlist.append([turnAngle,nbn])
		turn2hdnid =     analyze_crossing_given_turn_angle_nid(hdnidlist)
		'''----- check U turn '''
		isUturn=0
		if "u" in turn2hdnid and turn2hdnid["u"][1]==nextNid: 
			isUturn=1
		elif "ulist" in turn2hdnid:
			for hdnid in turn2hdnid["ulist"]:
				if hdnid[1]==nextNid: 
					isUturn=1
		if isUturn: return [spd0**2, 1.0, 1.0, 1.0] # TODO

		if     is_valid_cross(turn2hdnid):
			resdict=     get_turn_lv_spd_dict(turn2hdnid,nid1,nextNid)
			if "spd2" in resdict: 
				spd2= resdict["spd2"]
			if "spd3" in resdict:
				spd3= resdict["spd3"]
				vlevel3=resdict["vlv3"]
			if "spd1" in resdict: 
				spd1= resdict["spd1"]
				vlevel1=resdict["vlv1"]
			turntype=resdict["turntype"]
			assert turntype>0, str([nid0,nid1,nid2])+str(turn2hdnid)+str(resdict)
			proc_cross=True

		''' infer min speed / stop? at this cross: '''
		is_T, armInd= is_on_T_junction_arm(turn2hdnid)
		if not is_T: # 4-arm cross
			if turntype>0: # don't allow unknown turn.
				vlevel1=max(vlevel1,vlevel3)
				vself= spd0
				hlself=get_spd_level_in_its_own_type(vself,vlevel0)
				vleg=[]
				if spd1>=0: vleg.append(spd1)
				if spd3>=0: vleg.append(spd3)
				vleg= sum(vleg)/len(vleg)
				hlverti=get_spd_level_in_its_own_type(vleg,vlevel1)
				dv2prob= distrib[vlevel0][vlevel1][turntype][hlself][hlverti]
				nextspd= get_speed_nid2(nid1,nextNid)
				for vdec,prob in dv2prob.items():# prob of stopping
					minspd=(1.0-vdec)*avgspd
					if minspd<SpeedAsStop:
						if turntype==Turn_Left: stopleft+=prob
						if turntype==Turn_Straight: stopstraight+=prob
						if turntype==Turn_Right: stopright+=prob
					dspd2inc+= max(0.0, nextspd**2- minspd**2)*prob
		else: # is T-junc
			if armInd==0:
				vlevel1=vlevel1
				vleg=spd1
				if turntype==Turn_Straight: T_type=T_turn02
				elif turntype==Turn_Left: T_type=T_turn01
			elif armInd==2:
				vlevel1=vlevel3
				vleg=spd3
				if turntype==Turn_Straight: T_type=T_turn20
				elif turntype==Turn_Right: T_type=T_turn21
			elif armInd==1:
				vlevel1=max(vlevel1,vlevel3)
				vleg=(spd1+spd3)/2.0
				if turntype==Turn_Right: T_type=T_turn10
				elif turntype==Turn_Left: T_type=T_turn12
			hlself=get_spd_level_in_its_own_type(spd0,vlevel0)
			hlverti=get_spd_level_in_its_own_type(vleg,vlevel1)
			dv2prob= distrib_T[vlevel0][vlevel1][T_type][hlself][hlverti]
			nextspd= get_speed_nid2(nid1,nextNid)
			for vdec,prob in dv2prob.items():# prob of stop, sum prob spd inc
				minspd=(1.0-vdec)*avgspd
				if minspd<SpeedAsStop:
					if turntype==Turn_Left: stopleft_T+=prob
					if turntype==Turn_Straight: stopstraight_T+=prob
					if turntype==Turn_Right: stopright_T+=prob
				dspd2inc+= max(0.0, nextspd**2- minspd**2)*prob

	if not proc_cross: # not crossing
		nextspd= get_speed_nid2(nid1,nid2)
		dspd2inc+=max(0.0, nextspd**2- spd**2)
	return [dspd2inc, stopleft+stopleft_T, stopright+stopright_T, stopstraight+stopstraight_T]


def gen_x(sp, model_features, print_feat=False):
	tmpx=[]
	for feat in model_features:
		tmpx.append(sp[feat])
	if print_feat and iprint>=2:
		print("gen_x model_features",model_features)
	return tmpx

def model_predict(x, model, print_feat=True):
	if print_feat and iprint>=2: print("[ model_predict ] coef_:",model.coef_)
	y_t = model.predict(np.asarray(x).reshape(1, -1))
	return y_t[0]


def get_sample_dist_cov(spmean,sp1,shrink_dic={} ):
	diffvec= []
	for feat in cov_dims:
		diffvec.append(spmean[feat]-sp1[feat])
	simi = covvar.pdf(diffvec)/covvar.pdf([0 for i in range(len(diffvec))])
	return 1-simi


def avg_lv01_v01_list(extra_samples):
	extraSp={}
	extraSp["lv0"]=[-1e-10, 0, 0.0]# sum,cnt,mean
	extraSp["lv1"]=[-1e-10, 0, 0.0]
	extraSp["lv2"]=[-1e-10, 0, 0.0]
	extraSp["lv3"]=[-1e-10, 0, 0.0]
	extraSp["v0"]=[-1e-10, 0, 0.0]
	extraSp["v1"]=[-1e-10, 0, 0.0]
	extraSp["v2"]=[-1e-10, 0, 0.0]
	extraSp["v3"]=[-1e-10, 0, 0.0]
	for sp in extra_samples:
		if sp is None or len(sp)==0: continue
		for k,v in sp.items():
			extraSp[k][0]+=v
			extraSp[k][1]+=1
	for k in extraSp.keys():
		extraSp[k][2]=extraSp[k][0]/max(1,extraSp[k][1])
	return extraSp

def extract_lv01_v01(extraSp):
	vself=[-1e-10,0]
	vleg=[-1e-10,0]
	if "v0" in extraSp:
		vself[0]+=extraSp["v0"][-1] # [-1] is mean val
		vself[1]+=1
	if "v2" in extraSp:
		vself[0]+=extraSp["v2"][-1]
		vself[1]+=1
	if "v1" in extraSp:
		vleg[0]+=extraSp["v1"][-1]
		vleg[1]+=1
	if "v3" in extraSp:
		vleg[0]+=extraSp["v3"][-1]
		vleg[1]+=1
	lvself=[-1e-10,0]
	lvleg=[-1e-10,0]
	if "lv0" in extraSp:
		lvself[0]+=extraSp["lv0"][-1]
		lvself[1]+=1
	if "lv2" in extraSp:
		lvself[0]+=extraSp["lv2"][-1]
		lvself[1]+=1
	if "lv1" in extraSp:
		lvleg[0]+=extraSp["lv1"][-1]
		lvleg[1]+=1
	if "lv3" in extraSp:
		lvleg[0]+=extraSp["lv3"][-1]
		lvleg[1]+=1
	tmp={}
	tmp["v0"]=vself[0]/max(1,vself[1])
	tmp["v1"]=vleg[0]/max(1,vleg[1])
	tmp["lv0"]=lvself[0]/max(1,lvself[1])
	tmp["lv1"]=lvleg[0]/max(1,lvleg[1])
	return tmp

def get_lv01_v01(nid0,nid1,nid2=None,at_first_nid=False,print_str=False):
	''' used to get info to match samples '''
	latlng0 = mm_nid2latlng.get(nid0)
	latlng1 = mm_nid2latlng.get(nid1)
	dist = get_dist_meters_latlng2(latlng0,latlng1)
	exnid0,exlatlng0= extend_from_cross_center(nid1,nid0,mm_nid2latlng,mm_nid2neighbor)
	hd= get_bearing_latlng2(exlatlng0,latlng1)
	spd= get_speed_nid2(nid0,nid1)
	sp={}
	if not at_first_nid: # asp from nid1 on.
		nblist=mm_nid2neighbor.get(nid1)
		if len(nblist)>2: # at cross
			sp["lv0"]= get_spd_type_given_nid2(nid0,nid1)
			sp["v0"]=spd
			if nid0 in nblist: 
				nblist.remove(nid0) # remove u-turn leg
			hdnidlist=[]
			for nbn in nblist:
				exnbn,latlngnb= extend_from_cross_center(nid1,nbn,mm_nid2latlng,mm_nid2neighbor)
				hdn = get_bearing_latlng2(latlng1,latlngnb)
				turnAngle=get_turn_angle(hd,hdn)
				hdnidlist.append([turnAngle,nbn])
			if len(hdnidlist)==0: return None
			turn2hdnid = analyze_crossing_given_turn_angle_nid(hdnidlist)
			if is_valid_cross(turn2hdnid):
				if "s" in turn2hdnid:
					nbn=turn2hdnid["s"][1]
					spd2= get_speed_nid2(nid1,nbn)
					sp["v2"]=spd2
					sp["lv2"]=get_spd_type_given_nid2(nid1,nbn)
				if "r" in turn2hdnid:
					nbn=turn2hdnid["r"][1]
					spd3= get_speed_nid2(nid1,nbn)
					vlevel2=get_spd_type_given_nid2(nid1,nbn)
					sp["v3"]=spd3
					sp["lv3"]=vlevel2
				elif has_right_arm(turn2hdnid):
					howmany=len(turn2hdnid["rlist"])
					nbn=turn2hdnid["rlist"][howmany//2][1]
					spd3= get_speed_nid2(nid1,nbn)
					vlevel2=get_spd_type_given_nid2(nid1,nbn)
					sp["v3"]=spd3
					sp["lv3"]=vlevel2
				if "l" in turn2hdnid:
					nbn=turn2hdnid["l"][1]
					spd1= get_speed_nid2(nid1,nbn)
					vlevel1=get_spd_type_given_nid2(nid1,nbn)
					sp["v1"]=spd1
					sp["lv1"]=vlevel1
				elif has_left_arm(turn2hdnid):
					howmany=len(turn2hdnid["llist"])
					nbn=turn2hdnid["llist"][(howmany-1)//2][1]# avoid parallel split.
					spd1= get_speed_nid2(nid1,nbn)
					vlevel1=get_spd_type_given_nid2(nid1,nbn)
					sp["v1"]=spd1
					sp["lv1"]=vlevel1

	else: # consider both nid0 and nid1.
		hd= get_bearing_latlng2(latlng1,latlng0)
		nblist=mm_nid2neighbor.get(nid0) 
		if len(nblist)>2:#  at cross
			if nid1 in nblist: 
				nblist.remove(nid1) # nid1->nid0->...
			hdnidlist=[]
			for nbn in nblist:
				exnbn,latlngnb= extend_from_cross_center(nid0,nbn,mm_nid2latlng,mm_nid2neighbor)
				hdn = get_bearing_latlng2(latlng0,latlngnb)
				turnAngle=get_turn_angle(hd,hdn)
				hdnidlist.append([turnAngle,nbn])
			if len(hdnidlist)==0: return None
			turn2hdnid = analyze_crossing_given_turn_angle_nid(hdnidlist)
			if is_valid_cross(turn2hdnid):
				if "s" in turn2hdnid:
					nbn=turn2hdnid["s"][1]
					spd2= get_speed_nid2(nid0,nbn)
					sp["v2"]=spd2
					sp["lv2"]=get_spd_type_given_nid2(nid0,nbn)
				if "r" in turn2hdnid:
					nbn=turn2hdnid["r"][1]
					spd3= get_speed_nid2(nid0,nbn)
					vlevel2=get_spd_type_given_nid2(nid0,nbn)
					sp["v3"]=spd3
					sp["lv3"]=vlevel2
				elif has_right_arm(turn2hdnid):
					howmany=len(turn2hdnid["rlist"])
					nbn=turn2hdnid["rlist"][howmany//2][1]
					spd3= get_speed_nid2(nid0,nbn)
					vlevel2=get_spd_type_given_nid2(nid0,nbn)
					sp["v3"]=spd3
					sp["lv3"]=vlevel2
				if "l" in turn2hdnid:
					nbn=turn2hdnid["l"][1]
					spd1= get_speed_nid2(nid0,nbn)
					vlevel1=get_spd_type_given_nid2(nid0,nbn)
					sp["v1"]=spd1
					sp["lv1"]=vlevel1
				elif has_left_arm(turn2hdnid):
					howmany=len(turn2hdnid["llist"])
					nbn=turn2hdnid["llist"][(howmany-1)//2][1]
					spd1= get_speed_nid2(nid0,nbn)
					vlevel1=get_spd_type_given_nid2(nid0,nbn)
					sp["v1"]=spd1
					sp["lv1"]=vlevel1
	return sp


def get_lv01_v01_given_nlist(nlist):
	ex_splist=[]
	for i in range(len(nlist)-1):
		nid0=nlist[i]
		nid1=nlist[i+1]
		if i==0 and len(mm_nid2neighbor.get(nid0))>2: 
			sp=get_lv01_v01(nid0,nid1,at_first_nid=True)
			ex_splist.append(sp)
		if len(mm_nid2neighbor.get(nid1))>2:
			if i+2<len(nlist):
				nid2=nlist[i+2]
				sp=get_lv01_v01(nid0,nid1,nid2)
				ex_splist.append(sp)
			else:
				sp=get_lv01_v01(nid0,nid1)
				ex_splist.append(sp)
	extraSp=avg_lv01_v01_list(ex_splist)
	asp=extract_lv01_v01(extraSp)
	return asp


def get_avg_time_given_nlist(nlist):
	return get_time_given_nlist(nlist)
def get_time_given_nlist(nlist):
	totaltime=0.0
	for i in range(len(nlist)-1):
		dist=get_dist_meters_osm_nid2(nlist[i],nlist[i+1])
		spd=get_speed_nid2(nlist[i],nlist[i+1])
		totaltime+=dist/spd
	return totaltime
def get_dist_meters_given_nlist(nlist):
	totaldist=0.0
	for i in range(len(nlist)-1):
		totaldist+=get_dist_meters_osm_nid2(nlist[i],nlist[i+1])
	return totaldist
def get_dist_meters_osm_nid2(n1,n2):
	latlng0 = mm_nid2latlng.get(n1)
	latlng1 = mm_nid2latlng.get(n2)
	return get_dist_meters_latlng2(latlng1,latlng0)


def get_real_spd_given_nid2(*args): # same as in 5.py 
	if isinstance(args[0],tuple): 
		tup=args[0]
	else:
		tup=(args[0],args[1])
	return mm_nids2speed.get(tup)
def get_speed_nid2(nid0,nid1,lastspd=None): # use this func.
	spd = get_real_spd_given_nid2((nid0,nid1))
	if spd is None:
		spd = get_real_spd_given_nid2((nid1,nid0))
		if lastspd is not None:
			spd=lastspd
		else: spd=SpeedTagLow-0.1
	return spd

def get_avg_speed_given_nlist(nlist):
	avgt= get_avg_time_given_nlist(nlist)
	dist= get_dist_meters_given_nlist(nlist)
	return dist/avgt
def get_MPG_given_meters_gram(dist,gas):
	gallon = max(0.000000001,gas/GramPerGallon)
	miles=dist/MetersPerMile
	return miles/gallon
def get_mpg(d,g):
	return get_MPG_given_meters_gram(d,g)



def get_way_tag_given_nid2(n1,n2,strict=True): # n1->n2->'residential'
	if n2<0: return ""
	tg=mm_nid2waytag.get((n1,n2))
	if tg is not None: return tg
	tg=mm_nid2waytag.get((n2,n1))
	if tg is not None: return tg
	if not strict:
		tg=mm_nid2waytag.get(n1) # may be wrong tag!
		if tg is not None: return tg
		tg=mm_nid2waytag.get(n2)
		if tg is not None: return tg
	return ""

def get_spd_type_given_nid2(n1,n2): # n1,n2 -> 0,1,2 
	tg0=get_way_tag_given_nid2(n1,n2)
	if tg0=="": return Spd_Type_Slow
	if tg0 in Highway_Fast_taglist: return Spd_Type_Fast # primary
	elif tg0 in Highway_Medium_taglist: return Spd_Type_Medium
	return Spd_Type_Slow

def get_spd_level_in_its_own_type(v,typ):#v meters
	if typ==Spd_Type_Slow:
		if v<SpeedTagLow: return Vtype_lower
		else: return Vtype_higher
	elif typ==Spd_Type_Fast:
		if v<SpeedTagHigh: return Vtype_lower
		else: return Vtype_higher
	else:
		if v<SpeedTagMedium: return Vtype_lower
		else: return Vtype_higher

def get_highlow_given_v_tag(vmeters,typ):
	v = vmeters/Mph2MetersPerSec
	if typ==Spd_Type_Slow:
		if v<MedianSpeedTagLowInTrain: return 0
		else: return 1
	elif typ==Spd_Type_Fast:
		if v<MedianSpeedTagHighInTrain: return 0
		else: return 1
	else:
		if v<MedianSpeedTagMediumInTrain: return 0
		else: return 1




SpeedDiffThresh = get_conf_float(configfile,"SpeedDiffThresh")
SpeedAsStop = get_conf_float(configfile,"SpeedAsStop") # should be 3.0-3.5 m/s
MinSegDistForMPG = get_conf_float(configfile,"MinSegDistForMPG") # meters when calc penalty.
MinSegMpg = get_conf_float(configfile,"MinSegMpg") # min mpg cost
MaxSegMpg = get_conf_float(configfile,"MaxSegMpg") # max mpg cost
MultiplySegMpg = get_conf_float(configfile,"MultiplySegMpg") # boost mpg to emphasize turn cost 
AddTurnPenalty = get_conf_float(configfile,"AddTurnPenalty") # add more turn penalty.
UTurnTimePenalty = get_conf_float(configfile,"UTurnTimePenalty") 
RatioSegCostMpgComparedToKmh = get_conf_float(configfile,"RatioSegCostMpgComparedToKmh") 

SpeedTagHigh=get_conf_float(configfile,"SpeedTagHigh")  # primary,secondary..
SpeedTagMedium=get_conf_float(configfile,"SpeedTagMedium")  # v residential..
SpeedTagLow=get_conf_float(configfile,"SpeedTagLow")  # v service..
MedianSpeedTagHighInTrain=11.2/Mph2MetersPerSec # mi/h
MedianSpeedTagMediumInTrain=5.8/Mph2MetersPerSec
MedianSpeedTagLowInTrain=3.6/Mph2MetersPerSec

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
KeyGPSAltitude=get_conf(configfile,"KeyGPSAltitude")
KeyGas=get_conf(configfile,"KeyGas") # Gas g 
KeyRPM=get_conf(configfile,"KeyRPM") 
KeyOBDSpeed=get_conf(configfile,"KeyOBDSpeed")
KeyMAF=get_conf(configfile,"KeyMAF") 
KeyThrottle=get_conf(configfile,"KeyThrottle") 
KeyOriSysMs=get_conf(configfile,"KeyOriSysMs")
KeyCarMake=get_conf(configfile,"KeyCarMake")
KeyCarModel=get_conf(configfile,"KeyCarModel")
KeyCarYear=get_conf(configfile,"KeyCarYear")
KeyCarClass=get_conf(configfile,"KeyCarClass")
KeyTripID=get_conf(configfile,"KeyTripID")


def get_turn_lv_spd_dict(turn2hdnid,nidCenter,nextNid , validTurnAngleThresh=kMinTurnAngleDiff, print_str=False):
	res={}
	nid1=nidCenter
	turntype=-1
	if "s" in turn2hdnid:
		nbn=turn2hdnid["s"][1]
		res["nidstraight"]=nbn
		res["spd2"] = get_speed_nid2(nid1,nbn)
		if nextNid==nbn: turntype=Turn_Straight
	if "r" in turn2hdnid:
		nbn=turn2hdnid["r"][1]
		res["nidright"]=nbn
		res["spd3"] = get_speed_nid2(nid1,nbn)
		res["vlv3"]=get_spd_type_given_nid2(nid1,nbn)
		if nextNid==nbn: turntype=Turn_Right
	if "rlist" in turn2hdnid:
		for hdnid in turn2hdnid["rlist"]: 
			if nextNid==hdnid[1]: # matched right turn first priority.
				if abs(hdnid[0])>validTurnAngleThresh:
					turntype=Turn_Right
					res["nidright"]=nextNid
					res["spd3"] = get_speed_nid2(nid1,nextNid)
					res["vlv3"]=get_spd_type_given_nid2(nid1,nextNid)
				else: # "s" is wrong...
					res["spd2"] = get_speed_nid2(nid1,nextNid)
					res["nidstraight"]=nextNid
					turntype=Turn_Straight
				break
		if "spd3" not in res and abs(turn2hdnid["rlist"][-1][0])>validTurnAngleThresh:
			howmany=len(turn2hdnid["rlist"])
			nbn=turn2hdnid["rlist"][howmany//2][1]
			res["nidright"]=nbn
			res["spd3"] = get_speed_nid2(nid1,nbn)
			res["vlv3"]=get_spd_type_given_nid2(nid1,nbn)
	if "l" in turn2hdnid:
		nbn=turn2hdnid["l"][1]
		res["nidleft"]=nbn
		res["spd1"] = get_speed_nid2(nid1,nbn)
		res["vlv1"]=get_spd_type_given_nid2(nid1,nbn)
		if nextNid==nbn: turntype=Turn_Left
	if "llist" in turn2hdnid:
		for hdnid in turn2hdnid["llist"]: 
			if nextNid==hdnid[1]:
				if abs(hdnid[0])>validTurnAngleThresh:
					turntype=Turn_Left
					res["nidleft"]=nextNid
					res["spd1"] = get_speed_nid2(nid1,nextNid)
					res["vlv1"]=get_spd_type_given_nid2(nid1,nextNid)
				else:
					res["spd2"] = get_speed_nid2(nid1,nextNid)
					res["nidstraight"]=nextNid
					turntype=Turn_Straight
				break
		if "spd1" not in res and abs(turn2hdnid["llist"][0][0])>validTurnAngleThresh:
			howmany=len(turn2hdnid["llist"])
			nbn=turn2hdnid["llist"][(howmany-1)//2][1]
			res["nidleft"]=nbn
			res["spd1"] = get_speed_nid2(nid1,nbn)
			res["vlv1"]=get_spd_type_given_nid2(nid1,nbn)
	res["turntype"]=turntype
	if iprint and print_str: print("   turntype is %d\n"%turntype)
	return res


def analyze_crossing_given_turn_angle_nid(hdnidlist,UTurnAngleThresh=20):
	''' [ [heading, nid], ] analyze types and store in res dict.'''
	hdnidlist=sorted(hdnidlist) # left hd first, right hd last.
	res={}
	'''---------- remove left-side u-turn first '''
	Ut=None
	if abs(hdnidlist[0][0]+180.0)<UTurnAngleThresh:
		Ut=hdnidlist.pop(0)
	if Ut is not None:
		res={"u":Ut}
	''' remove more u-turns: more left/right-side uturns (e.g. hw merge link)'''
	for i in range(len(hdnidlist)):
		if abs(hdnidlist[i][0] +180.0)<UTurnAngleThresh or abs(hdnidlist[i][0] -180.0)<UTurnAngleThresh:
			if "ulist" not in res: res["ulist"]=[]
			res["ulist"].append(hdnidlist[i])
	if "ulist" in res:
		for hdnid in res["ulist"]: hdnidlist.remove(hdnid)
	'''------------- remove most likely straight turn, here NO slist in res!!! '''
	STurnAngleThresh=10
	sid=None
	for i in range(len(hdnidlist)): # however, I ignored one way info... ___
		if hdnidlist[i][0]<STurnAngleThresh and hdnidlist[i][0]> -2.0:
			sid=i
			break
	if sid is not None:
		res["s"]=hdnidlist.pop(sid)
	''' remove 2nd straight if not found '''
	if sid is None:
		STurnAngleThresh=20
		minangle=1e10
		for i in range(len(hdnidlist)):
			if abs(hdnidlist[i][0])<STurnAngleThresh and abs(hdnidlist[i][0])<minangle:
				minangle=abs(hdnidlist[i][0])
				sid=i
		if sid is not None:
			res["s"]=hdnidlist.pop(sid)
	'''------------- remove most likely Left turn '''
	LTurnAngleNorm=-90.0
	minangle=45
	lid=None
	for i in range(len(hdnidlist)):
		if abs(hdnidlist[i][0]-LTurnAngleNorm)<minangle:
			minangle=abs(hdnidlist[i][0])
			lid=i
	if lid is not None:
		res["l"]=hdnidlist.pop(lid)
	''' remove all other Left turns '''
	for i in range(len(hdnidlist)):
		if hdnidlist[i][0]<0:
			if "llist" not in res: res["llist"]=[]
			res["llist"].append(hdnidlist[i])
	if "llist" in res:
		for hdnid in res["llist"]: hdnidlist.remove(hdnid)
	'''------------- remove most likely Right turn '''
	RTurnAngleNorm=90.0
	minangle=45
	rid=None
	for i in range(len(hdnidlist)):
		if abs(hdnidlist[i][0]-RTurnAngleNorm)<minangle:
			minangle=abs(hdnidlist[i][0])
			rid=i
	if rid is not None:
		res["r"]=hdnidlist.pop(rid)
	''' remove all other Right turns '''
	for i in range(len(hdnidlist)):
		if hdnidlist[i][0]>0:
			if "rlist" not in res: res["rlist"]=[]
			res["rlist"].append(hdnidlist[i])
	if "rlist" in res:
		for hdnid in res["rlist"]: hdnidlist.remove(hdnid)
	if len(hdnidlist)>0: # remain some straight ones, by observation.
		STurnAngleThresh=20
		for i in range(len(hdnidlist)):
			if abs(hdnidlist[i][0])<STurnAngleThresh:
				if 'slist' not in res: res['slist']=[]
				res["slist"].append(hdnidlist[i])
		if "slist" in res:
			for hdnid in res["slist"]: hdnidlist.remove(hdnid)
	assert len(hdnidlist)==0, str(hdnidlist)+str(res)
	return res # =turn2hdnid


def is_valid_cross(turn2hdnid):
	''' split into parallel is not valid '''
	if has_straight_arm(turn2hdnid):
		if has_left_arm(turn2hdnid): 
			return True
		elif has_right_arm(turn2hdnid):
			return True
	elif has_left_arm(turn2hdnid) and has_right_arm(turn2hdnid): 
		return True
	elif has_left_arm(turn2hdnid) and count_left_arms(turn2hdnid)>1:
		return True
	elif has_right_arm(turn2hdnid) and count_right_arms(turn2hdnid)>1:
		return True
	return False


def has_left_arm(turn2hdnid,validTurnAngleThresh=kMinTurnAngleDiff):
	return "l" in turn2hdnid or ("llist" in turn2hdnid and abs(turn2hdnid["llist"][0][0])>validTurnAngleThresh)
def has_right_arm(turn2hdnid,validTurnAngleThresh=kMinTurnAngleDiff):
	return "r" in turn2hdnid or ("rlist" in turn2hdnid and abs(turn2hdnid["rlist"][-1][0])>validTurnAngleThresh)
def has_straight_arm(turn2hdnid):
	return "s" in turn2hdnid
def count_left_arms(turn2hdnid,validTurnAngleThresh=kMinTurnAngleDiff):
	cnt=0
	if "l" in turn2hdnid: cnt+=1
	if "llist" in turn2hdnid:
		for hdnid in turn2hdnid["llist"]:
			if abs(hdnid[0])>validTurnAngleThresh: cnt+=1
	return cnt
def count_right_arms(turn2hdnid,validTurnAngleThresh=kMinTurnAngleDiff):
	cnt=0
	if "r" in turn2hdnid: cnt+=1
	if "rlist" in turn2hdnid:
		for hdnid in turn2hdnid["rlist"]:
			if abs(hdnid[0])>validTurnAngleThresh: cnt+=1
	return cnt


def is_on_T_junction_arm(turn2hdnid):
	'''
	2 --- center ---- 0
			|
			1
	'''
	if has_straight_arm(turn2hdnid):
		if has_left_arm(turn2hdnid): 
			if has_right_arm(turn2hdnid):
				return False, -1
			return True,0 # from arm-0
		elif has_right_arm(turn2hdnid):
			return True,2 # from arm-2
	elif has_left_arm(turn2hdnid) and count_left_arms(turn2hdnid)==1 and has_right_arm(turn2hdnid) and count_right_arms(turn2hdnid)==1: 
		return True,1
	elif has_left_arm(turn2hdnid) and count_left_arms(turn2hdnid)>1 and not has_right_arm(turn2hdnid):
		return True,0
	elif has_right_arm(turn2hdnid) and count_right_arms(turn2hdnid)>1 and not has_left_arm(turn2hdnid):
		return True,2
	return False,-1


def extend_from_cross_center(nidCenter,nidArm,mm_nid2latlng,mm_nid2neighbor, SameDirAngleThresh=40, MinDistForTrueAngle=80):
	nid1=nidCenter
	nid0=nidArm
	latlng0 = mm_nid2latlng.get(nid0)
	latlng1 = mm_nid2latlng.get(nid1)
	hd01 = get_bearing_latlng2(latlng0,latlng1)
	dist01 = get_dist_meters_latlng2(latlng0,latlng1)
	''' extend 1->0->... backwards find in-angle '''
	lastdist= dist01
	lasthead = hd01
	lastnid= nid1
	nownid= nid0
	exnid0=None
	halfnid0=None # if arm extend angle change >90, back off.
	HalfDistForTrueAngle=    30
	while lastdist<=MinDistForTrueAngle:
		nblist=mm_nid2neighbor.get(nownid)
		if lastnid in nblist: 
			nblist.remove(lastnid)
		nowlatlng = mm_nid2latlng.get(nownid)
		mindiff=1000
		nextnid=None
		for nbnid in nblist:
			latlngnb = mm_nid2latlng.get(nbnid)
			nbhd = get_bearing_latlng2(latlngnb,nowlatlng)
			anglediff= min_angle_diff(nbhd,lasthead)
			if mindiff>anglediff:
				mindiff=anglediff
				nextnid=nbnid
				nextlatlng=latlngnb
				nexthead=nbhd
		if nextnid is None or mindiff> SameDirAngleThresh:# extend within this angle.
			if iprint>=3: print("nextnid,mindiff",nextnid,mindiff)
			break
		lastdist += get_dist_meters_latlng2(nowlatlng,nextlatlng)
		lastnid=nownid
		nownid = nextnid
		lasthead = nexthead
		absangle= min_angle_diff(hd01,nexthead)
		if absangle>    60: # change too much, maybe roundabout or hw cross. back off.
			if halfnid0 is not None:
				exnid0=halfnid0
				exlatlng=halflatlng
				if iprint>=3: print("angle change too much! back off",absangle,exnid0)
				break
		if lastdist>MinDistForTrueAngle:# meters to get true arm angle.
			exnid0=nownid
			exlatlng=nextlatlng
			if iprint>=3: print("lastdist>MinDistForTrueAngle",lastdist)
			break
		if lastdist>HalfDistForTrueAngle and halfnid0 is None:
			halfnid0=nownid
			halflatlng=nextlatlng
	if exnid0 is None: 
		if halfnid0 is not None:
			exnid0=halfnid0
			exlatlng=halflatlng
		else:
			exnid0=nid0
			exlatlng=latlng0
	return [exnid0,exlatlng]


def get_extended_turn_angle(nid0,nid1,nid2,mm_nid2latlng,mm_nid2neighbor,print_str=False):
	''' in case many short nodes change shape at cross, giving false angle at 1st hop'''
	latlng1 = mm_nid2latlng.get(nid1)
	exnid0,latlng0 = extend_from_cross_center(nid1,nid0,mm_nid2latlng,mm_nid2neighbor)
	exnid2,latlng2 = extend_from_cross_center(nid1,nid2,mm_nid2latlng,mm_nid2neighbor)
	hd01 = get_bearing_latlng2(latlng0,latlng1)
	hd12 = get_bearing_latlng2(latlng1,latlng2)
	if print_str and iprint: print(exnid0,nid1,exnid2,"ex hd",hd01,hd12)
	return get_turn_angle(hd01,hd12)


def find_bearing(path, pos, search_forward=1): # find heading near [pos] on path, in case missing. path: lst of dic.
	if KeyGPSBearing in path[pos]: 
		return path[pos][KeyGPSBearing]
	if search_forward>0:
		j=pos+1
		while j<len(path):
			if KeyGPSBearing in path[j]: return path[j][KeyGPSBearing]
			j+=1
	else:
		j=pos-1
		while j>=0:
			if KeyGPSBearing in path[j]: return path[j][KeyGPSBearing]
			j-=1
	''' heading according to moved direction is bad !!! hopefully not go below:'''
	j=pos+1
	while j<len(path) and get_dist_meters_latlng2([path[pos][KeyGPSLat],path[pos][KeyGPSLng]],[path[j][KeyGPSLat],path[j][KeyGPSLng]])<5:
		if KeyGPSBearing in path[j]: return path[j][KeyGPSBearing]
		j+=1
	if j<len(path): 
		return get_bearing_latlng2([path[pos][KeyGPSLat],path[pos][KeyGPSLng]],[path[j][KeyGPSLat],path[j][KeyGPSLng]])
	j=pos-1
	while j>=0 and get_dist_meters_latlng2([path[pos][KeyGPSLat],path[pos][KeyGPSLng]],[path[j][KeyGPSLat],path[j][KeyGPSLng]])<5:
		if KeyGPSBearing in path[j]: return path[j][KeyGPSBearing]
		j-=1
	if j>=0:
		return get_bearing_latlng2([path[j][KeyGPSLat],path[j][KeyGPSLng]],[path[pos][KeyGPSLat],path[pos][KeyGPSLng]])
	for pos in range(len(path)):
		if KeyGPSBearing in path[pos]: return path[pos][KeyGPSBearing]



def find_start_path_pos_on_seg(path,indfrom,indto,latlng0,latlng1, print_str=False):
	''' This func is used to both find start and find end.'''
	hd1 = get_bearing_latlng2(latlng0,latlng1)
	d1=get_dist_meters_latlng2(latlng1,latlng0)
	startpos=None
	distMatched=0
	angleMatched=0
	for pp in range( max(0,indfrom), len(path) ): # in case indfrom==indto 
		dic=path[pp]
		splat = dic[KeyGPSLat]
		splng = dic[KeyGPSLng]
		sphead= find_bearing(path, pp, search_forward=0)
		if KeyGPSAccuracy in dic:
			accu=max(5,dic[KeyGPSAccuracy])
		else:
			accu=10.0
		dist = get_dist_meters_latlng2([splat,splng],latlng0)
		if dist> accu+25 and distMatched==0: 
			continue
		distMatched=1
		angle90 = get_bearing_latlng2(latlng0, [splat,splng])

		if d1<kTrivialSegLength and min_angle_diff(angle90,sphead)<kAngle90Thresh: 
			return pp
		if not (headings_all_close([hd1,sphead],thresh=30) and min_angle_diff(angle90,hd1)<kAngle90Thresh ) and angleMatched==0:
			continue
		angleMatched=1
		dist = dist_point_to_line_of_2pts([splat,splng], latlng0, latlng1)
		if dist > accu+25:
			continue
		startpos=pp
		break
	return startpos

def find_end_path_pos_on_seg(path,indfrom,indto,latlng0,latlng1, tolerate_end_dist=0, tolerate_end_angle=0):
	''' This func only be used once at the last osm node, others use previous func to find end.'''
	hd1 = get_bearing_latlng2(latlng0,latlng1)
	d1=get_dist_meters_latlng2(latlng1,latlng0)
	endpos=None
	distMatched=0
	assert max(0,indfrom) <= len(path), "ERR common.py find_end_path_pos_on_seg()"
	for pp in range( max(0,indfrom), len(path) ):
		dic=path[pp]
		splat = dic[KeyGPSLat]
		splng = dic[KeyGPSLng]
		sphead= find_bearing(path, pp,search_forward=1)
		if KeyGPSAccuracy in dic:
			accu=max(5,dic[KeyGPSAccuracy])
		else:
			accu=10.0
		dist = get_dist_meters_latlng2([splat,splng],latlng1)
		if dist> accu+25+tolerate_end_dist and distMatched==0: 
			continue
		distMatched=1
		if iprint and tolerate_end_dist>0:
			print("dist matched!")
		angle90 = get_bearing_latlng2(latlng1, [splat,splng])
		if d1<kTrivialSegLength and min_angle_diff(angle90,sphead)<kAngle90Thresh: # is seg is too small to have angle match, use sphead.
			return pp
		if (headings_all_close([hd1,sphead], thresh=80) and min_angle_diff(angle90,hd1)>kAngle90Thresh+tolerate_end_angle):
			continue
		endpos=pp
		break
	return endpos



def mean_absolute_percentage_error(y_true, y_pred): 
	return np.mean(np.abs((y_true - y_pred) / y_true))
def mean_signed_percentage_error(y_true, y_pred): 
	return np.mean( (y_true - y_pred) / y_true)
def plot_hist_xlist(xlist,width):
	bin=PlotDistributionBins()
	bin.show_hist(  xlist, width, y_percentage=True)
def show_hist(y_true, y_pred, width): 
	plot_hist_xlist((y_pred-y_true) / y_true, width)
def is_invertible(a):
	check1= (a.shape[0] == a.shape[1] and matrix_rank(a) == a.shape[0])
	print("rank",matrix_rank(a))
	if not check1: return False
	det = np.linalg.det(a)
	print("det",det)
	if det == 0: return False
	if det<sys.float_info.epsilon: return False
	return True



def get_user_car_info(fpath):
	with gzip.open(fpath,"rb") as f:
		dic={}
		for l in f:
			st = l.split(CUT)
			for x in st:
				if EQU in x:
					dic[x.split(EQU)[0]] = x.split(EQU)[1]
	if dic[KeyUserName]=="": 
		dic[KeyUserName]=UnknownUserEmail
	userinfo = {
		KeyUserName:dic[KeyUserName],
		KeyCarMake:dic[KeyCarMake],
		KeyCarModel:dic[KeyCarModel],
		KeyCarYear:dic[KeyCarYear],
		KeyCarClass:dic[KeyCarClass],
	}
	return userinfo






