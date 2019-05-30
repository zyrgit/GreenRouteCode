#!/usr/bin/env python

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
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
if mypydir not in sys.path: sys.path.append(mypydir)
from namehostip import get_my_ip
from hostip import ip2tarekc
from readconf import get_conf,get_conf_int,get_conf_float,get_list_startswith,get_dic_startswith
from logger import Logger,SimpleAppendLogger,ErrorLogger
from util import read_lines_as_list,read_lines_as_dic,read_gzip,strip_illegal_char,strip_newline,unix2datetime,get_file_size_bytes,py_fname
from myexception import ErrorNotInCache
from myosrm import connect_dots,check_if_nodes_are_connected,convert_path_to_nids
from CacheManager import CacheManager
from osmutil import osmPipeline
from mem import Mem,AccessRestrictionContext
from geo import convert_line_to_dic, get_dist_meters_latlng, latlng_to_city_state_country, get_bearing_latlng2, min_angle_diff, dist_point_to_line_of_2pts, headings_all_close, get_dist_meters_latlng2


configfile = "conf.txt"
DirData = get_conf(configfile,"DirData") # ~/greendrive/proc
DirOSM = get_conf(configfile,"DirOSM") # ~/greendrive/osmdata
gpsfolder = "gps"
obdfolder = "obd"
combinefolder ="combine"
matchfolder="match"
HomeDir = os.path.expanduser("~")

iprint = 2  
Overwrite_Match_files= ("o" in sys.argv[1:]) and False # "rm "+matchDir+os.sep+truetimestr+"*"?
CheckNewlyApprovedCities = True # if you want to override -NotApprove suffix and redo them.
CheckAgainOutBBox = True # if you want to override -0OutBBox suffix and redo them.

err = ErrorLogger("allerror.txt", tag=py_fname(__file__,False))
lg = SimpleAppendLogger("logs/"+py_fname(__file__,False), maxsize=10000, overwrite=True)

tinyURL=True # prefix shortened
mm_nid2latlng = CacheManager(overwrite_prefix=tinyURL,)
mm_nid2elevation=CacheManager(overwrite_prefix=tinyURL,)
mm_nids2speed = CacheManager(overwrite_prefix=tinyURL,)
mm_nid2neighbor=CacheManager(overwrite_prefix=tinyURL,)

EXT = get_conf(configfile,"EXT") # .gz 
CUT = get_conf(configfile,"CUT") # ~| 
EQU = get_conf(configfile,"EQU",delimiter=":")
KeyUserEmail = get_conf(configfile,"KeyUserEmail") 
KeyUserName = get_conf(configfile,"KeyUserName") 
UnknownUserEmail = get_conf(configfile,"UnknownUserEmail") 
KeySysMs=get_conf(configfile,"KeySysMs")
KeyGPSTime=get_conf(configfile,"KeyGPSTime")
KeyGPSLat=get_conf(configfile,"KeyGPSLat")
KeyGPSLng=get_conf(configfile,"KeyGPSLng")
KeyGPSAccuracy=get_conf(configfile,"KeyGPSAccuracy")
KeyGPSSpeed=get_conf(configfile,"KeyGPSSpeed")
KeyGPSBearing=get_conf(configfile,"KeyGPSBearing")
KeyGPSAltitude=get_conf(configfile,"KeyGPSAltitude")
KeyGas=get_conf(configfile,"KeyGas") 
KeyRPM=get_conf(configfile,"KeyRPM") 
KeyOBDSpeed=get_conf(configfile,"KeyOBDSpeed")
KeyMAF=get_conf(configfile,"KeyMAF") 
KeyThrottle=get_conf(configfile,"KeyThrottle") 
KeyOriSysMs=get_conf(configfile,"KeyOriSysMs")

''' same as in 1.py, 2.py, 5.py '''
kCutTraceTimeGap = get_conf_int(configfile,"kCutTraceTimeGap") # timestamp between two lines > this then cut
kCutTraceDistGap = min(150,kCutTraceTimeGap* 50) # dist between two lines > this then cut.
kCombineFileMinBytes = 5000 # about 10+ lines, skip.

account_dirs = glob.glob(DirData+"/*")

''' This func writes osm node ids matched_nid_on_path[] and combined path line number ind_on_path[] as a pair to a file, match/*-*,   no need for semaphore'''
def write_matched_nids_pos(matched_nid_on_path,ind_on_path,segpath_pos_offset,fname,mm_external=None):
	brokenPosList=[]
	write_num_nids=len(matched_nid_on_path)
	if not check_if_nodes_are_connected(matched_nid_on_path, brokenPosList,mm_external=mm_external):
		lg.lg_str_once("[ nodes not connected ] "+combinefn)
		print("brokenPosList",brokenPosList,"matched_nid_on_path",matched_nid_on_path)
		for i in brokenPosList:
			print("At",matched_nid_on_path[i],matched_nid_on_path[i+1])
		print("[write_matched_nids_pos] check_if_nodes_are_connected False !")
		write_num_nids = brokenPosList[0]+1
		if write_num_nids<100: 
			print("skip since write_num_nids <100.")
			return
		print("instead write only %d"%write_num_nids,"from",len(matched_nid_on_path))

	if iprint>=3: 
		print("[ writing ] matched_nid_on_path",matched_nid_on_path[0:write_num_nids],write_num_nids)
		print("[ writing ] ind_on_path",ind_on_path[0:write_num_nids],"+offset",segpath_pos_offset)

	''' ind_on_path is just rough pos in path, may be Rough_dist_tolerate earlier.'''
	assert len(matched_nid_on_path)==len(ind_on_path)
	if len(matched_nid_on_path)>0:
		with open(fname,"w") as f:
			for i in range(write_num_nids):
				nid_ind = matched_nid_on_path[i]
				pind=ind_on_path.pop(0)+segpath_pos_offset
				f.write("%d %d\n"%(nid_ind,pind))


def search_along_path(path,last_path_pos,mlatlng0,mlatlng1):
	Rough_dist_tolerate = 52
	Rough_angle_tolerate = 80
	mhead = get_bearing_latlng2(mlatlng0,mlatlng1)
	mindist=1e6
	mindistline=1e6
	minangle=1e6
	for i in range(last_path_pos, len(path)-1):
		dic=path[i]
		splatlng = [dic[KeyGPSLat],dic[KeyGPSLng]]
		dist1 = get_dist_meters_latlng2(splatlng, mlatlng1)
		if dist1>300: continue
		dist12= dist_point_to_line_of_2pts(splatlng,mlatlng0,mlatlng1)
		if KeyGPSBearing in dic: 
			sphead = dic[KeyGPSBearing]
		else:
			''' find truly moved two pts to get heading'''
			j=i+1
			sphead=None
			while j<len(path) and get_dist_meters_latlng2(splatlng,[path[j][KeyGPSLat],path[j][KeyGPSLng]])<5:
				if iprint>=3: print("search_along_path find truly moved dist to get heading",i,j)
				if KeyGPSBearing in path[j] and sphead is None: 
					sphead=path[j][KeyGPSBearing]
				j+=1
			if j==len(path): j=i+1
			if sphead is None:
				sphead=get_bearing_latlng2(splatlng,[path[j][KeyGPSLat],path[j][KeyGPSLng]])
		if mindist>dist1: mindist=dist1
		if mindistline>dist12: mindistline=dist12
		if min_angle_diff(sphead,mhead)<minangle: minangle=min_angle_diff(sphead,mhead)
		if dist1<Rough_dist_tolerate and dist12<Rough_dist_tolerate and min_angle_diff(sphead,mhead)<Rough_angle_tolerate:
			return i
	if iprint>=3: print("search_along_path fail: mhead",mhead,"min dist",mindist,"min distline",mindistline,"min angle d",minangle) 
	return -1



lock = AccessRestrictionContext(
	prefix=py_fname(__file__,False)+"~mn~", 
	persistent_restriction= True,
	persist_seconds=600, 
	print_str=False,  
)

with AccessRestrictionContext(prefix=py_fname(__file__,False)+"-makedirs", max_access_num=1) as tmplock:
	''' make destination directory by 1 server '''
	tmplock.Access_Or_Wait_And_Skip("makedirs")
	for iddir in account_dirs:
		matchDir = iddir+os.sep+matchfolder
		if not os.path.exists(matchDir): 
			os.makedirs(matchDir)

if False: # remove old match dir
	''' remove current match files, start new '''
	with AccessRestrictionContext(prefix=py_fname(__file__,False)+"-mvdirs", max_access_num=1) as tmplock:
		tmplock.Access_Or_Wait_And_Skip("mv-dirs")
		for iddir in account_dirs:
			matchtmp = iddir+os.sep+matchfolder
			mvto = matchtmp+'201809'
			if os.path.exists(matchtmp) and os.path.isdir(matchtmp) and not os.path.exists(mvto): 
				print(matchtmp+' -> '+mvto)
				os.rename(matchtmp,mvto)
	sys.exit(0)


blacklistAddr=[] 
blacklist=[] # for emails/accounts
bugTimes=[]
bugEmails=[]  

if len(bugTimes)>0: 
	lock.no_restriction=True
	iprint=3


for iddir in account_dirs:
	email = iddir.split(os.sep)[-1]
	
	if email in blacklist: continue
	if len(bugEmails)>0 and email not in bugEmails: continue
	if iprint>=3: print(email)
	
	tmpdir = iddir+os.sep+gpsfolder
	if not ( os.path.exists(tmpdir) and os.path.isdir(tmpdir) ):
		if iprint>=1: print(__file__,"Empty account",iddir)
		continue
	tmpdir = iddir+os.sep+combinefolder
	time_list=[x.strip(os.sep).split(os.sep)[-1].rstrip(".txt") for x in glob.glob(tmpdir+"/*%s"%".txt")]
	matchDir = iddir+os.sep+matchfolder

	
	for truetimestr in time_list:
		# try:
			with lock:
				''' each truetimestr by 1 server '''
				lock.Access_Or_Skip(email+truetimestr)
				
				if len(bugTimes)>0 and truetimestr not in bugTimes: 
					continue

				if Overwrite_Match_files:
					cmd="rm "+matchDir+os.sep+truetimestr+"*"
					if iprint>=2:print(cmd)
					subprocess.call(cmd,shell=True) 

				OutBBoxflist= glob.glob(matchDir+os.sep+truetimestr+"-*OutBBox")

				existMfn=glob.glob(matchDir+os.sep+truetimestr+"-*")
				if CheckAgainOutBBox:
					for tmp in OutBBoxflist:
						existMfn.remove(tmp)
						os.remove(tmp)# delete mark and re-do

				if len(existMfn)>0 and not Overwrite_Match_files and len(bugTimes)==0:  # ___
					if iprint>=3: print("already "+matchDir+os.sep+truetimestr)
					continue

				Outfn= matchDir+os.sep+truetimestr+"~NotApprove*"
				Outfl=glob.glob(Outfn)
				if len(Outfl)>0:
					if not CheckNewlyApprovedCities:  
						if iprint>=3: print("Out city, not CheckNewlyApprovedCities "+matchDir+os.sep+truetimestr)
						continue
					else:
						for tmp in Outfl: # delete mark and re-do
							os.remove(tmp)

				combinefn=iddir+os.sep+combinefolder+os.sep+truetimestr+".txt"
				if os.path.exists(combinefn) and get_file_size_bytes(combinefn)<kCombineFileMinBytes:
					if iprint>=2: print("\nskip, Too small: %s "%combinefn)
					continue
				if not os.path.exists(combinefn): continue


				if iprint>=2: 
					if iprint>=3:print("\n\n\n\n\n")
					print("Proc %s "%combinefn)
				lastlat=None
				lastlng=None
				loclat=None
				loclng=None
				lasttime=None
				segs=[]
				path=[]
				with open(combinefn,"r") as f:
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
							loclat=lat
							loclng=lng
							lasttime=gti
						ddif=    get_dist_meters_latlng(lastlat,lastlng, lat, lng)
						dtime=  abs(lasttime-gti)
						TimeGap=kCutTraceTimeGap
						DistGap=kCutTraceDistGap
						if ddif>DistGap or dtime/1000.0 >TimeGap:
							if ddif>DistGap: 
								if iprint>=2: print("weird inside cut? dist %.2f "%ddif+combinefn)
							if dtime/1000.0 >TimeGap:
								if iprint>=2: print("weird inside cut? time %d "%dtime+combinefn)
							if iprint>=2: print("fragmented path len %d"%len(path))
							segs.append(path)
							path=[]
						path.append(dic)
					if iprint>=3: print("last path len %d"%len(path))
					segs.append(path)


				mf_ind=0 # count match segs.
				segpath_pos_offset=0
				lastpath=None

				for segpath in segs: # segpath: list of dict
					
					if lastpath is not None:
						segpath_pos_offset+=len(lastpath) # put here in case of many 'continue'
					lastpath=segpath

					if iprint>=3: 
						print("\nsegpath %d, len %d"%(mf_ind,len(segpath)))
					if len(segpath)<10: 
						if iprint>=2: print("segpath too short, skip")
						with open(matchDir+os.sep+truetimestr+"-%dTooShort"%mf_ind,"w") as f: 
							f.write("") # leave a mark, prevent re-run.
						continue

					dic=segpath[0]
					splat = dic[KeyGPSLat]
					splng = dic[KeyGPSLng]
					dic=segpath[-1]
					splat2 = dic[KeyGPSLat]
					splng2 = dic[KeyGPSLng]
					dist=get_dist_meters_latlng(splat,splng,splat2,splng2)
					if dist<50:
						print("segpath dist too small, skip. %.1f"%dist)
						with open(matchDir+os.sep+truetimestr+"-%dTooShort"%mf_ind,"w") as f: 
							f.write("") # leave a mark, prevent re-run.
						continue

					addr = latlng_to_city_state_country(splat,splng).replace(" ","")
					addr2 = latlng_to_city_state_country(splat2,splng2).replace(" ","")

					cross_city=False
					if addr!=addr2: 
						if iprint:print("\n[ Crossing city ] %s  ->  %s"%(addr,addr2))
						cross_city=True
					if addr in blacklistAddr or addr2 in blacklistAddr: 
						if iprint: print("skipping "+addr)
						continue

					requestFile = DirOSM+os.sep+"cityrequest.txt"
					approved=0
					if os.path.exists(requestFile): 
						with open(requestFile,"r") as f: 
							for l in f:
								l=l.strip()
								if len(l)>0:
									st=l.split("~|")
									if st[0].strip()==addr:
										if st[-1].strip()=="1":
											approved=1
					if approved==0:
						if iprint: print("Didn't approve "+addr)
						lg.lg_str_once("[ not approve ] "+addr)
						with open(matchDir+os.sep+truetimestr+"~NotApprove%d"%mf_ind,"w") as f: 
							f.write(addr)
						continue


					osm_folder_path= DirOSM+os.sep+addr
					osm= osmPipeline(folder_path=osm_folder_path)
					osmname= osm.get_osm_file_path().split(os.sep)[-1].rstrip(".osm")

					dic=segpath[0]
					splat = dic[KeyGPSLat]
					splng = dic[KeyGPSLng]
					if not osm.within_bbox([splat,splng]):
						if iprint: print("not osm.within_bbox! skip")
						with open(matchDir+os.sep+truetimestr+"-%dOutBBox"%mf_ind,"w") as f: 
							f.write("") # leave a mark, prevent re-run.
						continue
					
					out_ind=None
					if cross_city:
						for i in range(len(segpath)):
							dic=segpath[i]
							splat = dic[KeyGPSLat]
							splng = dic[KeyGPSLng]
							if not osm.within_bbox([splat,splng]):
								out_ind=i
								break

								
					if not cross_city or out_ind is None:
						res,respos =    convert_path_to_nids(segpath,addr=osmname)
					else:
						res,respos =    convert_path_to_nids(segpath[0:out_ind-2],addr=osmname)

					# for cluster only, Need Edit:
					mm_nid2latlng.use_cache(meta_file_name="osm/cache-%s-nodeid-to-lat-lng.txt"%osmname)
					mm_nid2elevation.use_cache(meta_file_name="osm/cache-%s-nid-to-elevation.txt"%osmname)
					mm_nids2speed.use_cache(meta_file_name="osm/cache-%s-nids-to-speed.txt"%osmname)
					mm_nid2neighbor.use_cache(meta_file_name="osm/cache-%s-nodeid-to-neighbor-nid.txt"%osmname)

					for resind in range(len(res)):
						nodeids=res[resind]
						''' re-define path and offset: '''
						path=segpath[respos[resind][0]:respos[resind][1]]
						offset_within_segpath=respos[resind][0]

						if iprint>=3: 
							print("\n#%d in segpath, #nid %d, len %d"%(resind,len(nodeids),len(path)))
						if len(nodeids)<=3: 
							if iprint: print("\nmatched nodeids too few <=3, skip path")
							continue
						if len(path)<=3: 
							if iprint: print("\nsegpath too short <=3, skip path")
							continue
						if len(nodeids)>=3 and nodeids[-1]==nodeids[-3]:
							if iprint: print("\nnodeids.pop() U")
							nodeids.pop() 

						''' store valid nodeids/pos here:'''
						matched_nid_on_path=[]
						ind_on_path=[]

						''' ---- Find Start Position'''

						matchPos=-1
						startpos=None
						while matchPos<len(nodeids)-2:
							matchPos+=1
							if iprint>=3: print("Finding Start Position, try matchPos=%d"%matchPos)
							latlng0 = mm_nid2latlng.get(nodeids[matchPos])
							latlng1 = mm_nid2latlng.get(nodeids[1+matchPos])
							if latlng0 is None or latlng1 is None:
								print("mm_nid2latlng.get None:",nodeids[matchPos],latlng0,nodeids[matchPos+1],latlng1)
								continue
							hd1 = get_bearing_latlng2(latlng0,latlng1)
							if iprint>=3: print("head nodeids 0-1",hd1)

							outsideBbox=0
							validStart=0
							for ind in range(len(path)-1):
								angleMatched=0
								distMatched=0

								dic=path[ind]
								splat = dic[KeyGPSLat]
								splng = dic[KeyGPSLng]
								if not osm.within_bbox([splat,splng]):
									outsideBbox=1
									break
								sphead=None
								if KeyGPSBearing in dic: 
									sphead = dic[KeyGPSBearing]
								else:
									j=ind+1
									while j<len(path) and get_dist_meters_latlng2([splat,splng],[path[j][KeyGPSLat],path[j][KeyGPSLng]])<5:
										if KeyGPSBearing in path[j]: 
											sphead = path[j][KeyGPSBearing]
											break
										j+=1
									if j==len(path): j=ind+1
									if sphead is None:
										sphead = get_bearing_latlng2([splat,splng],[path[j][KeyGPSLat],path[j][KeyGPSLng]])
								# angle of matched nodes with GPS sample:
								angle1 = get_bearing_latlng2(latlng0, [splat,splng])
								angle2 = get_bearing_latlng2([splat,splng], latlng1)
								if headings_all_close([hd1,angle1,sphead], thresh=20):
									if min_angle_diff(angle2,hd1)<90:
										angleMatched=1
								dist = dist_point_to_line_of_2pts([splat,splng], latlng0, latlng1)
								if iprint>=3 and ind%20==0: 
									print("angles",angle1,angle2 , "sphead",sphead, "pt2lineD", dist)
								if dist < 20:
									distMatched=1
								if angleMatched>0 and distMatched>0:
									startpos=ind
									validStart=1
									if iprint>=3: 
										print("Match! angles",angle1,angle2 , "sphead",sphead, "pt2lineD", dist)
										print("startpos",ind,"latlng",[splat,splng])
									break

							if validStart==0:
								if iprint>=2: print("[ not validStart ] try next nid")
							else: 
								break	
							if outsideBbox>0:
								break
							
						if validStart==0:
							if iprint>=2: print("[ not validStart ] skip path")
							lg.lg_str_once("[ not validStart ] "+combinefn)
							continue
						if outsideBbox>0:
							if iprint>=2: print("[ outsideBbox ] skip path")
							continue
						if startpos>3*len(path)/4:
							if iprint>=2: 
								print("[ too late start ] I'll try",startpos,len(path))
								dic=path[startpos]
								print("latlng",dic[KeyGPSLat],dic[KeyGPSLng])
							lg.lg_str_once("[ too late start ] "+combinefn)

						
						if iprint>=3: 
							if iprint: print("nodeids",nodeids,len(nodeids))
							print("[ Rough ck ] matchPos starts at %d"%matchPos)

						''' ---- Roughly check matched nodes. matchPos is usually 0 here.'''

						last_path_pos=max(0,startpos-5)

						while matchPos< len(nodeids)-1: # last one precluded. [-3,-2] last seg. [-2,-1] break
							mlatlng0 = mm_nid2latlng.get(nodeids[matchPos])
							mlatlng1 = mm_nid2latlng.get(nodeids[matchPos+1])
							if cross_city:
								if not osm.within_bbox(mlatlng1):
									print(nodeids[0:matchPos+2],"just ran out of city!")
									lg.lg_str_once("[ out of city ] "+combinefn)
									break
							
							found_match=0
							''' Find path pos for [matchPos+1]: '''
							ret=search_along_path(path,last_path_pos,mlatlng0,mlatlng1)
							if ret>=0: found_match=1
							if found_match:
								matched_nid_on_path.append(nodeids[matchPos+1])
								last_path_pos = ret
								ind_on_path.append(ret+offset_within_segpath)
								if iprint>=3: print("+ accept matchPos [%d], nid %d, pathPos %d"%(matchPos+1,nodeids[matchPos+1],ret+offset_within_segpath))
								matchPos+=1
								continue

							''' [matchPos] is valid, but [matchPos+1] is wrong:'''
							if found_match==0:
								if iprint>=3: print("- nodeids [%d] excluded: %d"%(matchPos+1,nodeids[matchPos+1]))
								
								'''--- Try jump to next few nodes: '''
								if matchPos+2>=len(nodeids):
									break
								jump=matchPos+2
								distj=0
								if len(matched_nid_on_path)>0:
									nb0=mm_nid2neighbor.get(matched_nid_on_path[-1])
								else:
									nb0=mm_nid2neighbor.get(nodeids[matchPos])
								while jump<len(nodeids) and (distj<300 or jump<matchPos+5):
									nj = nodeids[jump]
									mlatlng1 = mm_nid2latlng.get(nj)
									if nj in nb0:
										'''--- verify this new potential jump node: '''
										ret=search_along_path(path,last_path_pos,mlatlng0,mlatlng1)
										if ret>=0: found_match=1
										if found_match:
											matched_nid_on_path.append(nj)
											last_path_pos = ret
											ind_on_path.append(ret+offset_within_segpath)
											matchPos=jump
											if iprint>=3: print("+ accept Jump [%d], nid %d, pathPos %d"%(jump,nj,ret+offset_within_segpath))
											break
									distj=get_dist_meters_latlng2(mlatlng0,mlatlng1)
									jump+=1

							''' already reached last valid nid. quit.'''
							if matchPos+2>=len(nodeids):
								break

							'''--- Try connect_dots '''
							if found_match==0:
								if iprint>=3: print("? trying connect_dots... excluding [%d]"%(matchPos+1))
								tmpn1=nodeids[0:matchPos+1]
								tmpn2=nodeids[matchPos+2:]# get rid of [matchPos+1]
								if iprint>=3: print(" connect",tmpn1,tmpn2)
								connect_dots(tmpn1,tmpn2,mm_nid2neighbor=mm_nid2neighbor)
								if iprint>=3: print(" after connect",tmpn1)
								changed=0
								for i in range(min( len(nodeids), min(len(tmpn1),matchPos+2) )):
									if tmpn1[i]!=nodeids[i]: 
										changed=1
										break
								if changed:# it is connection problem~
									if iprint>=3: 
										print("connected nids diff from nodeids at [%d]"%i,tmpn1[i],nodeids[i])
									for j in range(matchPos+1-i):
										tmp1=matched_nid_on_path.pop()
										if len(ind_on_path)==1: last_path_pos=0
										else: last_path_pos-=ind_on_path[-1]-ind_on_path[-2]
										tmp2=ind_on_path.pop()
										if iprint>=3: print("  popping nid %d, path pos %d"%(tmp1,tmp2))
									matchPos=i-1
									nodeids=tmpn1
									if iprint>=3: print("! new matchPos %d"%(matchPos),"new nodeids",nodeids)
									found_match=1
								else: 
									if iprint>=3: 
										print("not connection problem... break before [%d] %d"%(matchPos+1,nodeids[matchPos+1]))
									if len(matched_nid_on_path)>3:
										fname=matchDir+os.sep+truetimestr+"-%d"%mf_ind
										write_matched_nids_pos(matched_nid_on_path,ind_on_path,segpath_pos_offset,fname,mm_external=mm_nid2neighbor)
										mf_ind+=1
									''' start over, add [matchPos+1] as node zero.'''
									ret=-1
									while ret<0 and matchPos+2<len(nodeids):
										matchPos+=1
										mlatlng0 = mm_nid2latlng.get(nodeids[matchPos])
										mlatlng1 = mm_nid2latlng.get(nodeids[matchPos+1])
										ret=search_along_path(path,last_path_pos,mlatlng0,mlatlng1)
									if iprint>=3: 
										print("start over at [%d] %d"%(matchPos,nodeids[matchPos]))
									matched_nid_on_path=[]
									ind_on_path=[]
									last_path_pos=ret

						if len(matched_nid_on_path)>3:
							fname=matchDir+os.sep+truetimestr+"-%d"%mf_ind
							write_matched_nids_pos(matched_nid_on_path,ind_on_path,segpath_pos_offset,fname,mm_external=mm_nid2neighbor)
							mf_ind+=1
						else:
							if iprint>=2: print("len(matched_nid_on_path)<=3, skip")

						if len(matched_nid_on_path)<len(nodeids)/4:
							lg.lg_str_once("[ Rough ck cut too many ] "+combinefn)

		# except Exception as e:
		# 	print(sys.exc_info())




