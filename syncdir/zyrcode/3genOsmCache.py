#!/usr/bin/env python
print('Make sure Redis is running on port 6380 on every server in the cluster!')
print("This code takes days to run. Make sure to use screen/tmux.")

import os, sys, getpass, glob
import subprocess
import random, time
import inspect
import collections
from shutil import copy2, move as movefile
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
if mypydir not in sys.path: sys.path.append(mypydir)
from readconf import get_conf,get_conf_int,get_conf_float,get_list_startswith,get_dic_startswith
from logger import Logger,SimpleAppendLogger,ErrorLogger
from util import read_lines_as_list,read_lines_as_dic,read_gzip,strip_illegal_char,strip_newline,unix2datetime,get_file_size_bytes,py_fname,replace_user_home
from namehostip import get_my_ip,get_platform
from geo import get_bearing_latlng2, min_angle_diff, convert_line_to_dic, get_dist_meters_latlng, latlng_to_city_state_country, get_osm_file_quote_given_file
from CacheManager import CacheManager
from osmutil import osmPipeline
from mem import AccessRestrictionContext,Synchronizer,Mem
from myosrm import crawl_nid_to_latlng

configfile = "conf.txt"
HomeDir = os.path.expanduser("~")
DirData = get_conf(configfile,"DirData") #~/greendrive/proc
DirOSM = get_conf(configfile,"DirOSM") #~/greendrive/osmdata
DirData = replace_user_home(DirData)
DirOSM = replace_user_home(DirOSM)
gpsfolder = "gps"
obdfolder = "obd"
combinefolder ="combine"
MyIp = get_my_ip()
My_Platform = get_platform() # "centos" means cluster 
On_Cluster = False
if My_Platform=='centos': On_Cluster = True

iprint = 2 

err = ErrorLogger("allerror.txt", py_fname(__file__,False))
lg = SimpleAppendLogger("./logs/"+py_fname(__file__,False))
mm = CacheManager() # using redis on cluster

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
KeyGPSAltitude=get_conf(configfile,"KeyGPSAltitude")
KeyGas=get_conf(configfile,"KeyGas")
KeyRPM=get_conf(configfile,"KeyRPM") 
KeyOBDSpeed=get_conf(configfile,"KeyOBDSpeed")
KeyMAF=get_conf(configfile,"KeyMAF") 
KeyThrottle=get_conf(configfile,"KeyThrottle") 
KeyOriSysMs=get_conf(configfile,"KeyOriSysMs")

''' the following may be diff from 1.py 4.py since they don't serve major purpose'''
kCutTraceTimeGap = 2 # timestamp between two lines > this then cut.
kCutTraceDistGap = kCutTraceTimeGap* 50 # dist between two lines > this then cut.

if On_Cluster: 
	Global_params={ 
		"expire":30*86400, # redis, in sec
		"num":30, # on cluster
	}
else:
	Global_params={ 
		"expire":30*86400, # redis
		"use_ips":['localhost'], # single PC
		"overwrite_servers":True, # used with 'use_ips'
	}

def gen_place_request(): # not using this.
	''' run by 1 server. Find new address. I usually manually add city request.'''
	account_dirs = glob.glob(DirData+"/*")
	for iddir in account_dirs:
		email = iddir.split(os.sep)[-1]
		tmpdir = iddir+"/%s"%gpsfolder
		if not ( os.path.exists(tmpdir) and os.path.isdir(tmpdir) ):
			if iprint>=1: print(__file__.split(os.sep)[-1],"Empty account",iddir)
			continue
		# gather unix time 
		tmpdir = iddir+"/%s"%obdfolder
		time_list=[x.strip(os.sep).split(os.sep)[-1].rstrip(EXT) for x in glob.glob(tmpdir+"/*%s"%EXT)]	
		for truetimestr in time_list:
			tmpf=iddir+os.sep+combinefolder+os.sep+truetimestr+".txt"
			if not os.path.exists(tmpf):
				if iprint>=2: print("skip, Not exists: %s "%tmpf)
				continue
			if os.path.exists(tmpf) and get_file_size_bytes(tmpf)<100:
				if iprint>=2: print("skip, Too small: %s "%tmpf)
				continue
			if iprint>=2: print("Proc: %s "%tmpf)
			lastlat=None
			lastlng=None
			loclat=None
			loclng=None
			lasttime=None
			segs=[]
			path=[]
			sample_loc=[]
			with open(tmpf,"r") as f:
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
					if ddif>kCutTraceDistGap or dtime/1000.0 >kCutTraceTimeGap:
						if iprint>=2: print("weird inside cut? "+tmpf)
						if iprint>=2: print("path len %d"%len(path))
						segs.append(path)
						sample_loc.append([[loclat, loclng],[lastlat, lastlng]])
						path=[]
					path.append(dic)
				if iprint>=2: print("path len %d"%len(path))
				segs.append(path)
				sample_loc.append([[loclat, loclng],[lastlat, lastlng]])

			for path in segs:
				latlngs = sample_loc.pop(0)
				addr = latlng_to_city_state_country(latlngs[0][0],latlngs[0][1]).replace(" ","")
				addr2 = latlng_to_city_state_country(latlngs[1][0],latlngs[1][1]).replace(" ","")
				if addr != addr2:
					dist=get_dist_meters_latlng(latlngs[0][0],latlngs[0][1],latlngs[1][0],latlngs[1][1])
					if dist>10000:
						if iprint: print(tmpf,addr,addr2,"cross region, skip addr2 ...")

				# I need manual approve of cities appearing here, as "addr~|1"
				requestFile = DirOSM+os.sep+"cityrequest.txt"
				appeared=0
				approved=0
				if os.path.exists(requestFile): 
					with open(requestFile,"r") as f: 
						for l in f:
							l=l.strip()
							if len(l)>0:
								st=l.split("~|")
								if st[0].strip()==addr:
									appeared=1
									if st[-1].strip()=="1": # if want to run gen cache.
										approved=1
									elif st[-1].strip()=="-1": # if purge all data!
										approved=1
				if appeared==0:
					with AccessRestrictionContext(prefix="requestFile") as lock:
						lock.Access_Or_Skip("requestFile")
						with open(requestFile,"a") as f: 
							f.write(addr+"~|0\n")
				if approved==0 or appeared==0:
					if iprint: 
						print("Please approve new city: "+addr)
						print("vim ~/greendrive/osmdata/cityrequest.txt")




def purge_osm_cache_from_addr():# run by 1 server.
	requestFile = DirOSM+os.sep+"cityrequest.txt"
	print("Checking "+requestFile)
	if os.path.exists(requestFile): 
		addrlist=[]
		with open(requestFile,"r") as f: 
			for l in f:
				l=l.strip()
				if len(l)>0:
					st=l.split("~|")
					if st[-1].strip()=="-1": # if want to delete
						addrlist.append(st[0].strip())
		print("going to delete",addrlist)
		for addr in addrlist:
			if iprint: 
				print("Deleting: "+addr)
			osm_folder_path= DirOSM+os.sep+addr
			osm= osmPipeline(folder_path=osm_folder_path)
			purge_all_data=True
			print("\nPurge all data !!! %s\n"%osm_folder_path)

			if purge_all_data:
				with AccessRestrictionContext(prefix="purge") as lock:
					lock.Access_Or_Wait_And_Skip("purge")
					osm.purge_all_data()
					print("makedirs "+osm_folder_path)
					os.makedirs(osm_folder_path)
			''' delete cache meta file as well. '''
			CacheMetaDir="~/cachemeta/osm/" # You Edit
			cmd="rm "+CacheMetaDir+"cache-"+addr+"*"
			print(cmd)
			subprocess.call(cmd,shell=True)


''' ---- func to be serialized, dill -------'''
def yield_nid2latlng(finQt): # [int,[lat,lng]]
	st=finQt.split(CUT)
	fin=st[0]
	QUOTE=st[1]
	with open(fin,"r") as f:
		lcnt = 0
		cnt=0
		thresh=10
		for l in f:
			cnt+=1
			if cnt>thresh: 
				thresh*=2 
				print(__file__,'yield_nid2latlng()', cnt)
			if l.strip().startswith("<node "):
				nidstr= l.split(" id=",1)[-1].split(" ",1)[0].strip(QUOTE)
				latstr= l.split(" lat=",1)[-1].split(" ",1)[0].strip(QUOTE)
				lonstr= l.split(" lon=",1)[-1].split(" ",1)[0].strip(QUOTE)
				yield [int(nidstr), [float(latstr) , float(lonstr)] ]
				lcnt+=1
		if iprint: print("\n%d nodes !\n"%lcnt)
	if iprint: print("yield_nid2latlng done "+fin)



def yield_nids2waytag(inputs): #[(int,int),str]
	''' cannot dill pickle imported func, has to relocate here from  mytools/geo.py '''
	def yield_way_from_osm_file(fpath, QUOTE, apply_filter=True ):
		assert fpath.startswith(os.sep)
		f=open(fpath,"r")
		kInit=1
		kWithin=2
		state=kInit
		while True:
			l= f.readline()
			if not l: break
			l= l.strip()
			if state==kInit:
				if l.startswith("<way "):
					data=[l]
					state=kWithin
					valid=False
			elif state==kWithin:
				if apply_filter and l.startswith('<tag k=%shighway%s'%(QUOTE,QUOTE)):
					v=l.split(" v=")[-1].split(QUOTE)[1]
					if v in ["motorway","trunk","primary","secondary","tertiary","unclassified","residential","motorway_link","trunk_link","primary_link","secondary_link","tertiary_link","service"]: # copied from geo.py 
						valid=True
					data.append(l)
				elif l.startswith("</way>"):
					state=kInit
					if apply_filter and not valid: continue
					yield data
				else:
					data.append(l)
		f.close()
	st=inputs.split(CUT)
	fin=st[0]
	QUOTE=st[1]
	cnt=0
	thresh=10
	for da in yield_way_from_osm_file( fin,QUOTE ):
		cnt+=1
		if cnt>thresh: 
			thresh*=2 
			print(__file__, 'yield_nids2waytag()', cnt)
		nlist=[]
		tg=""
		for e in da:
			if e.startswith("<nd "):
				nid = int(e.split(' ref=%s'%QUOTE)[-1].split(QUOTE)[0])
				nlist.append(nid)
			elif e.startswith('<tag k=%shighway%s'%(QUOTE,QUOTE)): # <tag k='highway' v='residential' />
				tg=e.split(' v=')[-1].split(QUOTE)[1]
		for i in range(len(nlist)-1):
			yield [(nlist[i],nlist[i+1]),tg]
			yield [(nlist[i+1],nlist[i]),tg]
	if iprint: print("yield_nids2waytag done "+fin)
	

def yield_nid2neighbor(inputs):
	st=inputs.split(CUT)
	fin=st[0]
	QUOTE=st[1]
	kInit=1
	kWithin=2
	state=kInit
	with open(fin,"r") as f:
		lcnt=0
		cnt=0
		thresh=10
		for l in f:
			cnt+=1
			if cnt>thresh: 
				thresh*=2 
				print(__file__,'yield_nid2neighbor()', cnt)
			lcnt+=1
			l=l.strip()
			if state==kInit:
				if l.startswith("<way "):
					state=kWithin
					nlist=[]
					valid=0
			elif state==kWithin:
				if l.startswith("<nd "):
					nidstr= l.split(' ref=%s'%QUOTE,1)[-1].split(QUOTE,1)[0]
					nlist.append(int(nidstr))
				elif l.startswith('<tag k=%shighway%s'%(QUOTE,QUOTE)):
					v=l.split(" v=")[-1].split(QUOTE)[1]
					if v in ["motorway","trunk","primary","secondary","tertiary","unclassified","residential","motorway_link","trunk_link","primary_link","secondary_link","tertiary_link","service"]: # copied from geo.py
						valid=1
				elif l.startswith("</way>"):
					state=kInit
					if valid>0:
						for i in range(len(nlist)):
							if i>0: yield [nlist[i], nlist[i-1]]
							if i<len(nlist)-1: yield [nlist[i], nlist[i+1]]
				elif l.startswith("<way"):
					raise Exception("Bug: <way> no closure at line %d!"%lcnt)
	if iprint: print("yield_nid2neighbor done "+fin)


def yield_nid2elevation(inputs): 
	st=inputs.split(CUT)
	fin=st[0]
	QUOTE=st[1]
	with open(fin,"r") as f:
		lcnt = 0
		cnt=0
		thresh=10
		for l in f:
			cnt+=1
			if cnt>thresh: 
				thresh*=2 
				print(__file__,'yield_nid2elevation()', cnt)
			st=l.split(" ")
			if len(st)==2:
				nidstr= st[0]
				ele= st[1]
				yield [ int(nidstr),float(ele) ]
				lcnt+=1
		if iprint: print("\ngen_cache_file  num: %d\n"%lcnt)
	if iprint: print("yield_nid2elevation done "+fin)


def yield_nids2speed(inputs): 
	st=inputs.split(CUT)
	fin=st[0]
	QUOTE=st[1]
	with open(fin,"r") as f:
		lcnt = 0
		cnt=0
		thresh=10
		for l in f:
			cnt+=1
			if cnt>thresh: 
				thresh*=2 
				print(__file__,'yield_nids2speed()', cnt)
			st=l.split(",")
			if len(st)==3:
				nid1= int(st[0])
				nid2= int(st[1])
				spd= st[-1]
				yield [ (nid1,nid2) , float(spd) ]
				lcnt+=1
		if iprint: print("\ngen_cache_file  num: %d\n"%lcnt)
	if iprint: print("yield_nids2speed done "+fin)





def gen_osm_cache_from_addr(overwrite=False, addr=None, max_num_servers=16, lock_sfx="", group=None, sync_wait_sec=1, test_try=('try' in sys.argv)):# run by multi server.
	''' Multi servers run, sync and lock. Use cmd-line to give max-server and lock suffix.
	If run on 1 PC: python 3genOsmCache.py gen_osm addr=Indiana,US max=1 s=0 t=0
	'''
	assert addr or group, "Please input either 'addr' or 'group' !!!"
	sync_group_name = addr if addr is not None else group
	sync_group_name += str(max_num_servers)+lock_sfx
	if On_Cluster:
		sync = Synchronizer(prefix=py_fname(__file__,True), sync_group_name=sync_group_name )
		sync.Clear()
	print('addr',addr,'max_num_servers',max_num_servers,'lock_sfx',lock_sfx,'sync_group_name',sync_group_name,'sync_wait_sec',sync_wait_sec)
	if test_try: sys.exit(0) # load pyc and exits.
	time.sleep(sync_wait_sec)# if not start at same time, need time to wait here
	if On_Cluster: sync.Register()
	time.sleep(sync_wait_sec)
	if On_Cluster: sync.Synchronize(print_str="init")
	TAG = addr+" "+MyIp
	if On_Cluster and iprint: 
		print("total_server_num",sync.total_server_num)
		time.sleep(2)
	
	requestFile = DirOSM+os.sep+"cityrequest.txt"
	addrlist=[]
	if addr is None: 
		if os.path.exists(requestFile): 
			print("looking at "+requestFile)
			with open(requestFile,"r") as f: 
				for l in f:
					l=l.strip()
					if len(l)>0:
						st=l.split("~|")
						if st[-1].strip()=="1": 
							addrlist.append(st[0].strip())
	else:
		addrlist.append(addr)

	if iprint: print(addrlist,len(addrlist))

	for addr in addrlist:
		if iprint: print("\nProc addr: "+addr)

		osm_folder_path= DirOSM+os.sep+addr
		print('osm_folder_path '+osm_folder_path)

		if not os.path.exists(osm_folder_path):
			with AccessRestrictionContext(prefix="mkdir_osm~", no_restriction= not On_Cluster) as lk:
				lk.Access_Or_Wait_And_Skip("mkdir_osm~")
				try:
					os.makedirs(osm_folder_path)
				except OSError as e:
					print(e)
					print("Already made "+osm_folder_path)
				except Exception as e:
					print(e)
					sys.exit(1)

		osm = osmPipeline(folder_path=osm_folder_path)
		overwrite_meta_file=False #True: if re-run, re-gen cache meta files.
		overwrite_cache_file=False
		overwrite_memory=False
		if overwrite: 
			overwrite_meta_file=True
			overwrite_cache_file=True
			overwrite_memory=True
			print("\nOverwrite !!! %s\n"%osm_folder_path)
			time.sleep(3)

		if On_Cluster: sync.Synchronize(print_str="before download_osm_given_address")

		params = Global_params # for Redis

		lock = AccessRestrictionContext( 
				prefix=osm_folder_path+lock_sfx, 
				write_completion_file_mark=True, # prevent re-run, like a mark
				completion_file_dir=osm_folder_path,
				print_str=False,
				no_restriction= not On_Cluster,
			)
		with lock:
			print("lock download_osm ? ")
			lock.Access_Or_Wait_And_Skip("download_osm")

			print("osm.download_osm_given_address() ...",addr)
			osm.download_osm_given_address(addr)

		if On_Cluster: 
			sync.Synchronize(print_str="downloaded, before running tasks with lock")
			sync.waitForNfsFile(osm_folder_path+os.sep+addr+".osm",sleeptime=3)
			sync.Synchronize(print_str="downloaded, wait till everyone sees the file")


		if iprint: print("\n"+addr)
		NFSsync=0 # on PC same here:
		while NFSsync==0:
			try: # wait for downloaded file on cluster.
				osmname= osm.get_osm_file_path().split(os.sep)[-1].rstrip(".osm")
				NFSsync=1
			except IOError as e:
				time.sleep(1) # NFS sync wait.
				print(e)
			except Exception as e:
				print(e)
				sys.exit(1)

		mustSeeFiles =[]
		QUOTE = get_osm_file_quote_given_file(osm_folder_path+"/%s.osm"%osmname)
		allow_dead_num=0

		lock.max_access_num = 1 
		mustSeeFiles.append(osm_folder_path+"/cache-%s-nids-to-waytag.txt"%osmname)
		with lock:
			print("lock nids2waytag ? ")
			lock.Access_Or_Skip("nids2waytag")
			metaf= "osm/cache-%s-nids-to-waytag.txt"%osmname
			print("proc "+metaf)
			outf= osm_folder_path+"/cache-%s-nids-to-waytag.txt"%osmname # outf not in use
			if not mm.exist_cache(metaf) or overwrite_meta_file:
				mm.create_cache(meta_file_name=metaf,
					gen_cache_file_cmd="python "+HomeDir+"/syncdir/zyrcode/gen_cache/extractNode2WayTag.py gen_cache_file "+osm_folder_path+"/%s.osm "%osmname +outf,
					cache_file_abs_path=outf,
					params=params,
					overwrite_meta_file=overwrite_meta_file,
					overwrite_cache_file=overwrite_cache_file,
					overwrite_memory=overwrite_memory,
					yield_func=yield_nids2waytag,
					yield_args=osm_folder_path+"/%s.osm"%osmname+CUT+QUOTE,
					overwrite_prefix=True,
				)
			lg.lg_str_once(TAG+" cache nids2waytag done")
		

		mustSeeFiles.append(osm_folder_path+"/cache-%s-nodeid-to-lat-lng.txt"%osmname)
		with lock:
			print("lock nid2latlng ? ")
			lock.Access_Or_Skip("nid2latlng")
			metaf= "osm/cache-%s-nodeid-to-lat-lng.txt"%osmname
			print("proc "+metaf)
			outf= osm_folder_path+"/cache-%s-nodeid-to-lat-lng.txt"%osmname # not in use
			if not mm.exist_cache(metaf) or overwrite_meta_file:
				mm.create_cache(meta_file_name=metaf,
					gen_cache_file_cmd="python "+HomeDir+"/syncdir/zyrcode/gen_cache/storeNodeId2LatLng.py gen_cache_file "+"%s "%osm.get_osm_file_path() +outf,
					cache_file_abs_path=outf,
					params=params,
					overwrite_meta_file=overwrite_meta_file,
					overwrite_cache_file=overwrite_cache_file,
					overwrite_memory=overwrite_memory,
					yield_func=yield_nid2latlng,
					yield_args=osm_folder_path+"/%s.osm"%osmname+CUT+QUOTE,
					overwrite_prefix=True,
				)
			lg.lg_str_once(TAG+" cache nid2latlng done")

		
		lock.max_access_num = 1 
		mustSeeFiles.append(osm_folder_path+"/cache-%s-nodeid-to-neighbor-nid.txt"%osmname)
		with lock:
			print("lock nid2neighbor ? ")
			lock.Access_Or_Skip("nid2neighbor")
			metaf= "osm/cache-%s-nodeid-to-neighbor-nid.txt"%osmname
			print("proc "+metaf)
			outf= osm_folder_path+"/cache-%s-nodeid-to-neighbor-nid.txt"%osmname # not in use
			if not mm.exist_cache(metaf) or overwrite_meta_file:
				mm.create_cache(meta_file_name=metaf,
					gen_cache_file_cmd="python "+HomeDir+"/syncdir/zyrcode/gen_cache/genNodeId2NeighborNid.py gen_cache_file "+ "%s "%osm.get_osm_file_path() +outf,
					cache_file_abs_path=outf,
					params=params,
					overwrite_meta_file=overwrite_meta_file,
					overwrite_cache_file=overwrite_cache_file,
					overwrite_memory=overwrite_memory,
					yield_func=yield_nid2neighbor,
					yield_args=osm_folder_path+"/%s.osm"%osmname+CUT+QUOTE,
					kv_action_type=1, # 1: append to k= vlist
					overwrite_prefix=True,
				)
			lg.lg_str_once(TAG+" cache nid2neighbor done")


		if iprint: print("\nMust sync here!!!\n\n"+addr)
		if On_Cluster: 
			sync.Synchronize(print_str="before write_node_elevation") # because this depends on finishing the previous.
			for fpath in mustSeeFiles:
				sync.waitForNfsFile(fpath,sleeptime=3)
			sync.Synchronize(print_str="mustSeeFiles: wait till everyone sees the file")
		mustSeeFiles=[]


		if overwrite: 
			lock.max_access_num = 1 
			with lock:
				lock.Access_Or_Wait_And_Skip("rm elevation")
				try:
					osm.remove_file_node_elevation()
				except: print("Exception: osm.remove_file_node_elevation() !")
				lg.lg_str_once(TAG+"rm node_elevation done")
		

		lock.max_access_num = int(max_num_servers//2) # allow more servers.
		done_elevation=0
		with lock:
			print("lock query elevation ? ")
			lock.Access_Or_Skip("elevation", print_detail=True)
			osm.write_node_elevation(ignore_mc=True, load_previous=True,lock_sfx=lock_sfx) 
			lg.lg_str_once(TAG+" osm.write_node_elevation done")
			done_elevation=1
		

		if done_elevation: # task depends on previous.
			lock.max_access_num = 1
			with lock:
				print("lock nid2elev yield into mem ? ")
				lock.Access_Or_Skip("nid2elevation")
				if On_Cluster: sync.waitForNfsFile(osm_folder_path+"/%s-nid-to-elevation.txt"%osmname,sleeptime=3)
				metaf= "osm/cache-%s-nid-to-elevation.txt"%osmname
				print("proc "+metaf)
				outf= osm_folder_path+"/cache-%s-nid-to-elevation.txt"%osmname
				if not mm.exist_cache(metaf) or overwrite_meta_file:
					mm.create_cache(meta_file_name=metaf,
						gen_cache_file_cmd="python "+HomeDir+"/syncdir/zyrcode/gen_cache/storeNodeId2elevation.py gen_cache_file "+ osm_folder_path+"/%s-nid-to-elevation.txt "%osmname +outf,
						cache_file_abs_path=outf,
						params=params,
						overwrite_meta_file=overwrite_meta_file,
						overwrite_cache_file=overwrite_cache_file,
						overwrite_memory=overwrite_memory,
						yield_func=yield_nid2elevation,
						yield_args=osm_folder_path+"/%s-nid-to-elevation.txt"%osmname+CUT+QUOTE,
						overwrite_prefix=True,
					)
				lg.lg_str_once(TAG+" cache nid-to-elevation done")
		

		if overwrite: 
			lock.max_access_num = 1 # lock.
			with lock: # allow 1.
				print("lock remove_file_way_speed ? ")
				lock.Access_Or_Wait_And_Skip("remove_file_way_speed")
				osm.remove_file_way_speed()
				lg.lg_str_once(TAG+" remove_file_way_speed done")


		lock.max_access_num = max_num_servers # allow more servers. 
		done_way_speed=0
		with lock:
			print("lock query way_speed ? ")
			lock.Access_Or_Skip("way-speed")
			print("start to run write_way_speed()")
			osm.write_way_speed() # generate nid2spd before storing them. everyone appends to file. Use Mem lock to avoid dup.
			lg.lg_str_once(TAG+" osm.write_way_speed done")
			done_way_speed=1


		if iprint: print("\n"+addr)
		if On_Cluster: 
			allow_dead_num = min(allow_dead_num+3, sync.total_server_num//2) # strict increasing.
			sync.Synchronize(print_str="before nids2speed, fix and cache",
			allow_dead_num=allow_dead_num) # because this depends on finishing the previous.
		
		if done_way_speed: # task depends. But if allows max_num_servers=all in previous step, then just enter:
			lock.max_access_num = 1 # lock.
			with lock:
				print("lock nids2speed ? ")
				lock.Access_Or_Skip("nids2speed", print_detail=True)
				if On_Cluster: sync.waitForNfsFile(osm_folder_path+"/%s-nids-to-speed.txt"%osmname,sleeptime=3)
				fix_no_speed(osmname, mm_loaded='mm_loaded' in sys.argv) 
				lg.lg_str_once(TAG+" fix_no_speed done")
				time.sleep(5)
				
				metaf= "osm/cache-%s-nids-to-speed.txt"%osmname
				print("proc "+metaf)
				outf= osm_folder_path+"/cache-%s-nids-to-speed.txt"%osmname
				if not mm.exist_cache(metaf) or overwrite_meta_file:
					mm.create_cache(meta_file_name=metaf,
						gen_cache_file_cmd="python "+HomeDir+"/syncdir/zyrcode/gen_cache/storeNodeId2speed.py gen_cache_file "+ osm_folder_path+"/%s-nids-to-speed.txt "%osmname +outf,
						cache_file_abs_path=outf,
						params=params,
						overwrite_meta_file=overwrite_meta_file,
						overwrite_cache_file=overwrite_cache_file,
						overwrite_memory=overwrite_memory,
						yield_func=yield_nids2speed,
						yield_args=osm_folder_path+"/%s-nids-to-speed.txt"%osmname+CUT+QUOTE,
						overwrite_prefix=True,
					)
				lg.lg_str_once(TAG+" cache nids-to-speed done")


''' --- delete and re-run : ---- '''
def delete_nid_to_speed(addr):
	osm_folder_path= DirOSM+os.sep+addr
	osm = osmPipeline(folder_path=osm_folder_path)
	osm.remove_file_way_speed()


''' --- after write_way_speed() , fix None speed from file ./cache/nospeed.txt! '''
def fix_no_speed(addr, mm_loaded=False):
	print("fix_no_speed():",addr, 'mm_loaded',mm_loaded)
	bugNoSpeedFn= mypydir+"/cache/nospeed-%s.txt"%addr
	if On_Cluster: 
		mm_tmp_nid2spd = Mem({ "num":30, "prefix":"~fx_n2spd~", "expire": 86400*30 }) 
	else:
		mm_tmp_nid2spd = Mem({ "use_ips":['localhost'], "prefix":"~fx_n2spd~", "expire": 86400*30 }) 
	aleady_mm_loaded= mm_loaded
	correctSpdFn= DirOSM+os.sep+addr+"/%s-nids-to-speed.txt"%addr
	if not aleady_mm_loaded:
		cnt=0
		pthresh=1
		print('Loading '+correctSpdFn)
		with open(correctSpdFn,"r") as f:
			for l in f:
				st=l.split(",")
				if len(st)<3: continue
				mm_tmp_nid2spd.set((int(st[0]),int(st[1])), float(st[2]))
				cnt+=1
				if cnt>=pthresh:
					print('fix_no_speed load cnt',cnt)
					pthresh*=2
		print("correct spd tup len=",cnt)
	mm_nid2latlng= CacheManager(overwrite_prefix=True) # will use existing config
	mm_nid2neighbor=CacheManager(overwrite_prefix=True) 
	mm_nid2latlng.use_cache(meta_file_name="osm/cache-%s-nodeid-to-lat-lng.txt"%addr)
	mm_nid2neighbor.use_cache(meta_file_name="osm/cache-%s-nodeid-to-neighbor-nid.txt"%addr)
	n1n2s={}
	print('Reading '+bugNoSpeedFn)
	with open(bugNoSpeedFn,"r") as f:
		for l in f:
			st=l.split(",")
			if len(st)<2: continue
			st=[int(x) for x in st]
			tup=(st[0],st[1])
			if tup not in n1n2s:
				n1n2s[tup]= -1
	print("no_speed nid tup len=",len(n1n2s))
	lastBugCnt=None
	while True:
		bugcnt=0
		for tup in n1n2s.keys():
			if n1n2s[tup]>0:
				continue
			n1=tup[0]
			n2=tup[1]
			hd1 = get_bear_given_nid12(n1,n2,mm_nid2latlng)
			if hd1 is None: continue
			nblist= mm_nid2neighbor.get(n1) # ?->n1->n2
			mindiff=1e10
			fixed=0
			if nblist:
				for nbn in nblist:
					hdn= get_bear_given_nid12(nbn,n1,mm_nid2latlng)
					if hdn is None: continue
					angle=min_angle_diff(hd1,hdn)
					if (nbn,n1) in n1n2s and n1n2s[(nbn,n1)]>0:
						spdn= n1n2s[(nbn,n1)]
					else:
						spdn= mm_tmp_nid2spd.get((nbn,n1))
					if angle<mindiff and spdn is not None:
						mindiff=angle
						n1n2s[tup]=spdn
						fixed=1
			if fixed: continue
			nblist= mm_nid2neighbor.get(n2) # n1->n2->?
			mindiff=1e10
			if nblist:
				for nbn in nblist:
					hdn= get_bear_given_nid12(n2,nbn,mm_nid2latlng)
					if hdn is None: continue
					angle=min_angle_diff(hd1,hdn)
					if (n2,nbn) in n1n2s and n1n2s[(n2,nbn)]>0:
						spdn= n1n2s[(n2,nbn)]
					else:
						spdn= mm_tmp_nid2spd.get((n2,nbn))
					if angle<mindiff and spdn is not None:
						mindiff=angle
						n1n2s[tup]=spdn
						fixed=1
			if fixed==0: bugcnt+=1
		if bugcnt==0:
			break
		print("bugcnt",bugcnt)
		if lastBugCnt is not None:
			if lastBugCnt==bugcnt:
				print("Give up #",bugcnt)
				break
		lastBugCnt=bugcnt
	
	with open(correctSpdFn,"a") as f:
		for tup in n1n2s.keys():
			if n1n2s[tup]<0: continue
			print("%d,%d,%.2f"%(tup[0],tup[1],n1n2s[tup]))
			f.write("%d,%d,%.2f\n"%(tup[0],tup[1],n1n2s[tup]))
	print("Give up #",bugcnt)


def get_bear_given_nid12(n1,n2,mm_nid2latlng):
	latlng1=mm_nid2latlng.get(n1)
	if latlng1 is None:
		try:
			latlng1=crawl_nid_to_latlng(n1)
		except: return None
	latlng2=mm_nid2latlng.get(n2)
	if latlng2 is None:
		try:
			latlng2=crawl_nid_to_latlng(n2)
		except: return  None
	return get_bearing_latlng2(latlng1,latlng2)



def test_load_mc(addr_list, overwrite_memory=False, ignore_lock=False, force_set_valid=False): 
	''' Check (or redo) if all redis k-v are good. 
	- overwrite_memory: if you don't believe mm is loaded but flag shows valid, set this to true.
	- ignore_lock: still load if other server already started loading.
	- force_set_valid: overwrite valid_flags, regard as all loaded without checking. Do not use this option with other options. Run on 1 server.
	'''
	mm=CacheManager(overwrite_prefix=True, overwrite_redis_servers=False , rt_servers= False) 
	Pretend_Valid=force_set_valid # force change mm valid flag to 1. Careful! <set_to_be_valid>
	tasks=[]
	for addr in addr_list:
		metas=["osm/cache-%s-nodeid-to-lat-lng.txt"%addr, #0
			"osm/cache-%s-nodeid-to-neighbor-nid.txt"%addr, #1
			"osm/cache-%s-nids-to-speed.txt"%addr,  #2
			"osm/cache-%s-nid-to-elevation.txt"%addr,#3
			"osm/cache-%s-nids-to-waytag.txt"%addr, #4
		]
		tasks.extend(metas)
	lock= AccessRestrictionContext(prefix="-test_load_mc~",
		persistent_restriction=True,
		persist_seconds=86400*3,
		no_restriction = not On_Cluster,)
	for task in tasks:
		if Pretend_Valid:
			mm.use_cache(meta_file_name=task,overwrite_prefix=True,set_to_be_valid=True, loading_msg=task)
			continue
		with lock:
			lock.Access_Or_Skip(task)
			print(task,MyIp)
			mm.use_cache(meta_file_name=task,overwrite_prefix=True,overwrite_memory=overwrite_memory, ignore_lock=ignore_lock, loading_msg=task)

	print("Done",addr_list,MyIp)


if __name__ == "__main__":
	
	arglist=sys.argv[1:]
	addr=None
	lock_sfx=""
	sync_wait_sec=8

	for arg in arglist: # for gen_osm_cache_from_addr() 
		if arg.startswith("max="):
			max_num_servers= int(arg.split("=")[-1])
			print('max_num_servers',max_num_servers)
		elif arg.startswith("s="):
			lock_sfx= str(arg.split("=")[-1])
			print('lock_sfx',lock_sfx)
		elif arg.startswith("addr="):
			addr= str(arg.split("=")[-1])
			print('addr',addr)
		elif arg.startswith("t="):
			sync_wait_sec= int(arg.split("=")[-1])
			print('sync_wait_sec',sync_wait_sec)


	if "gen_osm" in arglist:
		gen_osm_cache_from_addr(addr=addr, max_num_servers=max_num_servers, lock_sfx=lock_sfx,sync_wait_sec=sync_wait_sec, overwrite=("o" in arglist) )
	


	if "test_load_mc" in arglist: 
		#py 3genOsmCache.py test_load_mc o ig # force reload
		#py 3genOsmCache.py test_load_mc fv # do not actually reload
		test_load_mc(['Indiana,US','Illinois,US','NewYork,US'], overwrite_memory='o' in arglist, ignore_lock='ig' in arglist, force_set_valid='fv' in arglist)


