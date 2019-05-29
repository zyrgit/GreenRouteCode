#!/usr/bin/env python
print("RUN /4drive/1moveUploadGzipToData.py first !!!")

import os, sys, getpass, glob
import subprocess
import random, time
import inspect
import collections
from shutil import copy2, move as movefile
from validate_email import validate_email
import gzip
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
if mypydir not in sys.path: sys.path.append(mypydir)
from namehostip import get_my_ip
from hostip import ip2tarekc
from readconf import get_conf,get_conf_int,get_conf_float
from logger import Logger,SimpleAppendLogger
from util import read_lines_as_list,read_lines_as_dic,read_gzip,strip_illegal_char,strip_newline,unix2datetime,get_dist_2latlng,get_file_size_bytes,py_fname
from mem import Mem,AccessRestrictionContext,Synchronizer,Semaphore

''' after you run 1.py in /4drive to move from upload/ to raw/''' 

configfile = "conf.txt"
DirRaw = get_conf(configfile,"DirRaw")
DirData = get_conf(configfile,"DirData")# /proc
assert "/raw" in DirRaw
assert "/proc" in DirData
SubFolders = ["acc","gps","gyr","lin","mag","map","obd","user"]
NeedProc = ["acc","gps","gyr","lin","mag","obd"]
MustHave = ["acc","gps","gyr","lin","mag"] # if all had, already done skip.
JustCopy = [x for x in SubFolders if x not in NeedProc ]
iprint = 2  
Force_ReExtract_From_Raw = 0 #  redo extract part from dirraw to data
Overwrite_cutted = 1 #  overwrite if cut yielded timestamps 
lg = SimpleAppendLogger("logs/"+py_fname(__file__,False), maxsize=10000, overwrite=True)

Min_GPS_File_Size = 1000 # bytes, or skip
Min_OBD_File_Size = 100 # bytes, or skip
gpsfolder = "gps"
obdfolder = "obd"
mapfolder = "map"
StatsDir="stats"

STcut2oriTime="%d-cut2ori-%d"
STori2cutTime="%d-ori2cut-%d"

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
KeyOriSysMs=get_conf(configfile,"KeyOriSysMs")
PrivacyDist=get_conf_float(configfile,"PrivacyDist")
KeyMAF=get_conf(configfile,"KeyMAF") 

rawDirList = glob.glob(DirRaw+os.sep+"*")
emails = [tmp.split(os.sep)[-1] for tmp in rawDirList]

lock = AccessRestrictionContext(
	prefix=py_fname(__file__,False)+"~mr~", 
	persistent_restriction=True,
	persist_seconds=100, 
	print_str=False,
)
def find_time_diff(dl,ms):
	for dt in dl:
		if ms>=dt[0]:
			return dt[1]
	return dt[1]

with lock:
	''' make destination directory by 1 server '''
	lock.Access_Or_Wait_And_Skip("makedirs")
	for email in emails: 
		accountFolder = DirData+os.sep+email+os.sep
		if not os.path.exists(accountFolder):
			os.makedirs(accountFolder)
		for subdir in SubFolders:
			if not os.path.exists(accountFolder+subdir):
				if iprint>=1: print("makedirs "+accountFolder+subdir )
				os.makedirs(accountFolder+subdir)

semaphore=Semaphore(prefix=StatsDir+os.sep+"time2rawTime.txt", count=1) # not in use.
def write_time2rawTime(tstr,rawt):  # not in use.
	with semaphore:
		with open(StatsDir+os.sep+"time2rawTime.txt","a") as f:
			f.write(str(tstr)+" "+str(rawt)+"\n")


''' same in 2.py 4*.py 5*.py files: '''
kCutTraceTimeGap = get_conf_int(configfile,"kCutTraceTimeGap") # timestamp between two lines > this then cut.
kCutTraceDistGap = min(150,kCutTraceTimeGap* 50) # dist between two lines > this then cut.

accountRawList = glob.glob(DirRaw+os.sep+"*")


for iddir in accountRawList: 
	
	email = iddir.strip(os.sep).split(os.sep)[-1]
	accountFolder = DirData+os.sep+email+os.sep

	''' note: gps/obd seq is NOT always continuous, have gaps, have break!'''
	gflist = glob.glob(iddir+os.sep+gpsfolder+os.sep+"*"+EXT)

	for gfn in gflist: # e.g. ~/greendrive/proc/email/gps/utime.gz

		with lock:
			''' each gfn by 1 server '''
			lock.Access_Or_Skip(gfn)

			if get_file_size_bytes(gfn) < Min_GPS_File_Size: # smaller than # bytes? 
				if iprint: print("Too small, skip",gfn)
				continue
			fname = gfn.split(os.sep)[-1]

			''' already extracted from /raw/ to /proc before? '''
			utime = fname.rstrip(EXT)
			already = True
			for subdir in MustHave:
				tmpf = accountFolder+subdir+os.sep+utime+EXT
				if not os.path.exists(tmpf):
					already=False
			if already and Force_ReExtract_From_Raw==0:
				if iprint>=3: print("Already done "+gfn)
				continue

			obdfn= iddir+os.sep+obdfolder+os.sep+fname
			if os.path.exists(obdfn) and get_file_size_bytes(obdfn) < Min_OBD_File_Size: # 
				if iprint: print("Too small, skip",obdfn)
				continue

			

			'''---------------------- Check if obd MAF is all zero... ----------------'''
			try:
				with gzip.open(obdfn, 'rb') as f:
					badcnt=0
					for l in f:
						if l.startswith("TripID="): continue # log engine status line.
						if badcnt>5:
							break
						st=l.split(CUT)
						dic = {v.split(EQU)[0]:v.split(EQU)[1] for v in st}
						if KeyMAF not in dic:
							print(obdfn,l)
							badcnt+=1
							continue
						if float(dic[KeyMAF])==0:
							print(obdfn,l)
							badcnt+=1
							continue
			except IOError as e:
				print("IOError "+obdfn)
				print(e)
			if badcnt>5:
				if iprint: 
					print("\nMAF missing or All Zero.\n")
				continue
			
			rawlines=[]
			with gzip.open(gfn, 'rb') as f: # read all gps file lines.
				rawlines=f.readlines()


			'''---------------------- Priavcy? ----------------'''

			numLines = len(rawlines)
			privScale = 1.0

			''' Find gps start:'''
			st=rawlines[0].split(CUT)
			dic = {v.split(EQU)[0]:v.split(EQU)[1] for v in st}
			if KeyGPSLat in dic and KeyGPSLng in dic:
				stlat = float(dic[KeyGPSLat])
				stlng = float(dic[KeyGPSLng])
				sttime= float(dic[KeySysMs])
			''' Find gps end:'''
			st=rawlines[-1].split(CUT)
			dic = {v.split(EQU)[0]:v.split(EQU)[1] for v in st}
			if KeyGPSLat in dic and KeyGPSLng in dic:
				edlat = float(dic[KeyGPSLat])
				edlng = float(dic[KeyGPSLng])
				edtime= float(dic[KeySysMs])
			''' Exclude privacy distance, search '''
			startline =0
			for i in range(1,len(rawlines)/2):
				st=rawlines[i].split(CUT)
				dic = {v.split(EQU)[0]:v.split(EQU)[1] for v in st}
				if KeyGPSLat in dic and KeyGPSLng in dic:
					startline = i
					lat = float(dic[KeyGPSLat])
					lng = float(dic[KeyGPSLng])
					if get_dist_2latlng(lat,lng,stlat,stlng)>=privScale*PrivacyDist: # miles
						if iprint>=3: print(gfn, "startline", startline)
						break
			endline = len(rawlines)-1
			for i in range(len(rawlines)-1, len(rawlines)/2, -1):
				st=rawlines[i].split(CUT)
				dic = {v.split(EQU)[0]:v.split(EQU)[1] for v in st}
				if KeyGPSLat in dic and KeyGPSLng in dic:
					endline = i
					lat = float(dic[KeyGPSLat])
					lng = float(dic[KeyGPSLng])
					if get_dist_2latlng(lat,lng,edlat,edlng)>=privScale*PrivacyDist:# miles
						if iprint>=3: print(gfn, "num to endline", len(rawlines)-1-endline)
						break
			''' find ori sys ms range '''
			st=rawlines[startline].split(CUT)
			dic = {v.split(EQU)[0]:v.split(EQU)[1] for v in st}
			startOriSysTime = float(dic[KeySysMs])
			st=rawlines[endline].split(CUT)
			dic = {v.split(EQU)[0]:v.split(EQU)[1] for v in st}
			endOriSysTime = float(dic[KeySysMs])
			if iprint>=3: print("time range ori sys",startOriSysTime,endOriSysTime)
			assert startOriSysTime<endOriSysTime, "startOriSysTime >= endOriSysTime ?"


			'''------------- gather time gap between gps and sysms, change sys ms:------'''
			timeDiffs = [] # [ [ms,diff],  ]
			toCutGPSOriSysTimeList=[] # going to cut according to this later.
			lastgpstime=None
			for i in range(startline,endline+1):
				st=rawlines[i].strip().split(CUT)
				dic = {v.split(EQU)[0]:v.split(EQU)[1] for v in st}
				orisysms = float(dic[KeySysMs])
				gpstime =float( dic[KeyGPSTime])
				diff = gpstime-orisysms
				if timeDiffs==[] or abs(diff- timeDiffs[-1][1])>1000:
					timeDiffs.append([orisysms,diff])
				dic[KeyOriSysMs] = dic[KeySysMs]
				''' change sys ms to gps time'''
				dic[KeySysMs] = "%d"%gpstime
				st =[ KeySysMs+EQU+ dic.pop(KeySysMs) ]
				st.extend( [k+EQU+v for k,v in dic.items()] )
				stt=CUT.join(st)+"\n"
				''' re-write in place:'''
				rawlines[i]=stt
				if lastgpstime is not None:
					'''----------------- gather cut timestamps in gps:'''
					TimeGap=kCutTraceTimeGap
					if gpstime - lastgpstime> TimeGap*1000: 
						toCutGPSOriSysTimeList.append(orisysms)
				lastgpstime=gpstime

			'''----------------- gather cut timestamps in obd:'''
			lastobdtime=None
			toCutOBDOriSysTimeList=[]
			try:
				with gzip.open(iddir+os.sep+"obd"+os.sep+fname, 'rb') as f:
					cnt=0
					for l in f:
						cnt+=1
						st=l.strip().split(CUT)
						dic = {v.split(EQU)[0]:v.split(EQU)[1] for v in st}
						if KeySysMs not in dic:
							continue
						orisysms = float(dic[KeySysMs])
						if orisysms<startOriSysTime: continue
						if orisysms>endOriSysTime: break
						if lastobdtime is not None:
							TimeGap=kCutTraceTimeGap
							if orisysms - lastobdtime> TimeGap*1000: 
								toCutOBDOriSysTimeList.append(orisysms)
						lastobdtime=orisysms
			except IOError as e:
				print("IOError "+iddir+os.sep+"obd"+os.sep+fname)
				print(e)

			if iprint>=3: 
				print(toCutGPSOriSysTimeList,toCutOBDOriSysTimeList,"going to cut these times")
		
			''' combine times to cut:'''
			cutObdTimesCopy=toCutOBDOriSysTimeList[:]
			cutGpsTimesCopy=toCutGPSOriSysTimeList[:]
			toCutCombined=[]
			while len(toCutGPSOriSysTimeList)>0 or len(toCutOBDOriSysTimeList)>0 :
				if len(toCutGPSOriSysTimeList)>0 and len(toCutOBDOriSysTimeList)>0:
					if toCutGPSOriSysTimeList[0]<toCutOBDOriSysTimeList[0]:
						toCutCombined.append(toCutGPSOriSysTimeList.pop(0))
					elif toCutGPSOriSysTimeList[0]>toCutOBDOriSysTimeList[0]:
						toCutCombined.append(toCutOBDOriSysTimeList.pop(0))
					else:
						toCutCombined.append(toCutOBDOriSysTimeList.pop(0))
						toCutGPSOriSysTimeList.pop(0)
				else:
					if len(toCutGPSOriSysTimeList)>0:
						toCutCombined.append(toCutGPSOriSysTimeList.pop(0))
					if len(toCutOBDOriSysTimeList)>0:
						toCutCombined.append(toCutOBDOriSysTimeList.pop(0))
			''' remember which utime splits into multi-cut, used in other code'''
			for cutt in toCutCombined:
				with open(accountFolder+"/map/"+STori2cutTime%(int(utime),cutt),"w") as f:
					f.write("")
				with open(accountFolder+"/map/"+STcut2oriTime%(cutt,int(utime)),"w") as f:
					f.write("")



			''' -------------------- write/cut gps/.gz file ----------------'''
			if len(toCutCombined)>0:
				tocut=toCutCombined[:]
				lastgpstime=None
				gof=gzip.open(accountFolder+gpsfolder+os.sep+fname, 'wb')
				for i in range(startline,endline+1):
					st=rawlines[i].split(CUT)
					dic = {v.split(EQU)[0]:v.split(EQU)[1] for v in st}
					orisysms = float(dic[KeyOriSysMs])
					if len(tocut)>0 and orisysms>=tocut[0]:
						if tocut[0] not in cutGpsTimesCopy:
							gof.write(rawlines[i]) # if caused by obd, write this gps line twice.
						gof.close()
						newtime ="%d"%(tocut.pop(0))
						attemptgpsfn=accountFolder+gpsfolder+os.sep+newtime+".gz"
						if os.path.exists(attemptgpsfn):
							print(attemptgpsfn, unix2datetime(fname.split(".")[0]), unix2datetime(newtime))
						assert not os.path.exists(attemptgpsfn) or Force_ReExtract_From_Raw>0 or Overwrite_cutted>0, attemptgpsfn+" exists? cut but yields weird time."
						gof=gzip.open(accountFolder+gpsfolder+os.sep+newtime+".gz", 'wb')
						if iprint>=2: 
							print("\nCUT gps "+accountFolder+gpsfolder+os.sep+newtime+".gz")
					gof.write(rawlines[i])
				gof.close()
			else: # no need to cut
				with gzip.open(accountFolder+gpsfolder+os.sep+fname, 'wb') as f:
					for i in range(startline,endline+1):
						f.write(rawlines[i])


			for subdir in NeedProc:
				if subdir==gpsfolder: continue
				of = gzip.open(accountFolder+subdir+os.sep+fname, 'wb') 
				tocut=toCutCombined[:]
				try:
					with gzip.open(iddir+os.sep+subdir+os.sep+fname, 'rb') as f:
						state=0
						for l in f:
								if state==0: # find start
									st=l.split(CUT)
									if subdir=="obd": # has EQU 
										dic = {v.split(EQU)[0]:v.split(EQU)[1] for v in st}
										if KeySysMs in dic:
											tt= float(dic[KeySysMs])
										else:
											continue
									else:
										tt= float(st[0])
									if tt>startOriSysTime-2000: 
										state=1
								elif state==1:
									st= l.strip().split(CUT)
									if subdir=="obd": # has EQU 
										dic = {v.split(EQU)[0]:v.split(EQU)[1] for v in st}
										if KeySysMs in dic:
											tt= float(dic[KeySysMs])
										else:
											continue
										dic[KeyOriSysMs]=dic[KeySysMs]
										dic[KeySysMs]= "%d"%(tt+find_time_diff(timeDiffs, tt))
										st =[ KeySysMs+EQU+ dic.pop(KeySysMs) ]
										st.extend( [k+EQU+v for k,v in dic.items()] )
										stt= CUT.join(st)+"\n"
										if len(tocut)>0 and tt>tocut[0]:
											if tocut[0] not in cutObdTimesCopy:
												of.write(stt)
											of.close()
											newtime ="%d"%(tocut.pop(0))
											assert not os.path.exists(accountFolder+subdir+os.sep+newtime+".gz") or Force_ReExtract_From_Raw>0
											of=gzip.open(accountFolder+subdir+os.sep+newtime+".gz", 'wb')
											if iprint>=2: 
												print("\nCUT obd "+accountFolder+subdir+os.sep+newtime+".gz")
									else: # for acc/mag/gyr...
										tt= float(st[0])
										stt= "%d"%(tt+ find_time_diff(timeDiffs, tt))+CUT+st[1]+"\n"
										if len(tocut)>0 and tt>tocut[0]:
											of.write(stt)
											of.close()
											newtime ="%d"%(tocut.pop(0))
											assert not os.path.exists(accountFolder+subdir+os.sep+newtime+".gz") or Force_ReExtract_From_Raw>0
											of=gzip.open(accountFolder+subdir+os.sep+newtime+".gz", 'wb')
											if iprint>=2: 
												print("\nCUT %s "%subdir+accountFolder+subdir+os.sep+newtime+".gz")

									of.write(stt)

									if tt>endOriSysTime+2000:
										state=2

								elif state==2:
									break
				except IOError as e:
					print("IOError "+iddir+os.sep+subdir+os.sep+fname)
					print(e)
				of.close()

			for subdir in JustCopy:
				fnMove = iddir+os.sep+subdir+os.sep+fname
				tocut=toCutCombined[:]
				if iprint>=2: print(__file__.split(os.sep)[-1],"JustCopy",fnMove)
				if os.path.exists(fnMove): 
					copy2(fnMove, accountFolder+subdir+os.sep)
				else:
					if iprint: print(fnMove+" Not Exists! ! !")
				for newtime in tocut:
					newtimestr="%d"%newtime
					if iprint>=2: 
						print("\nCUT %s "%subdir+accountFolder+subdir+os.sep+newtimestr+".gz")
					if os.path.exists(fnMove): copy2(fnMove, accountFolder+subdir+os.sep+newtimestr+".gz")

