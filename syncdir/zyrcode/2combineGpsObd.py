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
from logger import Logger,SimpleAppendLogger
from util import read_lines_as_list,read_lines_as_dic,read_gzip,strip_illegal_char,strip_newline,unix2datetime,get_file_size_bytes,py_fname
from geo import convert_line_to_dic
from mem import Mem
from CacheManager import CacheManager
from mem import AccessRestrictionContext,Synchronizer,Semaphore

configfile = "conf.txt"
DirData = get_conf(configfile,"DirData")
gpsfolder = "gps"
obdfolder = "obd"
combinefolder ="combine"

iprint = 2  
Force_redo_combine=False # force re-run?
Delete_combine_files=False # rm output ?

lg = SimpleAppendLogger("logs/"+py_fname(__file__,False), maxsize=10000, overwrite=True)

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
KeyGas=get_conf(configfile,"KeyGas") # Gas gram
KeyRPM=get_conf(configfile,"KeyRPM") 
KeyOBDSpeed=get_conf(configfile,"KeyOBDSpeed")
KeyMAF=get_conf(configfile,"KeyMAF") 
KeyThrottle=get_conf(configfile,"KeyThrottle") 
KeyOriSysMs=get_conf(configfile,"KeyOriSysMs")

Min_GPS_File_Size = 1000 # bytes, or skip
Min_OBD_File_Size= 100 # same in 1.py

''' same in 1*.py: '''
kCutTraceTimeGap = get_conf_int(configfile,"kCutTraceTimeGap") # timestamp between two lines > this then cut
totalProcCnt=0

lock = AccessRestrictionContext(
	prefix=py_fname(__file__,False)+"~cb~", 
	persistent_restriction=True,
	persist_seconds=100, 
	print_str=False,
)

account_dirs = glob.glob(DirData+"/*")

bugEmails=[]
bugTimes=[]

if len(bugTimes)>0:
	lock.no_restriction=True
	iprint=3

with lock:
	''' make destination directory by 1 server '''
	lock.Access_Or_Wait_And_Skip("makedirs")
	for iddir in account_dirs:
		email = iddir.split(os.sep)[-1]
		if len(bugEmails)>0 and not email in bugEmails: 
			continue
		tmpdir = iddir+os.sep+combinefolder
		if not os.path.exists(tmpdir): 
			os.makedirs(tmpdir)
		if Delete_combine_files: 
			subprocess.call("rm -rf "+iddir+os.sep+combinefolder+"/*", shell=True)


for iddir in account_dirs:
	email = iddir.split(os.sep)[-1]
	if len(bugEmails)>0 and not email in bugEmails: 
		continue
	tmpdir = iddir+os.sep+gpsfolder
	if not ( os.path.exists(tmpdir) and os.path.isdir(tmpdir) ):
		if iprint>=1: print(__file__.split(os.sep)[-1],"Empty account",iddir)
		continue
	tl = [x.strip(os.sep).split(os.sep)[-1].rstrip(EXT) for x in glob.glob(tmpdir+"/*%s"%EXT)]	
	if iprint>=3: print(__file__.split(os.sep)[-1],"validtl",tl)
	if len(bugTimes)>0 and bugTimes[0] not in tl: print("bugTimes Not in Valid tl...")
	

	for i in range(len(tl)):
		truetimestr = tl[i]
		if iprint>=3: print(email+" "+truetimestr)

		with lock:
			''' each truetimestr by 1 server '''
			lock.Access_Or_Skip(email+truetimestr)
			
			if len(bugTimes)>0 and truetimestr not in bugTimes:
				if iprint>=4: print("Debug skip "+email+" "+truetimestr)
				continue

			tmpf=iddir+os.sep+combinefolder+os.sep+truetimestr+".txt"
			if os.path.exists(tmpf) and get_file_size_bytes(tmpf)>1 and not Force_redo_combine:
				if iprint>=3: print("Already "+tmpf)
				continue
			tmpf=iddir+os.sep+combinefolder+os.sep+truetimestr+"~invalid*"
			tmpfl=glob.glob(tmpf)
			if len(tmpfl)>0:
				if iprint>=2: print("Invalid "+tmpfl[0])
				continue
			if iprint>=2: print("Proc: %s "%email+combinefolder+os.sep+truetimestr+".txt")

			gfn=iddir+os.sep+gpsfolder+os.sep+truetimestr+EXT
			if get_file_size_bytes(gfn)<Min_GPS_File_Size:
				if iprint>=3: print("Too small gps skip: %s "%gfn)
				continue
			tmpfn=iddir+os.sep+obdfolder+os.sep+truetimestr+EXT
			if not os.path.exists(tmpfn):
				if iprint>=3: print("Not exists: %s "%tmpfn)
				continue
			if get_file_size_bytes(tmpfn)<Min_OBD_File_Size:
				if iprint>=3: print("Too small obd skip: %s "%tmpfn)
				continue


			# first extract ./obd/.gz  as {ms:{k:v}} higer sample rate:
			da={}
			obdtimes=[]
			orisystimes=[]
			cnt=0
			valid=3 # Num of chances of wrong MAF
			with gzip.open(iddir+os.sep+obdfolder+os.sep+truetimestr+EXT, 'rb') as f:
				for line in f:
					cnt+=1
					st = line.strip().split(CUT)
					dic = {x.split(EQU)[0] : x.split(EQU)[1] for x in st}
					try:
						dic[KeyGas] = float(dic[KeyMAF]) /14.7
					except:
						if len(obdtimes)>0 and KeyGas in da[obdtimes[-1]]:
							dic[KeyGas] = da[obdtimes[-1]][KeyGas]
							if iprint>=2:
								print(iddir+os.sep+obdfolder+os.sep+truetimestr+EXT+" missing MAF at %d\n\n\n"%cnt )
						else:
							valid-=1
							if valid<=0:
								if iprint>=2: print(iddir+os.sep+obdfolder+os.sep+truetimestr+EXT+" Too many missing MAF, break\n\n" )
								break
					try:
						dic[KeyThrottle] = float(dic[KeyThrottle]) 
					except:
						if len(obdtimes)>0 and KeyThrottle in da[obdtimes[-1]]:
							dic[KeyThrottle] = da[obdtimes[-1]][KeyThrottle]
							if iprint>=2:
								print(iddir+os.sep+obdfolder+os.sep+truetimestr+EXT+" missing Throttle at %d"%cnt )
					dic[KeySysMs] = int(dic[KeySysMs]) 
					ori=dic.pop(KeyOriSysMs) 
					try:
						dic[KeyOBDSpeed] = float(dic[KeyOBDSpeed]) 
					except:
						if len(obdtimes)>0 and KeyOBDSpeed in da[obdtimes[-1]]:
							dic[KeyOBDSpeed] = da[obdtimes[-1]][KeyOBDSpeed]
							if iprint>=2:
								print(iddir+os.sep+obdfolder+os.sep+truetimestr+EXT+" missing Spd at %d"%cnt )
					try:
						dic[KeyRPM] = float(dic[KeyRPM]) 
					except:
						if len(obdtimes)>0 and KeyRPM in da[obdtimes[-1]]:
							dic[KeyRPM] = da[obdtimes[-1]][KeyRPM]
							if iprint>=2:
								print(iddir+os.sep+obdfolder+os.sep+truetimestr+EXT+" missing RPM at %d"%cnt )
					obdtimes.append( dic.pop(KeySysMs) )
					orisystimes.append(ori)
					da[obdtimes[-1]] = dic
					if KeyGas in dic:
						if dic[KeyGas]<=0:
							print("OBD gas turns zero at line %d, cut"%cnt)
							break

# SYSTEMMILLIS=1510716138710~|TripID=1510716121776~|OBDMassAirFlow=3.13~|OBDThrottlePosition=12.156863~|OBDCommandEqRatio=1.0~|OriSysMs=1510716137542~|OBDSpeed=20.0~|OBDEngineRPM=1006.0
			if iprint>=2 and len(bugTimes)>0:
				print("#lines:%d"%cnt)
				print(obdtimes[0:10],"obdtimes",len(obdtimes))
				print(orisystimes[0:10],"orisystimes",len(orisystimes))
				if iprint>=4: print(da)
			if cnt<3 or valid<=0:
				if iprint: print("Invalid "+iddir+os.sep+obdfolder+os.sep+truetimestr+EXT)
				tmpf=iddir+os.sep+combinefolder+os.sep+truetimestr+"~invalidobd"
				with open(tmpf,"w") as f:
					f.write("")
				continue

			outf = open(iddir+os.sep+combinefolder+os.sep+truetimestr+".txt","w")
			# extract ./gps/.gz  as {ms:{k:v}} every 1s 
			gf= gzip.open(gfn, 'rb')
			line=gf.readline()
			st = line.strip().split(CUT)
			dic = {x.split(EQU)[0] : x.split(EQU)[1] for x in st}
			lasttime = int(dic[KeySysMs]) 
			pos1 = 0 # pointer in obd obdtimes
			pos2=pos1
			if iprint>=2 and len(bugTimes)>0: verbose=True
			else: verbose=False

# SYSTEMMILLIS=1510716135000~|GPSTime=1510716135000~|GPSLongitude=-88.25913684~|GPSAccuracy=4.0~|GPSLatitude=40.1452453~|GPSBearing=88.6~|OriSysMs=1510716133832~|GPSSpeed=8.75~|GPSAltitude=198.0
			while 1:
				line=gf.readline()
				if verbose: print("[ gfn ] "+line.strip())
				if not line: break
				st = line.strip().split(CUT)
				dic = {x.split(EQU)[0] : x.split(EQU)[1] for x in st}
				dic[KeySysMs] = int(dic[KeySysMs]) 
				gtime = dic.pop(KeySysMs)
				if gtime<obdtimes[0] or lasttime<obdtimes[0]: 
					if verbose: print("skip gtime,lasttime <obdtimes[0]",gtime,lasttime,obdtimes[0])
					lasttime=gtime
					continue
				while pos1<len(obdtimes) and obdtimes[pos1]<=lasttime:
					if verbose: print("skip obdtimes[pos1]<=lasttime",obdtimes[pos1],lasttime)
					pos1+=1
				if verbose: print("obdtimes[pos1]>lasttime",obdtimes[pos1],lasttime)
				pos2=pos1
				while pos2<len(obdtimes) and obdtimes[pos2]<gtime:
					if verbose: print("skip obdtimes[pos2]<gtime",obdtimes[pos2],gtime)
					pos2+=1
				if verbose: print(pos2,len(obdtimes))
				if verbose and pos2<len(obdtimes): print("obdtimes[pos2]>=gtime",obdtimes[pos2],gtime)
				if pos2>=len(obdtimes):
					if iprint>=2: print("OBD terminated early! "+iddir+os.sep+obdfolder+os.sep+truetimestr+EXT)
					break
				cumuGas=0.0
				cumuThrot=0
				cumuSpeed=0
				cumuRPM=0
				if pos1==pos2: # OBD Large Gap >1s !!!!
					obdstart= pos1-1
					obdend = pos2
				else:
					obdstart= pos1
					obdend = pos2
				obdcnt=0.000001
				for opos in range(obdstart,obdend+1):
					if KeyGas in da[obdtimes[opos]]:
						obdcnt+=1.0
						cumuGas+=da[obdtimes[opos]][KeyGas]
				cumuGas=cumuGas/obdcnt
				obdcnt=0.000001
				for opos in range(obdstart,obdend+1):
					if KeyThrottle in da[obdtimes[opos]]:
						obdcnt+=1.0
						cumuThrot+=da[obdtimes[opos]][KeyThrottle]
				cumuThrot=cumuThrot/obdcnt
				obdcnt=0.000001
				for opos in range(obdstart,obdend+1):
					if KeyOBDSpeed in da[obdtimes[opos]]:
						obdcnt+=1.0
						cumuSpeed+=da[obdtimes[opos]][KeyOBDSpeed]
				cumuSpeed=cumuSpeed/obdcnt
				obdcnt=0.000001
				for opos in range(obdstart,obdend+1):
					if KeyRPM in da[obdtimes[opos]]:
						obdcnt+=1.0
						cumuRPM+=da[obdtimes[opos]][KeyRPM]
				cumuRPM=cumuRPM/obdcnt
				if verbose:
					print("obdcnt",obdcnt,"cumuGas",cumuGas,"cumuThrot",cumuThrot,"cumuSpeed",cumuSpeed,"cumuRPM",cumuRPM)
				# gas = rate * time :
				cumuGas*= (gtime-lasttime)/1000.0 # MS
				TimeGap = kCutTraceTimeGap
				assert (gtime-lasttime)>0 and (gtime-lasttime)<100+ TimeGap*1000, "%d %d, %s %s"%(gtime,lasttime,email,truetimestr) + ". tl=%s"%truetimestr 
				dic[KeyGas] = "%.3f"%cumuGas
				if cumuGas<=0:
					print(orisystimes)
					print(obdtimes,lasttime,pos1,pos2)
					print(obdtimes[pos1-1],lasttime,obdtimes[pos1],obdtimes[pos2-1],gtime,obdtimes[pos2])
				assert cumuGas>0, iddir+os.sep+obdfolder+os.sep+truetimestr+EXT

				if cumuThrot>0:
					dic[KeyThrottle] = "%.2f"%cumuThrot
				else:
					if iprint>=3: print("Didn't write KeyThrottle to outfile.")
				if cumuSpeed>=0: 
					dic[KeyOBDSpeed] = "%.2f"%cumuSpeed 
				else:
					if iprint>=3: print("Didn't write KeyOBDSpeed to outfile.")
				if cumuRPM>0:
					dic[KeyRPM] = "%.1f"%cumuRPM
				else:
					if iprint>=3: print("Didn't write KeyRPM to outfile.")
				st=[k+EQU+v for k,v in dic.items()]
				stt=KeySysMs+EQU+"%d"%gtime+CUT+  CUT.join(st)+"\n"
				outf.write(stt)
				if iprint>=2 and len(bugTimes)>0: print("[ write ] "+stt)
				if pos1==pos2: 
					if iprint>=3: print("\nOBD Large Gap !!!!")
					if verbose:
						print(obdtimes[pos2-1],da[obdtimes[pos2-1]])
						print(obdtimes[pos2],da[obdtimes[pos2]])

				lasttime=gtime
				totalProcCnt+=1

			gf.close()
			outf.close()


print("totalProcCnt",totalProcCnt)


