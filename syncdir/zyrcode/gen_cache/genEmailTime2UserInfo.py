#!/usr/bin/env python
#  # not in use

import os, sys, getpass, glob
import subprocess
import random, time
import inspect
import gzip
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/../mytools"
if addpath not in sys.path: sys.path.append(addpath)
from readconf import get_conf,get_conf_int,get_conf_float,get_list_startswith,get_dic_startswith
from logger import Logger,SimpleAppendLogger,ErrorLogger
from util import read_lines_as_list,read_lines_as_dic,read_gzip,strip_illegal_char,strip_newline,unix2datetime,get_file_size_bytes


configfile = "conf.txt"
DirData = get_conf(configfile,"DirData") 
DirOSM = get_conf(configfile,"DirOSM") 
gpsfolder = "gps"
obdfolder = "obd"
combinefolder ="combine"
matchfolder="match"
userFolder="user"
HomeDir = os.path.expanduser("~")

iprint = 2
lg = SimpleAppendLogger("../logs/"+__file__, maxsize=10000, overwrite=True)

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
KeyCarMake=get_conf(configfile,"KeyCarMake")
KeyCarModel=get_conf(configfile,"KeyCarModel")
KeyCarYear=get_conf(configfile,"KeyCarYear")
KeyCarClass=get_conf(configfile,"KeyCarClass")

def gen_cache_file(fin,fout,overwrite): # not in use
	if os.path.exists(fout) and not overwrite:
		return
	account_dirs = glob.glob(DirData+"/*")
	emailTime2userInfo={}
	header=["email-time","dic"]
	dtype = [str,dict]
	keyPos = 0

	import cPickle as pickle
	emailTime2userInfo["header"]=header
	emailTime2userInfo["dtype"]=dtype
	emailTime2userInfo["keyPos"]=keyPos

	for iddir in account_dirs:
		email = iddir.split(os.sep)[-1]
		# gather unix time 
		tmpdir = iddir+"/%s"%combinefolder
		time_list=[x.strip(os.sep).split(os.sep)[-1].rstrip(".txt") for x in glob.glob(tmpdir+"/*.txt")]
		
		for truetimestr in time_list:
				ufn = iddir+os.sep+userFolder+os.sep+truetimestr+".gz"

				if not os.path.exists(ufn):
					if iprint>=2: print("\nskip: %s "%ufn)
					continue

				if iprint>=2: print("[ Reading User Info ] %s "%ufn)
				
				with gzip.open(ufn,"rb") as f:
					dic={}
					for l in f:
						st = l.split(CUT)
						for x in st:
							if EQU in x:
								dic[x.split(EQU)[0]] = x.split(EQU)[1]
				
				if dic[KeyUserName]=="": dic[KeyUserName]=UnknownUserEmail
				info= {
					KeyUserName:dic[KeyUserName],
					KeyCarMake:dic[KeyCarMake],
					KeyCarModel:dic[KeyCarModel],
					KeyCarYear:dic[KeyCarYear],
					KeyCarClass:dic[KeyCarClass],
				}

				emailTime2userInfo[ email+"-"+truetimestr ]=info
	
	print("Total emailTime2userInfo len",len(emailTime2userInfo))

	if iprint: print("pickle.dump "+fout)
	pickle.dump(emailTime2userInfo, open(fout,"wb")) 
	return

if __name__ == "__main__":
	
	if iprint: print(sys.argv)
	if len(sys.argv)>=4:
		if "gen_cache_file" == sys.argv[1]:
			overwrite = False
			if len(sys.argv)>=5:
				for i in range(4,len(sys.argv)):
					if sys.argv[i].lower().startswith('overwrite'):
						st = sys.argv[i].split("=",1)[-1].strip().lower()
						overwrite= st =='true'
			
			gen_cache_file(sys.argv[2],sys.argv[3], overwrite=overwrite)


