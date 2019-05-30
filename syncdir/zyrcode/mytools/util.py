#!/usr/bin/env python
import os, sys, glob
import subprocess,threading
import random, time
import inspect
import requests
import gzip,re
import datetime
import dateutil.parser
from math import fabs, cos, sin, sqrt, atan2, pi
import operator
try:
	from geopy.distance import great_circle
except: 
	print("RUN $HOME/anaconda2/bin/pip install geopy")
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
if mypydir not in sys.path: sys.path.append(mypydir)
HomeDir = os.path.expanduser("~")

iprint = 1
iprintverb =0

def replace_user_home(InDir): # mac or linux OK
	if InDir.startswith('/home/'):
		st = InDir.split('/',3)
		if len(st)==3: return HomeDir
		return  HomeDir + os.sep + st[3]
	return InDir

def py_fname(fpath, abs_path=False, strip_py=True):# short func freq used in prefix.
	if fpath.endswith('.py'):
		if strip_py: fpath=fpath.rstrip('.py') # strip file ext.
	elif fpath.endswith('.pyc'):
		if strip_py: fpath=fpath.rstrip('.pyc')
		else: fpath=fpath[:-1]
	elif fpath.endswith('.pyo'):
		if strip_py: fpath=fpath.rstrip('.pyo')
		else: fpath=fpath[:-1]
	if not abs_path:
		return fpath.split(os.sep)[-1]
	return fpath

def sort_return_list_val_ind(s):
	ind=sorted(range(len(s)), key=lambda k: s[k])
	val=sorted(s)
	ret=[]
	for i in range(len(ind)):
		ret.append([val[i],ind[i]])
	return ret

def sort_dic_by_value_return_list_val_key(dic):
	return sorted(dic.items(), key=operator.itemgetter(1)) #[(k,v),..] by v

def get_create_time(fpath):
	assert fpath.startswith(os.sep), "Please provide abs path!"
	try:
		return os.path.getmtime(fpath)
	except:
		return 0.0

def get_last_modify_time(fpath): # in seconds.  *1000=ms 
	assert fpath.startswith(os.sep), "Please provide abs path!"
	try:
		return os.path.getctime(fpath)
	except:
		return 0.0

def get_file_line_num(fpath):
	assert fpath.startswith(os.sep), "Please provide abs path!"
	try:
		output = int(subprocess.check_output(["wc", "-l", fpath]).split()[0])
		return output
	except:
		return 0

def get_file_size_bytes(fpath):
	assert fpath.startswith(os.sep), "Please provide abs path!"
	try:
		size=os.stat(fpath).st_size
	except:
		size=0.0
	return size

def get_dir_size_bytes(dpath):
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(dpath):
		for f in filenames:
			fp = os.path.join(dirpath, f)
			total_size += os.path.getsize(fp)
	return total_size


def download_file(url,fpath=""):
	if fpath=="":
		local_filename = url.split('/')[-1]
		fdir = mypydir+os.sep+".."+os.sep
	else:
		if not fpath.startswith(os.sep):
			fpath= mypydir+os.sep+".."+os.sep+fpath
		if fpath.endswith(os.sep):
			raise Exception("util download_file fpath Cannot end with '/' !!")
		local_filename = fpath.split(os.sep)[-1]
		fdir = os.sep.join(fpath.split(os.sep)[0:-1])
	if local_filename=="": 
		raise Exception(__file__.split(os.sep)[-1],'util download_file local_filename empty !\n')
	if iprint: print("Downloading "+url+" To: "+fdir +os.sep+local_filename)
	if not os.path.exists(fdir): os.makedirs(fdir)
	r = requests.get(url, stream=True)	# NOTE the stream=True parameter
	with open(fdir +os.sep+local_filename, 'wb') as f:
		for chunk in r.iter_content(chunk_size=1024): 
			if chunk: # filter out keep-alive new chunks
				f.write(chunk)
	return os.path.abspath(fdir +os.sep+local_filename)

def get_dist_2latlng(lat1, lon1, lat2, lon2):
	return great_circle((lat1, lon1), (lat2, lon2)).miles

def unix2datetime(inp, shift=0.0): # shift seconds 
	inp = float(inp)
	if inp>500000000000: inp/=1000 # ms or second? 
	inp += shift
	return datetime.datetime.fromtimestamp(inp).strftime('%Y-%m-%d,%H:%M:%S')

def utcTimeStr2UnixSec(timestr): #2017-03-14T01:42:46Z
	dt= dateutil.parser.parse(timestr)
	return int(time.mktime(dt.timetuple()))

def read_gzip(filepath):
	with gzip.open(filepath, 'rb') as f:
		file_content = f.read()
	return file_content

def strip_illegal_char(st):
	#  if you don't use r'...', you'll need a backslash escape 
	return re.sub(r'[\\/*?:"\'<>|\n\r]',"",st)
def strip_white_spaces(st):
	return re.sub(r"\s+","",st)
def strip_newline(st):
	return re.sub(r'[\n\r]',"",st)

def invoke_server(userip, cmd): 
	cmd='ssh '+userip+ " "+ cmd
	if iprint: print "coor cmd: "+cmd
	subprocess.call(cmd,shell=True) # call has to block and wait here

def use_thread_invoke_server(userip, cmd):
	t=threading.Thread(target=invoke_server,args=(userip, cmd))
	t.setDaemon(True)
	t.start()
	return t

def read_lines_as_dic(fname,sep = ":"):
	res = {}
	with open(fname,"r") as fd:
		for l in fd:
			l=l.strip()
			if len(l)>0:
				st = l.split(sep,1)
				res[st[0].strip()]=st[-1].strip()
	return res

def read_lines_as_list(fname):
	iflist = []
	with open(fname,"r") as fd:
		for l in fd:
			l=l.strip()
			if len(l)>0:
				iflist.append(l)
	return iflist

def load_key2vlist(fname, sep=" "): 
  dic={}
  with open(fname,'r') as fd:
	for line in fd:
	  st=line.strip().split(sep)
	  st[0]=st[0].strip()
	  dic[st[0]]=[]
	  for ee in st[1:]:
		dic[st[0]].append(ee.strip())
  return dic

def filter_w(arr,ax=0,wind=3,weights=[]):
	if len(weights)>0:
		wt=[]
		sm=float(sum(weights))
		for x in weights:
			wt.append(x/sm)
	else:
		wt=[]
		for i in range(wind):
			wt.append(1.0/wind)
	wl = len(wt)/2
	wr = len(wt)-1-wl
	res = []
	for i in range(len(arr)):
		res.append([])
		for j in range(len(arr[0])):
			if j!=ax:
				res[i].append(arr[i][j])
				continue
			sm=0.0
			sw=0.0
			for k in range(i-wl,i+1):
				if k>=0:
					sm+=arr[k][j]*wt[k-i+wl]
					sw+=wt[k-i+wl]
			for k in range(i+1,i+wr+1):
				if k<len(arr):
					sm+=arr[k][j]*wt[k-i+wl]
					sw+=wt[k-i+wl]
			res[i].append(sm/sw)
	return res

def bucket(x, low, high, dx): 
	if x<=low: return x
	nx = int((x-low)/dx)
	if nx>int((high-low)/dx): return low+int((high-low)/dx)*dx
	return nx*dx+low

def make_choice(weightlist):
	if len(weightlist)<=1: return 0
	wtsum=max(1e-6,sum(weightlist))
	weightlist=[float(tmp)/wtsum for tmp in weightlist]
	wt=[]
	cumuwt=0.0
	for i in weightlist:
		cumuwt+=i
		wt.append(cumuwt)
	rd=random.random()
	for i in range(len(wt)):
		if rd<=wt[i]:
			return i


def strip_letter(istr):
	ostr=[]
	for lt in istr:
		if ord(lt)>=ord('0') and ord('9')>=ord(lt): ostr.append(lt)
	return ''.join(ostr) 

def err(msg=""):
	'''
	Report an error message and exit.
	'''
	print('\nERROR: %s' % (msg))
	sys.exit(1)


def beep(num=1):
	for i in range(min(20,num)):
		print('\a')
