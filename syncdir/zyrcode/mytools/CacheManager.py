#!/usr/bin/env python
''' CacheManager, genPrefixTranslate '''
import os, sys, getpass
import random, time, subprocess
import inspect
import requests
import json, codecs
from copy import deepcopy
import cPickle as pickle
import dill
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
if mypydir not in sys.path: sys.path.append(mypydir)
from namehostip import get_my_ip
from mem import Mem,TaskLock,AccessRestrictionContext
from util import unix2datetime
from namehostip import get_platform

CACHE_META_DIR="/cachemeta"
FILE_SERVER_POST="http://greengps.cs.illinois.edu/files/post/" # not in use
FILE_SERVER_GET="http://greengps.cs.illinois.edu/files/read/"
Valid_Flag_Key="?VALID" # set this first, expire earliest, used for test.
End_Flag_Key="!END" # set this last, used for test along with Valid flag.
TypeStr_to_Type={"int":int, "float":float, "string":str, }
iprint =2

class CacheManager:
	def __init__(self,**kwargs): 
		self.my_ip = get_my_ip()
		self.my_home_dir = os.path.expanduser("~") #'/root'
		self.whoami = getpass.getuser() #'root'
		self.my_platform = get_platform() # 'mac','ubuntu','centos'
		assert not " " in self.my_ip, "[ CM ] my_ip cannot multi interface!"
		self.my_meta_dir = self.my_home_dir+CACHE_META_DIR
		# run which func if redis return None? params follow the format: func_if_get_none(self.meta, key, *args , **kwargs) and return val (->key)
		self.func_if_get_none=None 
		# shorten the prefix, like TinyURL.
		self.overwrite_prefix = kwargs.get("overwrite_prefix",False)
		# for Mem use. Use recent host ranking?
		self.rt_servers = kwargs.get("rt_servers",False) 
		# overwrite redis cluster config? need to reload mem !
		self.overwrite_redis_servers = kwargs.get("overwrite_redis_servers",False) 
		if self.overwrite_redis_servers: print('[ CM ] overwrite_redis_servers !!! Reload !')


	def set_meta_file_name(self, fname):
		"""Check and set self.meta_file_name, self.meta_file_abs_path.
		:param fname: = self.meta_file_name. str 
		"""
		assert fname!="", "Please provide  meta_file_name (cannot be empty)!"
		assert not fname.endswith(os.sep), "set_meta_file_name cannot endswith(os.sep)!"
		assert not ".." in fname, "meta_file_name cannot have '../' !"
		self.meta_file_name = fname
		self.meta_file_abs_path = self.my_meta_dir+os.sep+fname
		
	def exist_cache(self,meta_file_name):
		self.set_meta_file_name( meta_file_name )
		return os.path.exists(self.meta_file_abs_path)

	def create_cache(self,**kwargs):
		"""Create cache meta and file.
		:param meta_file_name: relative path to meta file. str
		:param overwrite_meta_file: overwrite meta_file? bool
		:param overwrite_cache_file: overwrite cache file? bool
		:param overwrite_memory: overwrite contents in cache memory? bool
		:param gen_cache_file_cmd: linux command for generating cache file on host. str
		:param gen_host_ip: the host ip who generated cache file. str
		:param gen_username: the user who generated cache file. str
		:param cache_file_abs_path: where to store generated cache file? str
		:param store_allKeys: if store all keys as list in 'allKeys'
		:param engine: redis or TODO. str
		:param params: a dict, params for redis. dict
		:param yield_func: if not cache-file, yield on the fly, give yield-in-file as well
		:param yield_args: yield-in-file CUT/+ other args
		"""
		self.set_meta_file_name(  kwargs.get("meta_file_name",""))
		overwrite_meta_file =  kwargs.get("overwrite_meta_file",False)
		overwrite_cache_file =  kwargs.get("overwrite_cache_file",False)
		if not self.overwrite_prefix:
			self.overwrite_prefix = kwargs.get("overwrite_prefix",False)

		if os.path.exists(self.meta_file_abs_path) and not overwrite_meta_file:
			raise Exception("Already exists locally: "+self.meta_file_abs_path+", overwrite_meta_file=True,? or use_cache() instead of create_cache()")
		gen_cache_file_cmd =  kwargs.get("gen_cache_file_cmd","")
		assert gen_cache_file_cmd!="", "gen_cache_file_cmd cannot be empty!"
		gen_host_ip =  kwargs.get("gen_host_ip",self.my_ip)
		if gen_host_ip=="127.0.0.1" or gen_host_ip=="localhost": 
			gen_host_ip=self.my_ip
		gen_username =  kwargs.get("gen_username",self.whoami)
		self.cache_file_abs_path =  kwargs.get("cache_file_abs_path","")
		self.store_allKeys =  kwargs.get("store_allKeys",False)
		assert self.cache_file_abs_path!="", "cache_file_abs_path cannot be empty!"
		engine =  kwargs.get("engine","redis")
		params =  kwargs.get("params",{})
		params["prefix"]= self.meta_file_name
		if self.overwrite_prefix:
			params["overwrite_prefix"] = True # ask Mem() to translate pref mapping
		params["engine"]= engine
		params["overwrite_memory"]=  kwargs.get("overwrite_memory",False)
		yield_func = kwargs.get("yield_func",None) # gen k,v on the fly, avoid cache file
		yield_args = kwargs.get("yield_args",None) # inputs to yield func
		kv_action_type = kwargs.get("kv_action_type",None) # yield func set key-value append/direct set
		loading_msg =  kwargs.get("loading_msg","")

		meta = {}
		meta["meta_file_name"]=self.meta_file_name
		meta["meta_file_abs_path"]=self.meta_file_abs_path
		meta["gen_cache_file_cmd"]=gen_cache_file_cmd
		meta["gen_host_ip"]=gen_host_ip
		meta["gen_username"]=gen_username
		meta["cache_file_abs_path"]=self.cache_file_abs_path
		meta["engine"]=engine
		meta["params"]=pickle.dumps(params)
		meta["create_time"]= unix2datetime(time.time())
		meta["yield_func"]=         dill.dumps(yield_func) # it is a function in 3.py
		meta["yield_args"]= yield_args
		meta["kv_action_type"]= kv_action_type

		if self.my_platform!="centos" and not overwrite_meta_file: # check if already exists on tarekc:
			r = requests.get(url=FILE_SERVER_GET+"cache_meta", params={"meta_file_name":self.meta_file_name})
			data= r.json()
			if not "not-found" in data['status']:
				raise Exception("Already exists on Centos: "+self.meta_file_name+", overwrite_meta_file=True,? or use_cache() instead of create_cache()")
			# auto set content type to application/json:
			r = requests.post(FILE_SERVER_POST+"cache_meta", json=meta) 
			# TODO not using json
			if r.status_code!=200:
				raise Exception("create_cache post fail status_code: %d"%r.status_code)
			
		if gen_host_ip==self.my_ip or self.my_platform=="centos":
			if yield_func is None or yield_args is None:
				if overwrite_cache_file:
					gen_cache_file_cmd+=" overwrite=true"
				if iprint: 
					print("RUN "+gen_cache_file_cmd)
				subprocess.call(gen_cache_file_cmd.split(" "))
			tmpdir = os.sep.join(self.meta_file_abs_path.split(os.sep)[0:-1])
			if not os.path.exists(tmpdir): 
				os.makedirs(tmpdir)
			if True:
				pickle.dump(meta, open(self.meta_file_abs_path, 'wb'))
			else: # TODO, no longer using json
				with open(self.meta_file_abs_path, 'wb') as f:
					json.dump(meta, codecs.getwriter('utf-8')(f), ensure_ascii=False)
			if iprint: print("create_cache(): Saved  meta_file "+self.meta_file_abs_path)
		else:
			if iprint: 
				print("Gen cmd not on local host: "+gen_cache_file_cmd)

		meta["params"]=pickle.loads(str(meta["params"]))
		if self.rt_servers: 
			meta["params"]["rt_servers"]=True
		if self.overwrite_redis_servers:
			meta["params"]["overwrite_servers"]=True

		ret = self.activate_mem( meta["params"] )
		if ret=="invalid" or params["overwrite_memory"]:
			if yield_func is None or yield_args is None:
				if iprint: print("RUN load_cache_file_into_mem() ...")
				self.load_cache_file_into_mem( meta["cache_file_abs_path"], meta["params"],self.store_allKeys, msg = loading_msg)
			else:
				if iprint: print("RUN yield_func() "+yield_args)
				self.yield_file_into_mem( yield_func, yield_args, meta["params"], kv_action_type=kv_action_type, msg = loading_msg )
			print( self.activate_mem( meta["params"] ) )
		self.meta = meta


	def use_cache(self,**kwargs):
		"""Use mem according to cache meta.
		:param meta_file_name: relative path to meta file. str
		:param overwrite_meta_by_centos: overwrite meta_file by that on centos? bool
		:param store_allKeys: upon re-do, if store all keys as list in 'allKeys'
		- overwrite_memory: force reload into memory.
		- ignore_invalid_mem: non-block call, ignore not in cache.
		- set_to_be_valid: overwrite flag=1 to regard as valid.
		- ignore_lock: no access lock on load mm func.
		- overwrite_prefix: use tiny short prefix.
		- check_pref_is_short: check if overwrite_prefix is SET.
		"""
		self.set_meta_file_name(  kwargs.get("meta_file_name",""))
		overwrite_meta_by_centos =  kwargs.get("overwrite_meta_by_centos",False)
		self.store_allKeys =  kwargs.get("store_allKeys",False)
		self.ignore_invalid_mem = kwargs.get("ignore_invalid_mem",False)
		overwrite_memory =  kwargs.get("overwrite_memory",False)
		set_to_be_valid =  kwargs.get("set_to_be_valid",False)
		ignore_lock =  kwargs.get("ignore_lock",False)
		check_pref_is_short =  kwargs.get("check_pref_is_short",True)
		if not self.overwrite_prefix:
			self.overwrite_prefix = kwargs.get("overwrite_prefix",False)
		loading_msg =  kwargs.get("loading_msg","")
		params =  kwargs.get("params",{}) # if you want to change params
		if "params" in kwargs:
			print("[CM] params update these:",params)

		if not os.path.exists(self.meta_file_abs_path):
			if self.my_platform!="centos":
				r = requests.get(url=FILE_SERVER_GET+"cache_meta", params={"meta_file_name":self.meta_file_name})
				meta= r.json() 
				# TODO not using json, changed syncdir/file-server/app.py: get_file()
				if "not-found" in meta['status']:
					raise Exception("Not exists? "+self.meta_file_name)
				meta.pop('status')
				tmpdir = os.sep.join(self.meta_file_abs_path.split(os.sep)[0:-1])
				if not os.path.exists(tmpdir): 
					os.makedirs(tmpdir)
				with open(self.meta_file_abs_path, 'wb') as f: # TODO not using json
					json.dump(meta, codecs.getwriter('utf-8')(f), ensure_ascii=False)
				if iprint: print("Copied from centos: "+self.meta_file_abs_path)
			else:
				raise Exception("Not exists? "+self.meta_file_name)
		else: # already exists locally:
			if self.my_platform!="centos" and overwrite_meta_by_centos:
				r = requests.get(url=FILE_SERVER_GET+"cache_meta", params={"meta_file_name":self.meta_file_name})
				meta= r.json()
				if not "not-found" in meta['status']:
					meta.pop('status')
					with open(self.meta_file_abs_path, 'wb') as f:# TODO not using json
						json.dump(meta, codecs.getwriter('utf-8')(f), ensure_ascii=False)
					if iprint: print("Copied from centos: "+self.meta_file_abs_path)
		try: 
			meta = pickle.load(open(self.meta_file_abs_path, 'rb'))
		except: # TODO not using json
			meta = json.load(open(self.meta_file_abs_path, 'rb'))
		meta["params"]=pickle.loads(str(meta["params"]))
		meta["params"].update(params)

		if self.overwrite_prefix:
			meta["params"]['overwrite_prefix'] = True
		
		meta["params"]["overwrite_memory"]=overwrite_memory
		meta["params"]["ignore_lock"]=ignore_lock
		if self.rt_servers: 
			meta["params"]["rt_servers"]=True
		if self.overwrite_redis_servers:
			meta["params"]["overwrite_servers"]=True

		if set_to_be_valid:
			print("force set to be valid !")
			self.set_mem_valid( meta["params"] )
			return 
		ret = self.activate_mem( meta["params"] )
		if (ret=="invalid" and not self.ignore_invalid_mem) or overwrite_memory:
			yield_func = dill.loads(meta["yield_func"]) if "yield_func" in meta else None
			yield_args = meta["yield_args"] if "yield_args" in meta else None
			if yield_func is None or yield_args is None:
				if iprint: print("Redo load_cache_file_into_mem() ...")
				self.load_cache_file_into_mem( meta["cache_file_abs_path"], meta["params"],self.store_allKeys, msg = loading_msg)
			else:
				if iprint: print("RUN yield_func() "+yield_args)
				kv_action_type = meta["kv_action_type"] if "kv_action_type" in meta else None
				self.yield_file_into_mem( yield_func, yield_args, meta["params"], kv_action_type=kv_action_type, msg = loading_msg )
			print( self.activate_mem( meta["params"] ) )
		self.meta = meta
		if check_pref_is_short:
			if len(self.mm.prefix)>4: # support more prefixes if > more.
				print(self.mm.prefix)
				print(" :prefix too long, not overwrite_prefix?")
				sys.exit(0) # You Edit


	def activate_mem(self,params):
		if params["engine"]=="redis":
			if iprint>=3: print("Redis params",params)
			self.mm = Mem(params)
			tmp = self.mm.get(Valid_Flag_Key)
			tmp2 = self.mm.get(End_Flag_Key)
			if (tmp is None or tmp==0) or (tmp2 is None or tmp2==0):
				if iprint>=2: print("[ CM ] Invalid activation")
				return "invalid"
			return 'ok'
		return "unknown-engine"

	def set_mem_valid(self,params): # force set to be valid
		if params["engine"]=="redis":
			self.mm = Mem(params)
			self.mm.set(Valid_Flag_Key,1)
			self.mm.set(End_Flag_Key,1)


	def load_cache_file_into_mem(self,cache_file_abs_path, params, store_allKeys=False, msg=''):
		'''cache_file assumed to be a dict of k:list, with header, dtype, keyPos info.
		'''
		if self.my_platform=="mac":
			single_server=True # not on cluster
		else:
			single_server=False
		assert params["engine"]=="redis"
		Force_Redo=("overwrite_memory" in params and params["overwrite_memory"])

		with AccessRestrictionContext(prefix=cache_file_abs_path, no_restriction=single_server) as lock:
			lock.Access_Or_Wait_And_Skip("load_cache_file_into_mem")
			if iprint: print("load_cache_file_into_mem params",params)
			self.mm = Mem(params)
			tmp = self.mm.get(Valid_Flag_Key)
			tmp2 = self.mm.get(End_Flag_Key)
			if tmp==1 and tmp2==1 and not Force_Redo:
				if iprint: print("Redis already cached, skip...")
				return
			print("pickle.load  %s ..."%cache_file_abs_path)
			da =pickle.load(open(cache_file_abs_path,"rb"))
			if iprint>=2: 
				try:
					print(" - Info - ",da["header"],da["dtype"],da["keyPos"])
				except:
					print(" - missing keyPos or header or dtype -")
			if not Force_Redo: self.mm.set(End_Flag_Key,0) 
			self.mm.set(Valid_Flag_Key,1)
			time.sleep(0.1) # redis checks expired at freq 10hz.
			allKeys=[]
			if "header" in da: 
				header=da.pop("header")
				self.mm.set("header",header)
			if "dtype" in da: 
				dtype=da.pop("dtype")
				self.mm.set("dtype",dtype)
			if "keyPos" in da: 
				keyPos=da.pop("keyPos")
				self.mm.set("keyPos",keyPos)
			cnt, thresh =0, 1024*8
			for k,v in da.items():
				self.mm.set(k,v)
				cnt+=1
				if store_allKeys:
					allKeys.append(k)
				if iprint and cnt>=thresh:
					thresh*=2 
					print("mm key cnt %d. %s"%(cnt,msg))
			if store_allKeys: self.mm.set("allKeys",allKeys)
			self.mm.set(End_Flag_Key,1)
			if iprint: print("loaded cache key num: %d"%cnt)


	def yield_file_into_mem(self,yield_func, yield_args, params, kv_action_type=None, msg = ''):
		if self.my_platform=="mac":
			single_server=True # not on cluster
		else:
			single_server=False
		assert params["engine"]=="redis"
		Force_Redo=("overwrite_memory" in params and params["overwrite_memory"])
		ignore_lock=("ignore_lock" in params and params["ignore_lock"])

		with AccessRestrictionContext( prefix=yield_args+self.meta_file_name, no_restriction=(ignore_lock or single_server) ) as lock:
			lock.Access_Or_Wait_And_Skip("yield_file_into_mem")
			self.mm = Mem(params)
			if iprint: 
				print("yield_file_into_mem params",params)
				print('mm.prefix',self.mm.prefix)
			tmp = self.mm.get(Valid_Flag_Key)
			tmp2 = self.mm.get(End_Flag_Key)
			if tmp==1 and tmp2==1 and not Force_Redo:
				if iprint: print("Redis already cached, skip...")
				return
			if not Force_Redo: self.mm.set(End_Flag_Key,0) 
			self.mm.set(Valid_Flag_Key,1)
			time.sleep(0.1) 
			cnt, thresh =0, 1024*8
			for k,v in yield_func(yield_args):
				if kv_action_type is None:
					self.mm.set(k,v)
				elif kv_action_type==1: # append to vlist
					tmp= self.mm.get(k)
					if tmp is None: tmp=[]
					if v not in tmp: tmp.append(v)
					self.mm.set(k,tmp)
				cnt+=1
				if iprint and cnt>=thresh:
					thresh*=2 
					print("mm key cnt %d. %s"%(cnt,msg))
			self.mm.set(End_Flag_Key,1)
			if iprint: print("yielded key num: %d"%cnt)


	''' ---- mm.get: what to do if redis returns null? perhaps low memory and you reload ?'''
	def get(self, key, *args , **kwargs):
		val = self.mm.get(key, *args,**kwargs)
		if val is None:
			if self.func_if_get_none is not None:
				val = self.func_if_get_none(self.meta, key, *args , **kwargs)
				if val is not None:
					self.mm.set(key, val, *args , **kwargs)
		return val

	def set(self, key,val, *args , **kwargs):
		# self.set = self.mm.set set only allowed in load_cache_file_into_mem ?
		_val = self.mm.get(key, *args,**kwargs)
		if _val is None:
			print("CacheManager SET !!", key, val)
			self.mm.set(key, val, *args , **kwargs)
			return True
		return False

	def get_id(self,):
		try:
			return self.meta_file_name
		except AttributeError:
			return None


	def invalidate_mem(self ): # Mark as invalid. force reload/re-yield k,v into mem 
		try: 
			meta = pickle.load(open(self.meta_file_abs_path, 'rb'))
		except: # TODO not using json
			meta = json.load(open(self.meta_file_abs_path, 'rb'))
		params = pickle.loads(str(meta["params"]))
		assert params["engine"]=="redis"
		self.mm = Mem(params)
		self.mm.set(End_Flag_Key,0) 
		self.mm.set(Valid_Flag_Key,0)


def genPrefixTranslate():
	'''Run this once, will generate usable output file for pref mapping '''
	fi = os.path.expanduser("~")+os.sep+CACHE_META_DIR+os.sep+'prefix.txt' # ~/cachemeta/
	fo = os.path.expanduser("~")+os.sep+CACHE_META_DIR+os.sep+'prefixTranslate.pkl'
	dic = {	}
	ind=0
	with open(fi,'r') as f:
		for l in f:
			l=l.strip()
			if len(l)>0:
				dic[l] = str(ind)+'~'
				ind+=1
	pickle.dump(dic, open(fo, 'wb'))
	print(dic)
	print("len",len(dic))


if __name__ == "__main__":

	if 1 or "genprefix" in sys.argv[1:]: genPrefixTranslate() # shorten prefix mappings.
