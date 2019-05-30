#!/usr/bin/env python
# Mem:class_Mem MemLock AccessRestrictionContext TaskLock Synchronizer Semaphore 
import os, sys, subprocess
import random, time
import inspect
import cPickle as pickle
import redis
from redis.exceptions import WatchError
import copy
from hostip import tarekc2ip,get_servers
from executor import time_limit
from util import py_fname
from namehostip import get_platform
My_Platform = get_platform()

iprint =1
_PORT = 6380
Mem_Server_Num=25
fprefixTranslate= os.path.expanduser("~")+os.sep+"cachemeta"+os.sep+'prefixTranslate.pkl' # contains mapping from long prefix to short prefix.
try:
	prefixTranslate = pickle.load(open(fprefixTranslate, 'rb'))
except: prefixTranslate={}

_mem_servers_log_dir = os.path.expanduser("~")+os.sep+"cachemeta"+os.sep+'mem'+os.sep # store list of server ips for some prefix. avoid hostrank.txt.

''' Core params (dict) to compare between connection pools: nodes, db.
 Prevent too many connections. [ Module Level Instance ] '''
Core_param_list=[]
Redis_Cluster_instances=[]

def check_redis_server(ip,port): # check if ip:port allows redis connect.
	cmd = "redis-cli -h %s -p %d ping"%(ip,port)
	tmp=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
	out, err = tmp.communicate()# ('PONG\n', None) if good.
	if out.strip().lower()=='pong':
		return 1
	return 0




class AccessDenied(Exception):
	"""Represents an Exception used by AccessRestrictionContext"""
	def __init__(self, message=None):
		self.message = "" if message is None else message
	def __str__(self):
		return "[ AccessDenied ] %s" % (self.message)


class AccessRestrictionContext(object):
	''' Task lock as a context manager. Use this if no mm.get() needed.
	--- Usage: with AccessRestrictionContext(**kwargs) as lock: ... OR
	with lock:
		lock.Access_Or_Skip(key)
		...
	Typically you should use mm.get first, if already set, don't bother to use this class.
	- persistent_restriction: If one server completes the context, other servers can enter again, if you don't want others to re-run later, set.
	- no_restriction: If no restriction needed, set.
	- write_completion_file_mark: if you need a meta file to mark task complete, to avoid re-run.
	- persistent_restriction: if not write_completion_file_mark, this prevents re-run.
	'''
	DENY = -999 # used in persistent_restriction. must <0. 
	def __init__(self, prefix,
				host=None,port=_PORT, 
				write_completion_file_mark=False, 
				completion_file_dir=None,
				max_access_num=1, 
				persistent_restriction=False,
				persist_seconds=10,
				no_restriction=False,
				redo_task=False,
				print_str=True ):
		self.print_str=print_str 
		if host is None: 
			serverlist=get_servers({"highLow":"high","num":Mem_Server_Num})
			host=serverlist[hash(prefix)%(len(serverlist))]
			if iprint: print("AccessRestrictionContext using host: "+host)
		self.access_granted=False
		self.no_restriction=no_restriction
		self.prefix = prefix # task name
		if not self.no_restriction: # if multi entities need restriction.
			self.host = host
			self.port = port
			self.r = redis.StrictRedis(host=host, port=port)
			''' assume task/run can complete in 14 days, just to prevent mem loss.'''
			self.task_timeout=86400*60 
			''' how many entities can access/run the context?  1 means lock.'''
			self.max_access_num=max_access_num 
			''' if you want restriction after task completion. for slow lagging behind servers.'''
			self.persistent_restriction=persistent_restriction
			self.persist_seconds=persist_seconds

		self.redo_task=redo_task
		self.write_completion_file_mark=write_completion_file_mark
		self.completion_file_dir=completion_file_dir
		if self.write_completion_file_mark and not os.path.exists(self.completion_file_dir+os.sep): 
			try:
				os.makedirs(self.completion_file_dir+os.sep)
			except: pass

	def __enter__(self):
		return self
	def __exit__(self, exc_type, exc_value, exc_traceback):
		if exc_type is None and exc_value is None and exc_traceback is None: exit_normally=True
		else: exit_normally=False

		if self.access_granted: # decrease num:
			with self.r.pipeline() as pipe:
				while True:
					try:
						pipe.watch(self.made_key)
						current_value = pipe.get(self.made_key)
						if current_value is not None:
							current_value=int(current_value)
							if current_value>1:
								pipe.multi()
								pipe.set(self.made_key, current_value - 1)
								pipe.expire(self.made_key, self.task_timeout)
								pipe.execute()
								if iprint>=2 and self.print_str: print("exit, access dec to #%d"%(current_value-1))
							elif self.persistent_restriction:
								pipe.multi()
								pipe.set(self.made_key, AccessRestrictionContext.DENY)
								pipe.expire(self.made_key, max(self.persist_seconds,1))
								pipe.execute()
								if iprint>=2 and self.print_str: print("exit in persistent_restriction")
								if self.write_completion_file_mark and exit_normally: self._write_file()
							else:
								pipe.multi()
								pipe.delete(self.made_key)
								pipe.execute()
								if iprint>=2 and self.print_str: print("exit access, delete "+self.made_key)
								if self.write_completion_file_mark and exit_normally: self._write_file()
						break
					except WatchError:
						continue

		if self.no_restriction and self.write_completion_file_mark and exit_normally: self._write_file() # if not using redis or already written it at above
		if exc_type == AccessDenied: return True # True means suppress exception.
		if exit_normally: return True # normal exit.
		return False # re-raise exception.

	def _write_file(self,):
		with open(self.completion_file_dir+os.sep+"COMPLETE-"+self.input_key.replace(os.sep,"-"), "a") as f: f.write("\n")
	def _task_done_before(self,):
		if os.path.exists(self.completion_file_dir+os.sep+"COMPLETE-"+self.input_key.replace(os.sep,"-")): return True
		return False
	def _make_key(self, k): 
		return '%s%s%s'%(self.prefix, "_AC_", str(k))

	def Access_Or_Wait_And_Skip(self, k, print_detail=False):
		''' This should be called at the first line in the context.'''
		self.input_key=str(k)
		if not self.redo_task and self.write_completion_file_mark and self._task_done_before(): 
			if (iprint>=1 and self.print_str) or print_detail: print(self.input_key+" task done before, denied")
			raise AccessDenied() # just exit.
		self.made_key=self._make_key(k)
		if self.no_restriction: return
		with self.r.pipeline() as pipe:
			while True:
				try:
					pipe.watch(self.made_key)
					current_value = pipe.get(self.made_key)
					if current_value is None: current_value=0
					current_value=int(current_value)
					if self.persistent_restriction and current_value==AccessRestrictionContext.DENY:
						self.access_granted=False # still deny.
						if (iprint>=1 and self.print_str) or print_detail: print("persistent restriction deny, "+self.input_key)
						break
					if current_value< self.max_access_num:
						pipe.multi()
						pipe.set(self.made_key, current_value+1)
						pipe.expire(self.made_key, self.task_timeout)
						pipe.execute()
						self.access_granted=True
						if (iprint>=2 and self.print_str) or print_detail: print("%d'th access granted, %s"%(1+current_value,self.input_key))
						break
					else:
						self.access_granted=False # someone already took it.
						if (iprint>=1 and self.print_str) or print_detail: 
							print("%d'th access denied, %s"%(current_value,self.input_key))
							print(self.host,self.port)
							print(self.made_key)
						break
				except WatchError:
					continue # try again to confirm.
		if self.access_granted: 
			if print_detail: print('access_granted!')
			return # go exec code within context.
		# not my task, wait:
		if (iprint and self.print_str) or print_detail: print("waiting at access lock...")
		tmpk= self.r.get(self.made_key)
		while (tmpk is not None) and int(tmpk)>0:
			tmpk= self.r.get(self.made_key)
			time.sleep(0.4)
		# raise exception to avoid exec the context. 
		raise AccessDenied()

	def Access_Or_Skip(self, k, print_detail=False):
		''' No waiting.'''
		self.input_key=str(k)
		if not self.redo_task and self.write_completion_file_mark and self._task_done_before(): 
			if (iprint>=1 and self.print_str) or print_detail: print(self.input_key+" task done before, denied")
			raise AccessDenied() # just exit.
		self.made_key=self._make_key(k)
		if self.no_restriction: 
			if print_detail: print("no_restriction.")
			return
		with self.r.pipeline() as pipe:
			while True:
				try:
					pipe.watch(self.made_key)
					current_value = pipe.get(self.made_key)
					if print_detail: 
						print(self.made_key)
						print("current_value", current_value)
					if current_value is None: current_value=0
					current_value=int(current_value)
					if self.persistent_restriction and current_value==AccessRestrictionContext.DENY:
						self.access_granted=False # still deny.
						if (iprint>=1 and self.print_str) or print_detail: 
							print("persistent restriction deny, "+self.input_key)
						break
					if current_value< self.max_access_num:
						pipe.multi()
						pipe.set(self.made_key, current_value+1)
						pipe.expire(self.made_key, self.task_timeout)
						pipe.execute()
						self.access_granted=True
						if (iprint>=2 and self.print_str) or print_detail: 
							print("%d'th access granted, %s"%(1+current_value,self.input_key))
						break
					else:
						self.access_granted=False # someone already took it.
						if (iprint>=1 and self.print_str) or print_detail: 
							print("%d'th access denied, %s"%(current_value,self.input_key))
						break
				except WatchError:
					continue # try again to confirm.
		if self.access_granted: 
			if print_detail: print("access_granted!")
			return # go exec code within context.
		if (iprint>=2 and self.print_str) or print_detail: print("skip access")
		# raise exception to avoid exec the context. 
		raise AccessDenied()


	def hard_reset_key(self,k): # reset if program gets killed without dec key cnt ...
		self.input_key=str(k)
		self.made_key=self._make_key(k)
		with self.r.pipeline() as pipe:
			try:
				pipe.watch(self.made_key)
				pipe.multi()
				pipe.set(self.made_key, 0)
				pipe.expire(self.made_key, self.task_timeout)
				pipe.execute()
				print("hard_reset_key: %s"%(self.made_key))
			except WatchError:
				print("WatchError: %s"%(self.made_key)) 




class Mem: # class_Mem
	'''General usage of Memory Cache: 
	Exposed:   set()  get()  flush() 
	Internal:  set_with_expire()  Must Use Expire !!!!
	'''
	
	def __init__(self,params={}): 
		# num of servers:
		num = params["num"] if "num" in params else 1 
		port = params["port"] if "port" in params else _PORT
		# high,low -end?
		highLow = params["highLow"] if "highLow" in params else "high" 
		self.prefix = str(params["prefix"]) if "prefix" in params else ""
		# shorten, like TinyURL.
		self.overwrite_prefix = params["overwrite_prefix"] if "overwrite_prefix" in params else False
		if self.overwrite_prefix:
			if self.prefix in prefixTranslate:
				self.prefix = prefixTranslate[self.prefix]
		# MUST set >0 seconds
		self.exp = params["expire"] if "expire" in params else 86400 
		self.engine = params["engine"] if "engine" in params else "redis"
		# if to use recent server rt ranking or old hostrank.txt.
		self.rt_servers = params["rt_servers"] if "rt_servers" in params else False 
		# if discard previous ips, start new.
		self.overwrite_servers = params["overwrite_servers"] if "overwrite_servers" in params else False 
		# use these IP addresses list. 
		self.use_ips = params["use_ips"] if "use_ips" in params else None 

		'''--- multi-server task allocation: ---'''
		# how long does task occupied flag expire? task_timeout should be less than re-run time gap, and bigger than task normal execution time.
		self.task_timeout = params["task_timeout"] if "task_timeout" in params else 10

		assert self.exp>0, "Please set expire > 0 !"
		pparams = params.copy() # shallow copy, must add following:
		pparams["num"]=num
		pparams["highLow"]=highLow
		pparams["realtime"]=self.rt_servers
		servers_dic ={} 
		servers_logf = _mem_servers_log_dir+ self.prefix.replace(os.sep,"~")
		if My_Platform !="mac":
			_lock = AccessRestrictionContext( # for internal use.
				prefix=py_fname(__file__,False)+self.prefix, 
				persistent_restriction=True,
				persist_seconds=60, print_str=False,
			)
		else: _lock=None

		if not self.overwrite_servers and os.path.exists(servers_logf): # servers that used to store mem.
			servers_dic = pickle.load(open(servers_logf, 'rb'))
		else:
			cnt , down = 0 , 0 
			for h in ( self.use_ips or get_servers(pparams) ):
				ret = 0
				with time_limit(2, msg=h +" Redis Fail to connect."):
					ret = check_redis_server(h,port) # timeout=2s
				if ret:
					cnt+=1
					servers_dic["node_%d"%cnt]={'host': h, 'port' : port}
				else:
					down +=1
			if _lock: 
				with _lock:
					_lock.Access_Or_Skip('pickle.dump')
					pickle.dump(servers_dic, open(servers_logf, 'wb'))
		cnt = len(servers_dic)
		assert(cnt>0)
		if iprint>=2: print("Cache servers ", servers_dic, "Num:%d"%cnt)
		self.cluster = {'nodes' : servers_dic}

		if self.engine=="redis":
			'''if use redis:'''
			self.redis_init(params)
			self.use_redis()

		if self.exp>0:
			'''if set expire:'''
			self.use_expire()
		self.expire_done_list=[] # set once and skip later.


	'''--------------- In general:'''
	def set_if_none(self, k, v, seg=""):
		if self.get(k,seg) is None:
			self.set(k,v,seg)
	def set_prefix(self,pref):
		self.prefix = str(pref)
	def set_expire(self,exp):
		self.exp = max(1,int(exp) ) # seconds, redis supports > 1 year.

	def _make_key(self, k, seg=""): # use  seg  for extra flexibility.
		return '%s%s%s'%(self.prefix, str(seg), str(k))

	def use_expire(self,):
		self.set = self.set_with_expire


	'''--------------- Define multi-server task allocation scheme:
	This scheme is NOT atomic or safe! '''

	def Grab_Task(self, k, seg=""):
		'''One line to test and set.'''
		if self.MC_Is_My_Task(k, "MC_T", seg):
			self.MC_Occupy(k, "MC_T", seg)
			return True
		else:
			return False # already taken.
	def Done_Task(self, k, seg=""):
		self.m.delete(self._make_key(k,"MC_T"+seg))

	def MC_Occupy(self, k, string, seg=""): # string input should be explicit.
		'''I am taking this task.'''
		self.set_with_explicit_expire(k,"1",self.task_timeout,string+seg)
		# it basically just sets a small expire value.
	def MC_Is_My_Task(self, k, string, seg=""): # string input should be explicit.
		'''Should I take this task?'''
		if self.get(k,string+seg):
			return False # already occupied.
		if self.get(k,seg):
			return False # already completed.
		return True


	'''-------------------- Specific to redis: '''
	def redis_init(self,params):
		global Redis_Cluster_instances, Core_param_list
		try:
			import rediscluster
		except:
			print("RUN $HOME/anaconda2/bin/pip install redis")
			raise Exception("RUN $HOME/anaconda2/bin/pip install rediscluster")
		m_db=params["db"] if "db" in params.keys() else 0
		''' module level same instance: '''
		core_params={ "nodes":self.cluster, "db":m_db }
		ind=0
		while ind < len(Core_param_list):
			if core_params==Core_param_list[ind]:
				self.r = Redis_Cluster_instances[ind]
				if iprint>=2: print("\n\nRe-using %d'th redis instance.\n\n"%ind)
				break
			ind+=1
		if ind==len(Core_param_list):
			if iprint>=2: print("\n\nCreating new redis instance\n\n")
			Core_param_list.append(copy.deepcopy(core_params)) # must deep copy!!!!
			self.r = rediscluster.StrictRedisCluster(cluster=self.cluster, db=m_db) 
			Redis_Cluster_instances.append(self.r)

	def use_redis(self,):
		self.m = self.r
		self.set = self.redis_set
		self.set_with_expire = self.redis_set_with_expire
		self.set_with_explicit_expire = self.redis_set_with_explicit_expire
		self.get = self.redis_get
		self.flush = self.flushdb

	def redis_set(self, k,v, seg=""): # segment provides further partition. 
		return self.m.set( self._make_key(k,seg) , pickle.dumps(v))
	def redis_set_with_expire(self, k,v, seg=""): 
		self.redis_set( k, v, seg)
		self.expire(k, self.exp, seg)
	def redis_set_with_explicit_expire(self, k,v, exp, seg=""): 
		self.redis_set( k, v, seg)
		self.expire(k, exp, seg)
	def redis_get(self, k, seg=""): # segment provides further partition.
		pk  = self.m.get( self._make_key(k,seg) ) 
		if pk:
			return pickle.loads(pk)
		else:
			return None
	def expire(self, k, time, seg=""):
		return self.m.expire( self._make_key(k,seg) , time)
	def flushdb(self,): # redis clear all 
		self.m.flushdb()
	def rpush(self,k,v):
		ret=self.m.rpush( self._make_key(k) , v)
		if k not in self.expire_done_list: 
			self.expire(k, self.exp)
			self.expire_done_list.append(k)
		return ret
	def lrange(self,k,i1,i2): # LRANGE key 0 10, will return 11 elements !!!
		return self.m.lrange( self._make_key(k) , i1,i2) # inclusive
	def lpop(self,k):
		ret = self.m.lpop( self._make_key(k) )
		if ret is None: # if empty, redis will delete expire config, need to reset list here
			if k in self.expire_done_list: 
				self.expire_done_list.remove(k)
		return ret
	def delete(self,k):
		if k in self.expire_done_list: 
			self.expire_done_list.remove(k)
		return self.m.delete(self._make_key(k))






class SyncTooSlow(Exception):
	""" Represents an Exception to erase dead servers"""
	def __init__(self, message=None):
		self.message = "" if message is None else message
	def __str__(self):
		return "[ SyncTooSlow ] %s" % (self.message)


class Synchronizer(object):
	''' Multi server execute in sync. No 'with' context. 
	'''
	SLOWDIE = -9999 # mark slow as dead.
	def __init__(self, prefix, sync_group_name="", host=None, port=_PORT ):
		if host is None: 
			serverlist=get_servers({"highLow":"high","num":Mem_Server_Num})
			host=serverlist[hash(prefix)%(len(serverlist))]
			if iprint: print("Synchronizer using host: "+host)
		self.prefix = prefix # for redis 
		self.sync_group_name = sync_group_name # for total servers num 
		self.r = redis.StrictRedis(host=host, port=port)
		self.task_timeout=86400*60

	def _make_key(self, k): 
		return '%s%s%s'%(self.prefix, "~Sync~", str(k))

	def Clear(self):
		''' Should be cleared at the beginning of python code, before Register(). 
		Let all sleep for a few seconds after calling this.
		This sets  total server size =0. Followed by Register()'''
		self.server_num_key = self._make_key(self.sync_group_name)+"~num"
		self.r.set(self.server_num_key, 0) # server num.
		self.r.expire(self.server_num_key, self.task_timeout)
		if iprint: print("Synchronizer clear.")

	def Register(self, k=""):
		''' Should be registered at the beginning of python code, to add to total server num.
		Need to clear all previous keys k, before registered.
		You can also use a diff k, in all later func.
		'''
		i=0
		while 1: # clear used keys.
			i+=1
			self.input_key=str(k)+"~"+str(i)+"~"+str(self.sync_group_name)
			self.made_key=self._make_key(self.input_key)
			if self.r.get(self.made_key) is None:
				break # already cleared at biggest step.
			self.r.delete(self.made_key)

		self.server_num_key = self._make_key(self.sync_group_name)+"~num"
		raw_input("Press any key, if all clear to go.")

		with self.r.pipeline() as pipe:
			while True:
				try:
					pipe.watch(self.server_num_key)
					current_value = pipe.get(self.server_num_key)
					if current_value is None: current_value=0
					current_value=int(current_value)
					pipe.multi()
					pipe.set(self.server_num_key, current_value+1)
					pipe.expire(self.server_num_key, self.task_timeout)
					pipe.execute()
					break
				except WatchError:
					continue # try again to confirm.
		self.step=0
		if iprint: print("Registered! %d'th "%(current_value+1))


	def Synchronize(self, k="", print_str="", allow_dead_num=0, wait_time=600, kill_slow=True):
		''' Call this to wait to sync. 
		If Step key k is same across all calls, then make sure self.step counter will not cause duplicate key and is consistent across servers. 
		'''
		self.step+=1
		self.input_key=str(k)+"~"+str(self.step)+"~"+str(self.sync_group_name)
		self.made_key=self._make_key(self.input_key)
		self.total_server_num = int(self.r.get(self.server_num_key))
		assert self.total_server_num>0

		with self.r.pipeline() as pipe:
			while True:
				try:
					pipe.watch(self.made_key)
					current_value = pipe.get(self.made_key)
					if current_value is None: 
						current_value=0
					current_value=int(current_value)
					if current_value == Synchronizer.SLOWDIE:
						print("I am too slow...")
						if kill_slow:
							raise SyncTooSlow("Die at: "+self.made_key)
						else: return
					pipe.multi()
					if current_value+1==self.total_server_num: #both must set same time.
						if iprint: print("All servers in sync")
					pipe.set(self.made_key, current_value+1)
					pipe.expire(self.made_key, self.task_timeout)
					pipe.execute()
					if iprint: print("%d are in sync"%(current_value+1))
					break
				except WatchError:
					continue # try again to confirm.

		starttime_key = self.made_key+"~starttime"
		self.r.set(starttime_key, time.time()) # last server also sets clock.
		self.r.expire(starttime_key, self.task_timeout)
		in_sync_cnt=0

		''' wait for all, increase counter, cannot decrease until max out full. '''
		cur_num = int(self.r.get(self.made_key))
		if cur_num>in_sync_cnt: in_sync_cnt=cur_num
		last_cur_num=-1
		while cur_num>=0 and cur_num < self.total_server_num and time.time()-float(self.r.get(starttime_key))<wait_time:
			if iprint and last_cur_num!=cur_num: 
				if last_cur_num<0: print("waiting at sync lock... "+print_str)
				last_cur_num=cur_num
				print("Now",cur_num,"Needs",self.total_server_num,"wait<",wait_time)
			time.sleep(0.6)
			cur_num=int(self.r.get(self.made_key))
			if cur_num>in_sync_cnt: in_sync_cnt=cur_num

		''' wait for alive, treat late-comer as dead, too slow. '''
		cur_num = int(self.r.get(self.made_key))
		if cur_num>in_sync_cnt: in_sync_cnt=cur_num
		last_cur_num=-1
		while cur_num>=0 and cur_num < self.total_server_num - allow_dead_num:
			if iprint and last_cur_num!=cur_num: 
				if last_cur_num<0: print("waiting at sync lock... "+print_str)
				last_cur_num=cur_num
				print("Now",cur_num,"Needs",self.total_server_num-allow_dead_num,"allow dead",allow_dead_num)
			time.sleep(0.4)
			cur_num=int(self.r.get(self.made_key))
			if cur_num>in_sync_cnt: in_sync_cnt=cur_num

		if float(self.r.get(starttime_key))>0: 
			self.r.set(starttime_key, 0) # clear clock so late comer won't block.
			self.r.expire(starttime_key, self.task_timeout)
		if int(self.r.get(self.made_key))>=0: # kill late comers. 
			self.r.set(self.made_key, Synchronizer.SLOWDIE ) # mark
			self.r.expire(self.made_key, self.task_timeout)
		if iprint: print("%d Sync, Go! %s"%(in_sync_cnt,print_str))


	def waitForNfsFile(self,fpath,sleeptime=1): # sleep if not seeing the NFS file.
		assert fpath.startswith(os.sep), "Please use abs fpath!"
		if not os.path.exists(fpath):
			time.sleep(sleeptime)



class NotAvailable(Exception):
	""" Raised when unable to aquire the Semaphore in non-blocking mode
	"""
	def __init__(self, message=None):
		self.message = "" if message is None else message
	def __str__(self):
		return "[ NotAvailable ] %s" % (self.message)
def __py2_dict_items(dic):
	return dic.iteritems()
def __py3_dict_items(dic):
	return dic.items()
dict_items = __py3_dict_items  if sys.version_info.major >= 3  else __py2_dict_items



class Semaphore(object):
	''' Semaphore from https://github.com/bluele/redis-semaphore/blob/master/redis_semaphore/__init__.py
	-- reset: if has lagging slow server, if semaphore created later, should not reset.
	'''
	def __init__(self, prefix, host=None, 
		count=1, stale_client_timeout=None, blocking=True, 
		port=_PORT, reset=True,
		no_restriction=False,
		):
		self.no_restriction = no_restriction
		if not self.no_restriction:
			if host is None: 
				serverlist=get_servers({"highLow":"high","num":Mem_Server_Num})
				host=serverlist[hash(prefix)%(len(serverlist))]
				if iprint: print("Semaphore using host: "+host)
			if count < 1:
				raise ValueError("Parameter 'count' must be >= 1")
			self.client = redis.StrictRedis(host=host, port=port)
		self.count = count
		self.namespace = prefix
		self.stale_client_timeout = stale_client_timeout
		self.is_use_local_time = False
		self.blocking = blocking
		self._local_tokens = list()
		self.exists_val = 'ok'
		if reset and not self.no_restriction: 
			self.client.delete(self.check_exists_key) # will refresh available locks.

	def _exists_or_init(self): # only 1 server gets to init available keys 1->count
		old_key = self.client.getset(self.check_exists_key, self.exists_val)
		if old_key:
			return False
		return self._init()

	def _init(self):
		self.client.expire(self.check_exists_key, 10)
		with self.client.pipeline() as pipe:
			pipe.multi()
			pipe.delete(self.grabbed_key, self.available_key)
			pipe.rpush(self.available_key, *range(self.count))
			pipe.execute()
		self.client.persist(self.check_exists_key)

	@property
	def available_count(self):
		return self.client.llen(self.available_key)

	def acquire(self, timeout=0, target=None):
		if self.no_restriction: return
		self._exists_or_init()
		if self.stale_client_timeout is not None:
			self.release_stale_locks()
		if self.blocking:
			pair = self.client.blpop(self.available_key, timeout)
			if pair is None:
				raise NotAvailable
			token = pair[1]
		else:
			token = self.client.lpop(self.available_key)
			if token is None:
				raise NotAvailable
		self._local_tokens.append(token)
		self.client.hset(self.grabbed_key, token, self.current_time)
		if target is not None:
			try:
				target(token)
			finally:
				self.signal(token)
		return token

	def release_stale_locks(self, expires=10):
		if self.no_restriction: return
		token = self.client.getset(self.check_release_locks_key, self.exists_val)
		if token:
			return False
		self.client.expire(self.check_release_locks_key, expires)
		try:
			for token, looked_at in dict_items(self.client.hgetall(self.grabbed_key)):
				timed_out_at = float(looked_at) + self.stale_client_timeout
				if timed_out_at < self.current_time:
					self.signal(token)
		finally:
			self.client.delete(self.check_release_locks_key)

	def _is_locked(self, token):
		return self.client.hexists(self.grabbed_key, token)

	def has_lock(self):
		for t in self._local_tokens:
			if self._is_locked(t):
				return True
		return False

	def release(self):
		if self.no_restriction: return
		if not self.has_lock():
			return False
		return self.signal(self._local_tokens.pop())

	def reset(self):
		self._init()

	def signal(self, token):
		if token is None:
			return None
		with self.client.pipeline() as pipe:
			pipe.multi()
			pipe.hdel(self.grabbed_key, token)
			pipe.lpush(self.available_key, token)
			pipe.execute()
			return token

	def get_namespaced_key(self, suffix):
		return '{0}:{1}'.format(self.namespace, suffix)
	@property
	def check_exists_key(self):
		return self._get_and_set_key('_exists_key', 'EX')
	@property
	def available_key(self):
		return self._get_and_set_key('_available_key', 'AV')
	@property
	def grabbed_key(self):
		return self._get_and_set_key('_grabbed_key', 'GR')
	@property
	def check_release_locks_key(self):
		return self._get_and_set_key('_release_locks_ley', 'RE')
	def _get_and_set_key(self, key_name, namespace_suffix):
		if not hasattr(self, key_name):
			setattr(self, key_name, self.get_namespaced_key(namespace_suffix))
		return getattr(self, key_name)
	@property
	def current_time(self):
		if self.is_use_local_time:
			return time.time()
		return float('.'.join(map(str, self.client.time())))

	def __enter__(self):
		if not self.no_restriction: 
			self.acquire()
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		if not self.no_restriction: 
			self.release()
		return True if exc_type is None else False





class MemLock:
	''' Use redis transactions to implement task lock, grab task if mm.get() return None then do the task.
	--- Usage: 
	if lock.Grab_Task(key):
		...
		lock.Done_Task()
	Use this if decision is made by the mm.get() value.
	Multi server/thread safe, atomic!
	Can have exception before Done_Task, can re-run. Can not be nested.
	https://redis.io/topics/transactions
	https://github.com/andymccurdy/redis-py/#pipelines,
	Also see: https://github.com/SPSCommerce/redlock-py, but not using it.'''
	def __init__(self,mem,host=None,port=_PORT,
		task_timeout=100,
		delete_after_done=True): 
		'''mem: class Mem or any memcache.'''
		'''host: IP. CAN NOT be 'localhost' unless single server multi thread! 1 server only.''' 
		if host is None: 
			serverlist=get_servers({"highLow":"high","num":Mem_Server_Num})
			host=serverlist[hash(mem.prefix)%(len(serverlist))]
			if iprint: print("MemLock using host: "+host)
		self.r = redis.StrictRedis(host=host, port=port)
		self.mem = mem
		self.prefix = mem.prefix
		assert task_timeout>0
		self.task_timeout=task_timeout
		self.delete_after_done=delete_after_done

	def _make_key(self, k): # add str to sep namespace.
		return '%s%s%s'%(self.prefix, "M_L", str(k))

	def Grab_Task(self, k):
		if self.mem.get(k) is not None: return False
		self.made_key=self._make_key(k)
		# transaction: 
		with self.r.pipeline() as pipe:
			while True:
				try:
					# put a WATCH on the key that holds our sequence value
					pipe.watch(self.made_key)
					# after WATCHing, the pipeline is put into immediate execution
					# mode until we tell it to start buffering commands again.
					# this allows us to get the current value of our sequence
					current_value = pipe.get(self.made_key)
					if current_value is None: 
						# now we can put the pipeline back into buffered mode with MULTI
						pipe.multi()
						pipe.set(self.made_key, 1)
						pipe.expire(self.made_key, self.task_timeout)
						# and finally, execute the pipeline (the set command)
						pipe.execute()
						# if a WatchError wasn't raised during execution, everything
						# we just did happened atomically.
						my_task=True
						break
					else:
						# someone already took it.
						my_task=False
						break
				except WatchError:
					# another client must have changed key between
					# the time we started WATCHing it and the pipeline's execution.
					# our best bet is to just retry.
					continue
		return my_task

	def Done_Task(self, k=""):
		if self.delete_after_done:
			if k=="": k=self.made_key
			self.r.delete(k)



class TaskLock:
	''' Use redis watch to implement atomic lock, only 1 server grabs the task, others wait.
	Use this if no mm.get() needed, and if allow slower server re-run.
	---- Usage:
	if lock.Take_Or_Wait():
		..
		lock.Done_Task()
	Multi server/thread safe, atomic!
	Must have "try, except, finally", and PUT Done_Task in finally!
	Use AccessRestrictionContext instead to avoid manual "try, except, finally".
	No need for Mem, set prefix instead.
	Also see: https://github.com/SPSCommerce/redlock-py, but not using it.'''
	def __init__(self,prefix,host=None,port=_PORT): 
		'''host: IP. CAN NOT be 'localhost' unless single server multi thread! one IP only.''' # tarekc67 '172.22.68.87'
		if host is None: 
			serverlist=get_servers({"highLow":"high","num":Mem_Server_Num})
			host=serverlist[hash(prefix)%(len(serverlist))]
			if iprint: print("TaskLock using host: "+host)
		self.r = redis.StrictRedis(host=host, port=port)
		self.prefix = prefix
		''' just to prevent mem loss:'''
		self.task_timeout=86400*30 

	def _make_key(self, k): 
		return '%s%s%s'%(self.prefix, "T_L", str(k))

	def Take_Or_Wait(self, k):
		self.made_key=self._make_key(k)
		with self.r.pipeline() as pipe:
			while True:
				try:
					pipe.watch(self.made_key)
					current_value = pipe.get(self.made_key)
					if current_value is None: 
						pipe.multi()
						pipe.set(self.made_key, 1)
						pipe.expire(self.made_key, self.task_timeout)
						pipe.execute()
						my_task=True
						break
					else:
						# someone already took it.
						my_task=False
						break
				except WatchError:
					continue
		if my_task: return True # go exec now
		# not my task, wait for Done_Task:
		print("waiting at task lock...")
		while self.r.get(self.made_key) and int(self.r.get(self.made_key))>0:
			time.sleep(0.4)
		return False # still not my task

	def Done_Task(self):
		self.r.delete(self.made_key)
		



