#!/usr/bin/env python

import os, sys, getpass
import random, time
import inspect, glob
from logger import ErrorLogger

iprint =2
err = ErrorLogger("allerror.txt", __file__)
Global_max_sleep= 86400 

class ErrorShouldDelay(Exception):
	"""Represents an Exception from delay executor"""
	def __init__(self, message=None):
		self.message = "" if message is None else message
	def __str__(self):
		return "[ ErrorShouldDelay ] %s" % (self.message)

class DelayRetryExecutor:
	def __init__(self,params): 
		self.init_sleep = params["init_sleep"] if "init_sleep" in params else 0.005
		self.sleep_multiply = params["sleep_multiply"] if "sleep_multiply" in params else 1.6
		self.sleep_shrink = params["sleep_shrink"] if "sleep_shrink" in params else 0.97
		self.shrink_faster = params["shrink_faster"] if "shrink_faster" in params else 0.8
		assert self.sleep_multiply>1 and self.sleep_shrink<1 and self.shrink_faster<1

		self.sleep_thresh = params["sleep_thresh"] if "sleep_thresh" in params else self.sleep_multiply*self.init_sleep
		self.max_sleep = params["max_sleep"] if "max_sleep" in params else 1+ 120.0
		assert self.max_sleep<=Global_max_sleep, 'Err config max_sleep'
		self.name = params["name"] if "name" in params else __file__
		self.quitUponMaxTime = params["quitUponMaxTime"] if "quitUponMaxTime" in params else False
		self.sleep_sec = self.init_sleep
		self.retryAllException = params["retryAllException"] if "retryAllException" in params else False # beside my ErrorShouldDelay, any error won't give up.

	def execute(self, func, *args, **kwargs):
		retry=True
		success=False
		ret=None
		while retry and not success:
			try:
				if self.sleep_sec >= self.sleep_thresh:
					if self.sleep_sec>=2*3600:
						print('... next exec time:')
						print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()+self.sleep_sec)))
					time.sleep(self.sleep_sec)
				ret = func(*args,**kwargs)
				success=True
				''' Try to decrease sleep '''
				if self.sleep_sec >= self.sleep_thresh:
					self.sleep_sec *= self.sleep_shrink
				''' if too long, dec faster '''
				if self.sleep_sec >= max(86400/6, self.sleep_thresh):
					self.sleep_sec *= 0.2  # 4 hours? down to 1 hour sleep.
				elif self.sleep_sec >= max(30, self.sleep_thresh):
					self.sleep_sec *= self.shrink_faster

			except ErrorShouldDelay: # same as below retryAllException 
				if self.sleep_sec < Global_max_sleep:
					self.sleep_sec= self.sleep_sec*self.sleep_multiply
					if iprint: 
						print("sleep_sec increased to", self.sleep_sec)
				if self.sleep_sec>self.max_sleep:
					if iprint: print("sleep_sec reached max_sleep")
					if self.quitUponMaxTime: 
						retry=False
						print("Quit...")
						err.lg_list([self.name, "sleep_sec reached max_sleep, quit"])
					else: print("Retry later...")

			except (KeyboardInterrupt, SystemExit):
				retry=False
				if iprint: print("[ KeyboardInterrupt ] executor")
				err.lg_list([self.name, "[ KeyboardInterrupt ]"])
				raise Exception("KeyboardInterrupt in executor.py")

			except:
				if not self.retryAllException:
					retry=False
					if iprint: print("[ Unknown Exception ] executor")
					err.lg_list([self.name, "[ Unknown Exception ]"])
					raise Exception("Unknown Exception in executor.py")
				else: # same as above ErrorShouldDelay 
					if self.sleep_sec < Global_max_sleep:
						self.sleep_sec= self.sleep_sec*self.sleep_multiply
						if iprint: 
							print("retryAllException sleep_sec increased to", self.sleep_sec)
					if self.sleep_sec>self.max_sleep:
						if iprint: print("retryAllException sleep_sec reached max_sleep")
						if self.quitUponMaxTime: 
							retry=False
							print("retryAllException Quit...")
							err.lg_list([self.name, "retryAllException sleep_sec reached max_sleep, quit"])
						else: print("retryAllException Retry later...")
		return ret




''' -------------- Timed Execution -----------------'''

import signal
import functools
from contextlib import contextmanager

class TimeoutError(Exception):
	def __init__(self, value = "Timed Out"):
		self.value = value
	def __str__(self):
		return repr(self.value)

def signal_handler(signum, frame):
	raise TimeoutError()

''' ---- various methods: ----'''

def timeout(func):
	''' 1. decorator. Only on Unix. Must use Try-except. See EG_timeout'''
	@functools.wraps(func)
	def wrapped_f(*args, **kwargs):
		if "timeout" in kwargs: 
			timeout=kwargs["timeout"]
		signal.signal(signal.SIGALRM, signal_handler)
		signal.alarm(timeout)
		result=None
		try:
			result = func(*args, **kwargs)
		finally:
			signal.alarm(0)
		return result
	return wrapped_f


@contextmanager
def time_limit(seconds, msg=''):
	''' 2. with statement. Only on Unix. No need Try-except. See EG_time_limit '''
	signal.signal(signal.SIGALRM, signal_handler)
	signal.alarm(seconds)
	try:
		yield # an obj for "with time_limit() as obj:"
	except TimeoutError as e: 
		if msg: print(msg)
	finally:
		signal.alarm(0)


def limited_time_func(timeout, func, *args, **kwargs):
	""" 3. Run func with the given timeout. If func didn't finish running within the timeout, raise error. See EG_limited_time_func
	- If your func might run forever, Do NOT use this method !!!!!
	- Must use Try-except. func() should take extra argv to use "msg". 
	"""
	import threading
	class FuncThread(threading.Thread):
		def __init__(self):
			threading.Thread.__init__(self)
			self.result = None
		def run(self):
			self.result = func(*args, **kwargs)
		def _stop(self):
			if self.isAlive():
				threading.Thread._Thread__stop(self)

	it = FuncThread()
	it.start()
	it.join(timeout)
	if it.isAlive():
		it._stop()#All it does is set the internal Thread.__stopped flag that allows join to return earlier. You can't kill threads in Python
		raise TimeoutError(kwargs.get("msg",""))
	else:
		return it.result
		

if __name__ == "__main__":
	if 0:
		exe=DelayRetryExecutor({})
		def myprint(st,lst):
			try:
				print(st,lst, foo)
			except:
				raise ErrorShouldDelay("bar")
		exe.execute(myprint, "st", ["lst","lol"])
	
	def test():
		for i in range(1,10):
			time.sleep(1)
			print( "mytest() message:  %d seconds have passed" % i )
		return 0

	if 0: # EG_timeout
		@timeout
		def mytest(*args, **kwargs):
			print( "mytest() message:  Started" )
			return test()
		try:
			ret = mytest(timeout=2)
		except TimeoutError as e:
			print("stopped executing mytest() because "+ str(e))
	
	if 0:#EG_time_limit
		with time_limit(2, "EG_time_limit"):
			ret = test()

	if 0: #EG_limited_time_func
		try:
			ret = limited_time_func(2, test, )#cannot add msg="extra", test() takes no argv.
		except TimeoutError as e:
			print("stopped executing mytest() because "+ str(e))
