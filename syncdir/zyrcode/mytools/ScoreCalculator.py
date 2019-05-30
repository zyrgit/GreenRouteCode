#!/usr/bin/env python

import os, sys, getpass
import random, time
import inspect
import math
from math import log, exp
import numpy as np

mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
sys.path.append(mypydir+"/mytools")
sys.path.append(mypydir)
from namehostip import get_my_ip
from readconf import get_conf,get_conf_int
from logger import Logger
from util import err,strip_illegal_char

iprint =1
configfile = "conf.txt"

class ScoreCalculator:

	def __init__(self,params={}): 
		self.sigmoida= params["sigmoida"] if "sigmoida" in params.keys() else 0
		self.sigmoidx= params["sigmoidx"] if "sigmoidx" in params.keys() else 1
		self.sigmoidy= params["sigmoidy"] if "sigmoidy" in params.keys() else 0.8
		self.sig = Sigmoid(self.sigmoida,self.sigmoidx,self.sigmoidy)

		self.weight= params["weight"] if "weight" in params.keys() else 1.0
	

	def getscore(self,x):
		return self.weight * self.sig.y(x)





class Sigmoid:
	def __init__(self,midx,x,y): 
		# 1/(x-midx)* ln(y/(1-y)) = k
		# y = 1/(1+ exp-k(x-a))
		if y>1: y=1 - 1e-10
		if y<0: y= 1e-10
		self.a = midx
		self.k = 1.0/(x-midx) * log(y/(1.0-y))

	def y(self,x):
		return 1.0/(1.0 + exp(-self.k*(x-self.a)) )

	def demo(self, width=10):
		for x in np.arange(self.a-width,self.a+width,width/100.0):
			y=self.y(x)
			print("%.3f  %.3f"%(x,y))


if __name__ == "__main__":
	sig = Sigmoid(1000,1,0.01)
	{"sigmoida":1000, "sigmoidx":1, "sigmoidy":0.0000001}

	sig.demo(1000)
