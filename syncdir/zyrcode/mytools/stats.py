#!/usr/bin/env python

import os, sys, getpass
import random, time
import subprocess
import inspect, glob
import numpy as np
import scipy
from scipy.stats import t
from numpy import average, std
from math import sqrt
from collections import OrderedDict
import matplotlib
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
if mypydir not in sys.path: sys.path.append(mypydir)

iprint =2


def plot_x_y_bar(x,y,xlabel="x",ylabel="y",width=None,show_hline=0):
	''' x is binned vals, y is vals '''
	import matplotlib.pyplot as plt
	if width is None:
		sx=sorted(x)
		width=max((sx[1]-sx[0])/2.0, (sx[-1]-sx[0])/500.0)
	plt.bar(x,y,align='center',width=width) # A bar chart
	plt.xlabel(xlabel)
	plt.ylabel(ylabel)
	if show_hline:
		for i in range(len(y)):
			plt.hlines(y[i],0,x[i]) # Here you are drawing the horizontal lines
	plt.show()


def plot_x_y_points(x,y):
	''' x is list of vals, y is list of vals '''
	import matplotlib.pyplot as plt
	plt.scatter(x, y)
	plt.show()


def plot_x_y_group_scatter(xdic,ydic):
	''' x = {"group":[vals] }, y same dict '''
	import matplotlib.pyplot as plt
	groups= xdic.keys()
	fig, ax = plt.subplots()
	for group in groups:
		ax.plot(xdic[group], ydic[group], marker='.', markersize=6, linestyle='', label=group)
	ax.legend()
	plt.show()


def to_percent(y, position):
    # Ignore the passed in position. This has the effect of scaling the default
    # tick locations.
    s = "%.1f"%(100 * y)
    # The percent symbol needs escaping in latex
    if matplotlib.rcParams['text.usetex'] is True:
        return s + r'$\%$'
    else:
        return s + '%'


class PlotDistributionBins:
	def __init__(self,params={}): 
		import matplotlib.pyplot as plt
		self.plt=plt

	def show_hist(self,x,bin_granularity=0.05,y_percentage=True,align="left", minVal=None, maxVal=None, xlabel='', ylabel=''):#left,mid,right
		from matplotlib.ticker import FuncFormatter
		minv = min(x)
		if minVal: minv = max(minv, minVal)
		maxv = max(x)
		if maxVal: maxv = min(maxv, maxVal)
		binmin= minv //bin_granularity*bin_granularity
		binmax= maxv //bin_granularity*bin_granularity+2*bin_granularity # exclusive ]
		font = {'family' : 'normal', 'size' :19}
		matplotlib.rc('font', **font)
		self.plt.xlabel(xlabel)
		self.plt.ylabel(ylabel)
		if y_percentage:
			weights = np.ones_like(x)/float(len(x))
			self.plt.hist(x,bins=np.arange(binmin,binmax,bin_granularity),weights=weights,rwidth=0.5,align=align)
			formatter = FuncFormatter(to_percent)
			self.plt.gca().yaxis.set_major_formatter(formatter)
		else:
			self.plt.hist(x, bins=np.arange(binmin,binmax,bin_granularity), rwidth=0.5,align=align)
		self.plt.tight_layout()  # otherwise the right y-label is slightly clipped
		self.plt.show()

	def show_hist_2_styles(self,x_multi,n_bins=None,bin_granularity=None,align="mid",colors=[],labels=[],binmin=None,binmax=None):# two hist, one stacked, one separate alongside.
		if binmin is not None and bin_granularity is not None:
			n_bins=np.arange(binmin,binmax,bin_granularity)
		fig, axes = self.plt.subplots(nrows=1, ncols=2)
		ax0, ax1 = axes.flatten()
		ax0.hist(x_multi, n_bins, normed=0, histtype='bar', stacked=True,rwidth=0.5,color=colors, label=labels)
		# ax0.set_title('ax0')
		ax0.legend(prop={'size': 8})
		# self.plt.hist(x, bins=np.arange(binmin,binmax,bin_granularity), rwidth=0.5,align=align)
		ax1.hist(x_multi, n_bins, histtype='bar',color=colors, label=labels)
		# ax1.set_title('ax1')
		ax1.legend(prop={'size': 8})
		fig.tight_layout()
		self.plt.show()


class TDistribution:
	def __init__(self,params={}): 
		self.name = params["name"] if "name" in params else ""

	def num_std_err_given_confidence_df(self,confidence,df):
		''' For df data size, given confidence ratio, what is stderr around mean?'''
		coverage=confidence
		# t_bounds = t.interval(coverage, df - 1)
		# print t.ppf((1-coverage)/2.0, df-1)
		z=t.ppf((1+coverage)/2.0, df-1)
		return z

	def build_std_err_to_confidence_table(self,df,cd_low=0.01,cd_high=0.99,cd_granularity=0.01):
		''' For df data size, given confidence range, mapping many stderr to probability'''
		self.std_err_to_confidence=OrderedDict()
		confidence=cd_high
		while confidence>=cd_low:
			z=self.num_std_err_given_confidence_df(confidence,df)
			self.std_err_to_confidence[z]=confidence
			confidence-=cd_granularity
		if iprint>=3: print(self.std_err_to_confidence)

	def get_confidence_given_interval_stddev(self,intv,stddev,num):
		''' For num data size, given intv and std deviation, what is prob of mean staying in intv?'''
		if stddev<=0: return 1.0
		stderr=stddev/sqrt(num)
		z=intv/2.0/stderr
		for k,v in self.std_err_to_confidence.items():# dec z to prob
			if z>=k:
				return v
		return 0.0

	def get_confidence_of_mean(self,data,dev_ratio):
		''' Given data, what is the prob of mean staying within #dev_ratio of deviation?'''
		mean = average(data)
		stddev = std(data, ddof=1)
		self.build_std_err_to_confidence_table(len(data))
		allow_dev = mean*dev_ratio
		confidence=self.get_confidence_given_interval_stddev(allow_dev,stddev,len(data))
		return confidence


if __name__ == "__main__":

	if 0:
		bin=PlotDistributionBins()
		bin.show_hist([0.0, 1.0, 8.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],bin_granularity=2)

	if 0:
		da=[1,2,3,4,5,6,7,8,9,1,2,3,4,5,6,7,8,9,1,2,3,4,5,6,7,8,9,4,5,4,5,4,5,4,5,4,5]
		tmpd=TDistribution()
		print(tmpd.get_confidence_of_mean(da,0.1))





