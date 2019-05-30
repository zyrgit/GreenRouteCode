#!/usr/bin/env python
import os,subprocess
from constants import *

iprint=0



def divide_by_col_means(data, col_means=None, exclude_cols=[], minus_sign={}):
	if isinstance(data, list):
		for i in range(len(data)):
			break
	else:
		'''pd.DataFrame, did not import pandas'''
		if col_means is None: 
			col_means= data.mean(axis=0, numeric_only=True)
		col_names= col_means.index
		for col in col_names:
			if col in exclude_cols: continue
			sign=1.0
			if col in minus_sign:
				sign = minus_sign[col]
			data.loc[:,col] = data.loc[:,col]/col_means[col]*sign
		


def nodes_overlap_ratio(nlist1,nlist2):
	if len(nlist2)>len(nlist1):
		lbig=nlist2
		lsmall=nlist1
	else:
		lbig=nlist1
		lsmall=nlist2
	cnt=0
	for n in lbig:
		if n in lsmall: cnt+=1
	return float(cnt)/max(1, len(lbig))
	

def minEditDistance(s1, s2):
	if len(s1) > len(s2):
		s1, s2 = s2, s1
	distances = range(len(s1) + 1)
	for i2, c2 in enumerate(s2):
		distances_ = [i2+1]
		for i1, c1 in enumerate(s1):
			if c1 == c2:
				distances_.append(distances[i1])
			else:
				distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
		distances = distances_
	return distances[-1]



def rm_html_dir_contents():
	DirHTML="html"
	HtmlPath= "./%s/"%DirHTML
	if not os.path.exists(HtmlPath):
		HtmlPath="."+HtmlPath
	if os.path.exists(HtmlPath): 
		subprocess.call("rm "+HtmlPath+"/*",shell=True) # clear html output.


def get_car_metadata(CUT="~|"):
	'''./stats/car_meta.txt: Make Model Year Class Mass FrontalArea DragCoefficient CityMPG HighwayMPG'''
	car2meta={}
	avgMass=0.0
	avgArea=0.0
	avgDrag=0.0
	tmpStatsDir="./stats"
	if not os.path.exists("%s/car_meta.txt"%tmpStatsDir):
		tmpStatsDir="."+tmpStatsDir
	with open("%s/car_meta.txt"%tmpStatsDir,"r") as f:
		for l in f:
			st=l.split(" ")
			stt=[]
			for e in st:
				if len(e)>0:
					stt.append(e)
			carkey=stt[0]+CUT+stt[1]+CUT+stt[2]
			if carkey not in car2meta: 
				car2meta[carkey]=[]
				car2meta[carkey].append(float(stt[4]))
				car2meta[carkey].append(float(stt[5]))
				car2meta[carkey].append(float(stt[6]))
				avgMass+=car2meta[carkey][indMass]
				avgArea+=car2meta[carkey][indArea]
				avgDrag+=car2meta[carkey][indDrag]
	avgMass/=len(car2meta)
	avgArea/=len(car2meta)
	avgDrag/=len(car2meta)
	if iprint: print("avg:",avgMass,avgArea,avgDrag)
	car2meta["~|~|"]=[avgMass,avgArea,avgDrag]
	car2meta["Volkswagen~|~|"]=car2meta["Volkswagen~|CC~|2015"][:]
	car2meta["Volkswagen~|CC~|"]=car2meta["Volkswagen~|CC~|2015"][:]
	return car2meta

def get_fuzzy_value_given_key(dic,key):# match carkey in car2meta
	if key in dic: return dic[key]
	return dic[match_dic_best_keys(dic, key)]

def match_dic_best_keys(dic, key):# match carkey in car2meta if not exists. 
	maxmatch=''
	maxl=0
	for k in dic.keys():
		i=0
		while i< min(len(key),len(k)):
			if key[i]==k[i]: i+=1
			else: break
		if i>maxl: 
			maxl=i
			maxmatch=k
	return maxmatch
