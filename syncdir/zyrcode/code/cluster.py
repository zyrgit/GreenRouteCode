#!/usr/bin/env python

import os, sys, getpass
import subprocess
import random, time
import inspect, glob
import collections
import math
from sklearn.cluster import KMeans
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import cPickle as pickle
from pprint import pprint
import collections
if os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))+"/../mytools" not in sys.path: sys.path.append(os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))+"/../mytools")
from mem import Mem
from constants import *
if os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))+"/../" not in sys.path: sys.path.append(os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))+"/../")
from common import mm_train_valid,StatsDir,cov_dims


def load_vectors():
	data=[]
	print("max_key",mm_train_valid.get("max_key"))
	for i in range(mm_train_valid.get("max_key")): 
		sp=mm_train_valid.get(i)
		dist=sp[Kdist]
		asp=sp["asp"]
		asp[Ktime]=sp[Ktime]/dist
		asp[Kv2d]=sp[Kv2d]/dist
		asp[Kvd]=sp[Kvd]/dist
		x=[]
		for feat in cov_dims:
			x.append(asp[feat])
		data.append(x)
	damean=np.mean(np.asarray(data),axis=0)[:]
	pickle.dump(damean, open("../"+StatsDir+"/datamean","wb"))
	print("cov_dims mean:",damean)
	data=[]
	for i in range(mm_train_valid.get("max_key")):
		sp=mm_train_valid.get(i)
		dist=sp[Kdist]
		asp=sp["asp"]
		asp[Ktime]=sp[Ktime]/dist
		asp[Kv2d]=sp[Kv2d]/dist
		asp[Kvd]=sp[Kvd]/dist
		x=[]
		for feat in cov_dims:
			x.append(asp[feat]/damean[cov_dims.index(feat)])
		data.append(x)
	pickle.dump(data, open("../"+StatsDir+"/datavectors","wb"))
	print("[ load_vectors ] done!")


def cluster(k=6):
	ind_vec = pickle.load(open("../"+StatsDir+"/datavectors","rb"))
	print("data len",len(ind_vec))
	x=np.asarray(ind_vec)
	kmeans = KMeans(n_clusters=k,random_state=0,n_jobs=-1)
	kmeans.fit(x)
	labels = kmeans.labels_
	centroids = kmeans.cluster_centers_
	print("centroids",centroids)
	k2score=[[] for i in range(k)]
	for i in range(len(ind_vec)):
		wd=i
		lb=labels[i]
		sc = cosine_similarity(x[i].reshape(1, -1),centroids[lb].reshape(1, -1))
		k2score[lb].append([sc[0][0],wd])
	pickle.dump(k2score, open("../"+StatsDir+"/datak2score","wb"))
	pickle.dump(centroids.tolist(), open("../"+StatsDir+"/datacentroids","wb"))
	print("[ cluster ] done!")
	

def k_to_mm_train_index_sorted_list():
	centroids = pickle.load(open("../"+StatsDir+"/datacentroids","rb"))
	damean=pickle.load( open("../"+StatsDir+"/datamean","rb"))
	res={}
	res["lb2center"]={}
	res["lb2dist2ind"]={}
	res["datamean"]=damean
	print(cov_dims)
	for lb in range(len(centroids)):
		center=centroids[lb]
		res["lb2center"][lb]=center
		print(lb,center)
		score2ind={}
		center=np.asarray(center)
		for i in range(mm_train_valid.get("max_key")): 
			sp=mm_train_valid.get(i)
			dist=sp[Kdist]
			asp=sp["asp"]
			asp[Ktime]=sp[Ktime]/dist
			asp[Kv2d]=sp[Kv2d]/dist
			asp[Kvd]=sp[Kvd]/dist
			x=[]
			for feat in cov_dims:
				x.append(asp[feat]/damean[cov_dims.index(feat)])
			x=np.asarray(x)
			sc = cosine_similarity(x.reshape(1, -1), center.reshape(1, -1))[0][0]
			score2ind[1.0-sc]=i
		res["lb2dist2ind"][lb]=collections.OrderedDict(sorted(score2ind.items()))
	pickle.dump(res, open("../"+StatsDir+"/datares","wb"))
	print("[ k_to_mm_train_index_sorted_list ] done!")


if __name__ == "__main__":
	arglist=sys.argv[1:]

	if "load_vectors" in arglist: load_vectors()
	if "cluster" in arglist: cluster()
	if "k_to_mm_train_index_sorted_list" in arglist: k_to_mm_train_index_sorted_list()
