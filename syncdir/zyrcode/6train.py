#!/usr/bin/env python
# train linear regression

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import os, sys
import inspect
from collections import defaultdict
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
from util import py_fname
addpath=mypydir+"/code"
if addpath not in sys.path: sys.path.append(addpath)
from common import * # mm_train, StatsDir, etc.

iprint = 2 
err = ErrorLogger("allerror.txt", tag=py_fname(__file__,False))

max_key = mm_train.get("max_key")
if max_key:
	print("max_key",max_key)
	max_key_turn = mm_train_turn.get("max_key_turn")
	print("max_key_turn",max_key_turn)
carkeyseen=dict()
emailseen=dict()


def train_multi_models():
	''' Use results from code/cluster.py. Run train_linear_regression() first. '''
	if "la" in sys.argv[1:]:
		ChosenModel=linear_model.Lasso
		modelKwargs={"alpha":0.1, "positive":True, "fit_intercept":False, "normalize":True}
	if "lan" in sys.argv[1:]:
		ChosenModel=linear_model.Lasso
		modelKwargs={"alpha":0.1, "positive":False, "fit_intercept":False, "normalize":True} 
	
	model_features = Infocom_features 
	print("features:",model_features)
	MinSpNum=300 
	MaxSpNum=800
	MaxSpDist=0.04
	res=pickle.load(open(StatsDir+"/datares","rb"))
	lb2center=res["lb2center"]
	lb2dist2ind=res["lb2dist2ind"]
	datamean = res["datamean"]
	lb2model={}
	for lb,center in lb2center.items():
		print("")
		indlist=[]
		dist2ind= lb2dist2ind[lb]
		for dist,ind in dist2ind.items(): # ordered 
			if len(indlist)<MinSpNum:
				indlist.append(ind)
			elif dist<MaxSpDist and len(indlist)<MaxSpNum:
				indlist.append(ind)
			else:
				break
		print(lb,"num %d"%len(indlist))
		print(center * datamean)
		x_all=[]
		y_all=[]
		for i in indlist:
			sp=mm_train_valid.get(i)
			y_all.append(sp["gas"])
			tmpx=[]
			for feat in model_features:
				tmpx.append(sp[feat])
			x_all.append(tmpx)
		x_all = np.asarray(x_all)
		y_all = np.asarray(y_all)
		regr = ChosenModel(**modelKwargs)
		regr.fit(x_all, y_all)
		print(regr.coef_)
		lb2model[lb]=regr
	joblib.dump(lb2model, "%s/lb2model"%StatsDir) 



def train_linear_regression(*args, **kwargs):
	train_carkey=kwargs.get("train_carkey",None)
	test_carkey=kwargs.get("test_carkey",None)
	out_suffix = kwargs.get("out_suffix",'')

	UseGlobal_model= 1 if "ugm" in sys.argv else 0 # do not train new model
	if train_carkey is not None and test_carkey is None: Use_train_carkey_also_test=1
	else: Use_train_carkey_also_test=0
	if train_carkey is not None and test_carkey is not None: Use_train_cars_test_cars=1
	else: Use_train_cars_test_cars=0

	print("\n --------- train_linear_regression tlr  ----------\n")
	to_show=1 if My_Platform!="centos" else 0
	if "la" in sys.argv[1:]:
		ChosenModel=linear_model.Lasso
		modelKwargs={"alpha":0.1, "positive":True, "fit_intercept":False, "normalize":True} 
	if "lan" in sys.argv[1:]:
		ChosenModel=linear_model.Lasso
		modelKwargs={"alpha":0.1, "positive":False, "fit_intercept":False, "normalize":True} 

	model_features = VTCPFEM_features#Infocom_features
	Scale_Back_Mass = 0 # old greengps. 
	Divide_Realgas_by_Mass=0 # not using normalized gas, but real gas, scale back by mass. 
	print(model_features, "#Fold", NumFold) # NumFold defined in constants.py
	gather_dic={"run_cnt":0}
	err_dic=defaultdict(dict)
	distSamples,mpgSamples,speedSamples=[],[],[]
	mm_train_cnt,mm_train_valid_cnt=0,0
	car2xlist,car2ylist ={},{}
	x_all,y_all = [],[]
	thiserr,thismodel=1e10,None
	res={}
	train_avgvself,train_avgvleg=[0.0,0],[0.0,0] # speed on seg
	train_avglvself,train_avglvleg=[0.0,0],[0.0,0] # level of seg
	xtraining,xtest,ytraining,ytest=[],[],[],[]
	verbose=1

	for i in   range(mm_train.get("max_key")):
		sp=mm_train.get(i)
		carkey=sp["carkey"]
		gasScale= carkey2scale[carkey]
		email=sp["email"]
		if carkey in G_blackListCars: continue
		if Use_train_carkey_also_test==1: 
			if carkey not in train_carkey: continue
		carkeyseen[carkey]=carkeyseen.get(carkey,0)+1
		if email in emailseen:
			emailseen[email].add(carkey)
		else: emailseen[email]=set([carkey])

		''' --------- filter bad trips '''
		thisMpg=sp["mpg"]
		if thisMpg<FilterMpgLowerThresh or thisMpg>FilterMpgUpperThresh: 
			print(thisMpg,"bad mpg")
			continue
		dist=sp[Kdist]
		if dist/MetersPerMile<FilterMinDist or dist/MetersPerMile>FilterMaxDist:  
			print(dist/MetersPerMile,sp["mfn"],"bad dist miles")
			continue
		spd=dist/sp[Ktime]/Mph2MetersPerSec # mph
		tmpMaxSpd=FilterSpeedUpperThresh
		tmpMinSpd=FilterSpeedLowerThresh
		if spd > tmpMaxSpd or spd < tmpMinSpd:
			print(spd,sp["mfn"],"bad speed mi/h")
			continue

		''' --------- if using mass area air features '''
		mass_multiplier = 1. if Scale_Back_Mass==1 else sp[KMmass]
		sp[KMmdv2]=mass_multiplier * sp[KincSpeed2]
		sp[KMmelev]=mass_multiplier * sp[Kelevation]
		sp[KMav2d ]= -1.*sp[KMarea]*sp[Kv2d]/mass_multiplier # air lift
		sp[KMdragv2d ]= sp[KMarea]*sp[KMair]*sp[Kv2d]/mass_multiplier # air drag
		sp[KMmd ]=mass_multiplier * sp[Kdist]
		sp[KMvd ]= sp[Kvd]
		sp[KMmleft]=mass_multiplier * sp[TstopLeft]
		sp[KMmright]=mass_multiplier * sp[TstopRight]
		sp[KMmstraight]=mass_multiplier * sp[TstopStraight]
		sp[KMmtime]=mass_multiplier * sp[Ktime]
		mass_divider = 1. if Divide_Realgas_by_Mass==0 else sp[KMmass]

		asp= sp["asp"]
		train_avgvself[0]+=asp["v0"] # stats.
		train_avgvself[1]+=1
		train_avgvleg[0]+=asp["v1"]
		train_avgvleg[1]+=1
		train_avglvself[0]+=asp["lv0"]
		train_avglvself[1]+=1
		train_avglvleg[0]+=asp["lv1"]
		train_avglvleg[1]+=1

		for feat in cov_dims:
			if feat == Kdist: continue
			if feat in asp: continue
			asp[feat]=sp[feat]/sp[Kdist]

		if carkey not in car2xlist:
			car2xlist[carkey]=[]
			car2ylist[carkey]=[]

		distSamples.append(dist)
		mpgSamples.append(sp["mpg"])
		speedSamples.append(spd)
		
		if model_features == Feat_Mass_area_air and Scale_Back_Mass==0:
			car2ylist[carkey].append( sp[Krealgas] )
			y_all.append(sp[Krealgas]) 
			res["y_label"]=Krealgas
		else:
			car2ylist[carkey].append( sp[Kgas] ) # normalized 
			res["y_label"]=Kgas
			if Divide_Realgas_by_Mass==1:# div by mass to normalize
				y_all.append(sp[Krealgas]/mass_divider) # 1/mass
			else:
				y_all.append(sp[Kgas]) # use normalized gas.
			if Use_train_cars_test_cars==1:
				if carkey in train_carkey:
					ytraining.append(sp[Kgas])
				if carkey in test_carkey:
					ytest.append(sp[Kgas])
		tmpx=[]
		try:
			for feat in model_features:
				tmpx.append(sp[feat])
		except:
			continue
		car2xlist[carkey].append(tmpx)
		x_all.append(tmpx)
		if Use_train_cars_test_cars==1:
			if carkey in train_carkey:
				xtraining.append(tmpx)
			if carkey in test_carkey:
				xtest.append(tmpx)

		mm_train_cnt+=1
		if mm_train_cnt%200==0: print(sp)
		verbose=0

		''' get valid samples with asp extra feature '''
		sp["asp"]=asp
		mm_train_valid.set(mm_train_valid_cnt,sp)
		mm_train_valid_cnt+=1

	mm_train_valid.set("max_key",mm_train_valid_cnt)
	res["features"]=model_features
	x_all = np.asarray(x_all)
	y_all = np.asarray(y_all)
	if Use_train_cars_test_cars==1:
		xtraining,xtest,ytraining,ytest = np.asarray(xtraining),np.asarray(xtest),np.asarray(ytraining),np.asarray(ytest)
	print("ALL X,Y shapes", x_all.shape, y_all.shape )
	
	res["y_mean"]=np.mean(y_all)
	res["x_mean"]=np.mean(x_all,axis=0)[:]
	train_avglvleg[0]/=train_avglvleg[1]
	train_avglvself[0]/=train_avglvself[1]
	train_avgvleg[0]/=train_avgvleg[1]
	train_avgvself[0]/=train_avgvself[1]
	if to_show: 
		plot_hist_xlist(speedSamples,1)

	for carkey in car2xlist.keys():
		if len(car2xlist[carkey])< 30: 
			print(carkey+" not enough data, skip\n") 
			del car2xlist[carkey]
			del car2ylist[carkey]
			continue
		car2xlist[carkey] = np.asarray(car2xlist[carkey])
		car2ylist[carkey] = np.asarray(car2ylist[carkey])
		print(carkey, car2xlist[carkey].shape, car2ylist[carkey].shape)
	np_distSamples,np_mpgSamples,np_speedSamples=np.asarray(distSamples),np.asarray(mpgSamples),np.asarray(speedSamples)

	verbose=1
	for run in range(3):
		gather_dic["run_cnt"]+=1

		if verbose: print("\n ------------ Fitting all using all -------------\n")
		size=x_all.shape[0]
		if Use_train_cars_test_cars==0:
			pm = np.random.permutation(size)
			x = x_all[pm]
			y = y_all[pm]
			CutLine = int((NumFold-1)*size//NumFold)
			xtraining, xtest = x[:CutLine,:], x[CutLine:,:]
			ytraining, ytest = y[:CutLine], y[CutLine:]
			np_distSamples = np_distSamples[pm]
			np_mpgSamples = np_mpgSamples[pm]
			np_speedSamples = np_speedSamples[pm]
			np_distSamples_t = np_distSamples[CutLine:]
			np_mpgSamples_t = np_mpgSamples[CutLine:]
			np_speedSamples_t = np_speedSamples[CutLine:]
		if verbose: print("train size",xtraining.shape)
		if verbose: print("test size",xtest.shape)

		if UseGlobal_model==0:
			regr = ChosenModel(**modelKwargs)
			regr.fit(xtraining, ytraining)
		else:
			regr = Global_model
		y_pred = regr.predict(xtest)
		if verbose>=1: print('Coefficients:', regr.coef_, "intercept: ", regr.intercept_)
		err=mean_absolute_percentage_error(ytest, y_pred)
		if verbose>=1: print("mean_absolute_percentage_error: %.2f"%err )
		if to_show: 
			print("Error histogram:")
			show_hist(ytest, y_pred, 0.05)
			abserror=np.abs((ytest - y_pred) / ytest)
			dist2err={}
			mpg2err={}
			spd2err={}
			assert len(abserror)==len(np_distSamples_t)
			for i in range(len(abserror)):
				err=abserror[i]
				dist=(np_distSamples_t[i]//10)*10
				mpg=(np_mpgSamples_t[i]//1)
				spd=(np_speedSamples_t[i]//1)
				if dist in dist2err: dist2err[dist].append(err)
				else: dist2err[dist]=[err]
				if mpg in mpg2err: mpg2err[mpg].append(err)
				else: mpg2err[mpg]=[err]
				if spd in spd2err: spd2err[spd].append(err)
				else: spd2err[spd]=[err]
			for k,v in dist2err.items():
				dist2err[k]=np.mean(v)
			for k,v in mpg2err.items():
				mpg2err[k]=np.mean(v)
			for k,v in spd2err.items():
				spd2err[k]=np.mean(v)
			plot_x_y_bar(dist2err.keys(),dist2err.values(),xlabel="Miles",ylabel="Error")
			plot_x_y_bar(mpg2err.keys(),mpg2err.values(),xlabel="MPG",ylabel="Error")
			plot_x_y_bar(spd2err.keys(),spd2err.values(),xlabel="Miles/hour",ylabel="Error")
			to_show=0

		if "all_err" in gather_dic:  
			gather_dic["all_err"]+=err
		else: gather_dic["all_err"]=err
		if err<thiserr or thiserr is None: 
			thiserr=err
			thismodel=regr
			print("Adopt this model",thismodel,"err",thiserr)
			print(thismodel.coef_)

		if verbose: print("\n --------- Fitting individual using indiv data ----------\n")
		car2error={}
		avgerr=[0.0,0.0]
		for carkey in car2xlist.keys():
			if Use_train_cars_test_cars==1: continue
			size=car2xlist[carkey].shape[0]
			pm = np.random.permutation(size)
			x = car2xlist[carkey][pm]
			y = car2ylist[carkey][pm]
			CutLine = int((NumFold-1)*size//NumFold)
			xtraining, xtest = x[:CutLine,:], x[CutLine:,:]
			ytraining, ytest = y[:CutLine], y[CutLine:]
			if verbose: print(carkey,"train size",xtraining.shape)
			regr = ChosenModel(**modelKwargs)
			regr.fit(xtraining, ytraining)
			y_pred = regr.predict(xtest)

			if verbose>=2: print('Coefficients:', regr.coef_, "intercept: ", regr.intercept_)
			err=mean_absolute_percentage_error(ytest, y_pred)
			if verbose: print(" error: %.2f"%err )
			car2error[carkey]=err
			if carkey in gather_dic:
				gather_dic[carkey]+=err
			else: gather_dic[carkey]=err
			avgerr[0]+= err 
			avgerr[-1]+= 1
			err_dic[carkey]['i2i']=err

		if verbose: print("indiv to indiv err:",avgerr[0]/max(1,avgerr[-1]))
		if "indiv_avg_err" in gather_dic:
			gather_dic["indiv_avg_err"]+=avgerr[0]/max(1,avgerr[-1])
		else: gather_dic["indiv_avg_err"]=avgerr[0]/max(1,avgerr[-1])
		verbose=0
		if UseGlobal_model: break
	
	if iprint: print("\n ------------ Using model from all to predict individual -------\n")
	errlist=[]
	for carkey in car2xlist.keys():
		xtest = car2xlist[carkey]
		ytest = car2ylist[carkey]
		y_pred = thismodel.predict(xtest)
		err=mean_absolute_percentage_error(ytest, y_pred)
		print(carkey+" mean_absolute_percentage_error: %.3f"%err )
		errlist.append(err)
		err_dic[carkey]['a2i']=err
	print("all to indiv err: ", sum(errlist)/float(max(1,len(errlist))))
	
	run_cnt=gather_dic["run_cnt"]
	for k,v in gather_dic.items():
		if k=="run_cnt": continue
		gather_dic[k]=v/run_cnt

	if iprint: print("\n ------------ Results -------\n")
	res["model"]=thismodel
	res["min_err"]=thiserr
	res["avg_err"]=gather_dic["all_err"]
	pprint.pprint(gather_dic)

	print("res to pickle:",res)
	print('Coefficients:', res["model"].coef_)
	if UseGlobal_model==0:joblib.dump(res, "%s/model_lr"%StatsDir+ out_suffix) 
	print("%s/model_lr"%StatsDir+ out_suffix, modelKwargs)

	print("mm_train_cnt",mm_train_cnt)
	print("mm_train_valid_cnt",mm_train_valid_cnt)




def analyze_data( ):
	print("\n --------- analyze_data, ana ----------\n")
	distSamples=[]
	mpgSamples=[]
	speedSamples=[]
	mm_analyze_cnt=0
	feat2var={}
	for feat in plot_corr_features: feat2var[feat]=[]
	cov_x=[]
	verbose=1
	for i in range(mm_train.get("max_key")): 
		sp=mm_train.get(i)
		carkey=sp["carkey"]
		gasScale= carkey2scale[carkey]
		if carkey in G_blackListCars: continue
		''' filter bad trips '''
		thisMpg=sp["mpg"]
		if thisMpg<FilterMpgLowerThresh or thisMpg>FilterMpgUpperThresh: 
			print(thisMpg,"bad mpg")
			continue
		dist=sp[Kdist]
		if dist/MetersPerMile<FilterMinDist or dist/MetersPerMile>FilterMaxDist:  
			print(dist/MetersPerMile,sp["mfn"],"bad dist miles")
			continue
		spd=dist/sp[Ktime]/Mph2MetersPerSec
		tmpMaxSpd=FilterSpeedUpperThresh
		tmpMinSpd=FilterSpeedLowerThresh
		if spd > tmpMaxSpd or spd < tmpMinSpd:
			print(spd,sp["mfn"],"bad speed mi/h")
			continue
		asp =sp["asp"]

		distSamples.append(dist)
		mpgSamples.append(sp["mpg"])
		speedSamples.append(spd)
		
		asp["gas"]=sp[Krealgas]*gasScale/sp[Kdist] 
		for feat in plot_corr_features:
			if feat == Kdist: continue
			asp[feat]=sp[feat]/sp[Kdist]
			if sp[feat]>0:
				feat2var[feat].append(asp[feat])

		for feat in ["lv0","v0","lv1","v1"]: # road context 
			if feat not in feat2var: feat2var[feat]=[]
			if asp[feat]>0:
				feat2var[feat].append(asp[feat])

		if asp["v0"]>=0 and asp["v1"]>=0 and asp["lv0"]>=0 and asp["lv1"]>=0:
			asp["carkey"]=sp["carkey"]
			mm_analyze.set(mm_analyze_cnt,asp)
			mm_analyze_cnt+=1
			if mm_analyze_cnt%200==0: 
				print(asp)
			tmpcovx=[]
			for feat in cov_dims:
				tmpcovx.append(asp[feat])
			cov_x.append(tmpcovx)
		verbose=0

	mm_analyze.set("max_key",mm_analyze_cnt)
	print("max_key",mm_analyze_cnt)
	for feat in feat2var.keys():
		feat2var[feat]=np.var(feat2var[feat])
	pickle.dump(feat2var, open("%s/feat2var"%StatsDir,"wb"))
	cov_mat=np.cov(np.asarray(cov_x),rowvar=False)
	pickle.dump(cov_mat, open("%s/cov_mat"%StatsDir,"wb"))
	print("is_invertible ?",is_invertible(cov_mat))

 

if __name__ == "__main__":
	
	arglist=sys.argv[1:]

	if "tm" in arglist:
		train_multi_models()

	if "train_linear_regression" in arglist or "tlr" in arglist: 
		train_linear_regression(train_carkey=G_all_cars) 

	if "ana" in arglist or "analyze_data" in arglist:
		analyze_data()



