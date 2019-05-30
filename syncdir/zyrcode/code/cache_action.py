#!/usr/bin/env python
import os,subprocess


def action_if_get_none(meta, key, *args , **kwargs):
	meta_file_name = meta["meta_file_name"]

	if meta_file_name.endswith('nid-to-elevation.txt'):
		if 'yield_args' in meta and meta['yield_args']:
			fn = meta['yield_args'].split("~|")[0]
		elif 'gen_cache_file_cmd' in meta and meta['gen_cache_file_cmd']:
			fn = meta['gen_cache_file_cmd'].split('gen_cache_file')[-1].split(' ')[0]
		if len(fn.strip())<1: 
			print(meta)
			print('[ cache_action ] empty fn='+fn)
			return None
		cmd = 'grep '+str(key)+' '+fn
		print('[ action_if_get_none ] '+cmd)
		result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		out, err = result.communicate()
		print("Got: "+ out)
		try:
			val = float(out.split(' ')[-1])
		except: val=None
		return val

	return None