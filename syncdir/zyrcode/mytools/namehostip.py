#!/usr/bin/env python

import os, sys
import subprocess
import random
import socket

import inspect
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
sys.path.append(mypydir)

from hostip import host2ip, ip2host, host2userip, tarekc2powerport, ip2tarekc

def get_host_name_short():
	return socket.gethostname().split(".")[0]

def get_my_ip():  
	try:
		return ([l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0])
	except:
		HomeDir = os.path.expanduser("~")
		cmd = "bash "+HomeDir+os.sep+"get_my_ip.sh"
		output = subprocess.check_output(cmd.split())
		return output
		
def get_ip_type():
	my_ip = get_my_ip()
	if my_ip.startswith("9.47."):
		return "ibm"
	elif my_ip.startswith("128.174.") or my_ip.startswith("192.17."):
		return "office"
	elif my_ip.startswith("172.22."):
		return "tarekc"
	return ""

def get_platform():
	import platform
	p=platform.platform().lower()
	if "darwin" in p: return "mac"
	elif "linux" in p and "debian" in p: return "ubuntu"
	elif "linux" in p and "centos" in p: return "centos"
	return ""

def get_namehostip(targettype,who):
	try:
		if who.startswith('tarekc'):
			inputtype='hname'
			name=who.split('.')[0]

		elif who[0].isalpha(): 
			inputtype='tname' 
			name=''
			hip=''
			hip=host2ip[who]
			name=ip2tarekc[hip]

		elif who.startswith('1'): 
			inputtype='ip' 
			name=''
			name=ip2tarekc[who] 

		else:
			print 'namehostip.py: '+who+' ????? '
			return ''
	except:
		print 'namehostip.py EXCEPTION translating '+who+' from '+inputtype+' to '+targettype
	
	if targettype=='hname': 
		if inputtype=='hname': 
			return name
		elif inputtype=='tname':
			if name!='':
				return name
			else:
				if hip!='':
					try:
						name=socket.gethostbyaddr(hip)[0].split('.')[0]
						return name
					except:
						print 'namehostip.py unknown ip: '+hip
						return ''
				else:
					print 'Please add '+who+' to /etc/hosts, hostip.py: host2ip !'
					return ''
		elif inputtype=='ip':# e.g. input '172.22.68.89'
			if name!='':
				return name
			else:
				try:
					name=socket.gethostbyaddr(who)[0].split('.')[0]
					return name
				except:
					print 'namehostip.py unknown ip: '+who
					return ''

	elif targettype=='tname': 
		if inputtype=='hname':
			ip=''
			for k in ip2tarekc.keys():
				if ip2tarekc[k]==name:
					ip= k
					break
			if ip!='':
				try:
					return ip2host[ip]
				except:
					print 'Please add '+ip+' '+who+' to /etc/hosts, hostip.py: ip2host !'
					return ''
			else:
				fullname=name+'.cs.illinois.edu'
				try:
					ip=socket.gethostbyname(fullname)
				except:
					print 'namehostip.py unknown host: '+fullname
					return ''
				try:
					return ip2host[ip]
				except:
					print 'Please add '+ip+' '+fullname+' to /etc/hosts, hostip.py: ip2host !'
					return ''

		elif inputtype=='tname':# e.g. input 't11'
			return who

		elif inputtype=='ip':# e.g. input '172.22.68.89'
			try:
				return ip2host[who]
			except:
				print 'Please add '+who+' to /etc/hosts, hostip.py: ip2host !'
				return ''

	elif targettype=='ip': 
		if inputtype=='hname':
			ip=''
			for k in ip2tarekc.keys():
				if ip2tarekc[k]==name:
					ip= k
					break
			if ip!='':
				return ip
			else:
				fullname=name+'.cs.illinois.edu'
				try:
					ip=socket.gethostbyname(fullname)
					return ip
				except:
					print 'namehostip.py unknown host: '+fullname
					return ''
				
		elif inputtype=='tname':
			if hip!='':
				return hip
			else:
				print 'Please add '+who+' to /etc/hosts, hostip.py: host2ip !'
				return ''

		elif inputtype=='ip':# e.g. input '172.22.68.89'
			return who
	else:
		print 'namehostip.py targettype unknown!'
		return ''
