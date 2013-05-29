import util
import pymongo
import requests
con = pymongo.Connection()
hdds = con.abhiabhi.scraped_harddisks
tablets = con.abhiabhi.scraped_tablets
laptops = con.abhiabhi.scraped_laptops
pds = con.abhiabhi.scraped_pendrives
mobiles = con.abhiabhi.scraped_mobiles
cameras = con.abhiabhi.scraped_cameras

docs = hdds.find()
for doc in docs:
    if doc['brand']=='wd':
	hdds.update({'_id' : doc['_id']},{'$set':{'brand':'western digital'}},safe = True)
    if doc['brand']=='adata':
	hdds.update({'_id' : doc['_id']},{'$set':{'brand':'a-data'}},safe = True)
	

docs = tablets.find()
for doc in docs:
    if doc['brand']=='bsnl':
	tablets.update({'_id' : doc['_id']},{'$set':{'brand':'pantel'}},safe = True)

tablets.update({'brand':'pental'},{'$set':{'brand':'pantel'}},multi = True)
tablets.update({'brand':'zync pad'},{'$set':{'brand':'zync'}},multi = True)
tablets.update({'brand':'asus eee pad transformer'},{'$set':{'brand':'asus'}},multi = True)
tablets.update({'brand':'domo slate'},{'$set':{'brand':'domo'}},multi = True)
tablets.update({'brand':'acer  iconia'},{'$set':{'brand':'acer'}},multi = True)
mobiles.update({'brand':'sony-ericsson'},{'$set':{'brand':'sony ericsson'}},multi = True)
mobiles.update({'brand':'xperia'},{'$set':{'brand':'sony ericsson'}},multi = True)
laptops.update({'brand':'sony vaio'},{'$set':{'brand':'sony'}},multi = True)

docs = tablets.find()
for doc in docs:
    if doc['brand']=='pental':
	tablets.update({'_id' : doc['_id']},{'$set':{'brand':'pantech'}},safe = True)


docs = pds.find()
for doc in docs:
    if doc['brand']=='adata':
	brand='a-data'
    if doc['brand']=='moser':
	brand='moser baer'
	pds.update({'_id' : doc['_id']},{'$set':{'brand' : brand}},safe = True)

import re
brand_pattern = re.compile('(.*?)\(\d+\)')
docs = tablets.find()
for doc in docs:
    m = brand_pattern.search(doc['brand'])
    if m:
	brand = m.group(1).strip()
	tablets.update({'_id' : doc['_id']},{'$set':{'brand' : brand}},safe = True)
	
docs = laptops.find()
for doc in docs:
    m = brand_pattern.search(doc['brand'])
    if m:
	brand = m.group(1).strip()
	laptops.update({'_id' : doc['_id']},{'$set':{'brand' : brand}},safe = True)
	
docs = pds.find()
for doc in docs:
    m = brand_pattern.search(doc['brand'])
    if m:
	brand = m.group(1).strip()
	pds.update({'_id' : doc['_id']},{'$set':{'brand' : brand}},safe = True)
    
docs = hdds.find()
for doc in docs:
    m = brand_pattern.search(doc['brand'])
    if m:
	brand = m.group(1).strip()
	hdds.update({'_id' : doc['_id']},{'$set':{'brand' : brand}},safe = True)
	
docs = mobiles.find()
for doc in docs:
    if re.match('sony-?err?icsson',doc['name'],re.I):
        mobiles.update({'_id' : doc['_id']},{'$set':{'brand':'sony ericsson'}},safe = True)	

	
docs = hdds.find()
for doc in docs:
    brand = doc['brand'].lower()
    hdds.update({'_id' : doc['_id']},{'$set':{'brand' : brand}},safe = True)

docs = tablets.find()
for doc in docs:
    brand = doc['brand'].lower()
    tablets.update({'_id' : doc['_id']},{'$set':{'brand' : brand}},safe = True)
    
docs = laptops.find()
for doc in docs:
    brand = doc['brand'].lower()
    laptops.update({'_id' : doc['_id']},{'$set':{'brand' : brand}},safe = True)

docs = mobiles.find()
for doc in docs:
    brand = doc['brand'].lower()
    mobiles.update({'_id' : doc['_id']},{'$set':{'brand' : brand}},safe = True)

docs = cameras.find()
for doc in docs:
    if 'brand' in doc:
	    brand = doc['brand'].lower()
	    cameras.update({'_id' : doc['_id']},{'$set':{'brand' : brand}},safe = True)
    
docs = pds.find()
for doc in docs:
    brand = doc['brand'].lower()
    pds.update({'_id' : doc['_id']},{'$set':{'brand' : brand}},safe = True)
    
docs = tablets.find()
for doc in docs:
    ph=[doc['product_history'],]
    tablets.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
    
docs = mobiles.find()
for doc in docs:
    ph=[doc['product_history'],]
    mobiles.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
    
docs = cameras.find()
for doc in docs:
    ph=[doc['product_history'],]
    cameras.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
    
docs = laptops.find()
for doc in docs:
    ph=[doc['product_history'],]
    laptops.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
    
docs = pds.find()
for doc in docs:
    ph=[doc['product_history'],]
    pds.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
    
docs = hdds.find()
for doc in docs:
    ph=[doc['product_history'],]
    hdds.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)

docs = hdds.find({'site':'flipkart'})
for doc in docs:
    if 'specification' in doc:
        specification={}
        for key in doc['specification']:
            specification[key.replace(u'\xa0','').strip(': ')]=doc['specification'][key]
            hdds.update({'_id' : doc['_id']},{'$set':{'specification' : specification}},safe = True)

docs = pds.find({'site':'flipkart'})
for doc in docs:
    if 'specification' in doc:
        specification={}
        for key in doc['specification']:
            specification[key.replace(u'\xa0','').strip(': ')]=doc['specification'][key]
            pds.update({'_id' : doc['_id']},{'$set':{'specification' : specification}},safe = True)

docs = hdds.find({'site':'flipkart'})
for doc in docs:
    if 'specification' in doc:
        if 'connectivity' in doc['specification']:
            interface = doc['specification']['connectivity']
            hdds.update({'_id' : doc['_id']},{'$set':{'specification.interface' : interface},'$unset':{'specification.connectivity' : True}},safe = True)

docs = cameras.find({'site':'flipkart'})
for doc in docs:
    ph = doc['product_history'][0]
    cameras.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)

docs = cameras.find({'site':'indiaplaza'})
for doc in docs:
    ph = doc['product_history'][0]
    while(True):
        if type(ph)==dict:
            ph=[ph,]
            cameras.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
            break
        ph = ph[0]

docs = mobiles.find({'site':{'$in':['flipkart','indiaplaza']}})
for doc in docs:
    ph = doc['product_history'][0]
    while(True):
        if type(ph)==dict:
            ph=[ph,]
            mobiles.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
            break
        ph = ph[0]

docs = laptops.find({'site':'flipkart'})
for doc in docs:
    ph = doc['product_history'][0]
    while(True):
        if type(ph)==dict:
            ph=[ph,]
            laptops.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
            break
        ph = ph[0]
	
docs = laptops.find({'site':'homeshop18'})
for doc in docs:
    ph = doc['product_history'][0]
    while(True):
        if type(ph)==dict:
            ph=[ph,]
            laptops.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
            break
        ph = ph[0]
	
docs = laptops.find({'site':'saholic'})
for doc in docs:
    ph = doc['product_history'][0]
    while(True):
        if type(ph)==dict:
            ph=[ph,]
            laptops.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
            break
        ph = ph[0]
	
docs = tablets.find({'site':'homeshop18'})
for doc in docs:
    ph = doc['product_history'][0]
    while(True):
        if type(ph)==dict:
            ph=[ph,]
            tablets.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
            break
        ph = ph[0]
	
docs = tablets.find({'site':'saholic'})
for doc in docs:
    ph = doc['product_history'][0]
    while(True):
        if type(ph)==dict:
            ph=[ph,]
            tablets.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
            break
        ph = ph[0]
	
docs = tablets.find({'site':'flipkart'})
for doc in docs:
    ph = doc['product_history'][0]
    while(True):
        if type(ph)==dict:
            ph=[ph,]
            tablets.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
            break
        ph = ph[0]

docs = tablets.find({'site':'buytheprice'})
for doc in docs:
    if 'specification' in doc:
        if 'internal memory' in doc['specification']:
            storage = doc['specification']['internal memory']
            tablets.update({'_id' : doc['_id']},{'$set':{'specification.storage' : storage},'$unset':{'specification.internal memory' : True}},safe = True)
	    
	if 'operating system version' in doc['specification']:
            os = doc['specification']['operating system version']
            tablets.update({'_id' : doc['_id']},{'$set':{'specification.os' : os},'$unset':{'specification.operating system version' : True}},safe = True)
	    
docs = tablets.find({'site':'saholic'})
for doc in docs:
    if 'specification' in doc:
        if 'built-in' in doc['specification']:
            storage = doc['specification']['built-in']
            tablets.update({'_id' : doc['_id']},{'$set':{'specification.storage' : storage},'$unset':{'specification.built-in' : True}},safe = True)

	    
docs = tablets.find({'site':'homeshop18'})
for doc in docs:
    if 'specification' in doc:
	
	if 'operating system' in doc['specification']:
            os = doc['specification']['operating system']
            tablets.update({'_id' : doc['_id']},{'$set':{'specification.os' : os},'$unset':{'specification.operating system' : True}},safe = True)
	    
        if 'internal memory' in doc['specification']:
            s = doc['specification']['internal memory']
            tablets.update({'_id' : doc['_id']},{'$set':{'specification.storage' : s},'$unset':{'specification.internal memory' : True}},safe = True)
	    
	if 'internal storage' in doc['specification']:
            s = doc['specification']['internal storage']
            tablets.update({'_id' : doc['_id']},{'$set':{'specification.storage' : s},'$unset':{'specification.internal storage' : True}},safe = True)
	    
	if 'memory size' in doc['specification']:
            s = doc['specification']['memory size']
            tablets.update({'_id' : doc['_id']},{'$set':{'specification.storage' : s},'$unset':{'specification.memory size' : True}},safe = True)
	
	if 'rom' in doc['specification']:
            s = doc['specification']['rom']
            tablets.update({'_id' : doc['_id']},{'$set':{'specification.storage' : s},'$unset':{'specification.rom' : True}},safe = True)
	
	if 'rom (storage)' in doc['specification']:
            s = doc['specification']['rom (storage)']
            tablets.update({'_id' : doc['_id']},{'$set':{'specification.storage' : s},'$unset':{'specification.rom (storage)' : True}},safe = True)
	    

docs = tablets.find({'site':'flipkart'})
for doc in docs:
    if 'specification' in doc:
        if 'internal storage' in doc['specification']:
            s = doc['specification']['internal storage']
	    tablets.update({'_id' : doc['_id']},{'$set':{'specification.storage' : s},'$unset':{'specification.internal storage' : True}},safe = True)
	    
	if 'operating system' in doc['specification']:
            s = doc['specification']['operating system']
	    tablets.update({'_id' : doc['_id']},{'$set':{'specification.os' : s},'$unset':{'specification.operating system' : True}},safe = True)

docs = tablets.find({'site':'buytheprice'})
for doc in docs:
    if 'specification' in doc:
	if 'operating system' in doc['specification']:
            s = doc['specification']['operating system']
	    tablets.update({'_id' : doc['_id']},{'$set':{'specification.os' : s},'$unset':{'specification.operating system' : True}},safe = True)
	    
docs = tablets.find({'site':'indiaplaza'})
g_pattern = re.compile('\W((3|4) ?g)\W',re.I)
for doc in docs:
    if 'g' not in doc['specification']:
	name = doc['name']
	m = g_pattern.search(name)
	if m:
	    tablets.update({'_id' : doc['_id']},{'$set':{'specification.g' : m.group(1).strip()}},safe = True)
	    
docs = tablets.find({'site':'indiaplaza'})
g_pattern = re.compile('\W((3|4) ?g)\W',re.I)
for doc in docs:
    if 'g' not in doc['specification']:
	name = doc['name']
	m = g_pattern.search(name)
	if m:
	    tablets.update({'_id' : doc['_id']},{'$set':{'specification.g' : m.group(1).strip()}},safe = True)

docs = tablets.find({'site':'saholic'})
for doc in docs:
    if 'specification' in doc:
	if 'wi-fi' in doc['specification']:
            s = doc['specification']['wi-fi']
	    tablets.update({'_id' : doc['_id']},{'$set':{'specification.wifi' : s},'$unset':{'specification.wi-fi' : True}},safe = True)
	    
docs = tablets.find({'site':'indiaplaza'})
for doc in docs:
    if 'specification' in doc:
	if '3g' in doc['specification']:
            s = doc['specification']['3g']
	    tablets.update({'_id' : doc['_id']},{'$set':{'specification.g' : s},'$unset':{'specification.3g' : True}},safe = True)
	    
docs = mobiles.find({'site':'buytheprice'})
for doc in docs:
    if 'specification' in doc:
	if 'operating system version' in doc['specification']:
            s = doc['specification']['operating system version']
	    mobiles.update({'_id' : doc['_id']},{'$set':{'specification.os' : s},'$unset':{'specification.operating system version' : True}},safe = True)
	    
docs = laptops.find({'site':'infibeam'})
for doc in docs:
    if 'specification' in doc:
	if 'hard disk size (gb)' in doc['specification']:
            s = doc['specification']['hard disk size (gb)']
	    laptops.update({'_id' : doc['_id']},{'$set':{'specification.storage capacity' : s},'$unset':{'specification.hard disk size (gb)' : True}},safe = True)
	    
docs = laptops.find({'site':'infibeam'})
for doc in docs:
    if 'specification' in doc:
	if 'hard disk size ( gb )' in doc['specification']:
            s = doc['specification']['hard disk size ( gb )']
	    laptops.update({'_id' : doc['_id']},{'$set':{'specification.storage capacity' : s},'$unset':{'specification.hard disk size ( gb )' : True}},safe = True)

docs = laptops.find({'site':'infibeam'})
for doc in docs:
    if 'specification' in doc:
	if 'hard disk size' in doc['specification']:
            s = doc['specification']['hard disk size']
	    laptops.update({'_id' : doc['_id']},{'$set':{'specification.storage capacity' : s},'$unset':{'specification.hard disk size' : True}},safe = True)

docs = laptops.find({'site':'infibeam'})
for doc in docs:
    if 'specification' in doc:
	if 'processor name' in doc['specification']:
            s = doc['specification']['processor name']
	    laptops.update({'_id' : doc['_id']},{'$set':{'specification.processor' : s},'$unset':{'specification.processor name' : True}},safe = True)
	    
docs = cameras.find({'site':'flipkart'})
category_junk_pattern = re.compile('\(\d+\)')
for doc in docs:
    if 'category' in doc:
	cats=[]
	for c in doc['category']:
	    cats.append(category_junk_pattern.sub('',c).strip())
	cameras.update({'_id' : doc['_id']},{'$set':{'category' : cats}},safe = True)
	
docs = cameras.find({'site':'homeshop18'})
for doc in docs:
    if 'category' in doc:
	cats=[]
	for c in doc['category']:
	    if c=='Digicam':
		c='point and shoot'
	    cats.append(c.lower())
	cameras.update({'_id' : doc['_id']},{'$set':{'category' : cats}},safe = True)
	
docs = cameras.find({'site':'indiaplaza'})
for doc in docs:
    if 'category' in doc:
	cats=[]
	for c in doc['category']:
	    if c=='digital-cameras':
		c='point and shoot'
	    if c=='dslr-cameras':
		c='dslr'
	    if c=='camcorders':
		c='camcorder'
	    cats.append(c)
	cameras.update({'_id' : doc['_id']},{'$set':{'category' : cats}},safe = True)

cameras.update({'category':'Semi SLR'},{'$set':{'category':['dslr',]}},multi = True)

docs = cameras.find({'site':'infibeam'})
for doc in docs:
    if 'category' in doc:
	cats=[]
	for c in doc['category']:
	    c = c.lower()
	    cats.append(c.strip())
	cameras.update({'_id' : doc['_id']},{'$set':{'category' : cats}},safe = True)

docs = mobiles.find({'site':'infibeam'})
for doc in docs:
    if 'specification' in doc:
	specification = doc['specification']
	if 'android os' in specification and 'os' not in specification:
	    if specification['android os'] in ['available','yes']:
		if 'os version' in specification:
		    specification['os']='android'+' '+specification['os version']
		    del(specification['os version'])
		else:
		    specification['os']='android'
		del(specification['android os'])
    
	if doc['brand']=='blackberry' and 'os version' in specification:
	    util.replaceKey(specification,'os version','os')
	mobiles.update({'_id' : doc['_id']},{'$set':{'specification' : specification}},safe = True)

#------------***********************----------------------------------------------------------------------#

mobiles.update({'specification':{'$exists' : False}},{'$set':{'specification':{}}},multi = True)
cameras.update({'specification':{'$exists' : False}},{'$set':{'specification':{}}},multi = True)
tablets.update({'specification':{'$exists' : False}},{'$set':{'specification':{}}},multi = True)
laptops.update({'specification':{'$exists' : False}},{'$set':{'specification':{}}},multi = True)
pds.update({'specification':{'$exists' : False}},{'$set':{'specification':{}}},multi = True)
hdds.update({'specification':{'$exists' : False}},{'$set':{'specification':{}}},multi = True)

docs = mobiles.find({'site':'indiaplaza'})
for doc in docs:
    ph = doc['product_history']
    for p in ph:
	if 'time' in p:
	    p['datetime']=p['time']
	    del(p['time'])
    mobiles.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)

docs = cameras.find({'site':'indiaplaza'})
for doc in docs:
    ph = doc['product_history']
    for p in ph:
	if 'time' in p:
	    p['datetime']=p['time']
	    del(p['time'])
    cameras.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
    
docs = laptops.find({'site':'indiaplaza'})
for doc in docs:
    ph = doc['product_history']
    for p in ph:
	if 'time' in p:
	    p['datetime']=p['time']
	    del(p['time'])
    laptops.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
    
docs = tablets.find({'site':'indiaplaza'})
for doc in docs:
    ph = doc['product_history']
    for p in ph:
	if 'time' in p:
	    p['datetime']=p['time']
	    del(p['time'])
    tablets.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
    
docs = pds.find({'site':'indiaplaza'})
for doc in docs:
    ph = doc['product_history']
    for p in ph:
	if 'time' in p:
	    p['datetime']=p['time']
	    del(p['time'])
    pds.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)
    
docs = hdds.find({'site':'indiaplaza'})
for doc in docs:
    ph = doc['product_history']
    for p in ph:
	if 'time' in p:
	    p['datetime']=p['time']
	    del(p['time'])
    hdds.update({'_id' : doc['_id']},{'$set':{'product_history' : ph}},safe = True)

docs = hdds.find()
for doc in docs:
    specification = doc['specification']
    if 'usb 20' in specification:
        if 'interface' in specification:
            del(specification['usb 20'])
        elif specification['usb 20']=='available' or specification['usb 20']=='yes':
            specification['inteface']='usb 2.0'
	    del(specification['usb 20'])


    if 'usb 30' in specification:
        if 'interface' in specification:
            del(specification['usb 30'])
        elif specification['usb 30']=='available' or specification['usb 30']=='yes':
            specification['inteface']='usb 3.0'
	    del(specification['usb 30'])
    hdds.update({'_id' : doc['_id']},{'$set':{'specification' : specification}},safe = True)

docs = pds.find()
for doc in docs:
    if 'img_url' in doc:
	pds.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : doc['img_url']}}},safe = True)

docs = hdds.find()
for doc in docs:
    if 'img_url' in doc:
	hdds.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : doc['img_url']}}},safe = True)
	
docs = laptops.find()
for doc in docs:
    if 'img_url' in doc:
	laptops.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : doc['img_url']}}},safe = True)
	
docs = tablets.find()
for doc in docs:
    if 'img_url' in doc:
	tablets.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : doc['img_url']}}},safe = True)

docs = cameras.find()
for doc in docs:
    if 'img_url' in doc:
	cameras.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : doc['img_url']}}},safe = True)
	
docs = mobiles.find()
for doc in docs:
    if 'img_url' in doc:
	mobiles.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : doc['img_url']}}},safe = True)

docs = pds.find({'img_url.0':{'$exists' : True}})
for doc in docs:
    if type(doc['img_url']['0']) == dict:
	url = doc['img_url']['0']['0']
	pds.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : url}}},safe = True)
	
docs = hdds.find({'img_url.0':{'$exists' : True}})
for doc in docs:
    if type(doc['img_url']['0']) == dict:
	url = doc['img_url']['0']['0']
	hdds.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : url}}},safe = True)
	
docs = mobiles.find({'img_url.0':{'$exists' : True}})
for doc in docs:
    if type(doc['img_url']['0']) == dict:
	url = doc['img_url']['0']['0']
	mobiles.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : url}}},safe = True)
	
docs = cameras.find({'img_url.0':{'$exists' : True}})
for doc in docs:
    if type(doc['img_url']['0']) == dict:
	url = doc['img_url']['0']['0']
	cameras.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : url}}},safe = True)
	
docs = laptops.find({'img_url.0':{'$exists' : True}})
for doc in docs:
    if type(doc['img_url']['0']) == dict:
	url = doc['img_url']['0']['0']
	laptops.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : url}}},safe = True)
	
docs = tablets.find({'img_url.0':{'$exists' : True}})
for doc in docs:
    if type(doc['img_url']['0']) == dict:
	url = doc['img_url']['0']['0']
	tablets.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : url}}},safe = True)
	

	
docs = mobiles.find({'img_url.0':{'$exists' : True}})
for doc in docs:
    if type(doc['img_url']['0']) == list:
	if len(doc['img_url']['0'])>0:
	    url = doc['img_url']['0'][0]
	    mobiles.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : url}}},safe = True)
	
docs = laptops.find({'img_url.0':{'$exists' : True}})
for doc in docs:
    if type(doc['img_url']['0']) == list:
	if len(doc['img_url']['0'])>0:
	    url = doc['img_url']['0'][0]
	    laptops.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : url}}},safe = True)
	
docs = pds.find({'img_url.0':{'$exists' : True}})
for doc in docs:
    if type(doc['img_url']['0']) == list:
	if len(doc['img_url']['0'])>0:
	    url = doc['img_url']['0'][0]
	    pds.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : url}}},safe = True)
	
docs = cameras.find({'img_url.0':{'$exists' : True}})
for doc in docs:
    if type(doc['img_url']['0']) == list:
	if len(doc['img_url']['0'])>0:
	    url = doc['img_url']['0'][0]
	    cameras.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : url}}},safe = True)
	
docs = hdds.find({'img_url.0':{'$exists' : True}})
for doc in docs:
    if type(doc['img_url']['0']) == list:
	if len(doc['img_url']['0'])>0:
	    url = doc['img_url']['0'][0]
	    hdds.update({'_id' : doc['_id']},{'$set':{'img_url':{'0' : url}}},safe = True)
	    
from fk_laptops_scrap import scrapAllLaptops
laps = scrapAllLaptops()
d = dict((l['url'],l) for l in laps)
docs = laptops.find({'site':'flipkart'})
for doc in docs:
    if doc['url'] in d:
	l = d[doc['url']]
	l['product_history']=doc['product_history']
	l['_id']=doc['_id']
	laptops.save(l,safe = True)
	del d[doc['url']]

for i in d:
    laptops.save(d[i])


### To live 

final=con.final
cats=final.categories
docs=cats.find()
for doc in docs:
    cats.update({'_id':doc['_id']},{'$set':{'name_chain':[doc['name'],]}})
    

prods=final.products
docs=prods.find()
for doc in docs:
    c=[doc['root_category'],]
    prods.update({'_id':doc['_id']},{'$set':{'categories':c}})

mobiles.update({'brand':'sonyericsson'},{'$set':{'brand':'sony ericsson'}},multi = True)
mobiles.update({'brand':'xolo'},{'$set':{'brand':'lava'}},multi = True)

new_final=con.new_final
prods=new_final.products
docs=prods.find()
for doc in docs:
    c=[doc['root_category'],]
    prods.update({'_id':doc['_id']},{'$set':{'categories':c}})
