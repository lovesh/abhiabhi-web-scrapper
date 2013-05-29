import compare_common
import pymongo
import difflib
import re
import datetime

con=pymongo.Connection()
s_coll=con.abhiabhi.scraped_tablets
p_coll=con.new_final.products

ram_pattern = re.compile('(\d{3}) ?mb|(1|2) ?gb',re.I)
hdd_full_pattern = re.compile('([3-9]|\d{2}) ?gb',re.I)
hdd_pattern = re.compile('([3-9]|\d{2}) ?(?:gb)?',re.I)
wifi_pattern = re.compile('wi ?-?fi',re.I)
g_pattern = re.compile('\W(3|4) ?g(\W|$)',re.I)
negative_value_pattern = re.compile('No',re.I)  	# pattern used for checking values like "No","Not Available"
os_pattern = re.compile('Android ?(ics)? ?(2.3|2.4|3.1|3.2|4.0)?',re.I)
screen_size_pattern = re.compile('\d ?(\'\'|\"|\\\"|inch|hd)',re.I)

site_preference = ['flipkart','buythprice','saholic','homeshop18','infibeam','indiaplaza']

junk=['tablet','imported','-','with','and','on','\+','ram','memory','only','pc','new','model','calling','mobile','phone','tab']
equivalents = [
        ('3','3rd gen'),
        ('Android','Google Android'),
	('wifi + 3g', 'wifi plus 3g', 'wifi and 3g'),
	('wifi + 4g','wifi plus 4g')
        ]

def get_ram(tablet):
    ram=0
    if 'ram' in tablet['specification']:
	m=ram_pattern.search(tablet['specification']['ram'])
	if m:
	    ram=m.group(1)
	    if ram is None:
		ram=ram_pattern.search(tablet['specification']['ram']).group(2)
	    
    return ram
    
def get_storage(tablet):
    storage=0
    if 'storage' in tablet['specification']:
	m=hdd_full_pattern.search(tablet['specification']['storage'])
	if m:
	    storage=m.group(1)
	    if storage is None:
		m=hdd_pattern.search(tablet['specification']['storage'])
		storage=m.group(1)
		if storage is not None:
		    return storage
    
    m=hdd_full_pattern.search(tablet['name'])
    if m:
	storage=m.group(1)
    return storage
    
def get_wifi(tablet):
    wifi=0
    m=wifi_pattern.search(tablet['name'])
    if m:
	return True
    
    if 'wifi' in tablet['specification']:
	if type(tablet['specification']['wifi']) in [dict,list]:
	    wifi=True
	else:
	    if negative_value_pattern.match(tablet['specification']['wifi']):
		wifi=False
	    else:
		wifi=True
    
    return wifi
    
def get_g(tablet):
    g=0

    m=g_pattern.search(tablet['name'])
    if m:
	g=m.group(1)
	return g
    
    if '3g' in tablet['specification']:
	if type(tablet['specification']['3g']) in [dict,list]:
	    g=3
	else:
	    if negative_value_pattern.match(tablet['specification']['3g']):
		g=0
	    else:
		g=3
		
    if '4g' in tablet['specification']:
	if type(tablet['specification']['4g']) == dict:
	    g=4
	else:
	    if negative_value_pattern.match(tablet['specification']['4g']):
		g=0
	    else:
		g=4

    return g
    
def normalize_name(name):
    name=compare_common.remove_colors(name)[0]
    name=compare_common.remove_junk(name,junk)
    for equivalent in equivalents:
        for item in equivalent[1:]:
            pattern = re.compile(item, re.I)
            m = pattern.search(name)
            if m:
                name = pattern.sub(equivalent[0], name)
                break
        
	    
    name=compare_common.remove_junk(name,junk)
    name=compare_common.remove_duplicate_words(name)
    name=compare_common.reduce_spaces(name)
    return name.strip().lower()

def compare(tablet1,tablet2):
    name1=normalize_name(tablet1['name'])
    name2=normalize_name(tablet2['name'])
    ram1=0
    ram2=0
    stor1=0
    stor2=0
    g1=0
    g2=0
    wifi1=1
    wifi2=1
    os1=0
    os2=0

    ram1=get_ram(tablet1)
    ram2=get_ram(tablet2)

    if ram1 != 0 and ram2 != 0 and ram1 != ram2:
        return False
 
    storage1=get_storage(tablet1)
    storage2=get_storage(tablet2)
    
    if storage1 != 0 and storage2 != 0 and storage1 != storage2:
        return False

    wifi1=get_wifi(tablet1)
    wifi2=get_wifi(tablet2)
	    
    #if (wifi1 and wifi2) == False:
	#return False
	
    g1=get_g(tablet1)
    g2=get_g(tablet2)
	 
    if g1 != g2:
        return False
    
    patterns=[ram_pattern,hdd_full_pattern,wifi_pattern,g_pattern,os_pattern,screen_size_pattern]
    for pattern in patterns: 
	name1=pattern.sub('',name1).strip()
	name2=pattern.sub('',name2).strip()
    name1=name1.strip(')(/ ')
    name2=name2.strip(')(/ ')
    
    if compare_common.unordered_comparison(name1,name2):
        return True
    sm=difflib.SequenceMatcher(lambda x: x in " ")
    sm.set_seq1(name1)
    sm.set_seq2(name2)
    if sm.ratio() > .99:
        return True
    else:
        return False

def go():
    tablets_count=p_coll.find({'root_category':'tablet'}).count()
    if tablets_count == 0:
	p_coll.create_index("specification.ram",sparse=True)
	p_coll.create_index("specification.storage capacity",sparse=True)
	p_coll.create_index("specification.os",sparse=True)
	
        return compare_common.firstTimeProductFilling('tablet',compare)
    else:
        compare_common.nextTimeProductFilling('tablet',compare)

if __name__ == '__main__':
    go()  
