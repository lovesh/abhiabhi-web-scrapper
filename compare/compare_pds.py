import compare_common
import pymongo
import difflib
import re
import datetime

con=pymongo.Connection()
s_coll=con.abhiabhi.scraped_pendrives
p_coll=con.new_final.products

capacity_pattern=re.compile('(\d+) ?gb',re.I)
interface_pattern=re.compile('\d\.?\d?',re.I)

site_preference=['flipkart','buythprice','saholic','homeshop18','infibeam','indiaplaza']

junk=['pendrive','pen-drive','usb pen drive','pen drive','flashdrive','flash-drive','usb flash drive','flash drive','-', 'usb', 'steel']
    
equivalents=[
        ('DataTraveler','DT','Data Traveler'),
	]

def get_capacity(pd):
    cap=0
    m=capacity_pattern.search(pd['name'])
    if m:
        cap=int(m.group(1))
	return cap
	
    if 'specification' in pd:
        if 'capacity' in pd['specification']:
            m=capacity_pattern.search(pd['specification']['capacity'])
	    if m:
		cap=m.group(1)
    
    if cap == 0:
	m=capacity_pattern.search(pd['url'])
	if m:
	    cap=m.group(1)
	    
    return cap
    
def get_interface(pd):
    interface=0
    
    if 'interface' in pd['specification'] and type(pd['specification']['interface']) == str:
	interface=interface_pattern.findall(pd['specification']['interface'])
	
    return interface

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
        
            
    name=compare_common.reduce_spaces(name)
    return name.strip().lower()


def compare(pd1,pd2):
    name1=normalize_name(pd1['name'])
    name2=normalize_name(pd2['name'])
    cap1=0
    cap2=0
    int1=0
    int2=0

    cap1=get_capacity(pd1)
    cap2=get_capacity(pd2)
    
    if cap1 != cap2:
        return False

    int1=get_interface(pd1)
    int2=get_interface(pd2)
    
    if int1 != 0 and int2 != 0:
        if '3.0' in int1 and '3.0' not in int2:
            return False
        if '3.0' in int2 and '3.0' not in int1:
            return False
    
    name1=capacity_pattern.sub('',name1).strip(')( ')
    name2=capacity_pattern.sub('',name2).strip(')( ')
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
    pd_count=p_coll.find({'root_category':'pendrive'}).count()
    if pd_count == 0:
	p_coll.create_index("specification.capacity",sparse=True)
	p_coll.create_index("specification.interface",sparse=True)
	
        compare_common.firstTimeProductFilling('pendrive',compare)
    else:
        compare_common.nextTimeProductFilling('pendrive',compare)

if __name__ == '__main__':
    go()  
