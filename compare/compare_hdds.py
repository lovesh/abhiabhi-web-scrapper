import compare_common
import pymongo
import difflib
import re
import datetime

con = pymongo.Connection()
s_db = con.abhiabhi
p_db = con.new_final
s_db.authenticate('root','hpalpha1911')
p_db.authenticate('root','hpalpha1911')

s_coll=s_db.scraped_harddisks
p_coll=p_db.products

capacity_pattern=re.compile('(\d+\.?\d?) ?(g|t)b',re.I)
interface_pattern=re.compile('3\.?\d?',re.I)
size_pattern=re.compile('(\d\.\d?) ?(\"|\'\'|inch)',re.I)

site_preference=['flipkart','buythprice','saholic','homeshop18','infibeam','indiaplaza']

junk=['hard disk drive','hard disk','hard drive','external','portable','drive','usb','-','hdd']
equivalents=[
        ('western digital','wd'),
        ('elements','element'),
        ('basics','basic')
        ]

def get_storage(hdd):
    cap = 0
    m=capacity_pattern.search(hdd['name'])
    if m:
        cap=float(m.group(1))
	return cap
    
    if 'specification' in hdd:
        if 'capacity' in hdd['specification']:
            m=capacity_pattern.search(hdd['specification']['capacity'])
	    if m:
		cap=float(m.group(1))
		
    return cap
    
def get_interface(hdd):
    interface=2.0
    
    m=interface_pattern.search(hdd['name'])
    if m:
	interface=m.group()
    
    if 'interface' in hdd['specification']:
	if type(hdd['specification']['interface']) == list:
	    return interface
	m=interface_pattern.search(hdd['specification']['interface'])
	if m:
	    interface=m.group()
	    
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
       
	    
    name=compare_common.remove_duplicate_words(name)
    name=compare_common.reduce_spaces(name)
    return name.strip().lower()

def compare(hdd1,hdd2):
    name1=normalize_name(hdd1['name'])
    name2=normalize_name(hdd2['name'])

    cap1=0
    cap2=0
    int1=2.0
    int2=2.0

    cap1=get_storage(hdd1)
    cap2=get_storage(hdd2)
    
    if cap1!=cap2:
        return False

    int1=get_interface(hdd1)
    int2=get_interface(hdd2)
    
    if int1 == 2.0 and int2 == 3.0:
	return False
    if int2 == 2.0 and int1 == 3.0:
	return False

    m1=size_pattern.search(name1)
    if m1:
        size1=float(m1.group(1))
        m2=size_pattern.search(name2)
        if m2:
            size2=float(m2.group(1))
            if size1!=size2:
                return False
	else:
	    return False	

    name1=capacity_pattern.sub('',name1).strip(')( ')
    name2=capacity_pattern.sub('',name2).strip(')( ')
    name1=interface_pattern.sub('',name1).strip(')( ')
    name2=interface_pattern.sub('',name2).strip(')( ')
    name1=size_pattern.sub('',name1).strip(')( ')
    name2=size_pattern.sub('',name2).strip(')( ')

    if compare_common.unordered_comparison(name1,name2):
        return True
    sm=difflib.SequenceMatcher(lambda x: x in " ")
    sm.set_seq1(name1)
    sm.set_seq2(name2)
    if sm.ratio() > .96:
        return True
    else:
        return False

def go():
    hdd_count=p_coll.find({'root_category':'harddisk'}).count()
    if hdd_count==0:
	p_coll.create_index("specification.capacity",sparse=True)
	p_coll.create_index("specification.interface",sparse=True)
	
        compare_common.firstTimeProductFilling('harddisk',compare)
    else:
        compare_common.nextTimeProductFilling('harddisk',compare)

if __name__=='__main__':
    go()  
