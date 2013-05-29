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

s_coll = s_db.scraped_laptops
p_coll = p_db.products

ram_pattern = re.compile('((?:1\d|(?:\W|^)\d) ?GB)(.+)?',re.I)
storage_capacity_pattern = re.compile('(\d{3,4}) ?GB|([1-2]\.?\d?) ?TB',re.I)
cpu_name_pattern = re.compile('((i(?:3|5|7))|pdc|pentium dual core|celeron dual core|atom dual core|core ?2|amd|apu dual core)(?:\W|$)',re.I)
cpu_clockspeed_pattern = re.compile('\d\.?\d{0,2} ?GHz',re.I)
gen_pattern = re.compile('\W(1|2|3)\w{2}? ?gen',re.I)
os_pattern = re.compile('windows|w7|win 7 basic|w7b|dos|linux|mac os|os x',re.I)
screen_size_pattern = re.compile('(1\d\.?\d?) ?(?:\"|\'\'|inch)')
variant_pattern = re.compile('\d{3,4}[a-z]\W',re.I)
series_pattern = re.compile('\W(\w+\WSeries).+',re.I)
series_sub_pattern = re.compile('\W(\w+\WSeries)',re.I)
macbook_pro_pattern = re.compile('apple macbook pro(.*?$)',re.I)
hp_junk_pattern = re.compile('B0.+(\W|$)')

junk=['laptop','notebook','netbook','-','with','and','\+','only','ideapad','new arrival','new','model','sleekbook','ultrabook','\[','\]','backlit','essentials series']
equivalents=[
        ('sony','sony vaio'),
	('pentium dual core','pdc'),
	('w7b','windows 7 home basic','windows 7 basic','win 7 basic','windows'),
	('mac os','os x')
        ]

def get_ram(laptop):
    ram = 0
    if 'memory' in laptop['specification']:
	if 'ram' in laptop['specification']['memory']:
	    m = ram_pattern.search(laptop['specification']['memory']['ram'])
	    if m:
		ram = m.group(1)
    else:
	if 'ram' in laptop['specification']:
	    m = ram_pattern.search(laptop['specification']['ram'])
	    if m:
		ram = m.group(1)
    return ram

def get_storage_cap(laptop):
    cap = 0
    if 'storage' in laptop['specification']:
	if 'capacity' in laptop['specification']['storage']:
	    m = storage_capacity_pattern.search(laptop['specification']['storage']['capacity'])
	    if m:
		cap = m.group(1)
		if cap is None:
		    cap = storage_capacity_pattern.search(laptop['specification']['storage']['capacity']).group(2)
    else:
	if 'storage capacity' in laptop['specification']:
	    m = storage_capacity_pattern.search(laptop['specification']['storage capacity'])
	    if m:
		cap = m.group(1)
		if cap is None:
		    cap = storage_capacity_pattern.search(laptop['specification']['storage capacity']).group(2)
    return cap

def get_processor_name(laptop):
    proc = 0
    
    if 'processor' in laptop['specification']:
	if type(laptop['specification']['processor'])  ==  dict and 'processor' in laptop['specification']['processor']:
	    m = cpu_name_pattern.search(laptop['specification']['processor']['processor'])
	    if m:
		proc = m.group().strip().lower()
	    
    else:
	if 'processor' in laptop['specification']:
	    m = cpu_name_pattern.search(laptop['specification']['processor'])
	    if m:
		proc = m.group().strip().lower()
	
    return proc
    
def get_os(laptop):
    os = 0
    if 'software' in laptop['specification']:
	if 'os' in laptop['specification']['software']:
	    m = os_pattern.search(laptop['specification']['software']['os'])
	    if m:
		os = m.group().lower()
	    
    else:
	if 'os' in laptop['specification']:
	    m = os_pattern.search(laptop['specification']['os'])
	    if m:
		os = m.group().lower()
    
    return os
    
def get_screen_size(laptop):
    screen = 0
    if 'display' in laptop['specification']:
	if 'size' in laptop['specification']['display']:
	    m = screen_size_pattern.search(laptop['specification']['display']['size'])
	    if m:
		screen = m.group(1)
	    
    else:
	if 'screen size' in laptop['specification']:
	    m = screen_size_pattern.search(laptop['specification']['screen size'])
	    if m:
		screen = m.group(1)
		
    return screen
    
def process_series(name):
    m = series_pattern.search(name)
    if m:
	name = series_sub_pattern.sub('',name)
    return name
    
def filter_macbook_pro(name):
    m = macbook_pro_pattern.match(name)
    if m:
	name = name.replace(m.group(1),'')
    return name

def normalize_name(name):
    name = compare_common.remove_colors(name)[0]
    name = compare_common.remove_junk(name,junk)
    for equivalent in equivalents:
        for item in equivalent[1:]:
            pattern = re.compile(item, re.I)
            m = pattern.search(name)
            if m:
                name = pattern.sub(equivalent[0], name)
                break
        
	    
    name = compare_common.remove_duplicate_words(name)
    name = compare_common.reduce_spaces(name)
    return name.strip().lower()
    
def compare(laptop1,laptop2):
    name1 = normalize_name(laptop1['name'])
    name2 = normalize_name(laptop2['name'])
    if laptop1['brand'] == 'hp':
	name1 = hp_junk_pattern.sub('',name1).strip()
	name2 = hp_junk_pattern.sub('',name2).strip()
	
    ram1 = 0
    ram2 = 0
    stor1 = 0
    stor2 = 0
    proc1 = 0
    proc2 = 0
    os1 = 0
    os2 = 0
    screen1 = 0
    screen2 = 0
    
    ram1 = get_ram(laptop1)
    ram2 = get_ram(laptop2)
    
    if ram1 != ram2:
        return False
	
    stor1 = get_storage_cap(laptop1)
    stor2 = get_storage_cap(laptop2)
    
    if stor1 != stor2:
        return False
    
    proc1 = get_processor_name(laptop1)
    proc2 = get_processor_name(laptop2)
    
    if proc1 != proc2:
        return False
	
    if laptop1['brand'] == 'toshiba':
	if ram1 == 0 and ram2 == 0 and stor1 == 0 and stor2 == 0 and proc1 == 0 and proc2 == 0:
	    name1 = process_series(name1)
	    name2 = process_series(name2)
	    if compare_common.unordered_comparison(name1,name2):
		return True 
	    sm = difflib.SequenceMatcher(lambda x: x in " ")
	    sm.set_seqs(name1,name2)
	    if sm.ratio() > .98:
		return True
	    else:
		return False
	    
    
    os1 = get_os(laptop1)
    os2 = get_os(laptop2)
    
    if os1 != os2:
        return False
	
    screen1 = get_screen_size(laptop1)
    screen2 = get_screen_size(laptop2)
    
    if screen1 != 0 and screen2 != 0 and abs(float(screen1) - float(screen2)) > 0.2:
        return False
    
    patterns=[ram_pattern,storage_capacity_pattern,cpu_name_pattern,cpu_clockspeed_pattern,screen_size_pattern,os_pattern,variant_pattern]
    for pattern in patterns: 
	name1 = pattern.sub('',name1).strip()
	name2 = pattern.sub('',name2).strip()
    name1.strip(')(/ ')
    name2.strip(')(/ ')
    
    if compare_common.unordered_comparison(name1,name2):
        return True
    
    name1 = process_series(name1)
    name2 = process_series(name2)
    
    name1 = filter_macbook_pro(name1)
    name2 = filter_macbook_pro(name2)
    
    sm = difflib.SequenceMatcher(lambda x: x in " ")
    sm.set_seqs(name1,name2)
    if sm.ratio() > .98:
        return True
    else:
        return False

def go():
    laptops_count = p_coll.find({'root_category':'laptop','status':1}).count()
    if laptops_count == 0:
	
	p_coll.create_index("specification.memory.ram",sparse = True)
	p_coll.create_index("specification.ram",sparse = True)
	
	p_coll.create_index("specification.display.size",sparse = True)
	p_coll.create_index("specification.screen size",sparse = True)
	
	p_coll.create_index("specification.processor.processor",sparse = True)
	p_coll.create_index("specification.processor",sparse = True)
	
	p_coll.create_index("specification.processor.clock speed",sparse = True)
	p_coll.create_index("specification.clock speed",sparse = True)
	
	p_coll.create_index("specification.storage.capacity",sparse = True)
	p_coll.create_index("specification.storage capacity",sparse = True)
	
	p_coll.create_index("specification.software.os",sparse = True)
	p_coll.create_index("specification.os",sparse = True)
	
        compare_common.firstTimeProductFilling('laptop',compare)
    else:
        compare_common.nextTimeProductFilling('laptop',compare)

if __name__ == '__main__':
    go()  
