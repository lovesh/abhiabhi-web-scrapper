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
s_coll = s_db.scraped_cameras
p_coll = p_db.products

mp_pattern = re.compile('(\d+\.?\d?) ?(?:megapixel|mp)',re.I)
lens_pattern = re.compile('(?:ef|is)? ?(\d{2}) ?- ?(\d{2,3}).*?(?:ef|is)?',re.I)
body_only_pattern = re.compile('body( only)?',re.I)

junk=['digital camera', 'camera', 'digital', 'with', 'and', 'camcorder', 'digicam', 'point & shoot', 'new', 'DSLR', 'slr', 'system', 'is', 'kit', 'lens', '\(', '\)', 'mm','-', 'handycam', 'ii']
    
equivalents=[
	('sony Cybershot','Cyber-shot'),
        ('panasonic Lumix DMC','DMC'),
        ('Sony Cybershot DSC','Sony DSC'),
	('Canon PowerShot','Canon Power Shot')
        ]

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

def compare(camera1,camera2):
    name1 = normalize_name(camera1['name'])
    name2 = normalize_name(camera2['name'])
    if camera1['brand'] == 'fujifilm':
	name1.replace('exr','')
	name2.replace('exr','')
    
    if 'dslr' not in camera1['category']:
	name1 = mp_pattern.sub('',name1).strip().replace('-','')
	name2 = mp_pattern.sub('',name2).strip().replace('-','')
	if compare_common.unordered_comparison(name1,name2):
	    return True
	sm = difflib.SequenceMatcher(lambda x: x in " ")
	sm.set_seq1(name1)
	sm.set_seq2(name2)
	if sm.ratio() > .97:
	    return True
	else:
	    return False
    
    m1 = lens_pattern.search(name1)
    if m1:
	lens1=(m1.group(1),m1.group(2))
	m2 = lens_pattern.search(name2)
	if m2:
	    lens2=(m2.group(1),m2.group(2))
	else:
	    return False
	if lens1!=lens2:
	    return False
	name1 = lens_pattern.sub('',name1).strip(')( ').replace('-','')
	name2 = lens_pattern.sub('',name2).strip(')( ').replace('-','')
	if compare_common.unordered_comparison(name1,name2):
	    return True
	sm = difflib.SequenceMatcher(lambda x: x in " ")
	sm.set_seq1(name1)
	sm.set_seq2(name2)
	if sm.ratio() > .955:
	    return True
	else:
	    return False
    
    '''
    the following lines for dslrs that dont have lens
    '''
    name1 = body_only_pattern.sub('',name1).strip(')( ').replace('-','')
    name2 = body_only_pattern.sub('',name2).strip(')( ').replace('-','')
    if compare_common.unordered_comparison(name1,name2):
	return True
    sm = difflib.SequenceMatcher(lambda x: x in " ")
    sm.set_seq1(name1)
    sm.set_seq2(name2)
    if sm.ratio() > .955:
	return True
    else:
	return False
    

def go():
    cameras_count = p_coll.find({'root_category':'camera'}).count()
    if cameras_count==0:
        return compare_common.firstTimeProductFilling('camera',compare)
    else:
        compare_common.nextTimeProductFilling('camera',compare)

if __name__=='__main__':
    go()  
