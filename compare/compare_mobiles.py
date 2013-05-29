import compare_common
import pymongo
import difflib
import re
import datetime
import time

con=pymongo.Connection()
#s_coll=con.abhiabhi.scraped_mobiles
collection_name='scraped_mobiles'
p_coll=con.new_final.products

storage_pattern=re.compile('(\d+) ?gb',re.I)
samsung_model_pattern = re.compile('[a-zA-Z]?\d{3,4}')

junk=['mobile','phone','smartphone','smart phone','dual', 'sim','-', '\'', 'unlock','full touch screen','full touch','3G','Android','With Beats Audio','gsm','cdma','carnival','\(','\)', 'touch screen', 'slider', 'airtel', 'vodafone', 'aircel', 'reliance', 'tata indicom', 'tata', 'touch and type', 'touch', 'intel atom processor', 'dust and water resistant', 'windows']
    
equivalents=[
        ('sony xperia p','lt22i'),
        ('sony xperia u','st25i'),
        ('sony xperia s', 'lt26i'),
        ('sony xperia neo l','MT25i'),
        ('sony ericsson xperia play', 'R800i'),
        ('sony ericsson xperia pro', 'MK16i'),
        ('sony xperia go', 'ST27i'),
        ('sony xperia sola', 'MT27i'),
        ('samsung galaxy note','galaxy note n 7000', 'galaxy note n-7000', 'galaxy note n7000'),
        ('samsung galaxy s ii', 's ii I9100','I9100', 'i9100G', 'i9100', '9100G'),
        ('samsung galaxy s iii','s iii I9300','I9300'),
        ('samsung Galaxy Ace Duos S6802','Galaxy Ace Duos','Galaxy S6802'),
        ('LG Optimus 4X HD','4X HD P880'),
        ('Motorola Razr Maxx', 'Razr Maxx', 'maxx XT912'),
        ('HTC One S', 'one S Z560E'),
        ('HTC One X', 'one X S720E'),
        ('HTC Incredible S', 'Incredible S S710E', 'Incredible S710E'),
        ('sony ericsson xperia arc s', 'arc s lt18i'),
        #('samsung galaxy S advance i9070', 'advance gt-i9070'),
        #('samsung galaxy SL i9003', 'S LCD i9003'),
        ('samsung wave iii', 'wave 3'),
        ('samsung wave m s750', 'wave m 750 s750 '),
        ('samsung galaxy pop', 'pop s 5570'),
        ('samsung galaxy Y', 'S5360'),
        ('samsung galaxy S Duos S7562', 'samsung galaxy S Duos'),
        #('Samsung Champ Deluxe Duos C3312', 'C3312'),
        #('Samsung Chat C3222', 'C3222'),
        #('Samsung Chat E2222', 'E2222'),
        ('LG Optimus 3D p920', 'P920'),
        ('LG Optimus p970', 'P970'),
        ]

def normalize_name(name):
    name=compare_common.remove_colors(name)[0]
    name=compare_common.remove_junk(name,junk)
    for equivalent in equivalents:
        for item in equivalent[1:]:
            pattern = re.compile(item, re.I)
            m = pattern.search(name)
            if m:
                name = equivalent[0]
                break
        
            
    name=compare_common.remove_duplicate_words(name)
    name=compare_common.reduce_spaces(name)
    return name.strip().lower()

def compare(phone1,phone2):
    brand = phone1['brand']
    name1=normalize_name(phone1['name'])
    name2=normalize_name(phone2['name'])
    
    m1=storage_pattern.search(name1)
    if m1:
        m2=storage_pattern.search(name2)
        if m2:
            if m1.group(1)!=m2.group(1):
                return False
                
    name1 = storage_pattern.sub(' ', name1)
    name2 = storage_pattern.sub(' ', name2)
    
    name1 = re.sub(phone1['brand'].lower(), '', name1, flags = re.I)
    name2 = re.sub(phone2['brand'].lower(), '', name2, flags = re.I)
    
    if brand == 'samsung':
        m = samsung_model_pattern.search(name1)
        if m:
            n = samsung_model_pattern.search(name2)
            if n:
                if m.group() == n.group():
                    return True
    
    if compare_common.unordered_comparison(name1,name2):
        return True
        
    sm=difflib.SequenceMatcher(lambda x: x in " ")
    sm.set_seq1(name1)
    sm.set_seq2(name2)
    if sm.ratio() > .98:
        return True
    else:
        return False

def go():
    mobiles_count=p_coll.find({'root_category':'mobile','status':1}).count()
    if mobiles_count==0:
        start=time.time()
        compare_common.firstTimeProductFilling('mobile',compare)
        finish=time.time()
        print "print time taken %f"%(finish-start)
	
    else:
        start = time.time()
        compare_common.nextTimeProductFilling('mobile',compare)
        finish = time.time()
        print "print time taken %f"%(finish-start)
	
if __name__ == '__main__':
    go()   
          
    


     
