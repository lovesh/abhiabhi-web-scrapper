import downloader
import dom
import urllib
import re
import datetime
import math
import simplejson as json
import pymongo
from collections import defaultdict
import util

siteurl='http://www.infibeam.com'
referer='http://www.infibeam.com/Laptop_Computers_Accessories/search'
ajax_url='http://www.infibeam.com/Laptop_Computers_Accessories/Search_ajax.action?category=Laptop&store=Computers_Accessories&page='
debug=True

#name_pattern=re.compile('([\w \/\d-]+?)\((.*?(\dgb).*?((?:\d{2,3}gb)|\dtb).*?)\).*?$',re.I)

name_pattern=re.compile('(^[\w \d\[\]-]+?)\((\/|2nd|3rd|Ci|dual|quad|c2d|core|dc|pentium|intel|amd|celeron|pdc|i3|i5|i7|$)',re.I)    # pattern for matching names which contain specs in braces
name_pattern_1=re.compile('(^[\w \d-]+?)(\/|2nd|3rd|Ci|dual|quad|c2d|core|dc|pentium|intel|amd|celeron|pdc|i3|i5|i7|\(|$)',re.I)

brand_pattern=re.compile('\w+',re.I)
shipping_pattern=re.compile('(\d+)-(\d+)',re.I)
ram_pattern=re.compile('(1\d|(?:\W|^)\d) ?gb',re.I)
ram_type_pattern=re.compile('DDR\d?',re.I)
hdd_capacity_pattern=re.compile('(\d{3,4} ?G|\d\.?\d? ?T)B',re.I)
cpu_clockspeed_pattern=re.compile('\d\.?\d{0,2} ?GHz',re.I)
cpu_name_pattern=re.compile('(i(3|5|7))|pdc|pentium dual core|core ?2|amd',re.I)

logfile=open('infibeam_laptop_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})

def getLaptopUrlsOfPage(html):
    laptop_url_path='//ul[@class="srch_result frame"]/li/a'
    page_dom=dom.DOM(string=html)
    links=set(siteurl+l[1] for l in page_dom.getLinksWithXpath(laptop_url_path))
    return links

def getAllLaptopUrls():
    count_path='//div[@id="resultsPane"]/div/div/b[2]'
    doc=dom.DOM(url=referer)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    num_pages=int(math.ceil(count/20.0))
    page_urls=[ajax_url+str(n) for n in xrange(1,num_pages+1)]
    dl.putUrls(page_urls)
    pages=dl.download()
    laptop_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            laptop_urls.extend(getLaptopUrlsOfPage(html))
    return laptop_urls

def getLaptopFromPage(url=None,string=None):
    laptop={}
    if url:
        doc=dom.DOM(url=url)
    else:
        doc=dom.DOM(string=string)
    addBox=doc.getNodesWithXpath('//input[@class="buyimg "]')

    if addBox:                           #availability check
        laptop['availability']=1
        details_path='//div[@id="ib_details"]'
        details=doc.getNodesWithXpath(details_path)
        if details:
            details=details[0].text_content()
            shipping=shipping_pattern.search(details)
            if shipping:
                laptop['shipping']=[shipping.group(1),shipping.group(2)]
    else:
        laptop['availability']=0
    
    title_path='//div[@id="ib_details"]/h1'
    title=doc.getNodesWithXpath(title_path)[0].text_content().strip()
      
    name=name_pattern.search(title)
    if name:
        laptop['name']=name.group(1).strip()
    else:
        laptop['name']=name_pattern_1.search(title).group(1).strip()

    extra_ram=None
    extra_hdd=None
    
    m=ram_pattern.search(title)
    if m:
        extra_ram=m.group()
    m=hdd_capacity_pattern.search(title)
    if m:
        extra_hdd=m.group()

    laptop['brand']=brand_pattern.search(laptop['name']).group().lower()
    color_path='//a[@class="colorlink"]'
    colors=doc.getNodesWithXpath(color_path)
    laptop['colors']=[color.get('text') for color in colors]
        
    price_path='//span[@class="infiPrice amount price"]'
    price=doc.getNodesWithXpath(price_path)
    if price:
        laptop['price']=int(price[0].text.replace(',',''))
    img_path="//div[@id='ib_img_viewer']/img"
    laptop['img_url']={'0':doc.getImgUrlWithXpath(img_path)}

    desc_path='//div[@class="reviews-box-cont-inner"]'
    desc=doc.getNodesWithXpath(desc_path)
    if desc:
        laptop['description']=desc[0].text_content.strip()
    
    key_features_path='//ul[@id="keylist"]'
    kf=doc.getNodesWithXpath(key_features_path)
    if len(kf)>0:
	key_features=kf[0].text_content()
	
    laptop['last_modified_datetime']=datetime.datetime.now()
 
    product_history={}
    if 'price' in laptop:
        product_history['price']=laptop['price']
    if 'shipping' in laptop:
        product_history['shipping']=laptop['shipping']
    product_history['availability']=laptop['availability']
    product_history['datetime']=laptop['last_modified_datetime']
    laptop['product_history']=[product_history,]
    laptop['site']='infibeam'
 
    offer_path='//div[@class="offer"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        laptop['offer']=offer[0].text_content().replace('\r\n ','')

    specs_path='//div[@id="specs"]/div'
    specs=doc.getNodesWithXpath(specs_path)
    specification={}
    for spec in specs:
        text=spec.xpath('a')[0].text.strip()
        if text=='Deliverable Locations' or text=='Disclaimer':
            continue
        if text=='Operating System':
            div=spec.xpath('.//div')
            if div:
                specification['os']=div[0].text_content().strip().lower()
                continue
        trs=spec.xpath('.//tr')
        for tr in trs:
            tds=tr.xpath('.//td')
            if len(tds)<2:
                continue
            key=tds[0].text_content().strip(':\n\t ').replace('.','').lower()
            value=tds[1].text_content().strip(':\n\t ').lower()
            specification[key]=value
    
    util.replaceKey(specification,'system memory','ram')
    util.replaceKey(specification,'hard disk size(gb)','storage capacity')
    util.replaceKey(specification,'hard disk size (gb)','storage capacity')
    util.replaceKey(specification,'hard disk size ( gb )','storage capacity')
    if 'storage capacity' in specification:
        specification['storage capacity']+=' gb'
    util.replaceKey(specification,'hard disk size','storage capacity')
    util.replaceKey(specification,'hardware interface','storage type')
    if 'ram' not in specification and extra_ram:
        specification['ram']=extra_ram
    if 'storage capacity' not in specification and extra_hdd:
        specification['storage capacity']=extra_hdd
    if 'storage capacity' not in specification and kf:
	m=hdd_capacity_pattern.search(key_features)
	if m:
	    specification['storage capacity']=m.group()
	    
    util.replaceKey(specification,'processor type','processor')
    util.replaceKey(specification,'processor name','processor')
    util.replaceKey(specification,'cpu type','processor')
    
    if 'processor' not in specification:
	m=cpu_name_pattern.search(title)
	if m:
	    specification['processor']=m.group()
	
    laptop['specification']=specification

    return laptop

def scrapAllLaptops():
    urls=getAllLaptopUrls()
    laptops=[]
    dl.putUrls(urls)
    result=dl.download()
    for r in result:
        print r
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            laptop=getLaptopFromPage(string=html)
            laptop['url']=r
            laptops.append(laptop)
    return laptops

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    laptop_coll=db['scraped_laptops']
    laptop_coll.create_index('url',unique=True)
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    laptops=scrapAllLaptops()
    for laptop in laptops:
        try:
            laptop_coll.insert(laptop,safe=True)
            inserted_count+=1
            inserted_urls.append(laptop['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability' in laptop:
                upd['availability']=laptop['availability']
            if 'price' in laptop:
                upd['price']=laptop['price']
            if 'shipping' in laptop:
                upd['shipping']=laptop['shipping']
	    if 'offer' in laptop:
		upd['offer']=laptop['offer']
	    else:
		upd['offer']=''
            laptop_coll.update({'url':laptop['url']},{'$push':{'product_history':laptop['product_history'][0]},'$set':upd})
            updated_count+=1
            updated_urls.append(laptop['url'])
    if log:
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'laptop','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
    
    print "%d inserted and %d updated"%(inserted_count,updated_count)
    
if __name__=='__main__':
    insertIntoDB()
