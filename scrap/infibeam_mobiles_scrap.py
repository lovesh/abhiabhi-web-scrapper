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
referer='http://www.infibeam.com/Mobiles/search'
ajax_url='http://www.infibeam.com/Mobiles/Search_ajax.action?store=Mobiles&page='
debug=True

brand_pattern=re.compile('\w+',re.I)
shipping_pattern=re.compile('(\d+)-(\d+)',re.I)

logfile=open('infibeam_mobile_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})

def getMobileUrlsOfPage(html):
    mobile_url_path='//ul[@class="srch_result portrait"]/li/a'
    page_dom=dom.DOM(string=html)
    links=set(siteurl+l[1] for l in page_dom.getLinksWithXpath(mobile_url_path))
    return links

def getAllMobileUrls():
    count_path='//div[@id="resultsPane"]/div/div/b[2]'
    doc=dom.DOM(url=referer)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    num_pages=int(math.ceil(count/20.0))
    page_urls=[ajax_url+str(n) for n in xrange(1,num_pages+1)]
    dl.putUrls(page_urls)
    pages=dl.download()
    print len(pages)
    mobile_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            mobile_urls.extend(getMobileUrlsOfPage(html))
    print len(mobile_urls)
    return mobile_urls

def getMobileFromPage(url=None,string=None):
    mobile={}
    if url:
        doc=dom.DOM(url=url)
    else:
        doc=dom.DOM(string=string)
    addBox=doc.getNodesWithXpath('//input[@class="buyimg "]')

    if addBox:                           #availability check
        mobile['availability']=1
        details_path='//div[@id="ib_details"]'
        details=doc.getNodesWithXpath(details_path)
        if details:
            details=details[0].text_content()
            shipping=shipping_pattern.search(details)
            if shipping:
                mobile['shipping']=[shipping.group(1),shipping.group(2)]
    else:
        mobile['availability']=0
    name_path='//div[@id="ib_details"]/h1'
    mobile['name']=doc.getNodesWithXpath(name_path)[0].text_content().strip()
    
    brand=brand_pattern.search(mobile['name']).group().lower()
    if re.match('sony ericsson',mobile['name'],re.I):
        mobile['brand']='sony ericsson'
    else:
        mobile['brand']=brand
    color_path='//a[@class="colorlink"]'
    colors=doc.getNodesWithXpath(color_path)
    mobile['colors']=[color.get('text') for color in colors]
        
    price_path='//span[@class="infiPrice amount price"]'
    price=doc.getNodesWithXpath(price_path)
    if price:
        mobile['price']=int(price[0].text.replace(',',''))
    img_path="//div[@id='ib_img_viewer']/img"
    mobile['img_url']={'0':doc.getImgUrlWithXpath(img_path)}

    desc_path='//div[@class="reviews-box-cont-inner"]'
    desc=doc.getNodesWithXpath(desc_path)
    if desc:
        mobile['description']=desc[0].text_content.strip()
    
    mobile['last_modified_datetime']=datetime.datetime.now()
 
    product_history={}
    if 'price' in mobile:
        product_history['price']=mobile['price']
    if 'shipping' in mobile:
        product_history['shipping']=mobile['shipping']
    product_history['availability']=mobile['availability']
    product_history['datetime']=mobile['last_modified_datetime']
    mobile['product_history']=[product_history,]
    mobile['site']='infibeam'
 
    offer_path='//div[@class="offer"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        mobile['offer']=offer[0].text_content().replace('\r\n ','')

    specs_path='//div[@id="specs"]/div'
    specs=doc.getNodesWithXpath(specs_path)
    specification={}
    for spec in specs:
        text=spec.xpath('a')[0].text.strip()
        if text=='Deliverable Locations' or text=='Disclaimer':
            continue
        trs=spec.xpath('.//tr')
        for tr in trs:
            tds=tr.xpath('.//td')
            if len(tds)<2:
                continue
            key=tds[0].text_content().strip(':\n\t ').replace('.','').lower()
            value=tds[1].text_content().strip(':\n\t ').lower()
            specification[key]=value
    
    if 'android os' in specification and 'os' not in specification:
	if specification['android os'] in ['available','yes']:
	    if 'os version' in specification:
		specification['os']='android'+' '+specification['os version']
		del(specification['os version'])
	    else:
		specification['os']='android'
	    del(specification['android os'])
    
    if mobile['brand']=='blackberry' and 'os version' in specification:
	util.replaceKey(specification,'os version','os')
	
    mobile['specification']=specification

    return mobile

def scrapAllMobiles():
    urls=getAllMobileUrls()
    mobiles=[]
    dl.putUrls(urls)
    result=dl.download()
    for r in result:
        print r
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            mobile=getMobileFromPage(string=html)
            mobile['url']=r
            mobiles.append(mobile)
    return mobiles

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    mobile_coll=db['scraped_mobiles']
    mobile_coll.create_index('url',unique=True)
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    mobiles=scrapAllMobiles()
    for mobile in mobiles:
        try:
            mobile_coll.insert(mobile,safe=True)
            inserted_count+=1
            inserted_urls.append(mobile['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability' in mobile:
                upd['availability']=mobile['availability']
            if 'price' in mobile:
                upd['price']=mobile['price']
            if 'shipping' in mobile:
                upd['shipping']=mobile['shipping']
	    if 'offer' in mobile:
                upd['offer']=mobile['offer']
	    else:
		upd['offer']=''
            mobile_coll.update({'url':mobile['url']},{'$push':{'product_history':mobile['product_history'][0]},'$set':upd})
            updated_count+=1
            updated_urls.append(mobile['url'])
    if log:
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'mobile','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
	
    print "%d inserted and %d updated"%(inserted_count,updated_count)

if __name__=='__main__':
    insertIntoDB()
