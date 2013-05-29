import downloader
import dom
import urllib
import re
import datetime
import math
import pymongo
import util
from saholic_common import *

siteurl='http://www.saholic.com'
mobile_home='http://www.saholic.com/all-mobile-phones/10001'

dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':mobile_home})

shipping_pattern=re.compile('(\d+)',re.I)

def getAllMobileUrls():
    count_path='//span[@class="resultLimit"]'
    doc=dom.DOM(url=mobile_home)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    num_pages=int(math.ceil(count/20.0))
    page_urls=[mobile_home+'?&page='+str(n) for n in xrange(1,num_pages+1)]
    dl.putUrls(page_urls)
    pages=dl.download()
    mobile_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            mobile_urls.extend(getMobileUrlsFromPage(html))
    return mobile_urls

def getMobileUrlsFromPage(html):
    mobile_url_path='//div[@class="title"]/a'
    page_dom=dom.DOM(string=html)
    links=set(siteurl+l[1] for l in page_dom.getLinksWithXpath(mobile_url_path))
    return links

def getMobileFromPage(url=None,string=None):
    mobile={}
    if url:
        doc=dom.DOM(url=url)
        mobile['url']=url
    else:
        doc=dom.DOM(string=string)
    
    brand_path='//div[@class="name"]/span[@class="brand"]'
    brand=doc.getNodesWithXpath(brand_path)[0].text.strip()
    mobile['brand']=brand.lower()
    name_path='//div[@class="name"]/span[@class="product-name"]'
    mobile['name']=brand+' '+doc.getNodesWithXpath(name_path)[0].text.strip()
    image_path='//meta[@property="og:image"]'
    mobile['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    price_path='//span[@id="sp"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        mobile['price']=int(price[0].text_content().strip())

    addBox=doc.getNodesWithXpath('//a[@id="addToCart"]')

    if addBox:                           #availability check
        mobile['availability']=1
        shipping_path='//div[@id="shipping_time"]'
        shipping=doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping=shipping[0].text_content()
            shipping=shipping_pattern.search(shipping)
            if shipping:
                mobile['shipping']=[shipping.group(1),]
    else:
        mobile['availability']=0
    
    specification={}

    dimensions_path='//div[@id="vtab-130002"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,dimensions_path))

    display_path='//div[@id="vtab-130003"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,display_path))

    calling_path='//div[@id="vtab-130005"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,calling_path))

    connectivity_path='//div[@id="vtab-130007"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,connectivity_path))

    memory_path='//div[@id="vtab-130011"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,memory_path))

    camera_path='//div[@id="vtab-130010"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,camera_path))

    battery_path='//div[@id="vtab-130043"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,battery_path))

    software_path='//div[@id="vtab-130020"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,software_path))
    
    mobile['specification']=specification

    mobile['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in mobile:
        product_history['price']=mobile['price']
    if 'shipping' in mobile:
        product_history['shipping']=mobile['shipping']
    product_history['availability']=mobile['availability']
    product_history['datetime']=mobile['last_modified_datetime']
    mobile['product_history']=[product_history,]
    mobile['site']='saholic'
    return mobile



def scrapAllMobiles():
    urls=getAllMobileUrls()
    mobiles=[]
    failed=[]
    dl.putUrls(urls)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            print r
            mobile=getMobileFromPage(string=html)
            if mobile:
                mobile['url']=r
                mobiles.append(mobile)
        else:
            failed.append('%s with %s'%(r,str(status)))
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

