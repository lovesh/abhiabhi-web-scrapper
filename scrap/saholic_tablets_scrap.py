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
tablet_home='http://www.saholic.com/all-tablets/10010'

logfile=open('saholic_tablet_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':tablet_home})

shipping_pattern=re.compile('(\d+)',re.I)

def getAllTabletUrls():
    count_path='//span[@class="resultLimit"]'
    doc=dom.DOM(url=tablet_home)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    num_pages=int(math.ceil(count/20.0))
    page_urls=[tablet_home+'?&page='+str(n) for n in xrange(1,num_pages+1)]
    dl.putUrls(page_urls)
    pages=dl.download()
    tablet_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            tablet_urls.extend(getTabletUrlsFromPage(html))
    return tablet_urls

def getTabletUrlsFromPage(html):
    tablet_url_path='//div[@class="title"]/a'
    page_dom=dom.DOM(string=html)
    links=set(siteurl+l[1] for l in page_dom.getLinksWithXpath(tablet_url_path))
    return links

def getTabletFromPage(url=None,string=None):
    tablet={}
    if url:
        doc=dom.DOM(url=url)
        tablet['url']=url
    else:
        doc=dom.DOM(string=string)
    
    brand_path='//div[@class="name"]/span[@class="brand"]'
    brand=doc.getNodesWithXpath(brand_path)[0].text.strip()
    tablet['brand']=brand.lower()
    name_path='//div[@class="name"]/span[@class="product-name"]'
    tablet['name']=brand+' '+doc.getNodesWithXpath(name_path)[0].text.strip()
    image_path='//meta[@property="og:image"]'
    tablet['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    price_path='//span[@id="sp"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        tablet['price']=int(price[0].text_content().strip())

    addBox=doc.getNodesWithXpath('//a[@id="addToCart"]')

    if addBox:                           #availability check
        tablet['availability']=1
        shipping_path='//div[@id="shipping_time"]'
        shipping=doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping=shipping[0].text_content()
            shipping=shipping_pattern.search(shipping)
            if shipping:
                tablet['shipping']=[shipping.group(1)]
    else:
        tablet['availability']=0

    specification={}
    
    dimensions_path='//div[@id="vtab-130002"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,dimensions_path)) 

    display_path='//div[@id="vtab-130003"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,display_path))

    connectivity_path='//div[@id="vtab-130080"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,connectivity_path))

    calling_path='//div[@id="vtab-130005"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,calling_path))

    music_path='//div[@id="vtab-130081"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,music_path))

    video_path='//div[@id="vtab-130031"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,video_path))
    
    camera_path='//div[@id="vtab-130010"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,camera_path))

    processor_path='//div[@id="vtab-130085"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,processor_path))

    software_path='//div[@id="vtab-130020"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,software_path))

    battery_path='//div[@id="vtab-130083"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,battery_path))

    storage_path='//div[@id="vtab-130082"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,storage_path))

    util.replaceKey(specification,'built-in','storage')
    util.replaceKey(specification,'wi-fi','wifi')

    tablet['specification']=specification

    tablet['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in tablet:
        product_history['price']=tablet['price']
    if 'shipping' in tablet:
        product_history['shipping']=tablet['shipping']
    product_history['availability']=tablet['availability']
    product_history['datetime']=tablet['last_modified_datetime']
    tablet['product_history']=[product_history,]
    tablet['site']='saholic'
    return tablet

def scrapAllTablets():
    urls=getAllTabletUrls()
    tablets=[]
    failed=[]
    dl.putUrls(urls)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            print r
            tablet=getTabletFromPage(string=html)
            if tablet:
                tablet['url']=r
                tablets.append(tablet)
        else:
            failed.append('%s with %s'%(r,str(status)))
    return tablets

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    tablet_coll=db['scraped_tablets']
    tablet_coll.create_index('url',unique=True)
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    tablets=scrapAllTablets()
    for tablet in tablets:
        try:
            tablet_coll.insert(tablet,safe=True)
            inserted_count+=1
            inserted_urls.append(tablet['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability' in tablet:
                upd['availability']=tablet['availability']
            if 'price' in tablet:
                upd['price']=tablet['price']
            if 'shipping' in tablet:
                upd['shipping']=tablet['shipping']
	    if 'offer' in tablet:
		upd['offer']=tablet['offer']
	    else:
		upd['offer']=''
            tablet_coll.update({'url':tablet['url']},{'$push':{'product_history':tablet['product_history'][0]},'$set':upd})
            updated_count+=1
            updated_urls.append(tablet['url'])
    if log:
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'tablet','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
	
    print "%d inserted and %d updated"%(inserted_count,updated_count)

if __name__=='__main__':
    insertIntoDB()


    


    



