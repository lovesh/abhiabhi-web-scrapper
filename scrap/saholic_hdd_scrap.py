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
hdd_home='http://www.saholic.com/external-hard-disks/10073'

junk_pattern=re.compile('\(|Hard|Portable',re.I)
shipping_pattern=re.compile('(\d+)-(\d+)',re.I)

logfile=open('saholic_hdd_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':hdd_home})

def getAllHDDUrls():
    count_path='//span[@class="resultLimit"]'
    doc=dom.DOM(url=hdd_home)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    num_pages=int(math.ceil(count/20.0))
    page_urls=[hdd_home+'?&page='+str(n) for n in xrange(1,num_pages+1)]
    dl.putUrls(page_urls)
    pages=dl.download()
    hdd_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            hdd_urls.extend(getHDDUrlsFromPage(html))
    return hdd_urls

def getHDDUrlsFromPage(html):
    hdd_url_path='//div[@class="title"]/a'
    page_dom=dom.DOM(string=html)
    links=set(siteurl+l[1] for l in page_dom.getLinksWithXpath(hdd_url_path))
    return links

def getHDDFromPage(url=None,string=None):
    hdd={}
    if url:
        doc=dom.DOM(url=url)
        hdd['url']=url
    else:
        doc=dom.DOM(string=string)
    
    brand_path='//div[@class="name"]/span[@class="brand"]'
    brand=doc.getNodesWithXpath(brand_path)[0].text.strip()
    if brand == 'wd':
        brand = 'western digital'
    hdd['brand']=brand.lower()
    
    title_path='//div[@class="name"]/span[@class="product-name"]'
    title=doc.getNodesWithXpath(title_path)[0].text.strip()

    junk=junk_pattern.search(title)
    if junk:
        hdd['name']=brand+title[:junk.start()].strip()
    else:
        hdd['name']=brand+title

    image_path='//meta[@property="og:image"]'
    hdd['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    price_path='//span[@id="sp"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        hdd['price']=int(price[0].text_content().strip())
	
    addBox=doc.getNodesWithXpath('//a[@id="addToCart"]')
    
    if addBox:                           #availability check
        hdd['availability']=1
        shipping_path='//div[@id="shipping_time"]'
        shipping=doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping=shipping[0].text_content()
            shipping=shipping_pattern.search(shipping)
            if shipping:
                hdd['shipping']=[shipping.group(1)]
    else:
        hdd['availability']=0
	
    specification={}
    
    physical_path='//div[@id="vtab-130119"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,physical_path))

    features_path='//div[@id="vtab-130120"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,features_path))

    util.replaceKey(specification,'storage capacity','capacity')
    util.replaceKey(specification,'connectivity','interface')
    util.replaceKey(specification,'hard disk rotational speed','rpm')
    util.replaceKey(specification,'data transfer speed','speed')

    hdd['specification']=specification

    hdd['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in hdd:
        product_history['price']=hdd['price']
    if 'shipping' in hdd:
        product_history['shipping']=hdd['shipping']
    product_history['availability']=hdd['availability']
    product_history['datetime']=hdd['last_modified_datetime']
    hdd['product_history']=[product_history,]
    hdd['site']='saholic'
    return hdd

def scrapAllHDDs():
    urls=getAllHDDUrls()
    hdds=[]
    failed=[]
    dl.putUrls(urls)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            print r
            hdd=getHDDFromPage(string=html)
            if hdd:
                hdd['url']=r
                hdds.append(hdd)
        else:
            failed.append('%s with %s'%(r,str(status)))
    return hdds

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    hdd_coll=db['scraped_harddisks']
    hdd_coll.create_index('url',unique=True)
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    hdds=scrapAllHDDs()
    for hdd in hdds:
        try:
            hdd_coll.insert(hdd,safe=True)
            inserted_count+=1
            inserted_urls.append(hdd['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability'in hdd:
                upd['availability']=hdd['availability']
            if 'price' in hdd:
                upd['price']=hdd['price']
            if 'shipping' in hdd:
                upd['shipping']=hdd['shipping']
	    if 'offer' in hdd:
		upd['offer']=hdd['offer']
	    else:
		upd['offer']=''
            hdd_coll.update({'url':hdd['url']},{'$push':{'product_history':hdd['product_history'][0]},'$set':upd})
            updated_count+=1
            updated_urls.append(hdd['url'])
    if log:
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'pendrive','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
	
    print "%d inserted and %d updated"%(inserted_count,updated_count)

if __name__=='__main__':
    insertIntoDB()

