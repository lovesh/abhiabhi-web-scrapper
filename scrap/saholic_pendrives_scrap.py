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
pd_home='http://www.saholic.com/pen-drive/10017'

logfile=open('saholic_pd_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':pd_home})

def getAllPDUrls():
    count_path='//span[@class="resultLimit"]'
    doc=dom.DOM(url=pd_home)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    num_pages=int(math.ceil(count/20.0))
    page_urls=[pd_home+'?&page='+str(n) for n in xrange(1,num_pages+1)]
    dl.putUrls(page_urls)
    pages=dl.download()
    pd_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            pd_urls.extend(getPDUrlsFromPage(html))
    return pd_urls

def getPDUrlsFromPage(html):
    pd_url_path='//div[@class="title"]/a'
    page_dom=dom.DOM(string=html)
    links=set(siteurl+l[1] for l in page_dom.getLinksWithXpath(pd_url_path))
    return links

def getPDFromPage(url=None,string=None):
    pd={}
    if url:
        doc=dom.DOM(url=url)
        pd['url']=url
    else:
        doc=dom.DOM(string=string)
    
    brand_path='//div[@class="name"]/span[@class="brand"]'
    brand=doc.getNodesWithXpath(brand_path)[0].text.strip()
    pd['brand']=brand.lower()
    name_path='//div[@class="name"]/span[@class="product-name"]'
    pd['name']=brand+' '+doc.getNodesWithXpath(name_path)[0].text.strip()
    image_path='//meta[@property="og:image"]'
    pd['img_url']=doc.getNodesWithXpath(image_path)[0].get('content')
    price_path='//span[@id="sp"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        pd['price']=int(price[0].text_content().strip())
    specification={}
    
    features_path='//div[@id="vtab-130002"]/div[@class="desc"]/ul/li'
    specification.update(get_specs_main(doc,features_path))

    util.replaceKey(specification,'storage capacity','capacity')
    util.replaceKey(specification,'usb version','interface')
    
    pd['specification']=specification

    pd['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in pd:
        product_history['price']=pd['price']
    if 'shipping' in pd:
        product_history['shipping']=pd['shipping']
    if 'availabilty' in pd:
        product_history['availabilty']=1
    product_history['time']=pd['last_modified_datetime']
    pd['product_history']=product_history
    pd['site']='saholic'
    return pd


def scrapAllPDs():
    urls=getAllPDUrls()
    pds=[]
    failed=[]
    dl.putUrls(urls)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            print r
            pd=getPDFromPage(string=html)
            if pd:
                pd['url']=r
                pds.append(pd)
        else:
            failed.append('%s with %s'%(r,str(status)))
    return pds

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    pd_coll=db['scraped_pendrives']
    pd_coll.create_index('url',unique=True)
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    pds=scrapAllPDs()
    for pd in pds:
        try:
            pd_coll.insert(pd,safe=True)
            inserted_count+=1
            inserted_urls.append(pd['url'])
        except pymongo.errors.DuplicateKeyError:
            pd_coll.update({'url':pd['url']},{'$push':{'product_history':pd['product_history']},'$set':{'last_modified_datetime':datetime.datetime.now()}})
            updated_count+=1
            updated_urls.append(pd['url'])
    if log:
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'pendrive','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
    print "%d inserted and %d updated"%(inserted_count,updated_count)

if __name__=='__main__':
    insertIntoDB()
