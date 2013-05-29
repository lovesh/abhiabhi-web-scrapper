import downloader
import dom
import urllib2
import re
import time
import datetime
import math
import pymongo
import util
import requests

siteurl='http://www.homeshop18.com'
pd_home='http://www.homeshop18.com/pen-drives/usb-pen-drives/categoryid:8899/search:pen+drives/'
ajax_url='http://www.homeshop18.com/shop/faces/servlet/PILServlet1?dummy='

dl=downloader.Downloader()
dl.addHeaders({'Host':'www.homeshop18.com','Referer':pd_home})
debug=True
DBName='abhiabhi'

count_pattern=re.compile('of (\d+)',re.I)
name_pattern=re.compile('(.*?)\(')
brand_pattern=re.compile('\w+')
shipping_pattern=re.compile('(\d+)-(\d+)')
capacity_pattern=re.compile('\d+ ?GB',re.I)
interface_pattern=re.compile('usb ?\d\.?\d',re.I)
warranty_pattern=re.compile('\d+ years?',re.I)
ajax_price_pattern=re.compile('\$\$[\d\.]+\$\$(\d+)\.\d*\$\$')

def getPDUrlsFromPage(html):
    pd_url_path='//p[@class="product_title"]/a'
    page_dom=dom.DOM(string=html)
    links=set(l[1] for l in page_dom.getLinksWithXpath(pd_url_path))
    return links

def getAllPDUrls():
    count_path='//div[@class="lf"]'
    doc=dom.DOM(url=pd_home)
    count=doc.getNodesWithXpath(count_path)[0].text_content()
    m=count_pattern.search(count)
    if m:
        count=int(m.group(1))
    pager_base_url=pd_home.replace('category:','categoryid:')
    page_urls=[pager_base_url+'search:*/start:'+str(n) for n in xrange(0,count,24)]
    dl.putUrls(page_urls)
    pages=dl.download()
    pd_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            pd_urls.extend(getPDUrlsFromPage(html))
    return pd_urls

def getPDFromPage(url=None,string=None):
    pd={}
    if url:
        doc=dom.DOM(url=url)
        pd['url']=url
    else:
        doc=dom.DOM(string=string)
        
    name_path='//h1[@id="productLayoutForm:pbiName"]'
    pd['name']=doc.getNodesWithXpath(name_path)[0].text.strip()
    brand=brand_pattern.search(pd['name']).group().lower()
    if brand=='silicon':
        pd['brand']='silicon power'
    elif brand=='moser':
	pd['brand']='moser baer'
    else:
        pd['brand']=brand
	
    image_path='//meta[@property="og:image"]'
    pd['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    price_path='//span[@id="productLayoutForm:OurPrice"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        pd['price']=int(price[0].text.strip('Rs. '))

    addBox=doc.getNodesWithXpath('//a[@id="productLayoutForm:addToCartAction"]')

    if addBox:                           #availability check
        pd['availability']=1
        shipping_path='//div[@class="pdp_details_deliveryTime"]'
        shipping=doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping=shipping_pattern.search(shipping[0].text)
            pd['shipping']=[int(shipping.group(1)),int(shipping.group(2))]
    else:
        pd['availability']=0

    warranty_path='//table[@class="productShippingInfo"]'
    warranty=doc.getNodesWithXpath(warranty_path)
    if warranty:
        m=warranty_pattern.search(warranty[0].text_content())
        if m:
            pd['warranty']=m.group()

    offer_path='//div[@class="pdp_details_offer_text"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        pd['offer']=offer[0].text.strip()

    pd['specification']={}

    sizedrop_path='//select[@id="productLayoutForm:sizedrop"]'
    sizedrop=doc.getNodesWithXpath(sizedrop_path)
    if sizedrop:
        prices=[]
        options=sizedrop[0].xpath('option')
        print len(options)
        for option in options:
            value=option.get('value')
            print value
            if value=="":
                continue
            r=requests.post(ajax_url,data={'itemColorCode':value+',2'})
            cap=capacity_pattern.search(r.content).group()
            price=int(ajax_price_pattern.search(r.content).group(1))
            prices.append((cap,price))
    else:
        m=capacity_pattern.search(pd['name'])
        if m:
            pd['specification']['capacity']=m.group()
    
    m=interface_pattern.search(pd['name'])
    if m:
        pd['specification']['interface']=m.group()

    specs_path='//table[@class="specs_txt"][2]/tbody'
    specs=doc.getNodesWithXpath(specs_path)
    if specs:
        specs=specs[0].text_content()
        m=interface_pattern.search(specs)
        if m:
            pd['specification']['interface']=m.group()
        m=capacity_pattern.search(specs)
        if m:
            pd['specification']['capacity']=m.group()

    pd['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in pd:
        product_history['price']=pd['price']
    if 'shipping' in pd:
        product_history['shipping']=pd['shipping']
    product_history['availability']=pd['availability']
    product_history['datetime']=pd['last_modified_datetime']
    pd['product_history']=[product_history,]
    pd['site']='homeshop18'
    if sizedrop:
        print prices
        pds=[]
        for price in prices:
            pd['specification']['capacity']=price[0]
            pd['price']=price[1]
            pd['product_history'][0]['price']=price[1]
            pds.append(pd)
        return pds
    else:        
        return pd

def scrapAllPDs():
    urls=getAllPDUrls()
    pds=[]
    failed=[]
    dl.putUrls(urls,2)
    result=dl.download()
    for r in result:
        print r
        status=result[r][0]
        html=result[r][1]
        if html is None or len(html) < 2000:
            print "bad data with status %s found"%str(status)
            status = 0
            failed.append(r)
        if status > 199 and status < 400:
            pd=getPDFromPage(string=html)
            if pd and type(pd) is dict:
                pd['url']=r
                pds.append(pd)
            if pd and type(pd) is list:
                for (offset,p) in enumerate(pd):
                    p['url']=r+'?#'+str(offset+1)
                    pds.append(p)
    
    while len(failed) > 0:
        dl.putUrls(failed, 2)
        result = dl.download()
        failed = []
        for r in result:
            print r
            status=result[r][0]
            html=result[r][1]
            if html is None or len(html) < 2000:
                print "bad data with status %s found"%str(status)
                status = 0
                failed.append(r)
            if status > 199 and status < 400:
                pd=getPDFromPage(string=html)
                if pd and type(pd) is dict:
                    pd['url']=r
                    pds.append(pd)
                if pd and type(pd) is list:
                    for (offset,p) in enumerate(pd):
                        p['url']=r+'?#'+str(offset+1)
                        pds.append(p)
    
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
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability' in pd:
                upd['availability']=pd['availability']
            if 'price' in pd:
                upd['price']=pd['price']
            if 'shipping' in pd:
                upd['shipping']=pd['shipping']
	    if 'offer' in pd:
                upd['offer']=pd['offer']
	    else:
		upd['offer']=''
            pd_coll.update({'url':pd['url']},{'$push':{'product_history':pd['product_history'][0]},'$set':upd})
            updated_count+=1
            updated_urls.append(pd['url'])
    if log:
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'pendrive','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
	
    print "%d inserted and %d updated"%(inserted_count,updated_count)

if __name__=='__main__':
    insertIntoDB()
