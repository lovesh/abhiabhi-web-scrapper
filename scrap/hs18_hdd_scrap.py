import downloader
import dom
import urllib2
import re
import time
import datetime
import math
import pymongo
import util

siteurl='http://www.homeshop18.com'
hdd_home='http://www.homeshop18.com/2-5-22-portable-hard-disk-drives/category:8857/'

dl=downloader.Downloader()
dl.addHeaders({'Host':'www.homeshop18.com','Referer':hdd_home})
debug=True
DBName='abhiabhi'

count_pattern = re.compile('\((\d+)\)')
name_pattern = re.compile('(.*?)\(')
brand_pattern = re.compile('\w+')
shipping_pattern = re.compile('(\d+)-(\d+)')
capacity_pattern = re.compile('\d+\.?\d? ?(G|T)B',re.I)
interface_pattern = re.compile('usb ?\d\.?\d',re.I)
speed_pattern = re.compile('\d+ ?Mbp\/s',re.I)
warranty_pattern = re.compile('\d+ years?',re.I)
size_pattern = re.compile('\d\.\d ?(\"|\'\'|inch)')

def getHDDUrlsFromPage(html):
    hdd_url_path='//p[@class="product_title"]/a'
    page_dom=dom.DOM(string=html)
    links=set(l[1] for l in page_dom.getLinksWithXpath(hdd_url_path))
    return links

def getAllHDDUrls():
    count_path='//div[@class="browse_result_title"]'
    doc=dom.DOM(url=hdd_home)
    count=doc.getNodesWithXpath(count_path)[0].text_content()
    m=count_pattern.search(count)
    if m:
        count=int(m.group(1))
    pager_base_url=hdd_home.replace('category:','categoryid:')
    page_urls=[pager_base_url+'search:*/start:'+str(n) for n in xrange(0,count,24)]
    dl.putUrls(page_urls)
    pages=dl.download()
    hdd_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            hdd_urls.extend(getHDDUrlsFromPage(html))
    return hdd_urls

def getHDDFromPage(url=None,string=None):
    hdd={}
    if url:
        doc=dom.DOM(url=url)
        hdd['url']=url
    else:
        doc=dom.DOM(string=string)
        
    name_path='//h1[@id="productLayoutForm:pbiName"]'
    hdd['name']=doc.getNodesWithXpath(name_path)[0].text.strip()
    brand=brand_pattern.search(hdd['name']).group().lower()
    if brand=='western':
        hdd['brand']='western digital'
    else:
        hdd['brand']=brand
    image_path='//meta[@property="og:image"]'
    hdd['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    price_path='//span[@id="productLayoutForm:OurPrice"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        hdd['price']=int(price[0].text.strip('Rs. '))

    addBox=doc.getNodesWithXpath('//a[@id="productLayoutForm:addToCartAction"]')

    if addBox:                           #availability check
        hdd['availability']=1
        shipping_path='//div[@class="pdp_details_deliveryTime"]'
        shipping=doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping=shipping_pattern.search(shipping[0].text)
            hdd['shipping']=[int(shipping.group(1)),int(shipping.group(2))]
    else:
        hdd['availability']=0

    warranty_path='//table[@class="productShippingInfo"]'
    warranty=doc.getNodesWithXpath(warranty_path)
    if warranty:
        m=warranty_pattern.search(warranty[0].text_content())
        if m:
            hdd['warranty']=m.group()

    offer_path='//div[@class="hddp_details_offer_text"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        hdd['offer']=offer[0].text.strip()

    hdd['specification']={}
    
    m=interface_pattern.search(hdd['name'])
    if m:
        hdd['specification']['interface']=m.group()
    m=capacity_pattern.search(hdd['name'])
    if m:
        hdd['specification']['capacity']=m.group()
    m=size_pattern.search(hdd['name'])
    if m:
        hdd['specification']['size']=m.group()

    specs_path='//table[@class="specs_txt"][2]/tbody'
    specs=doc.getNodesWithXpath(specs_path)
    if specs:
        specs=specs[0].text_content()
        m=interface_pattern.search(specs)
        if m:
            hdd['specification']['interface']=m.group()
        m=capacity_pattern.search(specs)
        if m:
            hdd['specification']['capacity']=m.group()
        m=speed_pattern.search(specs)
        if m:
            hdd['specification']['speed']=m.group()
	m=size_pattern.search(specs)
        if m:
            hdd['specification']['size']=m.group()
    
    hdd['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in hdd:
        product_history['price']=hdd['price']
    if 'shipping' in hdd:
        product_history['shipping']=hdd['shipping']
    product_history['availability']=hdd['availability']
    product_history['datetime']=hdd['last_modified_datetime']
    hdd['product_history']=[product_history,]
    hdd['site']='homeshop18'
    return hdd

def scrapAllHDDs():
    urls=getAllHDDUrls()
    hdds=[]
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
            hdd=getHDDFromPage(string=html)
            if hdd:
                hdd['url']=r
                hdds.append(hdd)
    
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
                hdd=getHDDFromPage(string = html)
                if hdd:
                    hdd['url'] = r
                    hdds.append(hdd)
                    
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
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'harddisk','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
	
    print "%d inserted and %d updated"%(inserted_count,updated_count)

if __name__=='__main__':
    insertIntoDB()
