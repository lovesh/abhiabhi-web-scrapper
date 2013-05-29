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
tablet_home='http://www.homeshop18.com/ipads-2f-tablets/category:8937/'

dl=downloader.Downloader()
dl.addHeaders({'Host':'www.homeshop18.com','Referer':tablet_home})
debug=True
DBName='abhiabhi'

count_pattern=re.compile('\((\d+)\)')
name_pattern=re.compile('(.*?)\(')
brand_pattern=re.compile('^\w+')
shipping_pattern=re.compile('(\d+)-(\d+)')
storage_pattern=re.compile('\d{3} ?(mb)?',re.I)
os_pattern=re.compile('Android ?(ics)? ?(2.3|2.4|3.1|3.2|4.0)?',re.I)

def getTabletUrlsFromPage(html):
    tablet_url_path='//p[@class="product_title"]/a'
    page_dom=dom.DOM(string=html)
    links=set(l[1] for l in page_dom.getLinksWithXpath(tablet_url_path))
    return links

def getAllTabletUrls():
    count_path='//div[@class="browse_result_title"]'
    doc=dom.DOM(url=tablet_home)
    count=doc.getNodesWithXpath(count_path)[0].text_content()
    m=count_pattern.search(count)
    if m:
        count=int(m.group(1))
    pager_base_url=tablet_home.replace('category:','categoryid:')
    page_urls=[pager_base_url+'search:*/start:'+str(n) for n in xrange(0,count,24)]
    dl.putUrls(page_urls)
    pages=dl.download()
    tablet_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            tablet_urls.extend(getTabletUrlsFromPage(html))
    return tablet_urls

def getTabletFromPage(url=None,string=None):
    tablet={}
    if url:
        doc=dom.DOM(url=url)
        tablet['url']=url
    else:
        doc=dom.DOM(string=string)
        
    name_path='//h1[@id="productLayoutForm:pbiName"]'
    tablet['name']=doc.getNodesWithXpath(name_path)[0].text.strip()
    image_path='//meta[@property="og:image"]'
    tablet['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    price_path='//span[@id="productLayoutForm:OurPrice"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        tablet['price']=int(price[0].text.strip('Rs. '))

    addBox=doc.getNodesWithXpath('//a[@id="productLayoutForm:addToCartAction"]')

    if addBox:                           #availability check
        tablet['availability']=1
        shipping_path='//div[@class="pdp_details_deliveryTime"]'
        shipping=doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping=shipping_pattern.search(shipping[0].text)
            tablet['shipping']=[int(shipping.group(1)),int(shipping.group(2))]
    else:
        tablet['availability']=0

    offer_path='//div[@class="pdp_details_offer_text"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        tablet['offer']=offer[0].text.strip()

    specs_path='//table[@class="specs_txt"]/tbody'
    specification_tables=doc.getNodesWithXpath(specs_path)
    specification={}
    if len(specification_tables)>0:
        for specs in specification_tables:
            specs=doc.parseTBodyNode(specs)
            if len(specs)>0:
                specification.update(specs)

    util.replaceKey(specification,'cpu/processor','processor')
    util.replaceKey(specification,'cpu','processor')
    util.replaceKey(specification,'operating system','os')

    if 'memory' in specification:
	m=re.search('([2-9]|\d{2}) ?gb',specification['memory'])
	if m:
	    util.replaceKey(specification,'memory','storage')
	else:
	    util.replaceKey(specification,'memory','ram')

    util.replaceKey(specification,'internal memory','storage')
    util.replaceKey(specification,'internal storage','storage')
    util.replaceKey(specification,'memory size','storage')
    util.replaceKey(specification,'rom','storage')
    util.replaceKey(specification,'rom (storage)','storage')

    if 'ram' not in specification:
        if 'storage' in specification:
            m=storage_pattern.search(specification['storage'])
            if m:
                specification['ram']=m.group()

    if 'os' not in specification:
        m=os_pattern.search(tablet['name'])
        if m:
            specification['os']=m.group()

    if 'brand' in specification:
        tablet['brand']=specification['brand']
    else:
        brand=brand_pattern.search(tablet['name']).group()
        if brand not in ['Google','Barnes','Ainol']:
            tablet['brand']=brand
        else:
            if brand=='Google':
                tablet['brand']='Google Nexus'
            if brand=='Barnes':
                tablet['brand']='Barnes & Noble'
            if brand=='Ainol':
                tablet['brand']='Aionol Novo'
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
    tablet['site']='homeshop18'
    return tablet


def scrapAllTablets():
    urls=getAllTabletUrls()
    tablets=[]
    failed=[]
    dl.putUrls(urls,2)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if len(html) < 2000:
            status = 0
            failed.append(r)
        if status > 199 and status < 400:
            print r
            tablet=getTabletFromPage(string=html)
            if tablet:
                tablet['url']=r
                tablets.append(tablet)
    
    while len(failed) > 0:
        dl.putUrls(failed, 2)
        result = dl.download()
        failed = []
        for r in result:
            status=result[r][0]
            html=result[r][1]
            if len(html) < 2000:
                status = 0
                failed.append(r)
            if status > 199 and status < 400:
                print r
                tablet=getTabletFromPage(string = html)
                if tablet:
                    tablet['url'] = r
                    tablets.append(tablet)
                    
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
