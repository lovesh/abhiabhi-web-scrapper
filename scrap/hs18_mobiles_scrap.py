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
mobile_home='http://www.homeshop18.com/mobiles/category:14569/'

dl=downloader.Downloader()
dl.addHeaders({'Host':'www.homeshop18.com','Referer':mobile_home})
debug=True
DBName='abhiabhi'

count_pattern=re.compile('\((\d+)\)')
name_pattern=re.compile('(.*?)\(')
brand_pattern=re.compile('\w+')
shipping_pattern=re.compile('(\d+)-(\d+)')
warranty_pattern=re.compile('\d+ years?',re.I)

def getMobileUrlsFromPage(html):
    mobile_url_path='//p[@class="product_title"]/a'
    page_dom=dom.DOM(string=html)
    links=set(l[1] for l in page_dom.getLinksWithXpath(mobile_url_path))
    return links

def getAllMobileUrls():
    count_path='//div[@class="browse_result_title"]'
    doc=dom.DOM(url=mobile_home)
    count=doc.getNodesWithXpath(count_path)[0].text_content()
    m=count_pattern.search(count)
    if m:
        count=int(m.group(1))
    pager_base_url=mobile_home.replace('category:','categoryid:')
    page_urls=[pager_base_url+'search:*/start:'+str(n) for n in xrange(0,count,24)]
    dl.putUrls(page_urls)
    pages=dl.download()
    mobile_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            mobile_urls.extend(getMobileUrlsFromPage(html))
    return mobile_urls

def getMobileFromPage(url=None,string=None):
    mobile={}
    if url:
        doc=dom.DOM(url=url)
        mobile['url']=url
    else:
        doc=dom.DOM(string=string)
        
    name_path='//h1[@id="productLayoutForm:pbiName"]'
    mobile['name']=doc.getNodesWithXpath(name_path)[0].text.strip()
    brand=brand_pattern.search(mobile['name']).group().lower()
    if re.match('sony ?ericsson',mobile['name'],re.I):
        mobile['brand']='sony ericsson'
    else:
        mobile['brand']=brand
    image_path='//meta[@property="og:image"]'
    mobile['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    price_path='//span[@id="productLayoutForm:OurPrice"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        mobile['price']=int(price[0].text.strip('Rs. '))

    addBox=doc.getNodesWithXpath('//a[@id="productLayoutForm:addToCartAction"]')

    if addBox:                           #availability check
        mobile['availability']=1
        shipping_path='//div[@class="pdp_details_deliveryTime"]'
        shipping=doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping=shipping_pattern.search(shipping[0].text_content())
            mobile['shipping']=[int(shipping.group(1)),int(shipping.group(2))]
    else:
        mobile['availability']=0
    warranty_path='//table[@class="productShippingInfo"]'
    warranty=doc.getNodesWithXpath(warranty_path)
    if warranty:
        m=warranty_pattern.search(warranty[0].text_content())
        if m:
            mobile['warranty']=m.group()

    offer_path='//div[@class="mobilep_details_offer_text"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        mobile['offer']=offer[0].text.strip()
    
    sizedrop_path='//select[@id="productLayoutForm:sizedrop"]'
    sizedrop=doc.getNodesWithXpath(sizedrop_path)
    if sizedrop:
        colors=[]
        options=sizedrop[0].xpath('option')
        for option in options:
            value=option.get('value')
            if value=="":
                continue
            colors.append(option.text.strip())
        mobile['color']=colors

    mobile['specification']={}
    specification_tables_path='//table[@class="specs_txt"]/tbody'
    specification_tables=doc.getNodesWithXpath(specification_tables_path)
    if len(specification_tables)>0:
        for table in specification_tables:
            specs=doc.parseTBodyNode(table)
            if len(specs)>0:
                if table.xpath('tr[1]/th'):
                    if table.xpath('tr[1]/th')[0].text=='Design &amp; Display':
                        util.replaceKey(specs,'type','display type')
                        util.replaceKey(specs,'size','display size')
                if table.xpath('tr[1]/th'):
                    if table.xpath('tr[1]/th')[0].text=='Battery':
                        util.replaceKey(specs,'type','battery type')
                mobile['specification'].update(specs) 
    util.replaceKey(mobile['specification'],'3.5 mm audio jack','3 1/2 mm jack')
    util.replaceKey(mobile['specification'],'3.5mm audio jack','3 1/2 mm jack')
    util.replaceKey(mobile['specification'],'3.5 mm jack','3 1/2 mm jack')
    util.replaceKey(mobile['specification'],'3.5mm jack','3 1/2 mm jack')
    if '3.5g' in mobile['specification']:
        del(mobile['specification']['3.5g'])

    mobile['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in mobile:
        product_history['price']=mobile['price']
    if 'shipping' in mobile:
        product_history['shipping']=mobile['shipping']
    product_history['availability']=mobile['availability']
    product_history['datetime']=mobile['last_modified_datetime']
    mobile['product_history']=[product_history,]
    mobile['site']='homeshop18'
    return mobile

def scrapAllMobiles():
    urls = getAllMobileUrls()
    mobiles = []
    failed = []
    dl.putUrls(urls,2)
    result = dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if len(html) < 2000:
            status = 0
            failed.append(r)
        if status > 199 and status < 400:
            print r
            mobile=getMobileFromPage(string = html)
            if mobile:
                mobile['url'] = r
                mobiles.append(mobile)
    
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
                mobile=getMobileFromPage(string = html)
                if mobile:
                    mobile['url'] = r
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

