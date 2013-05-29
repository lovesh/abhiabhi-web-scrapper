import downloader
import dom
import re
import urllib
import math
import datetime
import pymongo
import simplejson as json
import util

siteurl='http://www.flipkart.com'
referer='http://www.flipkart.com/computers/laptops/all'
logfile = open('fk_laptops_log.txt','w')
debug = True
dl = downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})
ajax_dl = downloader.Downloader()

name_pattern = re.compile('(^[\w \.\d-]+?)(\/|1st|2nd|3rd|Ci|dual|quad|c2d|core|dc|pentium|intel|amd|celeron|pdc|\(|$|\d{2}\.\d+)',re.I)
#junk_pattern = re.compile('\/|2nd|3rd|Ci|dual|quad|c2d|core|dc|pentium|intel|celeron|pdc',re.I)
shipping_pattern = re.compile('(\d+)-(\d+)')
ram_pattern = re.compile('((?:1\d|(?:\W|^)\d) ?GB)(.+)?',re.I)

def getAllLaptopUrls():
    count_path='//div[@class="unit fk-lres-header-text"]/b[2]'
    laptop_url_path='//a[@class="title tpadding5 fk-anchor-link"]'
    
    doc = dom.DOM(url = referer)
    count = int(doc.getNodesWithXpath(count_path)[0].text)
    page_urls=[referer+'?response-type=json&inf-start='+str(n) for n in xrange(0,count,20)]
    laptop_urls = set()
    ajax_dl.putUrls(page_urls)
    pages = ajax_dl.download()
    if debug:
        print '%d Pages found\n'%len(pages)
    for p in pages:
        status = pages[p][0]
        html = pages[p][1]
        if status > 199 and status < 400:
            json_response = json.loads(html)
            count = json_response['count']
            print count
            if count == 0:
                flag = False
                continue
            laptop_urls.update(getLaptopUrlsOfPage(html = json_response['html']))
    return laptop_urls

def getLaptopUrlsOfPage(url = None,html = None):
    html = html.replace('\n','')
    html = html.replace('\t','')
    html = html.replace('\"','"')
    doc = dom.DOM(string = html)
    laptop_url_path='//a[@class="title tpadding5 fk-anchor-link"]'
    urls = set(siteurl+link[1] for link in doc.getLinksWithXpath(laptop_url_path))
    return urls

def getLaptopFromPage(url = None,string = None):
    laptop={}
    if url:
        doc = dom.DOM(url = url)
        laptop['url']=url
    else:
        doc = dom.DOM(string = string)
        url_path='//link[@rel="canonical"]'        
        url = doc.getNodesWithXpath(url_path)
        if len(url)==0:
            return False
        laptop['url']=url[0].get('href')
    if debug:
        print laptop['url']
    addBox = doc.getNodesWithXpath('//div[@id="mprod-buy-btn"]')
    name_path='//div[@class="mprod-summary-title fksk-mprod-summary-title"]/h1'
    name = doc.getNodesWithXpath(name_path)[0].text
    #junk = junk_pattern.search(name)
    #if junk:
        #name = name[:junk.start()]
    #laptop['name']=name.strip(' (')
    m = name_pattern.search(name)
    if m:
	laptop['name']=m.group(1).strip(' -(')
    else:
	return False
    image_path='//meta[@property="og:image"]'
    laptop['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    laptop['brand']=re.search('\w+',laptop['name']).group().lower()
    price_path='//span[@id="fk-mprod-our-id"]'
    price = doc.getNodesWithXpath(price_path)
    if len(price)>0:
        laptop['price']=int(price[0].text_content().strip('Rs. '))
    if addBox:                           #availability check
        laptop['availability']=1
        shipping_path='//div[@class="shipping-details"]/span[@class="boldtext"]'
        shipping = doc.getNodesWithXpath(shipping_path)[0].text
        if shipping:
            m = shipping_pattern.search(shipping)
            if m:
                laptop['shipping']=(m.group(1),m.group(2))
    else:
        laptop['availability']=0

    laptop['specification']=[]
    specification_tables_path='//table[@class="fk-specs-type2"]'
    specification_tables = doc.getNodesWithXpath(specification_tables_path)
    specs_key_path='th[@class="specs-key"]'
    specs_value_path='td[@class="specs-value"]'

    offer_path='//div[@class="fk-product-page-offers rposition a-hover-underline"]//td[@class="fk-offers-text"]'
    offer = doc.getNodesWithXpath(offer_path)
    if offer:
        laptop['offer']=offer[0].text_content().replace('\r\n ','')

    specification={}
    if len(specification_tables)>0:
        for specs in specification_tables:
            trs = specs.xpath('tr')
            for tr in trs:
                type_node = tr.xpath('th[@class="group-head"]')
                if len(type_node)==1:
                    type = type_node[0].text_content().strip().lower()
                    specification[type]={}
                th = tr.xpath(specs_key_path)
                if len(th)>0:
                    key = th[0].text
                    if key:
                        key = key.strip().lower()
                        value = tr.xpath(specs_value_path)[0].text
                        if value:
                            value = value.strip().lower()
                            specification[type][key]=value     #only put specification if value is not None
    if 'memory' in specification:
        util.replaceKey(specification['memory'],'system memory','ram')
	if specification['memory']['ram']:
	    ram = ram_pattern.search(specification['memory']['ram']).group(1)
	    memory_type = ram_pattern.search(specification['memory']['ram']).group(2)
	    if ram:
		specification['memory']['ram']=ram.strip()
	    if memory_type:
		specification['memory']['memory type']=memory_type.strip()
    
    if 'storage' in specification:
	util.replaceKey(specification['storage'],'hdd capacity','capacity')
	util.replaceKey(specification['storage'],'hardware interface','type')
    
    if 'display' in specification:
	util.replaceKey(specification['display'],'screen size','size')
	util.replaceKey(specification['display'],'screen type','type')
	
    util.replaceKey(specification,'platform','software')
    
    if 'software' in specification:
	util.replaceKey(specification['software'],'operating system','os')
	
    laptop['specification']=specification
    laptop['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in laptop:
        product_history['price']=laptop['price']
    if 'shipping' in laptop:
        product_history['shipping']=laptop['shipping']
    product_history['availability']=laptop['availability']
    product_history['datetime']=laptop['last_modified_datetime']
    laptop['product_history']=[product_history,]
    laptop['site']='flipkart'
    return laptop

def scrapAllLaptops():
    urls = getAllLaptopUrls()
    print "%d urls found"%len(urls)
    laptops=[]
    failed=[]
    dl.putUrls(urls,5)
    result = dl.download()
    for r in result:
        status = result[r][0]
        html = result[r][1]
        if status > 199 and status < 400:
            laptop = getLaptopFromPage(string = html)
            if laptop:
                laptops.append(laptop)
        else:
            failed.append('%s with %s'%(r,str(status)))
    
    print "%d laptops found"%len(laptops)
    return laptops

def insertIntoDB(log = True):
    con = pymongo.Connection('localhost',27017)
    db = con['abhiabhi']
    laptop_coll = db['scraped_laptops']
    laptop_coll.create_index('url',unique = True)
    inserted_count = 0
    updated_count = 0
    inserted_urls=[]
    updated_urls=[]
    laptops = scrapAllLaptops()
    for laptop in laptops:
        try:
            laptop_coll.insert(laptop,safe = True)
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
        scrap_log = db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'laptop','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)

    print "%d inserted and %d updated"%(inserted_count,updated_count)
    
if __name__=='__main__':
    insertIntoDB()
