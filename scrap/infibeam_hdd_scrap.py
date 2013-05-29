import downloader
import dom
import re
import urllib
import math
import datetime
import pymongo
import simplejson as json
import util

siteurl='http://www.infibeam.com'
referer='http://www.infibeam.com/Hard_Disk_Computers_Accessories/search'
ajax_url='http://www.infibeam.com/Hard_Disk_Computers_Accessories/Search_ajax.action?category=Hard_Disk&store=Computers_Accessories&page='

logfile=open('infibeam_hdd_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})
debug=True

junk_pattern=re.compile('\(|Hard|Portable',re.I)
brand_pattern=re.compile('\w+')
shipping_pattern=re.compile('(\d+)-(\d+)',re.I)
capacity_pattern=re.compile('(\d+\.?\d?) ?(g|t)b',re.I)
interface_pattern=re.compile('usb ?\d\.?\d',re.I)

def getHDDUrlsOfPage(html):
    hdd_url_path='//ul[@class="srch_result frame"]/li/a'
    page_dom=dom.DOM(string=html)
    links=set(siteurl+l[1] for l in page_dom.getLinksWithXpath(hdd_url_path))
    return links

def getAllHDDUrls():
    count_path='//div[@id="resultsPane"]/div/div/b[2]'
    doc=dom.DOM(url=referer)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    num_pages=int(math.ceil(count/20.0))
    page_urls=[ajax_url+str(n) for n in xrange(1,num_pages+1)]
    dl.putUrls(page_urls)
    pages=dl.download()
    hdd_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            hdd_urls.extend(getHDDUrlsOfPage(html))
    return hdd_urls

def getHDDFromPage(url=None,string=None):
    hdd={}
    if url:
        doc=dom.DOM(url=url)
    else:
        doc=dom.DOM(string=string)
    addBox=doc.getNodesWithXpath('//input[@class="buyimg "]')

    title_path='//div[@id="ib_details"]/h1'
    title=doc.getNodesWithXpath(title_path)[0].text_content().strip()
      
    junk=junk_pattern.search(title)
    if junk:
        hdd['name']=title[:junk.start()].strip()
    else:
        hdd['name']=title

    brand=brand_pattern.search(hdd['name']).group().lower()
    if brand in ['western','wd']:
        hdd['brand']='western digital'
    elif brand=='a':
        hdd['brand']='a-data'
    elif brand=='silicon':
        hdd['brand']='silicon power'
    else:
        hdd['brand']=brand

    if addBox:                           #availability check
        hdd['availability']=1
        details_path='//div[@id="ib_details"]'
        details=doc.getNodesWithXpath(details_path)
        if details:
            details=details[0].text_content()
            shipping=shipping_pattern.search(details)
            if shipping:
                hdd['shipping']=[shipping.group(1),shipping.group(2)]
    else:
	hdd['availability']=0
    offer_path='//div[@class="offer"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        hdd['offer']=offer[0].text_content().replace('\r\n ','')
    
    color_path='//a[@class="colorlink"]'
    colors=doc.getNodesWithXpath(color_path)
    hdd['colors']=[color.get('text') for color in colors]
        
    price_path='//span[@class="infiPrice amount price"]'
    price=doc.getNodesWithXpath(price_path)
    if price:
        hdd['price']=int(price[0].text.replace(',',''))
    img_path="//div[@id='ib_img_viewer']/img"
    hdd['img_url']={'0':doc.getImgUrlWithXpath(img_path)}

    desc_path='//div[@class="reviews-box-cont-inner"]'
    desc=doc.getNodesWithXpath(desc_path)
    if desc:
        hdd['description']=desc[0].text_content.strip()
    
    specs_path='//div[@id="specs"]/div'
    specs=doc.getNodesWithXpath(specs_path)
    specification={}
    for spec in specs:
        text=spec.xpath('a')[0].text.strip()
        if text=='Deliverable Locations' or text=='Disclaimer':
            continue
        if text=='Warranty':
            div=spec.xpath('.//div')
            if div:
                hdd['warranty']=div[0].text_content().strip().lower()
                continue
        trs=spec.xpath('.//tr')
        for tr in trs:
            tds=tr.xpath('.//td')
            if len(tds)<2:
                continue
            key=tds[0].text_content().strip(':\n\t ').replace('.','').lower()
            value=tds[1].text_content().strip(':\n\t ').lower()
            specification[key]=value
    
    if 'usb 20' in specification:
        if 'interface' in specification:
            del(specification['usb 20'])
        elif specification['usb 20']=='available' or specification['usb 20']=='yes':
            specification['inteface']='usb 2.0'
	    del(specification['usb 20'])

    if 'usb 30' in specification:
        if 'interface' in specification:
            del(specification['usb 30'])
        elif specification['usb 30']=='available' or specification['usb 30']=='yes':
            specification['inteface']='usb 3.0'
	    del(specification['usb 30'])
	    
    util.replaceKey(specification,'hard disk size','capacity')
    util.replaceKey(specification,'spin speed (rpm)','speed')

    if 'interface' not in specification:
        m=interface_pattern.search(title)
        if m:
            specification['interface']=m.group()

    if 'capacity' not in specification:
        m=capacity_pattern.search(title) 
        if m:
            specification['capacity']=m.group()

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
    hdd['site']='infibeam'
    return hdd

def scrapAllHDDs():
    urls=getAllHDDUrls()
    hdds=[]
    dl.putUrls(urls)
    result=dl.download()
    for r in result:
        print r
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            print r
            hdd=getHDDFromPage(string=html)
            hdd['url']=r
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

