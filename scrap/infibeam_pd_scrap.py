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
referer='http://www.infibeam.com/Pen_Drives_Computers_Accessories/search'
ajax_url='http://www.infibeam.com/Pen_Drives_Computers_Accessories/Search_ajax.action?category=Pen_Drives&store=Computers_Accessories&page='


logfile=open('infibeam_pd_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})
debug=True

brand_pattern=re.compile('\w+')
shipping_pattern=re.compile('(\d+)-(\d+)',re.I)
capacity_pattern=re.compile('\d+ ?(G|T)B',re.I)
interface_pattern=re.compile('usb ?\d\.?\d',re.I)

def getPDUrlsOfPage(html):
    pd_url_path='//ul[@class="srch_result frame"]/li/a'
    page_dom=dom.DOM(string=html)
    links=set(siteurl+l[1] for l in page_dom.getLinksWithXpath(pd_url_path))
    return links

def getAllPDUrls():
    count_path='//div[@id="resultsPane"]/div/div/b[2]'
    doc=dom.DOM(url=referer)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    num_pages=int(math.ceil(count/20.0))
    page_urls=[ajax_url+str(n) for n in xrange(1,num_pages+1)]
    dl.putUrls(page_urls)
    pages=dl.download()
    pd_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            pd_urls.extend(getPDUrlsOfPage(html))
    return pd_urls

def getPDFromPage(url=None,string=None):
    pd={}
    if url:
        doc=dom.DOM(url=url)
    else:
        doc=dom.DOM(string=string)
    addBox=doc.getNodesWithXpath('//input[@class="buyimg "]')

    title_path='//div[@id="ib_details"]/h1'
    title=doc.getNodesWithXpath(title_path)[0].text_content().strip()
      
    pd['name']=title

    brand=brand_pattern.search(pd['name']).group().lower()
    if brand=='a':
        pd['brand']='a-data'     
    elif brand=='moser':
	pd['brand']='moser baer'
    else:
        pd['brand']=brand

    if addBox:                           #availability check
        pd['availability']=1
        details_path='//div[@id="ib_details"]'
        details=doc.getNodesWithXpath(details_path)
        if details:
            details=details[0].text_content()
            shipping=shipping_pattern.search(details)
            if shipping:
                pd['shipping']=[shipping.group(1),shipping.group(2)]
    else:
        pd['availability']=0

    offer_path='//div[@class="offer"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        pd['offer']=offer[0].text_content().replace('\r\n ','')
    
    color_path='//a[@class="colorlink"]'
    colors=doc.getNodesWithXpath(color_path)
    pd['colors']=[color.get('text') for color in colors]
        
    price_path='//span[@class="infiPrice amount price"]'
    price=doc.getNodesWithXpath(price_path)
    if price:
        pd['price']=int(price[0].text.replace(',',''))
    img_path="//div[@id='ib_img_viewer']/img"
    pd['img_url']={'0':doc.getImgUrlWithXpath(img_path)}

    desc_path='//div[@class="reviews-box-cont-inner"]'
    desc=doc.getNodesWithXpath(desc_path)
    if desc:
        pd['description']=desc[0].text_content.strip()
    
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
                pd['warranty']=div[0].text_content().strip().lower()
                continue
        trs=spec.xpath('.//tr')
        for tr in trs:
            tds=tr.xpath('.//td')
            if len(tds)<2:
                continue
            key=tds[0].text_content().strip(':\n\t ').replace('.','').lower()
            value=tds[1].text_content().strip(':\n\t ').lower()
            specification[key]=value

    util.replaceKey(specification,'usb 3.0','usb 3')
    util.replaceKey(specification,'hard disk size','capacity')
    util.replaceKey(specification,'spin speed (rpm)','speed')

    m=interface_pattern.search(title)
    if m:
        specification['interface']=m.group()

    if 'capacity' not in specification:
        m=capacity_pattern.search(title) 
        if m:
            specification['capacity']=m.group()

    pd['specification']=specification

    pd['last_modified_datetime']=datetime.datetime.now()
 
    product_history={}
    if 'price' in pd:
        product_history['price']=pd['price']
    if 'shipping' in pd:
        product_history['shipping']=pd['shipping']
    product_history['availability']=pd['availability']
    product_history['datetime']=pd['last_modified_datetime']
    pd['product_history']=[product_history,]
    pd['site']='infibeam'
    return pd


def scrapAllPDs():
    urls=getAllPDUrls()
    pds=[]
    dl.putUrls(urls,2)
    result=dl.download()
    for r in result:
        print r
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            pd=getPDFromPage(string=html)
            pd['url']=r
            pds.append(pd)
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
