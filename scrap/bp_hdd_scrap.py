import downloader
import dom
import re
import urllib
import math
import datetime
import pymongo
import simplejson as json
import util

siteurl='http://www.buytheprice.com'
referer='http://www.buytheprice.com/category__external-hard-drives-79'
logfile=open('bp_hdds_log.txt','w')
debug=True
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})
shipping_pattern=re.compile('(\d+)-(\d+)')
junk_pattern=re.compile('hard (drive|disk)',re.I)

def getAllHDDUrls():
    count_path='//div[@class="hdnos"]/span'
    doc=dom.DOM(url=referer)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    num_pages=int(math.ceil(count/31.0))
    #num_pages=4
    page_urls=[referer+'~'+str(n) for n in xrange(1,num_pages+1)]
    hdd_urls=set()
    dl.putUrls(page_urls)
    pages=dl.download()
    if debug:
        print '%d Pages found\n'%len(pages)
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            hdd_urls.update(getHDDUrlsOfPage(html=html))
    return hdd_urls

def getHDDUrlsOfPage(url=None,html=None):
    hdd_url_path='//div[@class="product-block1"]/a[1]'
    doc=dom.DOM(string=html)
    urls=set(link[1] for link in doc.getLinksWithXpath(hdd_url_path))
    return urls

def getHDDFromPage(url=None,string=None):
    hdd={}
    if url:
        doc=dom.DOM(url=url)
        hdd['url']=url
    else:
        doc=dom.DOM(string=string,utf8=True)
        url_path='//meta[@property="og:url"]'        
        url=doc.getNodesWithXpath(url_path)
        if len(url)==0:
            return False
        hdd['url']=url[0].get('content')
    if debug:
        print hdd['url']
    image_path='//meta[@property="og:image"]'
    hdd['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    addBox=doc.getNodesWithXpath('//button[@class="btn btn-warning btn-large"]')
    title_path='//div[@id="p-infocol"]/h1'
    title=doc.getNodesWithXpath(title_path)[0].text
    hdd['name']=junk_pattern.sub('',title)
    brand=re.search('\w+',hdd['name']).group().lower()
    if brand=='western' or brand=='wd':
        hdd['brand']='western digital'
    else:
        hdd['brand']=brand
    price_path='//span[@id="p-ourprice-m"]/span[@itemprop="price"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        hdd['price']=int(price[0].text.replace(',',''))
    if addBox and addBox[0].text_content().strip()=='Buy Now':                           #availability check
        hdd['availability']=1
        shipping_path='//div[@class="prblinfo"][2]'
        shipping=doc.getNodesWithXpath(shipping_path)[0].text_content()
        if shipping:
            m=shipping_pattern.search(shipping)
            if m:
                hdd['shipping']=(m.group(1),m.group(2))
    else:
        hdd['availability']=0

    hdd['specification']=[]
    specification_table_path='//div[@id="features"]/table'
    specification_table=doc.getNodesWithXpath(specification_table_path)
    specs_key_path='td[@class="prodspecleft"]'
    specs_value_path='td[@class="prodspecright"]'
    specification={}
    if len(specification_table)>0:
        for specs in specification_table:
            trs=specs.xpath('tr')
            for tr in trs:
                td=tr.xpath('td')
                if len(td)>1:
                    key=td[0].text
                    if key:
                        key=key.strip().lower()
                        value=tr.xpath(specs_value_path)[0].text
                        if value:
                            value=value.strip().lower()
                            specification[key]=value

    util.replaceKey(specification,'data speed','speed')
    util.replaceKey(specification,'storage capacity','capacity')
    util.replaceKey(specification,'connectivity','interface')
    util.replaceKey(specification,'usb 2.0','usb 2')
    util.replaceKey(specification,'usb 3.0','usb 3')
    
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
    hdd['site']='buytheprice'
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
            hdd=getHDDFromPage(string=html)
            if hdd:
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
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'harddisk','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)

    print "%d inserted and %d updated"%(inserted_count,updated_count)
    
if __name__=='__main__':
    insertIntoDB()

