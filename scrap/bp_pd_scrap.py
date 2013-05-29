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
referer='http://www.buytheprice.com/category__pen-drives-78'
logfile=open('bp_pds_log.txt','w')
debug=True
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})
shipping_pattern=re.compile('(\d+)-(\d+)')
junk_pattern=re.compile('usb flash drives?',re.I)
junk_pattern1=re.compile('pen ?drives?',re.I)

def getAllPDUrls():
    count_path='//div[@class="hdnos"]/span'
    pages_path='//div[@class="pagination"]/ul/li'
    
    doc=dom.DOM(url=referer)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    num_pages=int(math.ceil(count/31.0))
    #num_pages=4
    page_urls=[referer+'~'+str(n) for n in xrange(1,num_pages+1)]
    pd_urls=set()
    dl.putUrls(page_urls)
    pages=dl.download()
    if debug:
        print '%d Pages found\n'%len(pages)
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            pd_urls.update(getPDUrlsOfPage(html=html))
    return pd_urls

def getPDUrlsOfPage(url=None,html=None):
    pd_url_path='//div[@class="product-block1"]/a[1]'
    doc=dom.DOM(string=html)
    urls=set(link[1] for link in doc.getLinksWithXpath(pd_url_path))
    return urls

def getPDFromPage(url=None,string=None):
    pd={}
    if url:
        doc=dom.DOM(url=url)
        pd['url']=url
    else:
        doc=dom.DOM(string=string)
        url_path='//meta[@property="og:url"]'        
        url=doc.getNodesWithXpath(url_path)
        if len(url)==0:
            return False
        pd['url']=url[0].get('content')
    if debug:
        print pd['url']
    image_path='//meta[@property="og:image"]'
    pd['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    addBox=doc.getNodesWithXpath('//button[@class="btn btn-warning btn-large"]')
    title_path='//div[@id="p-infocol"]/h1'
    title=doc.getNodesWithXpath(title_path)[0].text
    name=junk_pattern.sub('',title)
    pd['name']=junk_pattern1.sub('',name)

    pd['brand']=re.search('\w+',pd['name']).group().lower()
    if pd['brand'] == 'moser':
        pd['brand'] = 'moser baer'
    if pd['brand'] == 'a':
        pd['brand'] = 'a-data'
    price_path='//span[@id="p-ourprice-m"]/span[@itemprop="price"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        pd['price']=int(price[0].text.replace(',',''))
    if addBox and addBox[0].text_content().strip()=='Buy Now':                           #availability check
        pd['availability']=1
        shipping_path='//div[@class="prblinfo"][2]'
        shipping=doc.getNodesWithXpath(shipping_path)[0].text_content()
        if shipping:
            m=shipping_pattern.search(shipping)
            if m:
                pd['shipping']=(m.group(1),m.group(2))
    else:
        pd['availability']=0

    pd['specification']=[]
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

    util.replaceKey(specification,'transfer speed','speed')

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
    pd['site']='buytheprice'
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
            pd=getPDFromPage(string=html)
            if pd:
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

