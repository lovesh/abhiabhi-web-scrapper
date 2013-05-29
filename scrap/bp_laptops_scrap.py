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
referer='http://www.buytheprice.com/category__laptops-83'
logfile=open('bp_laptops_log.txt','w')
debug=True
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})
shipping_pattern=re.compile('(\d+)-(\d+)')
name_pattern=re.compile('(.+?)(\(|2nd|3rd|Ci|dual|quad|c2d|core|dc|pentium|intel|celeron|pdc|\d{3,4}M|\d{2}\.?\d? ?inch|$)',re.I)

def getAllLaptopUrls():
    count_path='//div[@class="hdnos"]/span'
    pages_path='//div[@class="pagination"]/ul/li'
    
    doc=dom.DOM(url=referer)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    #num_pages=len(doc.getNodesWithXpath(pages_path))-1
    num_pages=13
    page_urls=[referer+'~'+str(n) for n in xrange(1,num_pages+1)]
    laptop_urls=set()
    dl.putUrls(page_urls)
    pages=dl.download()
    if debug:
        print '%d Pages found\n'%len(pages)
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            laptop_urls.update(getLaptopUrlsOfPage(html=html))
    return laptop_urls

def getLaptopUrlsOfPage(url=None,html=None):
    laptop_url_path='//div[@class="product-block1"]/a[1]'
    doc=dom.DOM(string=html)
    urls=set(link[1] for link in doc.getLinksWithXpath(laptop_url_path))
    return urls

def getLaptopFromPage(url=None,string=None):
    laptop={}
    if url:
        doc=dom.DOM(url=url)
        laptop['url']=url
    else:
        doc=dom.DOM(string=string)
        url_path='//meta[@property="og:url"]'        
        url=doc.getNodesWithXpath(url_path)
        if len(url)==0:
            return False
        laptop['url']=url[0].get('content')
    if debug:
        print laptop['url']
    image_path='//meta[@property="og:image"]'
    laptop['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    addBox=doc.getNodesWithXpath('//button[@class="btn btn-warning btn-large"]')
    title_path='//div[@id="p-infocol"]/h1'
    title=doc.getNodesWithXpath(title_path)[0].text
    #junk=junk_pattern.search(name)
    #if junk:
        #name=name[:junk.start()]
    #laptop['name']=name.strip(' (')
    laptop['name']=name_pattern.search(title).group(1).strip(' -(')
    laptop['brand']=re.search('\w+',laptop['name']).group().lower()
    price_path='//span[@id="p-ourprice-m"]/span[@itemprop="price"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        laptop['price']=int(price[0].text.replace(',',''))
    if addBox and addBox[0].text_content().strip()=='Buy Now':                           #availability check
        laptop['availability']=1
        shipping_path='//div[@class="prblinfo"][2]'
        shipping=doc.getNodesWithXpath(shipping_path)[0].text_content()
        if shipping:
            m=shipping_pattern.search(shipping)
            if m:
                laptop['shipping']=(m.group(1),m.group(2))
    else:
	laptop['availability']=0

    laptop['specification']=[]
    specification_table_path='//div[@id="features"]/table'
    specification_table=doc.getNodesWithXpath(specification_table_path)
    specs_key_path='td[@class="prodspecleft"]'
    specs_value_path='td[@class="prodspecright"]'
    specification={}
    if len(specification_table)>0:
        for specs in specification_table:
            trs=specs.xpath('tr')
            for tr in trs:
                type_node=tr.xpath('td')
                if len(type_node)==1:
                    type=type_node[0].text_content().strip().lower()
                    if len(type)<25:
                        specification[type]={}
                        continue
                td=tr.xpath(specs_key_path)
                if len(td)>0:
                    key=td[0].text
                    if key:
                        key=key.strip().lower()
                        value=tr.xpath(specs_value_path)[0].text
                        if value:
                            value=value.strip().lower()
                            specification[type][key]=value     #only put specification if value is not None 
    util.replaceKey(specification,'memory/ram','memory')
    if 'memory' in specification:
        util.replaceKey(specification['memory'],'memory/ram','ram')
        util.replaceKey(specification['memory'],'memory type','type')
    if 'processor' in specification:
        util.replaceKey(specification['processor'],'manufacturer','brand')
    util.replaceKey(specification,'hard disk','storage')
    if 'software' in specification:
        util.replaceKey(specification['software'],'os version','os')
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
    laptop['site']='buytheprice'
    return laptop

def scrapAllLaptops():
    urls=getAllLaptopUrls()
    laptops=[]
    failed=[]
    dl.putUrls(urls)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            laptop=getLaptopFromPage(string=html)
            if laptop:
                laptops.append(laptop)
        else:
            failed.append('%s with %s'%(r,str(status)))
    return laptops

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    laptop_coll=db['scraped_laptops']
    laptop_coll.create_index('url',unique=True)
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    laptops=scrapAllLaptops()
    for laptop in laptops:
        try:
            laptop_coll.insert(laptop,safe=True)
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
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'laptop','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)

    print "%d inserted and %d updated"%(inserted_count,updated_count)
    
if __name__=='__main__':
    insertIntoDB()
