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
referer='http://www.buytheprice.com/mobiles/mobile-phones_158'

valuephone='http://www.buytheprice.com/category__value-handsets-50'
smartphone='http://www.buytheprice.com/category__smartphones-51.html'


logfile=open('bp_mobiles_log.txt','w')
debug=True
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})
shipping_pattern=re.compile('(\d+)-(\d+)')
junk_pattern=re.compile('mobile',re.I)


def getAllMobileUrls():
    urls=set()
    for cat in [valuephone,smartphone]:
        urls.update(getMobileUrlsOfCategory(cat))
    return urls

def getMobileUrlsOfCategory(cat_url):
    count_path='//div[@class="hdnos"]/span'
    doc=dom.DOM(url=cat_url)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    #num_pages=len(doc.getNodesWithXpath(pages_path))-1
    #num_pages=4
    num_pages=int(math.ceil(count/31.0))
    page_urls=[cat_url+'~'+str(n) for n in xrange(1,num_pages+1)]
    mobile_urls=set()
    dl.putUrls(page_urls)
    pages=dl.download()
    if debug:
        print '%d Pages found\n'%len(pages)
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            mobile_urls.update(getMobileUrlsOfPage(html=html))
    return mobile_urls

def getMobileUrlsOfPage(url=None,html=None):
    mobile_url_path='//div[@class="product-block1"]/a[1]'
    doc=dom.DOM(string=html)
    urls=set(link[1] for link in doc.getLinksWithXpath(mobile_url_path))
    return urls

def getMobileFromPage(url=None,string=None):
    mobile={}
    if url:
        doc=dom.DOM(url=url)
        mobile['url']=url
    else:
        doc=dom.DOM(string=string, utf8=True)
    image_path='//meta[@property="og:image"]'
    mobile['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    addBox=doc.getNodesWithXpath('//button[@class="btn btn-warning btn-large"]')
    title_path='//div[@id="p-infocol"]/h1'
    title=doc.getNodesWithXpath(title_path)[0].text
    mobile['name']=junk_pattern.sub('',title)

    brand=re.search('\w+',mobile['name']).group().lower()
    if re.match('sony ?ericsson',mobile['name'],re.I):
        mobile['brand']='sony ericsson'
    else:
        mobile['brand']=brand
    price_path='//span[@id="p-ourprice-m"]/span[@itemprop="price"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        mobile['price']=int(price[0].text.replace(',',''))
    if addBox and addBox[0].text_content().strip()=='Buy Now':                           #availability check
        mobile['availability']=1
        shipping_path='//div[@class="prblinfo"][2]'
        shipping=doc.getNodesWithXpath(shipping_path)[0].text_content()
        if shipping:
            m=shipping_pattern.search(shipping)
            if m:
                mobile['shipping']=(m.group(1),m.group(2))
    else:
	mobile['availability']=0
    mobile['specification']=[]
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

    if 'operating system version' not in specification:
        util.replaceKey(specification,'operating system','os')
    util.replaceKey(specification,'operating system version','os')
    mobile['specification']=specification
    mobile['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in mobile:
        product_history['price']=mobile['price']
    if 'shipping' in mobile:
        product_history['shipping']=mobile['shipping']
    product_history['availability']=mobile['availability']
    product_history['datetime']=mobile['last_modified_datetime']
    mobile['product_history']=[product_history,]
    mobile['site']='buytheprice'
    return mobile

def scrapAllMobiles():
    urls=getAllMobileUrls()
    mobiles=[]
    failed=[]
    dl.putUrls(urls)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
	    print r
            mobile=getMobileFromPage(string=html)
            if mobile:
		mobile['url'] = r
                mobiles.append(mobile)
        else:
            failed.append('%s with %s'%(r,str(status)))
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

