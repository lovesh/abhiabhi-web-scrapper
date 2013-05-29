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
referer='http://www.buytheprice.com/category__tablets-85'
logfile=open('bp_tablets_log.txt','w')
debug=True
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})
shipping_pattern=re.compile('(\d+)-(\d+)')
junk_pattern=re.compile('tablet',re.I)

def getAllTabletUrls():
    count_path='//div[@class="hdnos"]/span'
    pages_path='//div[@class="pagination"]/ul/li'
    
    doc=dom.DOM(url=referer)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    #num_pages=len(doc.getNodesWithXpath(pages_path))-1
    num_pages=4
    page_urls=[referer+'~'+str(n) for n in xrange(1,num_pages+1)]
    tablet_urls=set()
    dl.putUrls(page_urls)
    pages=dl.download()
    if debug:
        print '%d Pages found\n'%len(pages)
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            tablet_urls.update(getTabletUrlsOfPage(html=html))
    return tablet_urls

def getTabletUrlsOfPage(url=None,html=None):
    tablet_url_path='//div[@class="product-block1"]/a[1]'
    doc=dom.DOM(string=html)
    urls=set(link[1] for link in doc.getLinksWithXpath(tablet_url_path))
    return urls

def getTabletFromPage(url=None,string=None):
    tablet={}
    if url:
        doc=dom.DOM(url=url)
        tablet['url']=url
    else:
        doc=dom.DOM(string=string)
        url_path='//meta[@property="og:url"]'        
        url=doc.getNodesWithXpath(url_path)
        if len(url)==0:
            return False
        tablet['url']=url[0].get('content')
    if debug:
        print tablet['url']
    image_path='//meta[@property="og:image"]'
    tablet['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    addBox=doc.getNodesWithXpath('//button[@class="btn btn-warning btn-large"]')
    title_path='//div[@id="p-infocol"]/h1'
    title=doc.getNodesWithXpath(title_path)[0].text
    tablet['name']=junk_pattern.sub('',title)

    tablet['brand']=re.search('\w+',tablet['name']).group().lower()
    price_path='//span[@id="p-ourprice-m"]/span[@itemprop="price"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        tablet['price']=int(price[0].text.replace(',',''))
    if addBox and addBox[0].text_content().strip()=='Buy Now':                           #availability check
        tablet['availability']=1
        shipping_path='//div[@class="prblinfo"][2]'
        shipping=doc.getNodesWithXpath(shipping_path)[0].text_content()
        if shipping:
            m=shipping_pattern.search(shipping)
            if m:
                tablet['shipping']=(m.group(1),m.group(2))
    else:
        tablet['availability']=0

    tablet['specification']=[]
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
    
    util.replaceKey(specification,'internal memory','storage')
    util.replaceKey(specification,'operating system version','os')

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
    tablet['site']='buytheprice'
    return tablet

def scrapAllTablets():
    urls=getAllTabletUrls()
    tablets=[]
    failed=[]
    dl.putUrls(urls)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            tablet=getTabletFromPage(string=html)
            if tablet:
                tablets.append(tablet)
        else:
            failed.append('%s with %s'%(r,str(status)))
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
            if 'availability'in tablet:
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
