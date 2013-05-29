import downloader
import dom
import re
import urllib2
import math
import datetime
import pymongo
import simplejson as json
import util

siteurl='http://www.flipkart.com'
referer='http://www.flipkart.com/computers/laptop-accessories/pen-drive-20219'

logfile=open('fk_pds_log.txt','w')
ajax_dl=downloader.Downloader()
#ajax_dl.addHeaders({'Host':siteurl,'X-Requested-With':'XMLHttpRequest'})

debug=False
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})

def getPDBrands():
    page=dom.DOM(url=referer)
    brand_path='//div[@id="fk-lfilters-Brand"]/ul/li/a'
    brands=page.getLinksWithXpath(brand_path)
    for brand in brands:
        brand[0]=re.sub('\(.*\)','',brand[0]).strip().replace(' ','-').lower()
	if brand[0] == 'moser':
	    brand[0] == 'moser baer'
        brand[1]=siteurl+brand[1]
    return brands

def getPDFromPage(url=None,string=None):
    pd={}
    if url:
        doc=dom.DOM(url=url)
        pd['url']=url
    else:
        doc=dom.DOM(string=string,utf8=True)
        url_path='//link[@rel="canonical"]'        
        url=doc.getNodesWithXpath(url_path)
        pd['url']=url[0].get('href')
    if debug:
        print pd['url']
    addBox=doc.getNodesWithXpath('//div[@id="mprod-buy-btn"]')
    name_path='//div[@class="mprod-summary-title fksk-mprod-summary-title"]/h1'
    pd['name']=doc.getNodesWithXpath(name_path)[0].text.strip()
    color_path='//div[@class="mprod-summary-title fksk-mprod-summary-title"]/span'
    color=doc.getNodesWithXpath(color_path)
    if len(color)>0:
        pd['color']=color[0].text.strip(')( ')
    img_url_path='//img[@id="visible-image-large"]'
    img_urls=doc.getImgUrlWithXpath(img_url_path)
    if len(img_urls)>0:
        pd['img_url']={'0':img_urls[0]}
    if addBox:                           #availability check
        pd['availability']=1
        shipping_path='//div[@class="shipping-details"]/span[@class="boldtext"]'
        shipping=doc.getNodesWithXpath(shipping_path)[0].text
        if shipping:
            m=re.search('(\d+)-(\d+)',shipping)
            if m:
                pd['shipping']=(m.group(1),m.group(2))
    else:
        pd['availability']=0

    price_path='//span[@id="fk-mprod-our-id"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        pd['price']=int(price[0].text_content().strip('Rs. '))

    offer_path='//div[@class="fk-product-page-offers rposition a-hover-underline"]//td[@class="fk-offers-text"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        pd['offer']=offer[0].text_content().replace('\r\n ','')

    description_path='//div[@class="item_desc_text description"]'
    description=doc.getNodesWithXpath(description_path)
    if len(description)>0:
        desc=description[0]
        if desc.text is not None:
            pd['description']=desc.text.strip()
    pd['specification']={}
    specification_tables_path='//div[@id="specifications"]/table'
    specification_tables=doc.getNodesWithXpath(specification_tables_path)
    specs_key_path='td[@class="specs-key"]'
    specs_value_path='td[@class="specs-value"]'
    if len(specification_tables)>0:
        for specs in specification_tables:
            specs=doc.parseTBodyNode(specs)
            if len(specs)>0:
                pd['specification'].update(specs)
    util.replaceKey(pd['specification'],'transfer speed','speed')
    pd['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in pd:
        product_history['price']=pd['price']
    if 'shipping' in pd:
        product_history['shipping']=pd['shipping']
    product_history['availability']=pd['availability']
    product_history['datetime']=pd['last_modified_datetime']
    pd['product_history']=[product_history,]
    pd['site']='flipkart'
    return pd

def getPDUrlsOfPage(url=None,html=None):
    html=html.replace('\n','')
    html=html.replace('\t','')
    html=html.replace('\"','"')
    doc=dom.DOM(string=html)
    urls=[]
    pd_url_path='//a[@class="title tpadding5 fk-anchor-link"]'
    urls=[siteurl+link[1] for link in doc.getLinksWithXpath(pd_url_path)]
    return urls

def getPDsOfBrand(brand):
    global logfile
    brand_page=dom.DOM(url=brand[1])
    count_path='//div[@class="unit fk-lres-header-text"]/b[2]'
    count=int(brand_page.getNodesWithXpath(count_path)[0].text)
    page_urls=[brand[1]+'&response-type=json&inf-start='+str(n) for n in xrange(0,count,20)]
    pd_urls=[]
    pds=[] 
    ajax_dl.putUrls(page_urls)
    pages=ajax_dl.download()
    if debug:
        print '%d Pages for brand %s\n'%(len(pages),brand[0])
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            json_response=json.loads(html)
            count=json_response['count']
            print count
            if count==0:
                flag=False
                continue
            pd_urls.extend(getPDUrlsOfPage(html=json_response['html']))
    dl.putUrls(pd_urls)
    logfile.write('Found %d urls for brand %s\n'%(len(pd_urls),brand[0]))
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            pds.append(getPDFromPage(string=html))
    for pd in pds:
        pd['brand']=brand[0].lower()
    return pds


def scrapAllPDs():
    global logfile
    brands=getPDBrands()
    pds=[]
    for brand in brands:
        pds.extend(getPDsOfBrand(brand))
        logfile.write("Got pds of brand %s\n"%brand[0])
        logfile.flush()
    return pds

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    pd_coll=db['scraped_pendrives']
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


