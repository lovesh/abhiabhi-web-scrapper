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
referer='http://www.flipkart.com/computers/laptop-accessories/external-hard-disk-20216'

logfile=open('fk_hdds_log.txt','w')
ajax_dl=downloader.Downloader()
#ajax_dl.addHeaders({'Host':siteurl,'X-Requested-With':'XMLHttpRequest'})

debug=False
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})

def getHDDBrands():
    page=dom.DOM(url=referer)
    brand_path='//div[@id="fk-lfilters-Brand"]/ul/li/a'
    brands=page.getLinksWithXpath(brand_path)
    for brand in brands:
        brand[0]=re.sub('\(.*\)','',brand[0]).strip().replace(' ','-').lower()
        if brand[0] == 'wd':
            brand[0] = 'western digital'
        if brand[0] == 'adata':
            brand[0] = 'a-data'
        brand[1]=siteurl+brand[1]
    return brands

def getHDDFromPage(url=None,string=None):
    hdd={}
    if url:
        doc=dom.DOM(url=url)
        hdd['url']=url
    else:
        doc=dom.DOM(string=string)
        url_path='//link[@rel="canonical"]'        
        url=doc.getNodesWithXpath(url_path)
        hdd['url']=url[0].get('href')
    if debug:
        print hdd['url']
    addBox=doc.getNodesWithXpath('//div[@id="mprod-buy-btn"]')
    name_path='//div[@class="mprod-summary-title fksk-mprod-summary-title"]/h1'
    hdd['name']=doc.getNodesWithXpath(name_path)[0].text.strip()
    color_path='//div[@class="mprod-summary-title fksk-mprod-summary-title"]/span'
    color=doc.getNodesWithXpath(color_path)
    if len(color)>0:
        hdd['color']=color[0].text.strip(')( ')
    img_url_path='//img[@id="visible-image-large"]'
    img_urls={'0':doc.getImgUrlWithXpath(img_url_path)}
    if addBox:                           #availability check
        hdd['availability']=1
        shipping_path='//div[@class="shipping-details"]/span[@class="boldtext"]'
        shipping=doc.getNodesWithXpath(shipping_path)[0].text
        if shipping:
            m=re.search('(\d+)-(\d+)',shipping)
            if m:
                hdd['shipping']=(m.group(1),m.group(2))
    else:
		hdd['availability']=0
    price_path='//span[@id="fk-mprod-our-id"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        hdd['price']=int(price[0].text_content().strip('Rs. '))

    offer_path='//div[@class="fk-product-page-offers rposition a-hover-underline"]//td[@class="fk-offers-text"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        hdd['offer']=offer[0].text_content().replace('\r\n ','')

    description_path='//div[@class="item_desc_text description"]'
    description=doc.getNodesWithXpath(description_path)
    if len(description)>0:
        desc=description[0]
        if desc.text is not None:
            hdd['description']=desc.text.strip()
    hdd['specification']={}
    specification_tables_path='//div[@id="specifications"]/table'
    specification_tables=doc.getNodesWithXpath(specification_tables_path)
    specs_key_path='td[@class="specs-key"]'
    specs_value_path='td[@class="specs-value"]'
    if len(specification_tables)>0:
        for specs in specification_tables:
            specs=doc.parseTBodyNode(specs)
            if len(specs)>0:
                hdd['specification'].update(specs)
    util.replaceKey(hdd['specification'],'transfer speed','speed')
    util.replaceKey(hdd['specification'],'connectivity','interface')
    hdd['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in hdd:
        product_history['price']=hdd['price']
    if 'shipping' in hdd:
        product_history['shipping']=hdd['shipping']
    product_history['availability']=hdd['availability']
    product_history['datetime']=hdd['last_modified_datetime']
    hdd['product_history']=[product_history,]
    hdd['site']='flipkart'
    return hdd

def getHDDUrlsOfPage(url=None,html=None):
    html=html.replace('\n','')
    html=html.replace('\t','')
    html=html.replace('\"','"')
    doc=dom.DOM(string=html)
    urls=[]
    hdd_url_path='//a[@class="title tpadding5 fk-anchor-link"]'
    urls=[siteurl+link[1] for link in doc.getLinksWithXpath(hdd_url_path)]
    return urls

def getHDDsOfBrand(brand):
    global logfile
    brand_page=dom.DOM(url=brand[1])
    count_path='//div[@class="unit fk-lres-header-text"]/b[2]'
    count=int(brand_page.getNodesWithXpath(count_path)[0].text)
    page_urls=[brand[1]+'&response-type=json&inf-start='+str(n) for n in xrange(0,count,20)]
    hdd_urls=[]
    hdds=[] 
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
            hdd_urls.extend(getHDDUrlsOfPage(html=json_response['html']))
    dl.putUrls(hdd_urls)
    logfile.write('Found %d urls for brand %s\n'%(len(hdd_urls),brand[0]))
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            hdds.append(getHDDFromPage(string=html))
    for hdd in hdds:
        hdd['brand']=brand[0]
    return hdds


def scrapAllHDDs():
    global logfile
    brands=getHDDBrands()
    hdds=[]
    for brand in brands:
        hdds.extend(getHDDsOfBrand(brand))
        logfile.write("Got pds of brand %s\n"%brand[0])
        logfile.flush()
    return hdds

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    hdd_coll=db['scraped_harddisks']
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

