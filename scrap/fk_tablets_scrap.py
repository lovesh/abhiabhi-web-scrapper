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
referer='http://www.flipkart.com/tablets/tablet-20278'
logfile=open('fk_tablets_log.txt','w')
ajax_dl=downloader.Downloader()
#ajax_dl.addHeaders({'Host':siteurl,'X-Requested-With':'XMLHttpRequest'})

debug=False
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})

def getTabletBrands():
    page=dom.DOM(url=referer)
    brand_path='//div[@id="fk-lfilters-Brand"]/ul/li/a'
    brands=page.getLinksWithXpath(brand_path)
    for brand in brands:
        brand[0]=re.sub('\(.*\)','',brand[0]).strip().replace(' ','-').lower()
        brand[1]=siteurl+brand[1]
    return brands

def getTabletFromPage(url=None,string=None):
    tablet={}
    if url:
        doc=dom.DOM(url=url)
        tablet['url']=url
    else:
        doc=dom.DOM(string=string)
        url_path='//link[@rel="canonical"]'        
        url=doc.getNodesWithXpath(url_path)
        tablet['url']=url[0].get('href')
    if debug:
        print tablet['url']
    addBox=doc.getNodesWithXpath('//div[@id="mprod-buy-btn"]')
    name_path='//div[@class="mprod-summary-title fksk-mprod-summary-title"]/h1'
    tablet['name']=doc.getNodesWithXpath(name_path)[0].text.strip()
    color_path='//div[@class="mprod-summary-title fksk-mprod-summary-title"]/span'
    color=doc.getNodesWithXpath(color_path)
    if len(color)>0:
        tablet['color']=color[0].text.strip(')( ')
    img_url_path='//img[@id="visible-image-large"]'
    img_urls=doc.getImgUrlWithXpath(img_url_path)
    if len(img_urls)>0:
        tablet['img_url']={'0':img_urls[0]}
    if addBox:                           #availability check
        tablet['availability']=1
        shipping_path='//div[@class="shipping-details"]/span[@class="boldtext"]'
        shipping=doc.getNodesWithXpath(shipping_path)[0].text
        if shipping:
            m=re.search('(\d+)-(\d+)',shipping)
            if m:
                tablet['shipping']=(m.group(1),m.group(2))
    else:
        tablet['availability']=0

    price_path='//span[@id="fk-mprod-our-id"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        tablet['price']=int(price[0].text_content().strip('Rs. '))

    offer_path='//div[@class="fk-product-page-offers rposition a-hover-underline"]//td[@class="fk-offers-text"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        tablet['offer']=offer[0].text_content().replace('\r\n ','')

    key_features_path='//div[@class="line"]/ul[@class="feature_bullet"]/li/span'
    key_features=doc.getNodesWithXpath(key_features_path)
    if len(key_features)==0:
        key_features_path='//div[@id="description"]/div[@class="item_desc_text"]/ul/li'    #for tablets pages have 2 kinds of layout so check for another kind of layout
        key_features=doc.getNodesWithXpath(key_features_path)
    tablet['features']=[]
    if key_features:
        for kf in key_features:
            tablet['features'].append(kf.text.strip())
    
    tablet['description']=''
    description_path='//div[@class="item_desc_text description"]'
    description=doc.getNodesWithXpath(description_path)
    if len(description)>0:
        desc=description[0]
        if desc.text is not None:
            tablet['description']=desc.text.strip()
    tablet['specification']={}
    specification_tables_path='//table[@class="fk-specs-type2"]'
    specification_tables=doc.getNodesWithXpath(specification_tables_path)
    specs_key_path='td[@class="specs-key"]'
    specs_value_path='td[@class="specs-value"]'
    if len(specification_tables)>0:
        for specs in specification_tables:
            specs=doc.parseTBodyNode(specs)
            if len(specs)>0:
                tablet['specification'].update(specs)

    util.replaceKey(tablet['specification'],'internal storage','storage')
    util.replaceKey(tablet['specification'],'operating system','os')
    if '3g' in tablet['specification']:
        if tablet['specification']['3g']=='No':
            del(tablet['specification']['3g'])

    tablet['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in tablet:
        product_history['price']=tablet['price']
    if 'shipping' in tablet:
        product_history['shipping']=tablet['shipping']
    product_history['availability']=tablet['availability']
    product_history['datetime']=tablet['last_modified_datetime']
    tablet['product_history']=[product_history,]
    tablet['site']='flipkart'
    return tablet

def getTabletUrlsOfPage(url=None,html=None):
    html=html.replace('\n','')
    html=html.replace('\t','')
    html=html.replace('\"','"')
    doc=dom.DOM(string=html)
    urls=[]
    tablet_url_path='//a[@class="title tpadding5 fk-anchor-link"]'
    urls=[siteurl+link[1] for link in doc.getLinksWithXpath(tablet_url_path)]
    return urls

def getTabletsOfBrand(brand):
    global logfile
    brand_page=dom.DOM(url=brand[1])
    count_path='//div[@class="unit fk-lres-header-text"]/b[2]'
    count=int(brand_page.getNodesWithXpath(count_path)[0].text)
    page_urls=[brand[1]+'&response-type=json&inf-start='+str(n) for n in xrange(0,count,20)]
    tablet_urls=[]
    tablets=[] 
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
            tablet_urls.extend(getTabletUrlsOfPage(html=json_response['html']))
    dl.putUrls(tablet_urls)
    logfile.write('Found %d urls for brand %s\n'%(len(tablet_urls),brand[0]))
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            tablets.append(getTabletFromPage(string=html))
    for tablet in tablets:
        tablet['brand']=brand[0].lower()
    return tablets


def scrapAllTablets():
    global logfile
    brands=getTabletBrands()
    tablets=[]
    for brand in brands:
        tablets.extend(getTabletsOfBrand(brand))
        logfile.write("Got tablets of brand %s\n"%brand[0])
        logfile.flush()
    return tablets

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    tablet_coll=db['scraped_tablets']
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
            if 'availability' in tablet:
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
