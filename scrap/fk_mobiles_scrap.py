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
referer='http://www.flipkart.com/browse/mobiles/all'
logfile=open('fk_mobiles_log.txt','w')
ajax_dl=downloader.Downloader()
#ajax_dl.addHeaders({'Host':siteurl,'X-Requested-With':'XMLHttpRequest'})

debug=True
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})
ajax_dl=downloader.Downloader()

def getMobileBrands():
    page=dom.DOM(url=referer)
    brand_path='//div[@id="fk-lfilters-Brand"]/ul/li/a'
    brands=page.getLinksWithXpath(brand_path)
    for brand in brands:
        brand[0]=re.sub('\(.*\)','',brand[0]).strip().replace(' ','-').lower()
	if brand[0]=='sony-ericcson':
            brand[0]='sony ericcson'
        brand[1]=siteurl+brand[1]
    return brands

def getMobileFromPage(url=None,string=None):
    mobile={}
    if url:
        doc=dom.DOM(url=url)
        mobile['url']=url
    else:
        doc=dom.DOM(string=string)
        url_path='//link[@rel="canonical"]'        
        url=doc.getNodesWithXpath(url_path)
        mobile['url']=url[0].get('href')
    if debug:
        print mobile['url']
    addBox=doc.getNodesWithXpath('//div[@id="mprod-buy-btn"]')
    name_path='//div[@class="mprod-summary-title fksk-mprod-summary-title"]/h1'
    mobile['name']=doc.getNodesWithXpath(name_path)[0].text.strip()
    color_path='//div[@class="mprod-summary-title fksk-mprod-summary-title"]/span'
    color=doc.getNodesWithXpath(color_path)
    if len(color)>0:
        mobile['color']=color[0].text.strip().strip(')(')
    img_url_path='//img[@id="visible-image-large"]'
    img_urls=doc.getImgUrlWithXpath(img_url_path)
    if len(img_urls)>0:
        mobile['img_url']={'0':img_urls[0]}
    if addBox:                           #availability check
        mobile['availability']=1
        shipping_path='//div[@class="shipping-details"]/span[@class="boldtext"]'
        shipping=doc.getNodesWithXpath(shipping_path)[0].text
        if shipping:
            m=re.search('(\d+)-(\d+)',shipping)
            if m:
                mobile['shipping']=(m.group(1),m.group(2))
    else:
        mobile['availability']=0

    price_path='//span[@id="fk-mprod-our-id"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        mobile['price']=int(price[0].text_content().strip('Rs. '))

    key_features_path='//div[@class="line"]/ul[@class="feature_bullet"]/li/span'
    key_features=doc.getNodesWithXpath(key_features_path)
    if len(key_features)==0:
        key_features_path='//div[@id="description"]/div[@class="item_desc_text"]/ul/li'    #for mobiles pages have 2 kinds of layout so check for another kind of layout
        key_features=doc.getNodesWithXpath(key_features_path)
    mobile['features']=[]
    if key_features:
        for kf in key_features:
            mobile['features'].append(kf.text.strip())
    
    mobile['description']=''
    description_path='//div[@class="item_desc_text description"]'
    description=doc.getNodesWithXpath(description_path)
    if len(description)>0:
        desc=description[0]
        if desc.text is not None:
            mobile['description']=desc.text.strip()

    offer_path='//div[@class="fk-product-page-offers rposition a-hover-underline"]//td[@class="fk-offers-text"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        mobile['offer']=offer[0].text_content().replace('\r\n ','')

    mobile['specification']={}
    specification_tables_path='//table[@class="fk-specs-type2"]'
    specification_tables=doc.getNodesWithXpath(specification_tables_path)
    if len(specification_tables)>0:
        for table in specification_tables:
            specs=doc.parseTBodyNode(table)
            if len(specs)>0:
                if table.xpath('tr[1]/th')[0].text=='Display':
                    util.replaceKey(specs,'type','display type')
                    util.replaceKey(specs,'size','display size')
                if table.xpath('tr[1]/th')[0].text=='Battery':
                    util.replaceKey(specs,'type','battery type')
                mobile['specification'].update(specs)

    mobile['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in mobile:
        product_history['price']=mobile['price']
    if 'shipping' in mobile:
        product_history['shipping']=mobile['shipping']
    product_history['availability']=mobile['availability']
    product_history['datetime']=mobile['last_modified_datetime']
    mobile['product_history']=[product_history,]
    mobile['site']='flipkart'
    return mobile

def getMobileUrlsOfPage(url=None,html=None):
    html=html.replace('\n','')
    html=html.replace('\t','')
    html=html.replace('\"','"')
    doc=dom.DOM(string=html)
    urls=[]
    mobile_url_path='//a[@class="title tpadding5 fk-anchor-link"]'
    urls=[siteurl+link[1] for link in doc.getLinksWithXpath(mobile_url_path)]
    return urls

def getMobilesOfBrand(brand):
    global logfile
    brand_page=dom.DOM(url=brand[1])
    count_path='//div[@class="unit fk-lres-header-text"]/b[2]'
    count=int(brand_page.getNodesWithXpath(count_path)[0].text)
    page_urls=[brand[1]+'&response-type=json&inf-start='+str(n) for n in xrange(0,count,20)]
    mobile_urls=[]
    mobiles=[] 
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
            mobile_urls.extend(getMobileUrlsOfPage(html=json_response['html']))
    dl.putUrls(mobile_urls)
    logfile.write('Found %d urls for brand %s\n'%(len(mobile_urls),brand[0]))
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            mobiles.append(getMobileFromPage(string=html))
    for mobile in mobiles:
        mobile['brand']=brand[0].lower()
    return mobiles


def scrapAllMobiles():
    global logfile
    brands=getMobileBrands()
    mobiles=[]
    for brand in brands:
        mobiles.extend(getMobilesOfBrand(brand))
        logfile.write("Got mobiles of brand %s\n"%brand[0])
        logfile.flush()
    if debug:
        print "Total %d mobiles found"%len(mobiles)
    return mobiles

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    mobile_coll=db['scraped_mobiles']
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
