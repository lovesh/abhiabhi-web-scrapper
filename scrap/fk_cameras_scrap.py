import downloader
import dom
import re
import urllib2
import math
import datetime
import pymongo
import simplejson as json
import util

debug=True

siteurl='http://www.flipkart.com'
camera_home='http://www.flipkart.com/cameras/all-digital-cameras'
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':camera_home})
ajax_dl=downloader.Downloader()
logfile=open('fk_cameras_log.txt','w')
#ajax_dl.addHeaders({'Host':siteurl,'X-Requested-With':'XMLHttpRequest'})

category_junk_pattern=re.compile('\(\d+\)')
f=open('fk_cameras_log.txt','w')

def getCameraCategories():
    page=dom.DOM(url=camera_home)
    category_path='//div[@id="fk-lfilters-Type"]/ul/li/a'
    categorys=page.getLinksWithXpath(category_path)
    categories=[]
    for cat in categorys:
	name=category_junk_pattern.sub('',cat[0]).strip().lower()
	if name=='point & shoot':
	    name='point and shoot'
	if name=='slr':
	    name='dslr'
	url=siteurl+cat[1]
        categories.append((name,url))
    return categories

def getCameraFromPage(url=None,string=None):
    camera={}
    if url:
        doc=dom.DOM(url=url)
        camera['url']=url
    else:
        doc=dom.DOM(string=string)
        url_path='//link[@rel="canonical"]'        
        url=doc.getNodesWithXpath(url_path)
        camera['url']=url[0].get('href')
    
    print camera['url']
    valid=re.search('/p/',camera['url'])
    if valid is None:
        return False
	
    addBox=doc.getNodesWithXpath('//div[@id="mprod-buy-btn"]')
    name_path='//div[@class="mprod-summary-title fksk-mprod-summary-title"]/h1'
    extra_path='//div[@class="mprod-summary-title fksk-mprod-summary-title"]/span[@class="extra_text"]'
    name=doc.getNodesWithXpath(name_path)[0].text.strip()
    camera['name']=name
    extra_text = doc.getNodesWithXpath(extra_path)
    if len(extra_text) > 0:
	extra_text=extra_text[0].text.strip()
        camera['name']=name+' '+extra_text
    img_url_path='//img[@id="visible-image-large"]'
    img_urls=doc.getImgUrlWithXpath(img_url_path)
    if len(img_urls)>0:
        camera['img_url']={'0':img_urls[0]}
    if addBox:                           #availability check
        camera['availability']=1
        shipping_path='//div[@class="shipping-details"]/span[@class="boldtext"]'
        shipping=doc.getNodesWithXpath(shipping_path)[0].text
        if shipping:
            m=re.search('(\d+)-(\d+)',shipping)
            if m:
                camera['shipping']=(m.group(1),m.group(2))
    else:
	camera['availability']=0

    price_path='//span[@id="fk-mprod-our-id"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
	camera['price']=int(price[0].text_content().strip('Rs. '))
    else:
	cashback_path='//span[@class="cashback-amount"]'
	cashback=doc.getNodesWithXpath(cashback_path)
	if len(cashback)>0:
	    cashback=int(cashback[0].text_content().strip('Rs. '))
	    alt_price_path='//span[@class="price final-price"]'
	    price=doc.getNodesWithXpath(alt_price_path)
	    if len(price)>0:
		price=int(price[0].text_content().strip('Rs. '))
		camera['price']=price-cashback
	    
    key_features_path='//div[@class="line"]/ul[@class="feature_bullet"]/li/span'
    key_features=doc.getNodesWithXpath(key_features_path)
    camera['features']=[]
    if key_features:
        for kf in key_features:
            camera['features'].append(kf.text.strip())
    
    camera['description']=''
    description_path='//div[@class="item_desc_text description"]'
    description=doc.getNodesWithXpath(description_path)
    if len(description)>0:
        desc=description[0]
        if desc.text is not None:
            camera['description']=desc.text.strip()
    specification={}
    specification_tables_path='//table[@class="fk-specs-type2"]'
    specification_tables=doc.getNodesWithXpath(specification_tables_path)
    specs_key_path='th[@class="specs-key"]'
    specs_value_path='td[@class="specs-value"]'
    if len(specification_tables)>0:
        for specs in specification_tables:
            trs=specs.xpath('tr')
            for tr in trs:
                th=tr.xpath(specs_key_path)
                if len(th)>0:
                    key=th[0].text
                    if key:
                        key=key.strip().lower()
                        value=tr.xpath(specs_value_path)[0].text
                        if value:
                            value=value.strip().lower()
                            specification[key]=value     #only put specification if value is not None
    util.replaceKey(specification,'built in flash','flash')
    util.replaceKey(specification,'optical sensor resolution (in megapixel)','sensor resolution')
    util.replaceKey(specification,'inbuilt memory','internal memory')
    camera['specification']=specification
    camera['last_modified_datetime']=datetime.datetime.now()

    offer_path='//div[@class="fk-product-page-offers rposition a-hover-underline"]//td[@class="fk-offers-text"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        camera['offer']=offer[0].text_content().replace('\r\n ','')

    product_history={}
    if 'price' in camera:
        product_history['price']=camera['price']
    if 'shipping' in camera:
        product_history['shipping']=camera['shipping']
    product_history['availability']=camera['availability']
    product_history['datetime']=camera['last_modified_datetime']
    camera['product_history']=[product_history,]
    camera['site']='flipkart'
    return camera

def getCameraUrlsOfPage(url=None,html=None):
    html=html.replace('\n','')
    html=html.replace('\t','')
    html=html.replace('\"','"')
    doc=dom.DOM(string=html)
    urls=[]
    camera_url_path='//a[@class="title tpadding5 fk-anchor-link"]'
    urls=[siteurl+link[1] for link in doc.getLinksWithXpath(camera_url_path)]
    return urls

def getCamerasOfBrand(brand):
    global f
    brand_page=dom.DOM(url=brand[1])
    count_path='//div[@class="unit fk-lres-header-text"]/b[2]'
    count=int(brand_page.getNodesWithXpath(count_path)[0].text)
    page_urls=[brand[1]+'&response-type=json&inf-start='+str(n) for n in xrange(0,count,20)]
    cam_urls=[]
    cameras=[] 
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
            cam_urls.extend(getCameraUrlsOfPage(html=json_response['html']))
    dl.putUrls(cam_urls)
    f.write('Found %d urls for brand %s\n'%(len(cam_urls),brand[0]))
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
	    camera = getCameraFromPage(string=html)
	    if camera:
		cameras.append(camera)
    for camera in cameras:
        camera['brand']=brand[0]
    return cameras

def getCamerasOfCategory(category):
    cat_page=dom.DOM(url=category[1])
    brand_path='//div[@id="fk-lfilters-Brand"]/ul/li/a'
    brands=cat_page.getLinksWithXpath(brand_path)
    cameras=[]
    for brand in brands:
        print "Getting cameras of brand %s of category %s\n"%(brand[0].lower(),category[0])
        #brand[1]=siteurl+brand
        cameras.extend(getCamerasOfBrand((re.sub('\(.*\)','',brand[0]).strip().lower(),siteurl+brand[1])))
        for cam in cameras:
            cam['name']=cam['name'].replace(category[0],'').strip()
            cam['category']=set([category[0],])
    return cameras

def scrapAllCameras():
    global f
    categories=getCameraCategories()
    cameras=[]
    for cat in categories:
        cameras.extend(getCamerasOfCategory(cat))
        f.write("Got cameras of category %s\n"%cat[0])
        f.flush()
    cam_dict={}
    for cam in cameras:
        if cam['url'] in cam_dict:
            cam_dict[cam['url']]['category'].update(cam['category'])
        else:
            cam_dict[cam['url']]=cam
    final_cameras=[]
    for cam in cam_dict:
        final_cameras.append(cam_dict[cam])
    return final_cameras

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    camera_coll=db['scraped_cameras']
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    cameras=scrapAllCameras()
    for camera in cameras:
        camera['category']=list(camera['category'])
        try:
            camera_coll.insert(camera,safe=True)
            inserted_count+=1
            inserted_urls.append(camera['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability' in camera:
                upd['availability']=camera['availability']
            if 'price' in camera:
                upd['price']=camera['price']
            if 'shipping' in camera:
                upd['shipping']=camera['shipping']
	    if 'offer' in camera:
		upd['offer']=camera['offer']
	    else:
		upd['offer']=''
            camera_coll.update({'url':camera['url']},{'$push':{'product_history':camera['product_history'][0]},'$set':upd})
            updated_count+=1
            updated_urls.append(camera['url'])
    if log:
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'camera','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
    
    print "%d inserted and %d updated"%(inserted_count,updated_count)
    
if __name__ == '__main__':
    insertIntoDB()
