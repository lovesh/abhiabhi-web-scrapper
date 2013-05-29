import downloader
import dom
import re
import urllib
import math
import datetime
import pymongo
import simplejson as json
import util

siteurl='http://www.homeshop18.com'

camera_home='http://www.homeshop18.com/camera-26-camcorders/category:3159/'
digicam_home='http://www.homeshop18.com/digital-cameras/category:3178/'
dslr_home='http://www.homeshop18.com/digital-slrs/category:3188/'
camcorder_home='http://www.homeshop18.com/camcorders/category:3164/'

dl=downloader.Downloader()
dl.addHeaders({'Host':'www.homeshop18.com','Referer':camera_home})
debug=True
DBName='abhiabhi'

count_pattern=re.compile('\((\d+)\)')
name_pattern=re.compile('(.*?)\(')
brand_pattern=re.compile('\w+')
shipping_pattern=re.compile('(\d+)-(\d+)')
warranty_pattern=re.compile('\d+ years?',re.I)

def getCameraUrlsFromPage(html):
    camera_url_path='//p[@class="product_title"]/a'
    page_dom=dom.DOM(string=html)
    links=set(l[1] for l in page_dom.getLinksWithXpath(camera_url_path))
    return links

def getAllCameras():
    cameras=[]
    cameras.extend(getDigicam())
    cameras.extend(getDslr())
    cameras.extend(getCamcorder())
    return cameras

def getCamerasOfCategory(cat_url):
    doc=dom.DOM(url=cat_url)
    count_path='//div[@class="browse_result_title"]'
    count=doc.getNodesWithXpath(count_path)[0].text_content()
    m=count_pattern.search(count)
    if m:
        count=int(m.group(1)) 
    pager_base_url=cat_url.replace('category:','categoryid:')
    page_urls=[pager_base_url+'search:*/start:'+str(n) for n in xrange(0,count,24)]
    dl.putUrls(page_urls)
    pages=dl.download()
    camera_urls=[]
    failed = []
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            camera_urls.extend(getCameraUrlsFromPage(html))
    dl.putUrls(camera_urls,2)
    result=dl.download()
    cameras=[]
    for r in result:
        print r
        status = result[r][0]
        html = result[r][1]
        if len(html) < 2000:
            status = 0
            failed.append(r)
        if status > 199 and status < 400:
            camera = getCameraFromPage(string = html)
            camera['url'] = r
            cameras.append(camera)
    
    while len(failed) > 0:
        dl.putUrls(failed, 2)
        result = dl.download()
        failed = []
        for r in result:
            status=result[r][0]
            html=result[r][1]
            if len(html) < 2000:
                status = 0
                failed.append(r)
            if status > 199 and status < 400:
                print r
                camera=getCameraFromPage(string = html)
                if camera:
                    camera['url'] = r
                    cameras.append(camera)
                    
    return cameras

def getCameraFromPage(url=None,string=None):
    camera={}
    if url:
        doc=dom.DOM(url=url)
        camera['url']=url
    else:
        doc=dom.DOM(string=string)
        
    name_path='//h1[@id="productLayoutForm:pbiName"]'
    camera['name']=doc.getNodesWithXpath(name_path)[0].text.strip()
    camera['brand']=brand_pattern.search(camera['name']).group().lower()
    image_path='//meta[@property="og:image"]'
    camera['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    price_path='//span[@id="productLayoutForm:OurPrice"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        camera['price']=int(price[0].text.strip('Rs. '))

    addBox=doc.getNodesWithXpath('//a[@id="productLayoutForm:addToCartAction"]')

    if addBox:                           #availability check
        camera['availability']=1
        shipping_path='//div[@class="pdp_details_deliveryTime"]'
        shipping=doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping=shipping_pattern.search(shipping[0].text)
            camera['shipping']=[int(shipping.group(1)),int(shipping.group(2))]
    else:
         camera['availability']=0

    warranty_path='//table[@class="productShippingInfo"]'
    warranty=doc.getNodesWithXpath(warranty_path)
    if warranty:
        m=warranty_pattern.search(warranty[0].text_content())
        if m:
            camera['warranty']=m.group()

    offer_path='//div[@class="camerap_details_offer_text"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        camera['offer']=offer[0].text.strip()
    
    camera['specification']={}
    specification_tables_path='//table[@class="specs_txt"]/tbody'
    specification_tables=doc.getNodesWithXpath(specification_tables_path)
    if len(specification_tables)>0:
        for table in specification_tables:
            specs=doc.parseTBodyNode(table)
            if len(specs)>0:
                if table.xpath('tr[1]/th'):
                    type=table.xpath('tr[1]/th')[0].text.strip()
                    camera['specification'][type]={}
                    camera['specification'][type].update(specs)
    if 'LCD' in camera['specification']:
        util.replaceKey(camera['specification']['LCD'],'image display res','image display resolution')
        util.replaceKey(camera['specification']['LCD'],'video display res','video display resolution')

    if 'Playback' in camera['specification']:
        util.replaceKey(camera['specification']['Playback'],'slide show music - no of tunes','slide show music - number of tunes')

    camera['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in camera:
        product_history['price']=camera['price']
    if 'shipping' in camera:
        product_history['shipping']=camera['shipping']
    product_history['availability']=camera['availability']
    product_history['datetime']=camera['last_modified_datetime']
    camera['product_history']=[product_history,]
    camera['site']='homeshop18'
    return camera

def getDigicam():
    cameras=getCamerasOfCategory(digicam_home)
    for cam in cameras:
        cam['category']=set(['point and shoot',])
    return cameras

def getDslr():
    cameras=getCamerasOfCategory(dslr_home)
    for cam in cameras:
        cam['category']=set(['dslr',])
    return cameras

def getCamcorder():
    cameras=getCamerasOfCategory(camcorder_home)
    for cam in cameras:
        cam['category']=set(['camcorder',])
    return cameras

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    camera_coll=db['scraped_cameras']
    camera_coll.create_index('url',unique=True)
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    cameras=getAllCameras()
    for camera in cameras:
        camera['category']=list(camera['category'])
        try:
            print camera['url']
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
    
if __name__=='__main__':
    insertIntoDB()
