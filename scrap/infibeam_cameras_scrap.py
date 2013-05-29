import downloader
import dom
import re
import urllib
import math
import datetime
import pymongo
import simplejson as json
import util

siteurl='http://www.infibeam.com'
point_shoot_home='http://www.infibeam.com/Cameras/search?subCategory=Point%20and%20Shoot'
dslr_home='http://www.infibeam.com/Cameras/search?subCategory=DSLR'
sslr_home='http://www.infibeam.com/Cameras/search?subCategory=Semi%20SLR'
camcorder_home='http://www.infibeam.com/Cameras/search?category=Camcorder'
debug=True

shipping_pattern=re.compile('Ships in (\d+)-(\d+)',re.I)
brand_pattern=re.compile('\w+')

dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})

def getCameraUrlsOfPage(html):
    camera_url_path='//ul[@class="srch_result landscape"]/li/a'
    page_dom=dom.DOM(string=html)
    links=set(siteurl+l[1] for l in page_dom.getLinksWithXpath(camera_url_path))
    return links

def getAllCameras():
    cameras=[]
    cameras.extend(getPointAndShoot())
    cameras.extend(getDslr())
    cameras.extend(getSdslr())
    cameras.extend(getCamcorder())
    return cameras

def getCamerasOfCategory(cat_url):
    doc=dom.DOM(url=cat_url)
    count_path='//div[@id="resultsPane"]/div/div/b[2]'
    count=doc.getNodesWithXpath(count_path)
    camera_urls=[]
    if len(count)>0:
        count=int(count[0].text)
        num_pages=int(math.ceil(count/20.0))
    else:
        num_pages=1
    page_urls=[cat_url+'&page='+str(n) for n in xrange(1,num_pages+1)]
    dl.putUrls(page_urls)
    pages=dl.download()
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            camera_urls.extend(getCameraUrlsOfPage(html))
    dl.putUrls(camera_urls,10)
    result=dl.download()
    cameras=[]
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            print r
            camera=getCameraFromPage(string=html)
            if camera:
                camera['url']=r
                cameras.append(camera)
    return cameras

def getPointAndShoot():
    cameras=getCamerasOfCategory(point_shoot_home)
    for cam in cameras:
        cam['category']=set(['point and shoot',])
    return cameras

def getDslr():
    cameras=getCamerasOfCategory(dslr_home)
    for cam in cameras:
        cam['category']=set(['dslr',])
    return cameras

def getSdslr():
    cameras=getCamerasOfCategory(sslr_home)
    for cam in cameras:
        cam['category']=set(['dslr',])
    return cameras

def getCamcorder():
    cameras=getCamerasOfCategory(camcorder_home)
    for cam in cameras:
        cam['category']=set(['camcorder',])
    return cameras

def getCameraFromPage(url=None,string=None):
    camera={}
    if url:
        doc=dom.DOM(url=url)
    else:
        doc=dom.DOM(string=string)

    showcase_path='//div[@id="tabshowcase"]'
    showcase=doc.getNodesWithXpath(showcase_path)
    if showcase:
        return False
    addBox=doc.getNodesWithXpath('//input[@class="buyimg "]')

    if addBox:                           #availability check
        camera['availability']=1
        details_path='//div[@id="ib_details"]'
        details=doc.getNodesWithXpath(details_path)
        if details:
            details=details[0].text_content()
            shipping=shipping_pattern.search(details[0])
            if shipping:
                camera['shipping']=[shipping.group(1),shipping.group(2)]
    else:
	camera['availability']=0
    name_path='//div[@id="ib_details"]/h1'
    camera['name']=doc.getNodesWithXpath(name_path)[0].text_content().strip()
    camera['brand']=brand_pattern.search(camera['name']).group().lower()

    color_path='//a[@class="colorlink"]'
    colors=doc.getNodesWithXpath(color_path)
    camera['colors']=[color.get('text') for color in colors]

    price_path='//span[@class="infiPrice amount price"]'
    price=doc.getNodesWithXpath(price_path)
    if price:
        camera['price']=int(price[0].text.replace(',',''))
    img_path="//div[@id='ib_img_viewer']/img"
    camera['img_url']={'0':doc.getImgUrlWithXpath(img_path)}

    desc_path='//div[@class="reviews-box-cont-inner"]'
    desc=doc.getNodesWithXpath(desc_path)
    if desc:
        camera['description']=desc[0].text_content.strip()
    
    camera['last_modified_datetime']=datetime.datetime.now()
    
    product_history={}
    if 'price' in camera:
        product_history['price']=camera['price']
    if 'shipping' in camera:
        product_history['shipping']=camera['shipping']
    product_history['availability']=camera['availability']
    product_history['datetime']=camera['last_modified_datetime']
    camera['product_history']=[product_history,]
    camera['site']='infibeam'

    offer_path='//div[@class="offer"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        camera['offer']=offer[0].text_content().replace('\r\n ','')

    specs_path='//div[@id="specs"]/div'
    specs=doc.getNodesWithXpath(specs_path)
    specification={}
    for spec in specs:
        text=spec.xpath('a')[0].text.strip()
        if text=='Deliverable Locations' or text=='Disclaimer':
            continue
        trs=spec.xpath('.//tr')
        for tr in trs:
            tds=tr.xpath('.//td')
            if len(tds)<2:
                continue
            key=tds[0].text_content().strip(':\n\t ').replace('.','').lower()
            value=tds[1].text_content().strip(':\n\t ').lower()
            specification[key]=value

    util.replaceKey(specification,'resolution (megapixel)','sensor resolution')
    util.replaceKey(specification,'built in flash','flash')
    util.replaceKey(specification,'inbuilt memory (mb)','internal memory')
    
    camera['specification']=specification
    return camera

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
