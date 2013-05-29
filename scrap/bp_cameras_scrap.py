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
point_shoot_home='http://www.buytheprice.com/category__point-and-shoot-cameras-125'
dslr_home='http://www.buytheprice.com/category__digital-slr-cameras-124'
camcorder_home='http://www.buytheprice.com/category__camcorders-128'
debug=True
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})
shipping_pattern=re.compile('(\d+)-(\d+)')

def getCategoryUrls(cat_url):
    doc=dom.DOM(url=cat_url)
    num_pages=7
    page_urls=[cat_url+'~'+str(n) for n in xrange(1,num_pages+1)]
    cat_urls=set()
    dl.putUrls(page_urls)
    pages=dl.download()
    if debug:
        print '%d Pages found\n'%len(pages)
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            cat_urls.update(getCameraUrlsOfPage(html=html))
    return cat_urls


def getCameraUrlsOfPage(url=None,html=None):
    camera_url_path='//div[@class="product-block1"]/a[1]'
    doc=dom.DOM(string=html)
    urls=set(link[1] for link in doc.getLinksWithXpath(camera_url_path))
    return urls

def getCameraFromPage(url=None,string=None):
    camera={}
    if url:
        doc=dom.DOM(url=url)
        camera['url']=url
    else:
        doc=dom.DOM(string=string, utf8 = True)
        url_path='//meta[@property="og:url"]'        
        url=doc.getNodesWithXpath(url_path)
        if len(url)==0:
            return False
        camera['url']=url[0].get('content')
    if debug:
        print camera['url']
    image_path='//meta[@property="og:image"]'
    camera['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    addBox=doc.getNodesWithXpath('//button[@class="btn btn-warning btn-large"]')
    name_path='//div[@id="p-infocol"]/h1'
    camera['name']=doc.getNodesWithXpath(name_path)[0].text
    #junk=junk_pattern.search(name)
    #if junk:
        #name=name[:junk.start()]
    #camera['name']=name.strip(' (')
    camera['brand']=re.search('\w+',camera['name']).group().lower()
    price_path='//span[@id="p-ourprice-m"]/span[@itemprop="price"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        camera['price']=int(price[0].text.replace(',',''))
    if addBox and addBox[0].text_content().strip()=='Buy Now':                           #availability check
        camera['availability']=1
        shipping_path='//div[@class="prblinfo"][2]'
        shipping=doc.getNodesWithXpath(shipping_path)[0].text_content()
        if shipping:
            m=shipping_pattern.search(shipping)
            if m:
                camera['shipping']=(m.group(1),m.group(2))
    else:
        camera['availability']=0

    camera['specification']=[]
    specification_table_path='//div[@id="features"]/table'
    specification_table=doc.getNodesWithXpath(specification_table_path)
    specs_key_path='td[@class="prodspecleft"]'
    specs_value_path='td[@class="prodspecright"]'
    specification={}
    if len(specification_table)>0:
        for specs in specification_table:
            trs=specs.xpath('tr')
            for tr in trs:
                td=tr.xpath(specs_key_path)
                if len(td)>0:
                    key=td[0].text
                    if key:
                        key=key.strip().lower()
                        value=tr.xpath(specs_value_path)[0].text
                        if value:
                            value=value.strip().lower()
                            specification[key]=value     #only put specification if value is not None 
    util.replaceKey(specification,'video capture resolution','video display resolution')
    camera['specification']=specification
    camera['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in camera:
        product_history['price']=camera['price']
    if 'shipping' in camera:
        product_history['shipping']=camera['shipping']
    product_history['availability']=camera['availability']
    product_history['datetime']=camera['last_modified_datetime']
    camera['product_history']=[product_history,]
    camera['site']='buytheprice'
    return camera

def scrapAllCameras():
    ps_urls=getCategoryUrls(point_shoot_home)
    dslr_urls=getCategoryUrls(dslr_home)
    cam_urls=getCategoryUrls(camcorder_home)
    cameras=[]
    failed=[]
    dl.putUrls(ps_urls)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            camera=getCameraFromPage(string=html)
            if camera:
                camera['category']=['point and shoot',]
                cameras.append(camera)
        else:
            failed.append('%s with %s'%(r,str(status)))
    
    dl.putUrls(dslr_urls)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            camera=getCameraFromPage(string=html)
            if camera:
                camera['category']=['dslr',]
                cameras.append(camera)
        else:
            failed.append('%s with %s'%(r,str(status)))
	    
    dl.putUrls(cam_urls)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            camera=getCameraFromPage(string=html)
            if camera:
                camera['category']=['camcorder',]
                cameras.append(camera)
        else:
            failed.append('%s with %s'%(r,str(status)))

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
    camera_coll.create_index('url',unique=True)
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
    
if __name__=='__main__':
    insertIntoDB()
