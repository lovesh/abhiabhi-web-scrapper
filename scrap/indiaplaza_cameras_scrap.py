import downloader
import dom
import re
import urllib2
import math
import datetime
import pymongo
import requests
import sys

siteurl='http://www.indiaplaza.com'
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})
if len(sys.argv)>1:
    proxy={'http':sys.argv[1]}
else:
    proxy=None

camera_categories=['dslr-cameras','camcorders','digital-cameras']

brand_junk_pattern=re.compile('\(\d+\)')
count_pattern = re.compile('\d+ of (\d+)', re.I)
shipping_pattern = re.compile('in (\d+)', re.I)
price_pattern = re.compile('\d+')
sku_pattern=re.compile('cameras-(\w+)-')
ajax_url='http://www.indiaplaza.com/buildfdppage.aspx'
ajax_post_data={'Gettabcontrol':'', 'store':'Cameras', 'category':'', 'subcategory':'', 'title':'', 'brand':''}

def getBrands(cat_url):
    if proxy:
        html=requests.get(url = cat_url, proxies = proxy).content
    else:
        html=requests.get(url = cat_url).content
    doc=dom.DOM(string=html)
    brand_path='//div[@id="divBrands"]/ul/li/a'
    brands=dict((brand_junk_pattern.sub('',link[0]).strip().lower(), siteurl + link[1]) for link in doc.getLinksWithXpath(brand_path))
    return brands

def getCameraFromPage(url=None,string=None):
    camera = {}
    
    if url:
	if proxy:
	    html = requests.get(url, proxies = proxy).content
	else:
	    html = requests.get(url).content
    else:
	html = string
    
    doc = dom.DOM(string = html)
    
    url_path = '//link[@rel="canonical"]'
    name_path = '//div[@class="descColSkuNamenew"]/h1'
    warranty_path = '//div[@class="warrantyBg"]'
    price_path = '//span[@id="ContentPlaceHolder1_FinalControlValuesHolder_ctl00_FDPMainSection_lblOurPrice"]/span[@class="blueFont"]'
    shipping_path = '//span[@class="delDateQuest"]'
    availability_path = '//div[@id="ContentPlaceHolder1_FinalControlValuesHolder_ctl00_FDPMainSection_AddtoCartDiv"]'
    description_path = '//div[@id="ContentPlaceHolder1_FinalControlValuesHolder_ctl00_FDPMainSection_fdpDescDiv"]'
    ajax_specs_path = '//div[@id="litDesc"]/table'
    img_path = '//img[@id="my_image"]'
    image = doc.getImgUrlWithXpath(img_path)
    if len(image) > 0:
	camera['img_url']={'0':image[0]}
    
    url = doc.getNodesWithXpath(url_path)
    if len(url) > 0:
	camera['url'] = url[0].get('href')
	
    name = doc.getNodesWithXpath(name_path)
    if len(name) > 0:
	camera['name'] = name[0].text.strip()
    warranty = doc.getNodesWithXpath(warranty_path)
    if len(warranty) > 0:
	camera['warranty'] = warranty[0].text_content()
    price = doc.getNodesWithXpath(price_path)
    if len(price) > 0:
	camera['price'] = price_pattern.search(price[0].text).group()
    description = doc.getNodesWithXpath(description_path)
    if len(description) > 0:
	camera['description'] = description[0].text_content()
	
    availability = doc.getNodesWithXpath(availability_path)
    if len(availability) > 0:
	presence = availability[0].get('style')
	if presence == '"display:block;"':
	    camera['availability'] = 1
	    shipping = doc.getNodesWithXpath(shipping_path)
	    if len(shipping) > 0:
		shipping = shipping_pattern.search(shipping[0].text)
		if shipping:
		    camera['shipping']=(shipping.group(1),)
	else:
	    camera['availability'] = 0
    else:
	camera['availability'] = 0
    
    offer_path = '//span[@class="fdpFree"]'
    offer = doc.getNodesWithXpath(offer_path)
    if len(offer) > 0:
	camera['offer'] = offer[0].text_content()
    
    sku=sku_pattern.search(camera['url']).group(1)
    ajax_post_data['sku'] = sku
    while(True):
	try:
	    if proxy:
		ajax_res = requests.post(ajax_url, data = ajax_post_data, proxies=proxy).content
	    else:
		ajax_res = requests.post(ajax_url, data = ajax_post_data).content
	    break
	except requests.exceptions.ConnectionError:
	    print 'exception raised'
	    pass
    specification = {}
    ajax_dom = dom.DOM(string = ajax_res)
    specs_node = ajax_dom.getNodesWithXpath(ajax_specs_path)
    if len(specs_node) > 0:
	divs = specs_node[0].xpath('//div')
	for div in divs:
	    clas = div.get('class')
	    if clas == 'fdpSpecCol1r':
		key = div.text_content()
	    if clas == 'fdpSpecCol2r':
		value = div.text_content()
		specification[key] = value
    
    camera['specification'] = specification
    
    camera['last_modified_datetime'] = datetime.datetime.now()
    product_history = {}
    if 'price' in camera:
        product_history['price'] = camera['price']
    if 'shipping' in camera:
        product_history['shipping'] = camera['shipping']
    product_history['availability'] = camera['availability']
    product_history['datetime'] = camera['last_modified_datetime']
    camera['product_history'] = [product_history,]
    camera['site'] = 'indiaplaza'
    
    return camera

def getCameraUrlsFromBrandPage(url = None, html = None):
    doc = dom.DOM(string = html)
    camera_url_path = '//div[@class="skuName"]/a'
    urls = [siteurl + link[1] for link in doc.getLinksWithXpath(camera_url_path)]
    return urls
    
def getCamerasOfBrand(brand):
    if proxy:
	html=requests.get(url = brand[1], proxies = proxy).content
    else:
	html=requests.get(url = brand[1]).content
    first_page=dom.DOM(string = html)
    brand = re.search('.com/(.+)-cameras-.',brand[1]).group(1)
    camera_urls = []
    camera_urls.extend(getCameraUrlsFromBrandPage(html = first_page.html))
    count_path = '//div[@class="prodNoArea"]'
    count_string = first_page.getNodesWithXpath(count_path)[0].text
    count = int(count_pattern.search(count_string).group(1))
    if count>20:
        num_pages = int(math.ceil(count/20.0))
        page_urls = [brand[1]+'&PageNo='+str(n) for n in xrange(2, num_pages+1)]
        dl.putUrls(page_urls)
	if proxy:
	    result = dl.download(proxy = proxy)
	else:
	    result = dl.download()
        for r in result:
            status = result[r][0]
            html = result[r][1]
            if status > 199 and status < 400:
                camera_urls.extend(getCameraUrlsFromBrandPage(html = html))
    
    cameras = []
    
    dl.putUrls(camera_urls)
    if proxy:
	result = dl.download(proxy = proxy)
    else:
	result = dl.download()
    
    for r in result:
	status=result[r][0]
	html=result[r][1]
	if status > 199 and status < 400:
	    print r
	    camera = getCameraFromPage(string=html)
	    if camera:
		camera['brand'] = brand[0]
		camera['url'] = r
		cameras.append(camera)
		
    return cameras

def getCamerasOfCategory(category):
    cameras=[]
    category_url='http://www.indiaplaza.com/%s-cameras-1.htm'%category
    brands = getBrands(category_url)
    for brand in brands:
        cameras.extend(getCamerasOfBrand((brand, brands[brand])))
    if category=='dslr-cameras':
	category='dslr'
    for camera in cameras:
	if category=='camcorders':
	    category='camcorder'
	if category=='digital-cameras':
	    category='point and shoot'
	if category=='dslr-cameras':
	    category='dslr'
	camera['category']=set([category,])
    return cameras

def scrapAllCameras():
    f=open('indiaplaza_cameras_log.txt','w')
    cameras=[]
    for category in camera_categories:
        cameras.extend(getCamerasOfCategory(category))
        f.write("Got cameras of category %s\n"%category)
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
    db.authenticate('root','hpalpha1911')
    camera_coll=db['scraped_cameras']
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    cameras=scrapAllCameras()
    for camera in cameras:
        try:
            camera['category']=list(camera['category'])
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
