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
    
cat_urls = ['http://www.indiaplaza.com/smart-phones-mobiles-1.htm', 'http://www.indiaplaza.com/business-phones-mobiles-1.htm', 'http://www.indiaplaza.com/value-phones-mobiles-1.htm']

brand_junk_pattern=re.compile('\(\d+\)')
count_pattern = re.compile('\d+ of (\d+)', re.I)
shipping_pattern = re.compile('in (\d+)', re.I)
price_pattern = re.compile('\d+')
sku_pattern=re.compile('mobiles-(\w+)-')
ajax_url='http://www.indiaplaza.com/buildfdppage.aspx'
ajax_post_data={'Gettabcontrol':'', 'store':'Mobiles', 'category':'', 'subcategory':'', 'title':'', 'brand':''}

def getBrands(cat_url):
    if proxy:
        html=requests.get(url = cat_url, proxies = proxy).content
    else:
        html=requests.get(url = cat_url).content
    doc=dom.DOM(string=html)
    brand_path='//div[@id="divBrands"]/ul/li/a'
    brands=dict((brand_junk_pattern.sub('',link[0]).strip().lower(), siteurl + link[1]) for link in doc.getLinksWithXpath(brand_path))
    return brands

def getMobilesOfBrand(brand):
    if proxy:
	html = requests.get(brand[1], proxies = proxy).content
    else:
	html = requests.get(brand[1]).content
    first_page=dom.DOM(string=html)
    mobile_urls=[]
    mobile_urls.extend(getMobileUrlsFromBrandPage(html = first_page.html))
    count_path = '//div[@class="prodNoArea"]'
    count_string = first_page.getNodesWithXpath(count_path)
    count=int(count_pattern.search(count_string[0].text_content()).group(1))
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
		mobile_urls.extend(getMobileUrlsFromBrandPage(html = html))
		
    mobiles = []
    dl.putUrls(mobile_urls)
    if proxy:
	result = dl.download(proxy = proxy)
    else:
	result = dl.download()
    
    for r in result:
	status=result[r][0]
	html=result[r][1]
	if status > 199 and status < 400:
	    print r
	    mobile = getMobileFromPage(string=html)
	    if mobile:
		mobile['brand'] = brand[0]
		mobile['url'] = r
		mobiles.append(mobile)
    return mobiles      
    
    
def getMobileUrlsFromBrandPage(url = None, html = None):
    doc = dom.DOM(string = html)
    mobile_url_path = '//div[@class="skuName"]/a'
    urls = [siteurl + link[1] for link in doc.getLinksWithXpath(mobile_url_path)]
    return urls
	

def getMobileFromPage(url = None, string = None):
    mobile = {}
    
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
	mobile['img_url']={'0':image[0]}
    
    url = doc.getNodesWithXpath(url_path)
    if len(url) > 0:
	mobile['url'] = url[0].get('href')
	
    name = doc.getNodesWithXpath(name_path)
    if len(name) > 0:
	mobile['name'] = name[0].text.strip()
    warranty = doc.getNodesWithXpath(warranty_path)
    if len(warranty) > 0:
	mobile['warranty'] = warranty[0].text_content()
    price = doc.getNodesWithXpath(price_path)
    if len(price) > 0:
	mobile['price'] = price_pattern.search(price[0].text).group()
    description = doc.getNodesWithXpath(description_path)
    if len(description) > 0:
	mobile['description'] = description[0].text_content()
	
    availability = doc.getNodesWithXpath(availability_path)
    if len(availability) > 0:
	presence = availability[0].get('style')
	if presence == '"display:block;"':
	    mobile['availability'] = 1
	    shipping = doc.getNodesWithXpath(shipping_path)
	    if len(shipping) > 0:
		shipping = shipping_pattern.search(shipping[0].text)
		if shipping:
		    mobile['shipping']=(shipping.group(1),)
	else:
	    mobile['availability'] = 0
    
    offer_path = '//span[@class="fdpFree"]'
    offer = doc.getNodesWithXpath(offer_path)
    if len(offer) > 0:
	mobile['offer'] = offer[0].text_content()
    
    sku=sku_pattern.search(mobile['url']).group(1)
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
    
    mobile['specification'] = specification
    
    mobile['last_modified_datetime'] = datetime.datetime.now()
    product_history = {}
    if 'price' in mobile:
        product_history['price'] = mobile['price']
    if 'shipping' in mobile:
        product_history['shipping'] = mobile['shipping']
    product_history['availability'] = mobile['availability']
    product_history['datetime'] = mobile['last_modified_datetime']
    mobile['product_history'] = [product_history,]
    mobile['site'] = 'indiaplaza'
    
    return mobile
	
def getMobilesOfCategories(cat_url):
    mobiles = []
    brands = getBrands(cat_url)
    for brand in brands:
	mobiles.extend(getMobilesOfBrand((brand, brands[brand])))
	
    return mobiles
	
def scrapAllMobiles():
    mobiles = []
    for cat_url in cat_urls:
	mobiles.extend(getMobilesOfCategories(cat_url))
    		    
    return mobiles
			
def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    mobile_coll=db['scraped_mobiles']
    mobile_coll.create_index('url',unique=True)
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
        log={'siteurl':siteurl, 'datetime':datetime.datetime.now(), 'product':'mobile', 'products_updated_count':updated_count, 'products_inserted_count':inserted_count, 'products_updated_urls':updated_urls, 'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
	
    print "%d inserted and %d updated"%(inserted_count,updated_count)

if __name__=='__main__':
    insertIntoDB()
