import downloader
import dom
import re
import urllib2
import math
import datetime
import pymongo
import requests
import sys
import util

siteurl='http://www.indiaplaza.com'
tablet_home = 'http://www.indiaplaza.com/tablet-pc-1.htm'
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':tablet_home})
if len(sys.argv)>1:
    proxy={'http':sys.argv[1]}
else:
    proxy=None
    
count_pattern = re.compile('\d+ of (\d+)', re.I)
brand_junk_pattern=re.compile('\(\d+\)')
shipping_pattern = re.compile('in (\d+)', re.I)
price_pattern = re.compile('\d+')
sku_pattern = re.compile('pc-(\w+)-')
ram_pattern=re.compile('(\d{3} ?mb|(1|2) ?gb)',re.I)
ram_type_pattern=re.compile('DDR\d?',re.I)
storage_pattern=re.compile('(([3-9]|\d{2}) ?gb)',re.I)
screen_size_pattern=re.compile('\d\.?\d? ?(\'\'|\"|inch|hd)',re.I)
g_pattern=re.compile('\W((3|4) ?g)\W',re.I)
wifi_pattern=re.compile('wifi',re.I)
resolution_pattern=re.compile('\d{3} ?x ?\d{3}')
cpu_clock_speed_pattern=re.compile('\d\.?\d{0,2} ?ghz',re.I)
android_pattern=re.compile('android',re.I)
windows_pattern=re.compile('windows|w7',re.I)

ajax_url = 'http://www.indiaplaza.com/buildfdppage.aspx'
ajax_post_data = {'Gettabcontrol':'','store':'PC','category':'Tablet','subcategory':'','title':'','brand':''}

def getBrands():
    if proxy:
        html=requests.get(url = tablet_home, proxies = proxy).content
    else:
        html=requests.get(url = tablet_home).content
    doc=dom.DOM(string=html)
    brand_path='//div[@id="divBrands"]/ul/li/a'
    brands=dict((brand_junk_pattern.sub('',link[0]).strip().lower(), siteurl + link[1]) for link in doc.getLinksWithXpath(brand_path))
    util.replaceKey(brands,'sony vaio','sony')
    util.replaceKey(brands,'bsnl pental','pantech')
    return brands
  
def getTabletUrlsFromBrandPage(url = None, string = None):
    doc = dom.DOM(string = string)
    tablet_url_path = '//div[@class="skuImg"]/a'
    urls = [siteurl + link[1] for link in doc.getLinksWithXpath(tablet_url_path)]
    return urls
    
def getTabletsOfBrand(brand,get_details=False):
    if proxy:
        html=requests.get(url=brand[1],proxies=proxy).content
    else:
        html=requests.get(url=brand[1]).content
        
    first_page=dom.DOM(string=html)
    tablet_urls=[]
    tablet_urls.extend(getTabletUrlsFromBrandPage(string=first_page.html))
    count_path='//div[@class="prodNoArea"]'
    count_string=first_page.getNodesWithXpath(count_path)[0].text
    count=int(count_pattern.search(count_string).group(1))
    if count>20:
        num_pages=int(math.ceil(count/20.0))
        page_urls=[brand[1]+'&PageNo='+str(n) for n in xrange(2,num_pages+1)]
        dl.putUrls(page_urls)
	if proxy:
	    result=dl.download(proxy=proxy)
	else:
	    result=dl.download()
        for r in result:
            status=result[r][0]
            html=result[r][1]
            if status > 199 and status < 400:
                tablet_urls.extend(getTabletUrlsFromBrandPage(string=html))
    
    tablets = []
    
    dl.putUrls(tablet_urls)
    tablets = []
    if proxy:
        result = dl.download(proxy = proxy)
    else:
        result = dl.download()
    
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            print r
            tablet = getTabletFromPage(string=html)
            if tablet:
                tablet['url'] = r
                tablets.append(tablet)
    
    for tablet in tablets:
        tablet['brand']=brand[0]
    print "%d tablets of brand %s"%(len(tablets),brand[0])
    return tablets

def getTabletFromPage(url = None, string = None):
    tablet = {}
    
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
        tablet['img_url']={'0':image[0]}
    url = doc.getNodesWithXpath(url_path)
    if len(url) > 0:
	tablet['url'] = url[0].get('href')
    name = doc.getNodesWithXpath(name_path)
    if len(name) > 0:
        tablet['name'] = name[0].text.strip()
    warranty = doc.getNodesWithXpath(warranty_path)
    if len(warranty) > 0:
        tablet['warranty'] = warranty[0].text_content()
    price = doc.getNodesWithXpath(price_path)
    if len(price) > 0:
        tablet['price'] = price_pattern.search(price[0].text).group()
    description = doc.getNodesWithXpath(description_path)
    if len(description) > 0:
        tablet['description'] = description[0].text_content()
	
    availability = doc.getNodesWithXpath(availability_path)
    if len(availability) > 0:
        presence = availability[0].get('style')
        if presence == '"display:block;"':
            tablet['availability'] = 1
            shipping = doc.getNodesWithXpath(shipping_path)
            if len(shipping) > 0:
                shipping = shipping_pattern.search(shipping[0].text)
                if shipping:
		    tablet['shipping']=(shipping.group(1),)
	else:
	    tablet['availability'] = 0
    
    sku=sku_pattern.search(tablet['url']).group(1)
    ajax_post_data['sku'] = sku
    while(True):
	try:
	    if proxy:
		ajax_res = requests.post(ajax_url, data = ajax_post_data,proxies=proxy).content
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
                key = div.text_content().strip().lower().replace('.', '')
            if clas == 'fdpSpecCol2r':
                value = div.text_content().strip()
                specification[key] = value
    
    
    tablet['specification'] = specification
    
    if len(specs_node) > 0:
	specs=specs_node[0].text_content()
	m = ram_pattern.search(specs)
	if m:
	    tablet['specification']['ram'] = m.group()
	m = ram_type_pattern.search(specs)
	if m:
	    tablet['specification']['memory type'] = m.group()
	m = storage_pattern.search(specs)
	if m:
	    tablet['specification']['storage'] = m.group()
	m = screen_size_pattern.search(specs)
	if m:
	    tablet['specification']['screen size'] = m.group()
	m = g_pattern.search(specs)
	if m:
	    tablet['specification']['g'] = m.group(1).strip()
	else:
	 m = g_pattern.search(tablet['name'])
	 if m:
	    tablet['specification']['g'] = m.group(1).strip()
	m = cpu_clock_speed_pattern.search(specs)
	if m:
	    tablet['specification']['clock speed'] = m.group()
	m = resolution_pattern.search(specs)
	if m:
	    tablet['specification']['resolotion'] = m.group()
	m = wifi_pattern.search(specs)
	if m:
	    tablet['specification']['wifi'] = 'yes'
	m = android_pattern.search(specs)
	if m:
	    tablet['specification']['os'] = 'Android'
	m = windows_pattern.search(specs)
	if m:
	    tablet['specification']['os'] = 'Windows'
    
    tablet['last_modified_datetime'] = datetime.datetime.now()
    product_history = {}
    if 'price' in tablet:
        product_history['price'] = tablet['price']
    if 'shipping' in tablet:
        product_history['shipping'] = tablet['shipping']
	
    product_history['availability'] = tablet['availability']
    product_history['datetime'] = tablet['last_modified_datetime']
    tablet['product_history'] = [product_history, ]
    tablet['site'] = 'indiaplaza'
    
    return tablet

def scrapAllTablets():
    f=open('indiaplaza_tablets_log.txt','w')
    tablets=[]
    brands=getBrands()
    for brand in brands:
        tablets.extend(getTabletsOfBrand((brand,brands[brand])))
        f.write("Got tablets of brand %s\n"%brand)
        f.flush()
    return tablets
    
def insertIntoDB(log=True):
    con = pymongo.Connection('localhost',27017)
    db = con['abhiabhi']
    tablet_coll = db['scraped_tablets']
    tablet_coll.create_index('url',unique=True)
    inserted_count = 0
    updated_count = 0
    inserted_urls = []
    updated_urls = []
    tablets = scrapAllTablets()
    for tablet in tablets:
        try:
            tablet_coll.insert(tablet,safe = True)
            inserted_count += 1
            inserted_urls.append(tablet['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability' in tablet:
                upd['availability'] = tablet['availability']
            if 'price' in tablet:
                upd['price'] = tablet['price']
            if 'shipping' in tablet:
                upd['shipping'] = tablet['shipping']
	    if 'offer' in tablet:
                upd['offer'] = tablet['offer']
	    else:
		upd['offer'] = ''
            tablet_coll.update({'url':tablet['url']},{'$push':{'product_history':tablet['product_history'][0]},'$set':upd})
            updated_count += 1
            updated_urls.append(tablet['url'])
    if log:
        scrap_log = db['scrap_log']
        log = {'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'tablet','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
	
    print "%d inserted and %d updated"%(inserted_count,updated_count)

if __name__=='__main__':
    insertIntoDB()

