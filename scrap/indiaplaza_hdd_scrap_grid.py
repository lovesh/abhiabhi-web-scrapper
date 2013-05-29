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
hdd_home = 'http://www.indiaplaza.com/hard-drives-pc-2.htm?Category=data-storage'
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':hdd_home})
if len(sys.argv)>1:
    proxy={'http':sys.argv[1]}
else:
    proxy=None
    
count_pattern = re.compile('\d+ of (\d+)', re.I)
brand_junk_pattern=re.compile('\(\d+\)')
shipping_pattern = re.compile('in (\d+)', re.I)
price_pattern = re.compile('\d+')
sku_pattern = re.compile('pc-(\w+)-')
ajax_url = 'http://www.indiaplaza.com/buildfdppage.aspx'
ajax_post_data = {'Gettabcontrol':'','store':'PC','category':'Data+Storage','subcategory':'','title':'','brand':''}
capacity_pattern=re.compile('(\d+\.?\d?) ?(g|t)b',re.I)
interface_pattern=re.compile('usb ?\d\.?\d?',re.I)


def getBrands():
    if proxy:
        html=requests.get(url = hdd_home, proxies = proxy).content
    else:
        html=requests.get(url = hdd_home).content
    doc=dom.DOM(string=html)
    brand_path='//div[@id="divBrands"]/ul/li/a'
    brands=dict((brand_junk_pattern.sub('',link[0]).strip().lower(), siteurl + link[1]) for link in doc.getLinksWithXpath(brand_path))
    return brands
  
def getHDDUrlsFromBrandPage(url = None, string = None):
    doc = dom.DOM(string = string)
    hdd_url_path = '//div[@class="skuImg"]/a'
    urls = [siteurl + link[1] for link in doc.getLinksWithXpath(hdd_url_path)]
    return urls
    
def getHDDsOfBrand(brand,get_details=False):
    if proxy:
        html=requests.get(url=brand[1],proxies=proxy).content
    else:
        html=requests.get(url=brand[1]).content
        
    first_page=dom.DOM(string=html)
    hdd_urls=[]
    hdd_urls.extend(getHDDUrlsFromBrandPage(string = first_page.html))
    count_path = '//div[@class="prodNoArea"]'
    count_string = first_page.getNodesWithXpath(count_path)[0].text
    count = int(count_pattern.search(count_string).group(1))
    if count>20:
        num_pages = int(math.ceil(count/20.0))
        page_urls = [brand[1]+'&PageNo='+str(n) for n in xrange(2,num_pages+1)]
        dl.putUrls(page_urls)
	if proxy:
	    result = dl.download(proxy=proxy)
	else:
	    result = dl.download()
        for r in result:
            status = result[r][0]
            html = result[r][1]
            if status > 199 and status < 400:
                hdd_urls.extend(getHDDUrlsFromBrandPage(string=html))
    
    hdds = []
    
    dl.putUrls(hdd_urls)
    hdds = []
    if proxy:
        result = dl.download(proxy = proxy)
    else:
        result = dl.download()
    
    for r in result:
        status = result[r][0]
        html = result[r][1]
        if status > 199 and status < 400:
            print r
            hdd = getHDDFromPage(string=html)
            if hdd:
                hdd['url'] = r
                hdds.append(hdd)
    
    for hdd in hdds:
        hdd['brand'] = brand[0]
    print "%d hdds of brand %s"%(len(hdds),brand[0])
    return hdds

def getHDDFromPage(url = None, string = None):
    hdd = {}
    
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
        hdd['img_url']={'0':image[0]}
    url = doc.getNodesWithXpath(url_path)
    if len(url) > 0:
	hdd['url'] = url[0].get('href')
    name = doc.getNodesWithXpath(name_path)
    if len(name) > 0:
        hdd['name'] = name[0].text.strip()
    warranty = doc.getNodesWithXpath(warranty_path)
    if len(warranty) > 0:
        hdd['warranty'] = warranty[0].text_content()
    price = doc.getNodesWithXpath(price_path)
    if len(price) > 0:
        hdd['price'] = price_pattern.search(price[0].text).group()
    description = doc.getNodesWithXpath(description_path)
    if len(description) > 0:
        hdd['description'] = description[0].text_content()
	
    availability = doc.getNodesWithXpath(availability_path)
    if len(availability) > 0:
        presence = availability[0].get('style')
        if presence == '"display:block;"':
            hdd['availability'] = 1
            shipping = doc.getNodesWithXpath(shipping_path)
            if len(shipping) > 0:
                shipping = shipping_pattern.search(shipping[0].text)
                if shipping:
		    hdd['shipping']=(shipping.group(1),)
	else:
	    hdd['availability'] = 0
    
    sku=sku_pattern.search(hdd['url']).group(1)
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
                key = div.text_content().strip().lower()
            if clas == 'fdpSpecCol2r':
                value = div.text_content().strip()
                specification[key] = value
    
    util.replaceKey(specification, 'storage capacity', 'capacity')
    
    hdd['specification'] = specification
    
    if 'capacity' not in hdd['specification']:
	m=capacity_pattern.search(hdd['name']) 
	if m:
	    hdd['specification']['capacity']=m.group()
	    
    if 'interface' not in hdd['specification']:
	m = interface_pattern.search(hdd['name'])
	if m:
	    hdd['specification']['interface']=m.group()
    
    hdd['last_modified_datetime'] = datetime.datetime.now()
    product_history = {}
    if 'price' in hdd:
        product_history['price'] = hdd['price']
    if 'shipping' in hdd:
        product_history['shipping'] = hdd['shipping']
    product_history['availability'] = hdd['availability']
    product_history['datetime'] = hdd['last_modified_datetime']
    hdd['product_history'] = [product_history, ]
    hdd['site'] = 'indiaplaza'
    
    return hdd

def scrapAllHDDs():
    f=open('indiaplaza_hdds_log.txt','w')
    hdds=[]
    brands=getBrands()
    for brand in brands:
        hdds.extend(getHDDsOfBrand((brand,brands[brand])))
        f.write("Got hdds of brand %s\n"%brand)
        f.flush()
    return hdds
    
def insertIntoDB(log=True):
    con = pymongo.Connection('localhost',27017)
    db = con['abhiabhi']
    hdd_coll = db['scraped_harddisks']
    hdd_coll.create_index('url',unique=True)
    inserted_count = 0
    updated_count = 0
    inserted_urls = []
    updated_urls = []
    hdds = scrapAllHDDs()
    for hdd in hdds:
        try:
            hdd_coll.insert(hdd,safe = True)
            inserted_count += 1
            inserted_urls.append(hdd['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability' in hdd:
                upd['availability'] = hdd['availability']
            if 'price' in hdd:
                upd['price'] = hdd['price']
            if 'shipping' in hdd:
                upd['shipping'] = hdd['shipping']
	    if 'offer' in hdd:
                upd['offer'] = hdd['offer']
	    else:
		upd['offer'] = ''
            hdd_coll.update({'url':hdd['url']},{'$push':{'product_history':hdd['product_history'][0]},'$set':upd})
            updated_count += 1
            updated_urls.append(hdd['url'])
    if log:
        scrap_log = db['scrap_log']
        log = {'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'harddisk','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
	
    print "%d inserted and %d updated"%(inserted_count,updated_count)

if __name__=='__main__':
    insertIntoDB()

