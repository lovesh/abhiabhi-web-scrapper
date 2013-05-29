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
referer='http://www.indiaplaza.com/hard-drives-pc-2.htm?Category=data-storage'
ajax_url='http://www.indiaplaza.com/buildfdppage.aspx'
ajax_post_data={'Gettabcontrol':'','store':'PC','category':'Data+Storage','subcategory':'','title':'','brand':''}

dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})
if len(sys.argv)>1:
    proxy={'http':sys.argv[1]}
else:
    proxy={'http':'187.72.86.10:3128'}
    
brand_junk_pattern=re.compile('\(\d+\)')
sku_pattern=re.compile('pc-(\w+)-')
shipping_pattern=re.compile('ships in (\d+)',re.I)
capacity_pattern=re.compile('(\d+\.?\d?) ?(g|t)b',re.I)
interface_pattern=re.compile('usb ?\d\.?\d?',re.I)

def getBrands():
    html=requests.get(url=referer,proxies=proxy).content
    doc=dom.DOM(string=html)
    brand_path='//div[@id="divBrands"]/ul/li/a'
    brands=dict((brand_junk_pattern.sub('',link[0]).strip().lower(),'http://www.indiaplaza.com'+link[1]) for link in doc.getLinksWithXpath(brand_path))
    return brands

def getHDDUrlsFromBrandPage(string=None,url=None):
    if string:
	page=dom.DOM(string=string)
    else:
	page=dom.DOM(url=url)
    hdd_url_path='//div[@class="skuImg"]/a'
    
    hdd_urls=[siteurl+url[1] for url in page.getLinksWithXpath(hdd_url_path)]
    return hdd_urls
	
def getHDDFromPage(url=None,string=None):
    hdds=[]
    #hdd_block_path='//div[@class="skuRow"]'
    if string:
        page=dom.DOM(string=string)
    else:
	html=requests.get(url=url,proxies=proxy).content
        page=dom.DOM(string=html)
    url_path='//link[@rel="canonical"]'
    img_path='//img[@id="my_image"]'
    name_path='//div[@class="descColSkuNamenew"]/h1'
    price_path='//span[@id="ContentPlaceHolder1_FinalControlValuesHolder_ctl00_FDPMainSection_lblOurPrice"]/span[2]'
    ajax_specs_path='//div[@id="litDesc"]/table'
    hdd={}
    hdd['url']=page.getNodesWithXpath(url_path)[0].get('href')
    print hdd['url']
    hdd['img_url']={'0':page.getImgUrlWithXpath(img_path)[0]}
    name=page.getNodesWithXpath(name_path)[0].text_content().encode('ascii','ignore').strip()
    hdd['name']=name
    price_string=page.getNodesWithXpath(price_path)[0].text_content()
    hdd['price']=int(re.search('(\D)+(\d+)',price_string).group(2))
    
    addBox=page.getNodesWithXpath('//div[@id="ContentPlaceHolder1_FinalControlValuesHolder_ctl00_FDPMainSection_AddtoCartDiv"]')[0]

    if addBox.get('style') == "display:block;":                           #availability check
        hdd['availability']=1
        shipping_path='//span[@class="delDateQuest"]'
        shipping=shipping_pattern.search(page.getNodesWithXpath(shipping_path)[0].text_content())
	if shipping:
	    hdd['shipping']=(shipping.group(1),)
    else:
        hdd['availability']=0
    
    hdd['specification']={}

    m=interface_pattern.search(name)
    if m:
	hdd['specification']['interface']=m.group()

    m=capacity_pattern.search(name) 
    if m:
	hdd['specification']['capacity']=m.group()
    
    print hdd['specification'].keys()
    if set(['interface','capacity']).issubset(set(hdd['specification'].keys())) == False:
	print hdd['url']
	sku=sku_pattern.search(hdd['url']).group(1)
	ajax_post_data['sku']=sku
	while(True):
	    try:
		ajax_res=requests.post(ajax_url,data=ajax_post_data,proxies=proxy).content
		break
	    except requests.exceptions.ConnectionError:
		print 'exception raised'
		pass
    
        ajax_dom=dom.DOM(string=ajax_res)
        specs_node=ajax_dom.getNodesWithXpath(ajax_specs_path)
        if len(specs_node)==0:						#for pages that dont have specification table
	    specs_node=ajax_dom.getNodesWithXpath('//div[@id="litDesc"]')
    
        if len(specs_node)>0:
	    specs=specs_node[0].text_content()
	    if 'capacity' not in hdd['specification']:
	        m=capacity_pattern.search(specs)
	        if m:
		    hdd['specification']['capacity']=m.group()
	    if 'interface' not in hdd['specification']:
	        m=interface_pattern.findall(specs)
	        if len(m)>0:
		    hdd['specification']['interface']=' '.join(m)
    
    hdd['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in hdd:
	product_history['price']=hdd['price']
    if 'shipping' in hdd:
	product_history['shipping']=hdd['shipping']
    product_history['availability']=hdd['availability']
    product_history['datetime']=hdd['last_modified_datetime']
    hdd['product_history']=[product_history,]
    hdd['site']='indiaplaza'
    return hdd

def getHDDsOfBrand(brand,get_details=False):
    html=requests.get(url=brand[1],proxies=proxy).content
    first_page=dom.DOM(string=html)
    hdd_urls=[]
    hdd_urls.extend(getHDDUrlsFromBrandPage(string=first_page.html))
    count_path='//div[@class="prodNoArea"]'
    print brand[1]
    count_string=first_page.getNodesWithXpath(count_path)[0].text
    count=int(re.search('Showing.+of (\d+)',count_string).group(1))
    if count>20:
        num_pages=int(math.ceil(count/20.0))
        page_urls=[brand[1]+'&PageNo='+str(n) for n in xrange(2,num_pages+1)]
        dl.putUrls(page_urls,1)
        result=dl.download(proxy=proxy)
        for r in result:
            status=result[r][0]
            html=result[r][1]
            if status > 199 and status < 400:
                hdd_urls.extend(getHDDUrlsFromBrandPage(string=html))
    hdds=[]
    dl.putUrls(hdd_urls,2)
    result=dl.download(proxy=proxy)
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            print r
            hdd=getHDDFromPage(string=html)
            if hdd:
                hdd['brand']=brand[0]
                hdds.append(hdd)
		
    print "%d hdds of brand %s"%(len(hdds),brand[0])
    return hdds

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
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    hdd_coll=db['scraped_harddisks']
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    hdds=scrapAllHDDs()
    for hdd in hdds:
        try:
            hdd_coll.insert(hdd,safe=True)
            inserted_count+=1
            inserted_urls.append(hdd['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability'in hdd:
                upd['availability']=hdd['availability']
            if 'price' in hdd:
                upd['price']=hdd['price']
            if 'shipping' in hdd:
                upd['shipping']=hdd['shipping']
	    if 'offer' in hdd:
		upd['offer']=hdd['offer']
	    else:
		upd['offer']=''
            hdd_coll.update({'url':hdd['url']},{'$push':{'product_history':hdd['product_history'][0]},'$set':upd})
            updated_count+=1
            updated_urls.append(hdd['url'])
    if log:
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'harddisk','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)

    print "%d inserted and %d updated"%(inserted_count,updated_count)
    
if __name__=='__main__':
    insertIntoDB()
