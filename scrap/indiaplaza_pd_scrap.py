import downloader
import dom
import re
import urllib2
import math
import datetime
import pymongo
import requests
import util
import sys

siteurl='http://www.indiaplaza.com'
referer='http://www.indiaplaza.com/pen-drives-pc-2.htm?Category=data-storage'

dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})
if len(sys.argv)>1:
    proxy={'http':sys.argv[1]}
else:
    proxy={'http':'92.62.161.205:8080'}
    
name_junk_pattern=re.compile('Pendrive',re.I)
brand_junk_pattern=re.compile('\(\d+\)')
cruze_pattern=re.compile('\((cruze\w+)\)',re.I)
shipping_pattern=re.compile('ships in (\d+)',re.I)
capacity_pattern=re.compile('\d+ ?(G|T)B',re.I)
interface_pattern=re.compile('usb ?\d\.?\d?',re.I)

def getBrands():
    html=requests.get(url=referer,proxies=proxy).content
    doc=dom.DOM(string=html)
    brand_path='//div[@id="divBrands"]/ul/li/a'
    brands=dict((brand_junk_pattern.sub('',link[0]).strip(),'http://www.indiaplaza.com'+link[1]) for link in doc.getLinksWithXpath(brand_path))
    return brands

def getPDsFromBrandPage(url=None,string=None):
    pds=[]
    pd_block_path='//div[@class="skuRow"]'
    if string:
        page=dom.DOM(string=string)
    else:
        page=dom.DOM(url=url)
    pd_blocks=page.getNodesWithXpath(pd_block_path)
    img_path='.//div[@class="skuImg"]/a/img'
    name_path='.//div[@class="skuName"]/a'
    price_path='.//div[@class="ourPrice"]/span'
    shipping_path='.//span[@class="delDateQuest"]'
    features_path='.//div[@class="col2"]/ul/li'
    for pd_block in pd_blocks:
        pd={}
        pd['img_url']={'0':pd_block.xpath(img_path)[0].get('src')}
        name=pd_block.xpath(name_path)[0].text.encode('ascii','ignore').strip()
        name=name_junk_pattern.sub('',name)
        cruzer=cruze_pattern.search(name)
        if cruzer:
            cruzer=cruzer.group(1)
            name=re.sub('\(','',name)
            name=re.sub('\)','',name)
        
        pd['name']=name
        pd['url']=siteurl+pd_block.xpath(name_path)[0].get('href')
        price_string=pd_block.xpath(price_path)[0].text
        pd['price']=int(re.search('(\D)+(\d+)',price_string).group(2))
        shipping=shipping_pattern.search(pd_block.xpath(shipping_path)[0].text)
        if shipping:
            pd['shipping']=(shipping.group(1),)
        pd['availability']=1
        pd['specification']={}
        feature_nodes=pd_block.xpath(features_path)
        features=[]
        if feature_nodes:
            for node in feature_nodes:
                features.append(node.text.strip())
            pd['specification']=features
        
        m=interface_pattern.search(name)
        if m:
            pd['specification']['interface']=m.group()

        m=capacity_pattern.search(name) 
        if m:
            pd['specification']['capacity']=m.group()

        pd['last_modified_datetime']=datetime.datetime.now()
        product_history={}
        if 'price' in pd:
            product_history['price']=pd['price']
        if 'shipping' in pd:
            product_history['shipping']=pd['shipping']
        product_history['availability']=pd['availability']
        product_history['datetime']=pd['last_modified_datetime']
        pd['product_history']=[product_history,]
        pd['site']='indiaplaza'
        pds.append(pd)
    print len(pds)
    return pds

def getPDsOfBrand(brand,get_details=False):
    html=requests.get(url=brand[1],proxies=proxy).content
    first_page=dom.DOM(string=html)
    pds=[]
    pds.extend(getPDsFromBrandPage(string=first_page.html))
    count_path='//div[@class="prodNoArea"]'
    count_string=first_page.getNodesWithXpath(count_path)[0].text
    count=int(re.search('Showing.+of (\d+)',count_string).group(1))
    if count>20:
        num_pages=int(math.ceil(count/20.0))
        page_urls=[brand[1]+'&PageNo='+str(n) for n in xrange(2,num_pages+1)]
        dl.putUrls(page_urls,2)
        result=dl.download()
        for r in result:
            status=result[r][0]
            html=result[r][1]
            if status > 199 and status < 400:
                pds.extend(getPDsFromBrandPage(string=html))
    for pd in pds:
        pd['brand']=brand[0].lower()
    print "%d pds of brand %s"%(len(pds),brand[0])
    return pds

def scrapAllPDs():
    f=open('indiaplaza_pds_log.txt','w')
    pds=[]
    brands=getBrands()
    for brand in brands:
        pds.extend(getPDsOfBrand((brand,brands[brand])))
        f.write("Got pds of brand %s\n"%brand)
        f.flush()
    return pds

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    pd_coll=db['scraped_pendrives']
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    pds=scrapAllPDs()
    for pd in pds:
        try:
            pd_coll.insert(pd,safe=True)
            inserted_count+=1
            inserted_urls.append(pd['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability' in pd:
                upd['availability']=pd['availability']
            if 'price' in pd:
                upd['price']=pd['price']
            if 'shipping' in pd:
                upd['shipping']=pd['shipping']
	    if 'offer' in pd:
                upd['offer']=pd['offer']
	    else:
		upd['offer']=''
            pd_coll.update({'url':pd['url']},{'$push':{'product_history':pd['product_history'][0]},'$set':upd})
            updated_count+=1
            updated_urls.append(pd['url'])
    if log:
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'pendrive','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)

    print "%d inserted and %d updated"%(inserted_count,updated_count)
    
if __name__=='__main__':
    insertIntoDB()
