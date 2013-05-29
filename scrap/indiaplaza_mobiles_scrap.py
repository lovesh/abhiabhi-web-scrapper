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
    proxy={'http':'92.62.161.205:8080'}

brand_urls={}
brands=('apple','blackberry','dell','htc','karbonn','micromax','nokia','samsung','sony-ericsson','videocon','sansui','lg','spice','motorola','iball','alcatel','rage','wynncom','gfive','sony','salora')

for brand in brands:
    brand_urls[brand]='%s/%s-mobiles-4.htm'%(siteurl,brand)

def getPhonesFromBrandPage(url=None,string=None):
    mobiles=[]
    mobile_block_path='//div[@class="skuRow"]'
    if string:
        page=dom.DOM(string=string)
    else:
        page=dom.DOM(url=url)
    mobile_blocks=page.getNodesWithXpath(mobile_block_path)
    img_path='.//div[@class="skuImg"]/a/img'
    name_path='.//div[@class="skuName"]/a'
    price_path='.//div[@class="ourPrice"]/span'
    shipping_path='.//span[@class="delDateQuest"]'
    name_junk_pattern=re.compile('.+(\(.+\)).*')
    features_path='.//div[@class="col2"]/ul/li'
    for mobile_block in mobile_blocks:
        mobile={}
        mobile['img_url']={'0':mobile_block.xpath(img_path)[0].get('src')}
        name=mobile_block.xpath(name_path)[0].text.encode('ascii','ignore').strip()
        junk=name_junk_pattern.search(name)
        if junk:
            junk=junk.group(1)
            name=name.replace(junk,'').strip()
        mobile['name']=name
        mobile['url']=siteurl+mobile_block.xpath(name_path)[0].get('href')
        price_string=mobile_block.xpath(price_path)[0].text
        mobile['price']=int(re.search('(\D)+(\d+)',price_string).group(2))
        shipping=re.search('Ships In (\d+)',mobile_block.xpath(shipping_path)[0].text)
        if shipping:
            mobile['shipping']=(shipping.group(1),)
        mobile['availability']=1
        feature_nodes=mobile_block.xpath(features_path)
        features=[]
        if feature_nodes:
            for node in feature_nodes:
                features.append(node.text.strip())
            mobile['features']=features
        if junk:
            mobile['features'].append(junk.strip(')('))
        
        mobile['last_modified_datetime']=datetime.datetime.now()
        product_history={}
        if 'price' in mobile:
            product_history['price']=mobile['price']
        if 'shipping' in mobile:
            product_history['shipping']=mobile['shipping']
        product_history['availability']=mobile['availability']
        product_history['datetime']=mobile['last_modified_datetime']
        mobile['product_history']=[product_history,]
        mobile['site']='indiaplaza'
        mobiles.append(mobile)
    return mobiles

def getPhonesOfBrand(brand_url,get_details=False):
    html=requests.get(url=brand_url,proxies=proxy).content
    first_page=dom.DOM(string=html)
    brand_validity_path='//div[@id="ContentPlaceHolder1_SpecificValuesHolder_ctl00_ErrorDiv"]'
    brand_validity=first_page.getNodesWithXpath(brand_validity_path)
    if len(brand_validity)==0:
        return {}
    if len(brand_validity)>0:
	if brand_validity[0].text.strip()=='':
	    return {}
    brand=re.search('.com/(.+)-mobiles-.',brand_url).group(1)
    mobiles=[]
    mobiles.extend(getPhonesFromBrandPage(string=first_page.html))
    count_path='//div[@class="prodNoArea"]'
    count_string=first_page.getNodesWithXpath(count_path)[0].text
    count=int(re.search('Showing.+of (\d+)',count_string).group(1))
    if count>20:
        num_pages=int(math.ceil(count/20.0))
        page_urls=[brand_url+'?PageNo='+str(n) for n in xrange(2,num_pages+1)]
        dl.putUrls(page_urls)
        result=dl.download(proxy=proxy)
        for r in result:
            status=result[r][0]
            html=result[r][1]
            if status > 199 and status < 400:
                mobiles.extend(getPhonesFromBrandPage(string=html))
    for mobile in mobiles:
        mobile['brand']=brand
    return mobiles

def scrapAllPhones():
    f=open('indiaplaza_mobiles_log.txt','w')
    mobiles=[]
    for brand in brand_urls:
        mobiles.extend(getPhonesOfBrand(brand_urls[brand]))
        f.write("Got mobiles of brand %s\n"%brand)
        f.flush()
    return mobiles

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    mobile_coll=db['scraped_mobiles']
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    mobiles=scrapAllPhones()
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
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'mobile','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
    
    print "%d inserted and %d updated"%(inserted_count,updated_count)
    
if __name__=='__main__':
    insertIntoDB()    
